"""
针对 ``my_scripts.video_tools`` 的视频信息读取和截图生成测试。
"""

import importlib.util
import json
import sys
import tempfile
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "video_tools.py"


def load_video_tools():
    """隔离外部依赖后加载 ``video_tools``。"""
    fake_pil = types.ModuleType("PIL")
    fake_pil.Image = types.SimpleNamespace(open=lambda *_args, **_kwargs: None)
    fake_pil.ImageDraw = types.SimpleNamespace(Draw=lambda *_args, **_kwargs: None)
    fake_pil.ImageFont = types.SimpleNamespace(
        truetype=lambda *_args, **_kwargs: object(),
        load_default=lambda *_args, **_kwargs: object(),
    )
    fake_moviepy = types.ModuleType("moviepy")
    fake_moviepy.VideoFileClip = object

    fake_my_module = types.ModuleType("my_module")
    fake_my_module.read_json_to_dict = lambda _path: {
        "source_list": ["WEB", "BluRay", "BDRemux"],
        "video_extensions": [".mkv", ".mp4"],
        "ffprobe_path": "",
        "ffmpeg_path": "",
        "mtn_path": "",
        "mediainfo_path": "",
    }

    spec = importlib.util.spec_from_file_location(
        f"video_tools_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "PIL": fake_pil,
            "moviepy": fake_moviepy,
            "my_module": fake_my_module,
        },
    ):
        spec.loader.exec_module(module)
    return module


class TestVideoTools(unittest.TestCase):
    """验证不依赖真实外部程序的视频工具逻辑。"""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.module = load_video_tools()

    def tearDown(self):
        self.temp_dir.cleanup()

    def make_fake_contact_clip(self, size=(1920, 800), duration=170, aspect_ratio=2.4):
        """生成 ``generate_video_contact`` 测试用的最小 clip 对象。"""
        class FakeRawFrame:
            def astype(self, dtype):
                self.dtype = dtype
                return self

        class FakeClip:
            def __init__(self):
                self.size = size
                self.duration = duration
                self.aspect_ratio = aspect_ratio
                self.times = []
                self.closed = False

            def get_frame(self, time_point):
                self.times.append(time_point)
                return FakeRawFrame()

            def close(self):
                self.closed = True

        return FakeClip()

    def make_fake_contact_image_api(self, output_path: Path, write_output=True):
        """生成 ``generate_video_contact`` 测试用的最小 Image API。"""
        resized_sizes = []
        grid_sizes = []
        timestamp_texts = []

        class FakeFrameImage:
            def __init__(self, size=(0, 0)):
                self.size = size

            def resize(self, size, _resample):
                resized_sizes.append(size)
                self.size = size
                return self

        class FakeGridImage:
            def __init__(self, size):
                self.size = size
                self.pastes = []

            def paste(self, img, position):
                self.pastes.append((img, position))

            def save(self, path):
                if write_output:
                    Path(path).write_text("jpg", encoding="utf-8")

        def fake_new(_mode, size):
            grid_sizes.append(size)
            return FakeGridImage(size)

        class FakeDraw:
            def __init__(self, image):
                self.image = image

            def textbbox(self, _xy, text, font=None, stroke_width=0):
                return -stroke_width, -stroke_width, len(text) * 8 + stroke_width, 16 + stroke_width

            def rectangle(self, _xy, fill=None):
                pass

            def text(self, _xy, text, font=None, fill=None, **_kwargs):
                timestamp_texts.append(text)

        fake_image = types.SimpleNamespace(
            fromarray=lambda _frame: FakeFrameImage(),
            new=fake_new,
            Resampling=types.SimpleNamespace(LANCZOS="lanczos"),
        )
        fake_image_draw = types.SimpleNamespace(Draw=lambda image, _mode=None: FakeDraw(image))
        fake_image_font = types.SimpleNamespace(
            truetype=lambda *_args, **_kwargs: object(),
            load_default=lambda *_args, **_kwargs: object(),
        )
        return fake_image, fake_image_draw, fake_image_font, resized_sizes, grid_sizes, timestamp_texts

    def test_resolution_classifier_covers_movie_aspect_ratios(self):
        """按像素量归类时，应兼顾常见宽银幕和高分辨率边界。"""
        cases = {
            (320, 240): "240p",
            (720, 480): "480p",
            (1280, 720): "720p",
            (1440, 900): "1080p",
            (1920, 800): "1080p",
            (3840, 1600): "2160p",
            (7680, 4320): "4320p",
        }

        for resolution, expected in cases.items():
            width, height = resolution
            with self.subTest(resolution=f"{width}x{height}"):
                self.assertEqual(self.module.classify_resolution_by_pixels(width, height), expected)

    def test_resolution_classifier_locks_current_pixel_boundaries(self):
        """边界值应保持当前整理规则，避免后续调整时无意漂移。"""
        cases = {
            (400, 320): "240p",
            (401, 320): "480p",
            (791, 576): "480p",
            (792, 576): "720p",
            (1280, 960): "720p",
            (1281, 960): "1080p",
            (1950, 1080): "1080p",
            (1951, 1080): "2160p",
            (3860, 2160): "2160p",
            (3861, 2160): "4320p",
        }

        for resolution, expected in cases.items():
            width, height = resolution
            with self.subTest(resolution=f"{width}x{height}"):
                self.assertEqual(self.module.classify_resolution_by_pixels(width, height), expected)

    def test_resolution_classifier_rejects_invalid_dimensions(self):
        """非正数宽高应显式报错，避免生成无意义质量标签。"""
        with self.assertRaisesRegex(ValueError, "Invalid resolution"):
            self.module.classify_resolution_by_pixels(0, 1080)

    def test_format_video_contact_timestamp_uses_centiseconds(self):
        """截图时间戳应保留百分秒，并在超过一小时时带小时位。"""
        self.assertEqual(self.module.format_video_contact_timestamp(0), "0:00.00")
        self.assertEqual(self.module.format_video_contact_timestamp(65.432), "1:05.43")
        self.assertEqual(self.module.format_video_contact_timestamp(3661.987), "1:01:01.99")

    def test_get_largest_file_recurses_and_ignores_non_video_files(self):
        """最大视频文件扫描应递归目录，并忽略非配置后缀。"""
        nested = self.root / "nested"
        nested.mkdir()
        small = self.root / "small.mkv"
        large = nested / "large.mp4"
        ignored = nested / "huge.txt"
        small.write_bytes(b"1" * 10)
        large.write_bytes(b"1" * 20)
        ignored.write_bytes(b"1" * 100)

        self.assertEqual(self.module.get_largest_file(str(self.root)), str(large))

    def test_get_largest_file_returns_none_when_no_video_file_exists(self):
        """目录中没有视频文件时应返回 None。"""
        (self.root / "note.txt").write_text("not video", encoding="utf-8")

        self.assertIsNone(self.module.get_largest_file(self.root))

    def test_get_largest_file_accepts_uppercase_suffix_and_uses_stable_order_for_ties(self):
        """视频后缀大小写不敏感；同体积文件按稳定遍历顺序保留第一个。"""
        upper = self.root / "Movie.MKV"
        upper.write_bytes(b"1" * 10)

        self.assertEqual(self.module.get_largest_file(self.root), str(upper))

        first = self.root / "Another.mp4"
        first.write_bytes(b"1" * 10)

        self.assertEqual(self.module.get_largest_file(self.root), str(first))

    def test_get_largest_file_skips_files_when_size_lookup_fails(self):
        """单个视频文件大小读取失败时应跳过并继续扫描其它视频。"""
        bad = self.root / "bad.mkv"
        good = self.root / "good.mp4"
        bad.write_bytes(b"1" * 20)
        good.write_bytes(b"1" * 10)

        def fake_getsize(path):
            if Path(path).name == bad.name:
                raise OSError("file moved")
            return 10

        with patch.object(self.module.os.path, "getsize", side_effect=fake_getsize):
            with patch.object(self.module.logger, "warning") as mock_warning:
                self.assertEqual(self.module.get_largest_file(self.root), str(good))

        mock_warning.assert_called_once()

    def test_check_video_codec_reads_mediainfo_video_track(self):
        """MediaInfo JSON 中的视频编码器和 CRF 信息应组合成可读 codec 字段。"""
        mediainfo = {
            "media": {
                "track": [
                    {"@type": "Audio", "Format": "AAC"},
                    {
                        "@type": "Video",
                        "Encoded_Library_Name": "x264",
                        "Encoded_Library_Settings": "rc=crf / crf=18.0 / bitrate=5000",
                    },
                ]
            }
        }

        with patch.object(self.module.subprocess, "run", return_value=types.SimpleNamespace(stdout=json.dumps(mediainfo))):
            self.assertEqual(self.module.check_video_codec("movie.mkv"), "x264.crf18")

    def test_check_video_codec_handles_mediainfo_failures(self):
        """MediaInfo 失败、坏 JSON 或无视频流时应返回 None。"""
        with patch.object(self.module.logger, "warning"):
            with patch.object(self.module.subprocess, "run", return_value=types.SimpleNamespace(returncode=1, stdout="", stderr="boom")):
                self.assertIsNone(self.module.check_video_codec(self.root / "movie.mkv"))
            with patch.object(self.module.subprocess, "run", return_value=types.SimpleNamespace(returncode=0, stdout="not-json", stderr="")):
                self.assertIsNone(self.module.check_video_codec(self.root / "movie.mkv"))
            with patch.object(
                self.module.subprocess,
                "run",
                return_value=types.SimpleNamespace(returncode=0, stdout=json.dumps({"media": {"track": [{"@type": "Audio"}]}}), stderr=""),
            ):
                self.assertIsNone(self.module.check_video_codec(self.root / "movie.mkv"))

    def test_check_video_codec_accepts_single_track_dict_and_uppercase_settings(self):
        """单个 Video track 字典和大写 CRF/RC 设置也应能解析。"""
        mediainfo = {
            "media": {
                "track": {
                    "@type": "Video",
                    "Encoded_Library_Name": "x265",
                    "Encoded_Library_Settings": "RC=crf / CRF=19.0 / BITRATE=6000",
                }
            }
        }

        with patch.object(self.module.subprocess, "run", return_value=types.SimpleNamespace(returncode=0, stdout=json.dumps(mediainfo), stderr="")):
            self.assertEqual(self.module.check_video_codec(self.root / "movie.mkv"), "x265.crf19")

    def test_generate_video_contact_mtn_runs_external_tool(self):
        """mtn 兜底截图应按固定网格参数调用外部工具，并支持 PathLike 输入。"""
        video_path = self.root / "movie sample.mkv"
        output_path = self.root / "movie sample_s.jpg"

        def fake_run(cmd, **kwargs):
            self.assertEqual(cmd, ["mtn.exe", "-c", "4", "-r", "4", "-h", "100", "-P", str(video_path)])
            self.assertEqual(kwargs["timeout"], self.module.MTN_TIMEOUT_SECONDS)
            output_path.write_text("jpg", encoding="utf-8")
            return types.SimpleNamespace(returncode=0, stderr="")

        with patch.object(self.module, "MTN_PATH", "mtn.exe"):
            with patch.object(self.module.subprocess, "run", side_effect=fake_run) as mock_run:
                self.module.generate_video_contact_mtn(video_path)

        mock_run.assert_called_once()

    def test_generate_video_contact_mtn_logs_failures(self):
        """mtn 执行失败、超时、启动失败或未生成文件时应记录 warning 并返回。"""
        video_path = self.root / "movie.mkv"

        with patch.object(self.module.logger, "warning") as mock_warning:
            with patch.object(self.module.subprocess, "run", return_value=types.SimpleNamespace(returncode=1, stderr="boom")):
                self.module.generate_video_contact_mtn(video_path)
            with patch.object(self.module.subprocess, "run", side_effect=self.module.subprocess.TimeoutExpired("mtn", timeout=1)):
                self.module.generate_video_contact_mtn(video_path)
            with patch.object(self.module.subprocess, "run", side_effect=OSError("missing mtn")):
                self.module.generate_video_contact_mtn(video_path)
            with patch.object(self.module.subprocess, "run", return_value=types.SimpleNamespace(returncode=0, stderr="")):
                self.module.generate_video_contact_mtn(video_path)

        self.assertEqual(mock_warning.call_count, 4)

    def test_generate_video_contact_uses_dar_and_closes_clip(self):
        """Python 版截图应使用 DAR 修正尺寸，均匀取 16 帧并关闭 clip。"""
        video_path = self.root / "movie sample.mkv"
        output_path = self.root / "movie sample_s.jpg"
        clip = self.make_fake_contact_clip(size=(1920, 800), duration=170, aspect_ratio=2.0)
        fake_image, fake_image_draw, fake_image_font, resized_sizes, grid_sizes, timestamp_texts = self.make_fake_contact_image_api(output_path)

        with patch.object(self.module, "VideoFileClip", return_value=clip) as mock_video_file_clip:
            with patch.object(self.module, "Image", fake_image):
                with patch.object(self.module, "ImageDraw", fake_image_draw):
                    with patch.object(self.module, "ImageFont", fake_image_font):
                        with patch.object(self.module, "extract_video_probe", return_value={"file_info": {"dar": 2.4}, "video_stream": {}}):
                            with patch.object(self.module, "is_hdr_video", return_value=False):
                                self.module.generate_video_contact(video_path)

        mock_video_file_clip.assert_called_once_with(str(video_path))
        self.assertTrue(output_path.exists())
        self.assertTrue(clip.closed)
        self.assertEqual(len(clip.times), 16)
        self.assertEqual(resized_sizes, [(1920, 800)] * 16)
        self.assertEqual(grid_sizes, [(7680, 3200)])
        self.assertEqual(timestamp_texts[0], "0:10.00")
        self.assertEqual(timestamp_texts[-1], "2:40.00")

    def test_generate_video_contact_reuses_supplied_probe_metadata(self):
        """调用方已传入 DAR/HDR 元数据时，截图不应再次读取视频信息。"""
        video_path = self.root / "movie sample.mkv"
        output_path = self.root / "movie sample_s.jpg"
        clip = self.make_fake_contact_clip(size=(1920, 800), duration=170, aspect_ratio=2.0)
        fake_image, fake_image_draw, fake_image_font, resized_sizes, grid_sizes, _timestamp_texts = self.make_fake_contact_image_api(output_path)
        video_info = {"dar": 2.4}
        video_stream = {"color_transfer": "bt709"}

        with patch.object(self.module, "VideoFileClip", return_value=clip):
            with patch.object(self.module, "Image", fake_image):
                with patch.object(self.module, "ImageDraw", fake_image_draw):
                    with patch.object(self.module, "ImageFont", fake_image_font):
                        with patch.object(self.module, "extract_video_probe") as extract_video_probe:
                            with patch.object(self.module, "get_video_contact_stream_metadata") as stream_metadata:
                                self.module.generate_video_contact(video_path, video_info=video_info, video_stream=video_stream)

        self.assertTrue(output_path.exists())
        self.assertEqual(resized_sizes, [(1920, 800)] * 16)
        self.assertEqual(grid_sizes, [(7680, 3200)])
        extract_video_probe.assert_not_called()
        stream_metadata.assert_not_called()

    def test_generate_video_contact_falls_back_to_clip_aspect_ratio_and_warns_when_output_missing(self):
        """DAR 读取失败时应退回 clip 宽高比；未生成输出文件时只记录 warning。"""
        video_path = self.root / "movie.mkv"
        output_path = self.root / "movie_s.jpg"
        clip = self.make_fake_contact_clip(size=(1280, 720), duration=17, aspect_ratio=16 / 9)
        fake_image, fake_image_draw, fake_image_font, resized_sizes, grid_sizes, timestamp_texts = self.make_fake_contact_image_api(output_path, write_output=False)

        with patch.object(self.module, "VideoFileClip", return_value=clip):
            with patch.object(self.module, "Image", fake_image):
                with patch.object(self.module, "ImageDraw", fake_image_draw):
                    with patch.object(self.module, "ImageFont", fake_image_font):
                        with patch.object(self.module, "extract_video_probe", side_effect=RuntimeError("ffprobe failed")):
                            with patch.object(self.module.logger, "warning") as mock_warning:
                                with patch.object(self.module, "is_hdr_video", return_value=False):
                                    self.module.generate_video_contact(video_path)

        self.assertFalse(output_path.exists())
        self.assertTrue(clip.closed)
        self.assertEqual(resized_sizes, [(1280, 720)] * 16)
        self.assertEqual(grid_sizes, [(5120, 2880)])
        self.assertEqual(timestamp_texts, [f"0:{second:02d}.00" for second in range(1, 17)])
        self.assertGreaterEqual(mock_warning.call_count, 2)

    def test_generate_video_contact_uses_ffmpeg_tonemap_frames_for_hdr(self):
        """HDR 视频应使用 ffmpeg tone mapping 抽帧，并保留现有拼图和时间戳流程。"""
        video_path = self.root / "hdr movie.mkv"
        output_path = self.root / "hdr movie_s.jpg"
        clip = self.make_fake_contact_clip(size=(3840, 2160), duration=170, aspect_ratio=16 / 9)
        fake_image, fake_image_draw, fake_image_font, resized_sizes, grid_sizes, timestamp_texts = self.make_fake_contact_image_api(output_path)
        hdr_times = []

        class FakeHdrFrame:
            def __init__(self):
                self.size = (3840, 2160)

            def resize(self, size, _resample):
                resized_sizes.append(size)
                self.size = size
                return self

        def fake_hdr_frame(_video_path, seconds):
            hdr_times.append(seconds)
            return FakeHdrFrame()

        with patch.object(self.module, "VideoFileClip", return_value=clip):
            with patch.object(self.module, "Image", fake_image):
                with patch.object(self.module, "ImageDraw", fake_image_draw):
                    with patch.object(self.module, "ImageFont", fake_image_font):
                        with patch.object(self.module, "extract_video_probe", return_value={"file_info": {"dar": 16 / 9}, "video_stream": {}}):
                            with patch.object(self.module, "is_hdr_video", return_value=True):
                                with patch.object(self.module, "extract_video_contact_hdr_frame", side_effect=fake_hdr_frame):
                                    self.module.generate_video_contact(video_path)

        self.assertTrue(output_path.exists())
        self.assertTrue(clip.closed)
        self.assertEqual(len(hdr_times), 16)
        self.assertEqual(clip.times, [])
        self.assertEqual(resized_sizes, [(3840, 2160)] * 16)
        self.assertEqual(grid_sizes, [(15360, 8640)])
        self.assertEqual(timestamp_texts[0], "0:10.00")

    def test_is_hdr_video_detects_hdr_color_metadata(self):
        """PQ/HLG/BT.2020 或 HDR side data 应被识别为需要 tone mapping。"""
        hdr_ffprobe = {
            "streams": [
                {
                    "codec_type": "video",
                    "color_transfer": "smpte2084",
                    "color_primaries": "bt2020",
                }
            ]
        }
        sdr_ffprobe = {
            "streams": [
                {
                    "codec_type": "video",
                    "color_transfer": "bt709",
                    "color_primaries": "bt709",
                }
            ]
        }

        with patch.object(self.module.subprocess, "run", return_value=types.SimpleNamespace(returncode=0, stdout=json.dumps(hdr_ffprobe), stderr="")):
            self.assertTrue(self.module.is_hdr_video(self.root / "hdr.mkv"))
        with patch.object(self.module.subprocess, "run", return_value=types.SimpleNamespace(returncode=0, stdout=json.dumps(sdr_ffprobe), stderr="")):
            self.assertFalse(self.module.is_hdr_video(self.root / "sdr.mkv"))

    def test_extract_video_contact_hdr_frame_runs_ffmpeg_tonemap(self):
        """HDR 抽帧 helper 应调用 ffmpeg tone mapping，并返回 RGB 图像。"""
        video_path = self.root / "hdr.mkv"

        class FakeOpenedImage:
            def convert(self, mode):
                self.mode = mode
                return self

        def fake_run(cmd, **kwargs):
            self.assertEqual(cmd[0], "ffmpeg")
            self.assertTrue(any("tonemap=tonemap=mobius" in str(part) for part in cmd))
            self.assertEqual(kwargs["timeout"], self.module.FFMPEG_TIMEOUT_SECONDS)
            return types.SimpleNamespace(returncode=0, stdout=b"png", stderr=b"")

        with patch.object(self.module.subprocess, "run", side_effect=fake_run) as mock_run:
            with patch.object(self.module.Image, "open", return_value=FakeOpenedImage()):
                frame = self.module.extract_video_contact_hdr_frame(video_path, 12.3456)

        self.assertEqual(frame.mode, "RGB")
        mock_run.assert_called_once()

    def test_generate_video_contact_closes_clip_when_video_metadata_is_invalid(self):
        """视频尺寸或时长异常时应抛出明确错误，并仍然关闭 clip。"""
        video_path = self.root / "movie.mkv"
        clip = self.make_fake_contact_clip(size=(1920, 0), duration=17, aspect_ratio=2.4)

        with patch.object(self.module, "VideoFileClip", return_value=clip):
            with self.assertRaisesRegex(ValueError, "视频高度无效"):
                self.module.generate_video_contact(video_path)

        self.assertTrue(clip.closed)

    def test_extract_video_probe_uses_first_video_stream_and_filename_overrides(self):
        """ffprobe/mediainfo 输出应被归一化成下游入库需要的字段。"""
        ffprobe = {
            "streams": [
                {"codec_type": "audio", "codec_name": "aac"},
                {
                    "codec_type": "video",
                    "width": 1920,
                    "height": 800,
                    "display_aspect_ratio": "12:5",
                    "codec_tag_string": "[0][0][0][0]",
                    "codec_name": "hevc",
                },
            ],
            "format": {"bit_rate": "5000000", "duration": "7199"},
        }
        filename = str(self.root / "Movie.BluRay.Remux「Director Cut」.mkv")

        with patch.object(self.module.subprocess, "run", return_value=types.SimpleNamespace(stdout=json.dumps(ffprobe))):
            with patch.object(self.module, "check_video_codec", return_value="x264"):
                result = self.module.extract_video_probe(filename)

        file_info = result["file_info"]
        self.assertEqual(file_info["resolution"], "1920x800")
        self.assertEqual(file_info["quality"], "1080p")
        self.assertEqual(file_info["dar"], 12 / 5)
        self.assertEqual(file_info["codec"], "x264")
        self.assertEqual(file_info["bitrate"], "5000kbps")
        self.assertEqual(file_info["duration"], 120)
        self.assertEqual(file_info["source"], "BDRemux")
        self.assertEqual(file_info["comment"], "Director Cut")

    def test_extract_video_probe_keeps_reusable_stream_metadata(self):
        """完整探测结果应保留 file_info 和截图可复用的视频流元数据。"""
        video_stream = {
            "codec_type": "video",
            "width": 1920,
            "height": 1080,
            "codec_tag_string": "[0][0][0][0]",
            "codec_name": "hevc",
            "bit_rate": "6000000",
            "duration": "3600",
            "color_transfer": "smpte2084",
        }
        ffprobe = {"streams": [{"codec_type": "audio"}, video_stream], "format": {"duration": "3600"}}
        filename = str(self.root / "Movie.WEB-DL.mkv")

        with patch.object(self.module.subprocess, "run", return_value=types.SimpleNamespace(returncode=0, stdout=json.dumps(ffprobe), stderr="")) as mock_run:
            with patch.object(self.module, "check_video_codec", return_value="x265"):
                result = self.module.extract_video_probe(filename)

        self.assertEqual(result["video_path"], filename)
        self.assertEqual(result["file_info"]["codec"], "x265")
        self.assertEqual(result["file_info"]["dar"], 16 / 9)
        self.assertEqual(result["video_stream"], video_stream)
        self.assertEqual(result["format"], ffprobe["format"])
        mock_run.assert_called_once()

    def test_extract_video_probe_returns_none_when_ffprobe_fails_or_outputs_bad_json(self):
        """ffprobe 执行失败、超时或输出非 JSON 时应返回 None。"""
        filename = self.root / "Movie.mkv"

        with patch.object(self.module.logger, "exception"):
            with patch.object(
                self.module.subprocess,
                "run",
                side_effect=self.module.subprocess.TimeoutExpired(cmd="ffprobe", timeout=60),
            ):
                self.assertIsNone(self.module.extract_video_probe(filename))

        with patch.object(self.module.logger, "error"):
            with patch.object(self.module.subprocess, "run", return_value=types.SimpleNamespace(returncode=1, stdout="", stderr="boom")):
                self.assertIsNone(self.module.extract_video_probe(filename))
            with patch.object(self.module.subprocess, "run", return_value=types.SimpleNamespace(returncode=0, stdout="not-json", stderr="")):
                self.assertIsNone(self.module.extract_video_probe(filename))

    def test_extract_video_probe_returns_none_when_required_media_fields_are_missing(self):
        """缺少视频流、码率或时长时应返回 None。"""
        filename = self.root / "Movie.mkv"
        no_video = {"streams": [{"codec_type": "audio"}], "format": {"bit_rate": "5000000", "duration": "7200"}}
        no_bitrate = {
            "streams": [{"codec_type": "video", "width": 1920, "height": 1080, "duration": "7200"}],
            "format": {},
        }
        no_duration = {
            "streams": [{"codec_type": "video", "width": 1920, "height": 1080, "bit_rate": "5000000"}],
            "format": {},
        }

        with patch.object(self.module.logger, "error"), patch.object(self.module, "check_video_codec", return_value=None):
            with patch.object(self.module.subprocess, "run", return_value=types.SimpleNamespace(returncode=0, stdout=json.dumps(no_video), stderr="")):
                self.assertIsNone(self.module.extract_video_probe(filename))
            with patch.object(self.module.subprocess, "run", return_value=types.SimpleNamespace(returncode=0, stdout=json.dumps(no_bitrate), stderr="")):
                self.assertIsNone(self.module.extract_video_probe(filename))
            with patch.object(self.module.subprocess, "run", return_value=types.SimpleNamespace(returncode=0, stdout=json.dumps(no_duration), stderr="")):
                self.assertIsNone(self.module.extract_video_probe(filename))

    def test_extract_video_probe_falls_back_for_bad_dar_and_mediainfo_failure(self):
        """DAR 无法解析时回退到存储宽高比；MediaInfo 失败时回退到 ffprobe codec。"""
        ffprobe = {
            "streams": [
                {
                    "codec_type": "video",
                    "width": 1920,
                    "height": 1080,
                    "display_aspect_ratio": "N/A",
                    "codec_tag_string": "[0][0][0][0]",
                    "codec_name": "hevc",
                    "bit_rate": "6000000",
                    "duration": "3600",
                },
            ],
            "format": {},
        }
        filename = self.root / "Movie.WEB-DL.mkv"

        with patch.object(self.module.subprocess, "run", return_value=types.SimpleNamespace(returncode=0, stdout=json.dumps(ffprobe), stderr="")):
            with patch.object(self.module, "check_video_codec", side_effect=ValueError("mediainfo broken")):
                with patch.object(self.module.logger, "warning") as mock_warning:
                    result = self.module.extract_video_probe(filename)

        file_info = result["file_info"]
        self.assertEqual(file_info["dar"], 1920 / 1080)
        self.assertEqual(file_info["codec"], "hevc")
        self.assertEqual(file_info["bitrate"], "6000kbps")
        self.assertEqual(file_info["duration"], 60)
        self.assertEqual(file_info["source"], "WEB-DL")
        mock_warning.assert_called_once()

    def test_extract_video_probe_prefers_filename_source_before_parent_path(self):
        """父目录和文件名都含来源标记时，应优先使用文件名里的来源。"""
        source_dir = self.root / "WEB"
        source_dir.mkdir()
        filename = source_dir / "Movie.BluRay.mkv"
        ffprobe = {
            "streams": [
                {
                    "codec_type": "video",
                    "width": 1920,
                    "height": 1080,
                    "codec_tag_string": "[0][0][0][0]",
                    "codec_name": "hevc",
                    "bit_rate": "6000000",
                    "duration": "3600",
                },
            ],
            "format": {},
        }

        with patch.object(self.module, "SOURCE_LIST", ["WEB", "BluRay"]):
            with patch.object(self.module.subprocess, "run", return_value=types.SimpleNamespace(returncode=0, stdout=json.dumps(ffprobe), stderr="")):
                with patch.object(self.module, "check_video_codec", return_value=None):
                    result = self.module.extract_video_probe(filename)

        self.assertEqual(result["file_info"]["source"], "BluRay")

    def test_get_video_probe_returns_none_when_no_video_file_exists(self):
        """没有视频文件时入口函数应返回 None，不调用 ffprobe。"""
        with patch.object(self.module.logger, "error"), patch.object(self.module, "extract_video_probe") as extract_video_probe:
            self.assertIsNone(self.module.get_video_probe(str(self.root)))

        extract_video_probe.assert_not_called()

    def test_get_video_probe_uses_largest_video_file(self):
        """入口函数应选择目录中体积最大的视频文件进行解析。"""
        small = self.root / "small.mkv"
        large = self.root / "large.mp4"
        small.write_bytes(b"1" * 10)
        large.write_bytes(b"1" * 20)
        video_info = {"resolution": "1920x1080", "duration": 120}
        video_probe = {"video_path": str(large), "file_info": video_info, "video_stream": {}, "format": {}}

        with patch.object(self.module, "extract_video_probe", return_value=video_probe) as extract_video_probe:
            result = self.module.get_video_probe(self.root)

        self.assertIs(result, video_probe)
        extract_video_probe.assert_called_once_with(str(large))

    def test_get_video_probe_returns_none_when_extract_video_probe_fails(self):
        """视频解析异常应记录日志并返回 None。"""
        video_file = self.root / "movie.mkv"
        video_file.write_bytes(b"1" * 10)

        with patch.object(self.module.logger, "exception") as mock_exception:
            with patch.object(self.module, "extract_video_probe", side_effect=ValueError("bad ffprobe")):
                self.assertIsNone(self.module.get_video_probe(self.root))

        mock_exception.assert_called_once()
