"""
针对 ``my_scripts.sort_movie_ops`` 中仍作为公共底层能力的函数做定向测试。
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

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "sort_movie_ops.py"


def load_sort_movie_ops(check_target: str):
    """隔离外部依赖后加载 ``sort_movie_ops``。"""
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
        "trash_list": [],
        "source_list": [],
        "video_extensions": [".mkv", ".mp4"],
        "magnet_path": "magnet:",
        "rarbg_source": "",
        "ttg_source": "",
        "dhd_source": "",
        "sk_source": "",
        "rare_source": "",
        "rls_source": "",
        "check_target": check_target,
        "everything_path": "",
        "ffprobe_path": "",
        "ffmpeg_path": "",
        "mtn_path": "",
        "mediainfo_path": "",
        "mirror_path": "",
        "ru_path": "",
        "yts_path": "",
        "dhd_path": "",
        "ttg_path": "",
        "sk_path": "",
        "rare_path": "",
    }
    fake_my_module.sanitize_filename = lambda text: text
    fake_my_module.read_file_to_list = lambda _path: []
    fake_my_module.get_file_paths = lambda _path: []
    fake_my_module.remove_target = lambda _path: None
    fake_my_module.get_folder_paths = lambda _path: []

    fake_sort_movie_request = types.ModuleType("sort_movie_request")
    fake_sort_movie_request.check_kpk_for_better_quality = lambda _imdb, _quality: False

    spec = importlib.util.spec_from_file_location(
        f"sort_movie_ops_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "PIL": fake_pil,
            "moviepy": fake_moviepy,
            "my_module": fake_my_module,
            "sort_movie_request": fake_sort_movie_request,
        },
    ):
        spec.loader.exec_module(module)
    return module


def build_yts_json(torrents):
    """生成 ``select_best_yts_magnet`` 需要的最小 YTS 结构。"""
    return {"data": {"movie": {"torrents": torrents}}}


class TestSharedHelpers(unittest.TestCase):
    """验证跨电影/导演流程复用的小工具。"""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.module = load_sort_movie_ops(str(self.root / "check"))

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_format_bytes_accepts_int_and_string_values(self):
        """字节格式化应支持数字字符串和整数，并统一零字节格式。"""
        self.assertEqual(self.module.format_bytes(0), "0.00 B")
        self.assertEqual(self.module.format_bytes("1023"), "1023.00 B")
        self.assertEqual(self.module.format_bytes(1024), "1.00 KB")
        self.assertEqual(self.module.format_bytes(str(1024 ** 2)), "1.00 MB")

    def test_format_bytes_raises_for_invalid_values(self):
        """非法字节值应继续由 int 转换显式抛错。"""
        with self.assertRaises(ValueError):
            self.module.format_bytes("not-a-number")

    def test_build_unique_path_appends_counter_before_suffix(self):
        """目标文件已存在时，应生成 ``name(1).ext`` 形式的新路径。"""
        target = self.root / "Movie.torrent"
        target.write_text("first", encoding="utf-8")
        (self.root / "Movie(1).torrent").write_text("second", encoding="utf-8")

        result = self.module.build_unique_path(target)

        self.assertEqual(result, self.root / "Movie(2).torrent")

    def test_build_unique_path_returns_original_when_target_is_available(self):
        """目标路径未被占用时应原样返回，且不创建文件。"""
        target = self.root / "New Movie.torrent"

        result = self.module.build_unique_path(target)

        self.assertEqual(result, target)
        self.assertFalse(target.exists())

    def test_build_unique_path_handles_no_suffix_and_directory_collision(self):
        """无后缀路径和目录占用也应按同一规则追加计数。"""
        target = self.root / "Alias"
        target.mkdir()
        (self.root / "Alias(1)").touch()

        result = self.module.build_unique_path(target)

        self.assertEqual(result, self.root / "Alias(2)")

    def test_id_marker_helpers_round_trip_and_report_duplicates(self):
        """编号空文件读写应和 ``scan_ids`` 使用同一套规则。"""
        movie_dir = self.root / "movie"
        movie_dir.mkdir()

        self.module.touch_id_marker(str(movie_dir), "tt1234567", "imdb")
        self.module.touch_id_marker(str(movie_dir), "12345tv", "tmdb")
        ids, error = self.module.get_existing_id_files(str(movie_dir))

        self.assertIsNone(error)
        self.assertEqual(ids, {"imdb": "tt1234567", "tmdb": "12345tv", "douban": None})
        self.assertEqual(self.module.scan_ids(str(movie_dir)), {"imdb": "tt1234567", "tmdb": "12345tv", "douban": None})

        self.module.remove_id_marker(str(movie_dir), "tt1234567", "imdb")
        self.assertFalse((movie_dir / "tt1234567.imdb").exists())
        self.module.remove_id_marker(movie_dir, "tt1234567", "imdb")

        (movie_dir / "111.tmdb").touch()
        _ids, duplicate_error = self.module.get_existing_id_files(str(movie_dir))
        self.assertIn("TMDB 编号文件太多", duplicate_error)
        self.assertIn("['111.tmdb', '12345tv.tmdb']", duplicate_error)

    def test_id_marker_helpers_reject_unsupported_suffixes(self):
        """创建和删除编号标记时只允许已支持的小写后缀。"""
        movie_dir = self.root / "movie"
        movie_dir.mkdir()

        with self.assertRaisesRegex(ValueError, "Unsupported ID marker suffix: IMDB"):
            self.module.touch_id_marker(movie_dir, "tt1234567", "IMDB")

        with self.assertRaisesRegex(ValueError, "Unsupported ID marker suffix: foo"):
            self.module.remove_id_marker(movie_dir, "tt1234567", "foo")

        self.assertFalse((movie_dir / "tt1234567.IMDB").exists())

    def test_get_existing_id_files_ignores_directories_and_rejects_uppercase_marker_suffixes(self):
        """编号目录不应当作标记文件；受支持标记后缀必须使用小写。"""
        movie_dir = self.root / "movie"
        movie_dir.mkdir()
        (movie_dir / "999.tmdb").mkdir()

        ids, error = self.module.get_existing_id_files(movie_dir)

        self.assertIsNone(error)
        self.assertEqual(ids, {"imdb": None, "tmdb": None, "douban": None})

        (movie_dir / "tt1234567.IMDB").touch()
        ids, error = self.module.get_existing_id_files(movie_dir)

        self.assertEqual(ids, {"imdb": None, "tmdb": None, "douban": None})
        self.assertIn("编号文件后缀必须小写", error)
        self.assertIn("tt1234567.IMDB", error)

    def test_remove_duplicates_ignore_case_handles_non_string_values(self):
        """去重工具应保留首次出现项，并兼容非字符串和不可 hash 值。"""
        items = ["Movie", "movie", 1, 1, ["a"], ["a"], {"x": 1}, {"x": 1}]

        result = self.module.remove_duplicates_ignore_case(items)

        self.assertEqual(result, ["Movie", 1, ["a"], {"x": 1}])

    def test_remove_duplicates_ignore_case_accepts_iterables_and_uses_casefold(self):
        """去重工具应支持任意可迭代对象，并使用 casefold 比较字符串。"""
        result = self.module.remove_duplicates_ignore_case(iter(["Straße", "strasse", "Other"]))

        self.assertEqual(result, ["Straße", "Other"])

    def test_parse_and_normalize_common_identifiers(self):
        """IMDb/TMDb/豆瓣编号解析应保持调用方依赖的规范格式。"""
        text = "https://www.imdb.com/title/TT1234567/?ref_=x plus tt7654321 and TT1234567"

        self.assertEqual(self.module.extract_imdb_id(text), "tt1234567")
        self.assertEqual(self.module.extract_imdb_id("prefix TT9999999 suffix"), "tt9999999")
        self.assertEqual(self.module.extract_imdb_id("tt1111111 https://www.imdb.com/title/TT2222222/"), "tt2222222")
        self.assertEqual(self.module.extract_imdb_id("tt1111111 and tt2222222"), "tt1111111")
        self.assertIsNone(self.module.extract_imdb_id("abcTT1234567xyz"))
        self.assertIsNone(self.module.extract_imdb_id(""))
        self.assertIsNone(self.module.extract_imdb_id(None))

        self.assertEqual(self.module.parse_movie_id(" TT1234567 "), None)
        self.assertEqual(self.module.parse_movie_id(" tt1234567 "), ("imdb", "tt1234567"))
        self.assertEqual(self.module.parse_movie_id("tmdb12345"), ("tmdb", "12345"))
        self.assertEqual(self.module.parse_movie_id("db67890"), ("douban", "67890"))
        self.assertIsNone(self.module.parse_movie_id(None))
        self.assertIsNone(self.module.parse_movie_id(""))
        self.assertIsNone(self.module.parse_movie_id("tmdb"))
        self.assertIsNone(self.module.parse_movie_id("db"))
        self.assertIsNone(self.module.parse_movie_id("unknown"))

    def test_extract_imdb_id_from_links_prefers_canonical_imdb_link(self):
        """链接列表中即使先出现宽松匹配，也应优先取 IMDb 标准标题页。"""
        links = [
            "https://example.test/download/tt1111111",
            "https://www.imdb.com/title/TT2222222/?ref_=test",
            "https://imdb.com/title/tt3333333",
        ]

        self.assertEqual(self.module.extract_imdb_id_from_links(links), "tt2222222")
        self.assertEqual(
            self.module.extract_imdb_id_from_links(["https://example.test/TT4444444"]),
            "tt4444444",
        )
        self.assertEqual(
            self.module.extract_imdb_id_from_links([None, "", "https://example.test/tt1111111", "https://example.test/tt2222222"]),
            "tt1111111",
        )
        self.assertIsNone(self.module.extract_imdb_id_from_links([]))
        self.assertIsNone(self.module.extract_imdb_id_from_links(["https://example.test/no-id"]))

    def test_director_name_helpers_keep_existing_name_rules(self):
        """导演名和豆瓣名清理应保留既有命名规则。"""
        self.assertEqual(self.module.split_director_name("John Smith 约翰·史密斯"), ["John Smith", "约翰·史密斯"])
        self.assertEqual(self.module.split_director_name("约翰·史密斯"), ["约翰·史密斯"])
        self.assertEqual(self.module.split_director_name("John Smith"), ["John Smith"])
        self.assertEqual(self.module.fix_douban_name("电影名 (导演剪辑版)（蓝光）"), "电影名")

    def test_merge_and_create_aka_director_deduplicate_case_insensitively(self):
        """别名合并和空文件创建都应按大小写不敏感规则去重。"""
        merged = self.module.merge_and_dedup(
            {"aka": ["Movie", "Film"], "year": [2020]},
            {"aka": ["movie", "Cinema"], "imdb": ["tt1234567"]},
        )

        self.assertEqual(merged["aka"], ["Movie", "Film", "Cinema"])
        self.assertEqual(merged["year"], [2020])
        self.assertEqual(merged["imdb"], ["tt1234567"])

        director_dir = self.root / "director"
        director_dir.mkdir()
        with patch.object(self.module, "sanitize_filename", side_effect=lambda text: text.replace("/", "_")):
            self.module.create_aka_director(str(director_dir), ["A/B", "a/b", "Alias"])

        self.assertEqual(sorted(path.name for path in director_dir.iterdir()), ["A_B", "Alias"])


class TestYtsTorrentSelection(unittest.TestCase):
    """验证 YTS 种子优先级筛选。"""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.module = load_sort_movie_ops(str(self.root / "check"))

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_select_best_yts_magnet_applies_priority_chain_then_size(self):
        """同画质下应依次按编码、位深、来源过滤，最后才比较大小。"""
        torrents = [
            {"quality": "1080p", "video_codec": "x264", "bit_depth": "10", "type": "bluray", "size_bytes": 900, "hash": "X264"},
            {"quality": "1080p", "video_codec": "x265", "bit_depth": "8", "type": "bluray", "size_bytes": 1000, "hash": "EIGHT_BIT"},
            {"quality": "1080p", "video_codec": "x265", "bit_depth": "10", "type": "web", "size_bytes": 1200, "hash": "WEB"},
            {"quality": "1080p", "video_codec": "x265", "bit_depth": "10", "type": "bluray", "size_bytes": "700", "hash": "SMALL"},
            {"quality": "1080p", "video_codec": "x265", "bit_depth": "10", "type": "bluray", "size_bytes": "1500", "hash": "BEST"},
        ]

        result = self.module.select_best_yts_magnet(build_yts_json(torrents), "magnet:?xt=urn:btih:")

        self.assertEqual(result, "magnet:?xt=urn:btih:BEST")

    def test_select_best_yts_magnet_prefers_quality_before_other_fields(self):
        """更高画质应优先于编码、位深和体积。"""
        torrents = [
            {"quality": "720p", "video_codec": "x265", "bit_depth": "10", "type": "bluray", "size_bytes": 9999, "hash": "LOW"},
            {"quality": "1080p", "video_codec": "x264", "bit_depth": "8", "type": "web", "size_bytes": 1, "hash": "HIGH"},
        ]

        result = self.module.select_best_yts_magnet(build_yts_json(torrents), "magnet:")

        self.assertEqual(result, "magnet:HIGH")

    def test_select_best_yts_magnet_rejects_empty_torrent_list(self):
        """YTS 返回空 torrents 时应抛出明确错误。"""
        with self.assertRaisesRegex(ValueError, "YTS torrents is empty"):
            self.module.select_best_yts_magnet(build_yts_json([]), "magnet:")

    def test_filter_torrents_by_priority_rejects_unknown_values(self):
        """配置外字段值出现时应显式报错，避免静默选错种子。"""
        torrents = [
            {"quality": "1080p", "video_codec": "x265", "bit_depth": "10", "type": "bluray", "size_bytes": 1, "hash": "OK"},
            {"quality": "z-unknown", "video_codec": "x265", "bit_depth": "10", "type": "bluray", "size_bytes": 2, "hash": "BAD"},
            {"quality": "4K", "video_codec": "x265", "bit_depth": "10", "type": "bluray", "size_bytes": 3, "hash": "BAD2"},
        ]

        with self.assertRaisesRegex(ValueError, r"Unexpected value for quality: \['4K', 'z-unknown'\]"):
            self.module.filter_torrents_by_priority(torrents, "quality", ["1080p", "720p"])


class TestDownloadLinkExtraction(unittest.TestCase):
    """验证来源文件到下载链接的转换。"""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.module = load_sort_movie_ops(str(self.root / "check"))

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_extract_torrent_download_link_from_json_selects_best_yts_magnet(self):
        """JSON 来源应走 YTS 优先级选择并返回磁链。"""
        json_path = self.root / "movie.json"
        json_path.write_text("{}", encoding="utf-8")
        torrents = [
            {"quality": "720p", "video_codec": "x265", "bit_depth": "10", "type": "bluray", "size_bytes": 1, "hash": "LOW"},
            {"quality": "1080p", "video_codec": "x264", "bit_depth": "8", "type": "web", "size_bytes": 1, "hash": "HIGH"},
        ]

        with patch.object(self.module, "read_json_to_dict", return_value=build_yts_json(torrents)):
            result = self.module.extract_torrent_download_link(json_path, "magnet:")

        self.assertEqual(result, "magnet:HIGH")

    def test_extract_torrent_download_link_from_log_strips_bom_and_spaces(self):
        """LOG 来源只取首行，并去掉 UTF-8 BOM 和首尾空白。"""
        log_path = self.root / "movie.log"
        log_path.write_text("placeholder", encoding="utf-8")

        with patch.object(self.module, "read_file_to_list", return_value=["\ufeff  https://example.test/torrent  ", "ignored"]):
            result = self.module.extract_torrent_download_link(log_path, "magnet:")

        self.assertEqual(result, "https://example.test/torrent")

    def test_extract_torrent_download_link_handles_empty_or_unsupported_sources(self):
        """空 JSON、空 LOG、空白首行和不支持后缀都应返回 None。"""
        json_path = self.root / "empty.json"
        log_path = self.root / "empty.log"
        txt_path = self.root / "movie.txt"

        with patch.object(self.module.logger, "error"):
            with patch.object(self.module, "read_json_to_dict", return_value=None):
                self.assertIsNone(self.module.extract_torrent_download_link(json_path, "magnet:"))
            with patch.object(self.module, "read_file_to_list", return_value=[]):
                self.assertIsNone(self.module.extract_torrent_download_link(log_path, "magnet:"))
            with patch.object(self.module, "read_file_to_list", return_value=["\ufeff  "]):
                self.assertIsNone(self.module.extract_torrent_download_link(log_path, "magnet:"))
        self.assertIsNone(self.module.extract_torrent_download_link(txt_path, "magnet:"))

    def test_extract_torrent_download_link_handles_json_read_exception(self):
        """JSON 读取异常应记录日志并返回 None。"""
        json_path = self.root / "broken.json"

        with patch.object(self.module.logger, "exception") as mock_exception:
            with patch.object(self.module, "read_json_to_dict", side_effect=ValueError("broken")):
                self.assertIsNone(self.module.extract_torrent_download_link(json_path, "magnet:"))

        mock_exception.assert_called_once()


class TestFilesystemWorkflows(unittest.TestCase):
    """验证小型目录整理流程。"""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.module = load_sort_movie_ops(str(self.root / "check"))

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_sort_torrents_auto_moves_log_files_to_matching_folders(self):
        """旧整理入口只保留 LOG 文件名的简单目录匹配。"""
        director = self.root / "Director"
        log_folder = director / "Downloaded.Movie"
        log_folder.mkdir(parents=True)
        json_file = director / "Sonic(2019)[1080p]{tt8108200}.json"
        log_file = director / "Downloaded.Movie.release.log"
        json_file.write_text("{}", encoding="utf-8")
        log_file.write_text("https://example.test/torrent", encoding="utf-8")

        with patch.object(self.module, "delete_trash_files") as delete_trash_files:
            self.module.sort_torrents_auto(str(self.root))

        self.assertTrue(json_file.exists())
        self.assertTrue((log_folder / log_file.name).exists())
        delete_trash_files.assert_called_once_with(str(self.root))

    def test_extract_movie_ids_stops_on_first_folder_without_braced_id(self):
        """影片目录缺少花括号 ID 时应返回 None，避免生成不完整结果。"""
        director = self.root / "Director"
        (director / "Movie One {tt1111111}").mkdir(parents=True)
        (director / "Movie Two").mkdir()

        with patch.object(self.module.logger, "error"):
            self.assertIsNone(self.module.extract_movie_ids(str(self.root)))

    def test_find_and_filter_video_files_use_expected_name_rules(self):
        """视频扫描和疑似错误文件名过滤应覆盖常用后缀和例外前缀。"""
        good = self.root / "ex_sample.mkv"
        sub = self.root / "SUB-sample.mp4"
        bad = self.root / "movie.avi"
        duplicate = self.root / "movie (1).mkv"
        ignored = self.root / "movie.txt"
        for path in [good, sub, bad, duplicate, ignored]:
            path.write_text("x", encoding="utf-8")

        videos = self.module.find_video_files(str(self.root))
        self.assertCountEqual(videos, [str(good), str(sub), str(bad), str(duplicate)])
        self.assertCountEqual(self.module.filter_video_files(videos), [str(bad), str(duplicate)])


class TestCheckLocalTorrent(unittest.TestCase):
    """验证本地库存种子命中后的移动策略。"""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.check_target = self.root / "check"
        self.module = load_sort_movie_ops(str(self.check_target))

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_check_local_torrent_moves_all_matches_without_deleting(self):
        """命中 IMDb 编号的库存种子都应移动到检查目录，且目标重名时保留两份。"""
        source_dir = self.root / "source"
        source_dir.mkdir()
        self.check_target.mkdir()
        first = source_dir / "Movie [tt1234567].torrent"
        second = source_dir / "Movie [tt1234567] 2160p.torrent"
        other = source_dir / "Other [tt7654321].torrent"
        existing = self.check_target / first.name
        first.write_text("first", encoding="utf-8")
        second.write_text("second", encoding="utf-8")
        other.write_text("other", encoding="utf-8")
        existing.write_text("existing", encoding="utf-8")
        self.module.PRE_LOAD_FP = [str(first), str(second), str(other)]

        result = self.module.check_local_torrent("tt1234567")

        self.assertEqual(result["move_counts"], 2)
        self.assertEqual(result["move_files"], [str(self.check_target / "Movie [tt1234567](1).torrent"), str(self.check_target / second.name)])
        self.assertFalse(first.exists())
        self.assertFalse(second.exists())
        self.assertTrue(other.exists())
        self.assertEqual(existing.read_text(encoding="utf-8"), "existing")
        self.assertEqual((self.check_target / "Movie [tt1234567](1).torrent").read_text(encoding="utf-8"), "first")
        self.assertEqual((self.check_target / second.name).read_text(encoding="utf-8"), "second")


if __name__ == "__main__":
    unittest.main()
