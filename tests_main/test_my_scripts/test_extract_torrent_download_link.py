"""
针对 ``my_scripts.sort_movie_ops`` 中共享 helper 的单元测试。

这些测试使用真实的临时文件，但不会依赖真实的 YTS 配置或网络。
重点验证：
1. JSON 来源会调用共享的 YTS 磁链选择逻辑。
2. LOG 来源会返回首行并去掉 BOM。
3. 坏 JSON / 空 LOG 会返回 ``None``。
4. IMDb 编号提取 helper 的统一行为。
"""

import importlib.util
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch


MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "sort_movie_ops.py"


def load_extract_torrent_download_link(select_best_yts_magnet=None):
    """在隔离环境中加载 ``sort_movie_ops`` 模块。"""
    fake_scrapy_yts = types.ModuleType("scrapy_yts")
    fake_scrapy_yts.select_best_yts_magnet = select_best_yts_magnet or (
        lambda json_data, magnet_path: f"{magnet_path}{json_data['data']['movie']['torrents'][0]['hash']}"
    )

    fake_config = {
        "trash_list": [],
        "source_list": [],
        "video_extensions": [".mkv", ".mp4", ".avi"],
        "max_bitrate": 1000,
        "magnet_path": "magnet:?xt=urn:btih:",
        "rarbg_source": "",
        "ttg_source": "",
        "dhd_source": "",
        "sk_source": "",
        "rare_source": "",
        "rls_source": "",
        "check_target": "",
        "everything_path": "",
        "ffprobe_path": "",
        "mtm_path": "",
        "mediainfo_path": "",
        "mirror_path": "",
        "ru_path": "",
        "yts_path": "",
        "dhd_path": "",
        "ttg_path": "",
        "sk_path": "",
        "rare_path": "",
    }

    fake_my_module = types.ModuleType("my_module")

    def fake_read_json_to_dict(target_path):
        if str(target_path).replace("\\", "/").endswith("config/sort_movie_ops.json"):
            return dict(fake_config)
        try:
            return json.loads(Path(target_path).read_text(encoding="utf-8"))
        except Exception:
            return None

    fake_my_module.read_json_to_dict = fake_read_json_to_dict
    fake_my_module.read_file_to_list = lambda target_path: [
        line.strip()
        for line in Path(target_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    fake_my_module.sanitize_filename = lambda text: text
    fake_my_module.get_file_paths = lambda _path: []
    fake_my_module.remove_target = lambda _path: None
    fake_my_module.get_folder_paths = lambda _path: []

    fake_scrapy_kpk = types.ModuleType("scrapy_kpk")
    fake_scrapy_kpk.scrapy_kpk = lambda *_args, **_kwargs: None

    fake_pil = types.ModuleType("PIL")
    fake_pil.Image = object()

    fake_moviepy = types.ModuleType("moviepy")
    fake_moviepy.VideoFileClip = object

    spec = importlib.util.spec_from_file_location(
        "sort_movie_ops",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "scrapy_yts": fake_scrapy_yts,
            "my_module": fake_my_module,
            "scrapy_kpk": fake_scrapy_kpk,
            "PIL": fake_pil,
            "moviepy": fake_moviepy,
        },
    ):
        spec.loader.exec_module(module)

    return module


def build_movie_json(hash_value: str = "JSONHASH") -> str:
    return (
        '{'
        '"data": {'
        '"movie": {'
        '"torrents": ['
        '{'
        f'"quality": "1080p", "video_codec": "x265", "bit_depth": "10", "type": "bluray", "size_bytes": 1000, "hash": "{hash_value}"'
        '}'
        ']'
        '}'
        '}'
        '}'
    )


class TestExtractTorrentDownloadLink(unittest.TestCase):
    def test_extracts_magnet_from_json_with_passed_prefix(self):
        module = load_extract_torrent_download_link()

        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "movie.json"
            target.write_text(build_movie_json("JSONHASH"), encoding="utf-8")

            result = module.extract_torrent_download_link(target, "prefix:")

        self.assertEqual(result, "prefix:JSONHASH")

    def test_calls_shared_yts_helper_for_json(self):
        fake_select = lambda json_data, magnet_path: f"{magnet_path}{json_data['data']['movie']['torrents'][0]['hash']}"
        module = load_extract_torrent_download_link(select_best_yts_magnet=fake_select)

        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "movie.json"
            target.write_text(build_movie_json("JSONHASH"), encoding="utf-8")

            with patch.object(module, "select_best_yts_magnet", return_value="prefix:JSONHASH") as mock_select:
                result = module.extract_torrent_download_link(target, "prefix:")

        self.assertEqual(result, "prefix:JSONHASH")
        mock_select.assert_called_once()

    def test_extracts_first_line_from_log_and_strips_bom(self):
        module = load_extract_torrent_download_link()

        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "movie.log"
            target.write_text("\ufeffmagnet:?xt=urn:btih:LOGHASH\nsecond line\n", encoding="utf-8")

            result = module.extract_torrent_download_link(target, "prefix:")

        self.assertEqual(result, "magnet:?xt=urn:btih:LOGHASH")

    def test_returns_none_for_empty_log(self):
        module = load_extract_torrent_download_link()

        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "movie.log"
            target.write_text("", encoding="utf-8")

            with patch.object(module.logger, "error"):
                result = module.extract_torrent_download_link(target, "prefix:")

        self.assertIsNone(result)

    def test_returns_none_for_invalid_json(self):
        module = load_extract_torrent_download_link()

        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "movie.json"
            target.write_text("{not json}", encoding="utf-8")

            with patch.object(module.logger, "error"):
                result = module.extract_torrent_download_link(target, "prefix:")

        self.assertIsNone(result)


class TestImdbHelpers(unittest.TestCase):
    def test_extract_imdb_id_prefers_canonical_url(self):
        module = load_extract_torrent_download_link()

        result = module.extract_imdb_id("https://www.imdb.com/title/tt1234567/?ref_=fn_al_tt_1 and tt7654321")

        self.assertEqual(result, "tt1234567")

    def test_extract_imdb_id_from_links_falls_back_to_loose_match(self):
        module = load_extract_torrent_download_link()

        result = module.extract_imdb_id_from_links(
            [
                "https://example.com/post?id=42",
                "https://example.com/redirect?target=tt7654321",
            ]
        )

        self.assertEqual(result, "tt7654321")

    def test_extract_imdb_ids_preserves_first_seen_order(self):
        module = load_extract_torrent_download_link()

        result = module.extract_imdb_ids("tt1234567 and TT7654321 and tt1234567 again")

        self.assertEqual(result, ["tt1234567", "tt7654321"])


if __name__ == "__main__":
    unittest.main()
