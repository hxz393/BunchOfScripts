"""
针对 ``my_scripts.torrent_to_magnet`` 的单元测试。
"""

import hashlib
import importlib.util
import sys
import tempfile
import types
import unittest
import urllib.parse
import uuid
from pathlib import Path
from unittest.mock import patch

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "torrent_to_magnet.py"


def fake_bencode_encode(value):
    """最小实现：支持 ``bytes`` / ``str`` / ``int`` / ``list`` / ``dict``。"""
    if isinstance(value, dict):
        encoded_items = []
        for key in sorted(value):
            encoded_items.append(fake_bencode_encode(key))
            encoded_items.append(fake_bencode_encode(value[key]))
        return b"d" + b"".join(encoded_items) + b"e"
    if isinstance(value, list):
        return b"l" + b"".join(fake_bencode_encode(item) for item in value) + b"e"
    if isinstance(value, int):
        return f"i{value}e".encode("ascii")
    if isinstance(value, str):
        value = value.encode("utf-8")
    if isinstance(value, bytes):
        return str(len(value)).encode("ascii") + b":" + value
    raise TypeError(f"Unsupported type for fake bencode: {type(value)!r}")


def fake_bencode_decode(data: bytes):
    """最小实现：只解析本测试会生成的 bencode 结构。"""

    def parse(index: int):
        token = data[index:index + 1]
        if token == b"i":
            end = data.index(b"e", index)
            return int(data[index + 1:end]), end + 1
        if token == b"l":
            index += 1
            items = []
            while data[index:index + 1] != b"e":
                item, index = parse(index)
                items.append(item)
            return items, index + 1
        if token == b"d":
            index += 1
            mapping = {}
            while data[index:index + 1] != b"e":
                key, index = parse(index)
                value, index = parse(index)
                mapping[key] = value
            return mapping, index + 1
        if token.isdigit():
            colon = data.index(b":", index)
            length = int(data[index:colon])
            start = colon + 1
            end = start + length
            return data[start:end], end
        raise ValueError(f"Invalid bencode token at {index}: {token!r}")

    result, next_index = parse(0)
    if next_index != len(data):
        raise ValueError("Trailing bytes after fake bencode decode")
    return result


def load_torrent_to_magnet():
    """在隔离环境中加载 ``torrent_to_magnet`` 模块。"""
    fake_bencodepy = types.ModuleType("bencodepy")
    fake_bencodepy.encode = fake_bencode_encode
    fake_bencodepy.decode = fake_bencode_decode

    spec = importlib.util.spec_from_file_location(
        f"torrent_to_magnet_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"bencodepy": fake_bencodepy}):
        spec.loader.exec_module(module)
    return module


class TestTorrentToMagnet(unittest.TestCase):
    """验证 torrent 文件到磁链的转换逻辑。"""

    def setUp(self):
        self.module = load_torrent_to_magnet()
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_torrent_to_magnet_keeps_all_trackers_from_announce_list(self):
        """存在 ``announce-list`` 时，应保留全部 tracker 并带上显示名。"""
        torrent_dict = {
            b"announce-list": [
                [b"https://tracker.example/announce"],
                [b"https://backup.example/announce"],
            ],
            b"info": {
                b"length": 123,
                b"name": "Example Name".encode("utf-8"),
                b"piece length": 16384,
                b"pieces": b"12345678901234567890",
            },
        }
        torrent_path = Path(self.temp_dir.name) / "movie.torrent"
        torrent_path.write_bytes(fake_bencode_encode(torrent_dict))
        info_hash = hashlib.sha1(fake_bencode_encode(torrent_dict[b"info"])).hexdigest()

        result = self.module.torrent_to_magnet(str(torrent_path))

        expected = (
            f"magnet:?xt=urn:btih:{info_hash}"
            f"&dn={urllib.parse.quote('Example Name')}"
            f"&tr={urllib.parse.quote('https://tracker.example/announce')}"
            f"&tr={urllib.parse.quote('https://backup.example/announce')}"
        )
        self.assertEqual(result, expected)

    def test_torrent_to_magnet_falls_back_to_announce_when_list_is_missing(self):
        """不存在 ``announce-list`` 时，应回退到 ``announce`` 字段。"""
        torrent_dict = {
            b"announce": b"https://single-tracker.example/announce",
            b"info": {
                b"length": 456,
                b"name": "Latin Name".encode("utf-8"),
                b"piece length": 16384,
                b"pieces": b"abcdefghijabcdefghij",
            },
        }
        torrent_path = Path(self.temp_dir.name) / "movie.torrent"
        torrent_path.write_bytes(fake_bencode_encode(torrent_dict))
        info_hash = hashlib.sha1(fake_bencode_encode(torrent_dict[b"info"])).hexdigest()

        result = self.module.torrent_to_magnet(str(torrent_path))

        expected = (
            f"magnet:?xt=urn:btih:{info_hash}"
            f"&dn={urllib.parse.quote('Latin Name')}"
            f"&tr={urllib.parse.quote('https://single-tracker.example/announce')}"
        )
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
