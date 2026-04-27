"""
Torrent 文件转磁链辅助函数。
"""
import hashlib
import urllib.parse

import bencodepy


def torrent_to_magnet(torrent_file_path: str) -> str:
    """
    将 torrent 文件转换为磁链。
    """
    with open(torrent_file_path, "rb") as file:
        torrent_data = file.read()

    decoded_data = bencodepy.decode(torrent_data)
    if not isinstance(decoded_data, dict):
        raise ValueError("无效 torrent：顶层 bencode 结果不是字典")
    torrent_dict = decoded_data

    info = torrent_dict.get(b"info")
    if not isinstance(info, dict):
        raise ValueError("无效 torrent：缺少 info 字典")

    info_bencoded = bencodepy.encode(info)
    info_hash = hashlib.sha1(info_bencoded).hexdigest()

    display_name = ""
    if b"name" in info:
        try:
            display_name = info[b"name"].decode("utf-8")
        except UnicodeDecodeError:
            display_name = info[b"name"].decode("latin1")
    display_name_encoded = urllib.parse.quote(display_name) if display_name else ""

    tracker_urls = []
    if b"announce-list" in torrent_dict:
        for tracker_group in torrent_dict[b"announce-list"]:
            candidates = tracker_group if isinstance(tracker_group, (list, tuple)) else [tracker_group]
            for tracker in candidates:
                try:
                    tracker_url = tracker.decode("utf-8")
                except Exception:
                    continue
                if tracker_url and tracker_url not in tracker_urls:
                    tracker_urls.append(tracker_url)
    elif b"announce" in torrent_dict:
        tracker_urls.append(torrent_dict[b"announce"].decode("utf-8"))

    magnet_link = f"magnet:?xt=urn:btih:{info_hash}"
    if display_name_encoded:
        magnet_link += f"&dn={display_name_encoded}"
    for tracker_url in tracker_urls:
        magnet_link += f"&tr={urllib.parse.quote(tracker_url)}"

    return magnet_link
