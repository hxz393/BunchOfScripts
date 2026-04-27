"""
Google Drive 下载辅助函数。
"""
from dataclasses import dataclass
import html
import re
from pathlib import Path
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter
from retrying import retry

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
}
DRIVE_URL_RE = re.compile(
    r"https://(?:"
    r"drive\.google\.com/(?:file/d/[A-Za-z0-9_-]+/view[^\s\"'<>]*|open\?[^\s\"'<>]*\bid=[A-Za-z0-9_-]+[^\s\"'<>]*|uc\?[^\s\"'<>]*\bid=[A-Za-z0-9_-]+[^\s\"'<>]*)|"
    r"drive\.usercontent\.google\.com/(?:uc|download)\?[^\s\"'<>]*\bid=[A-Za-z0-9_-]+[^\s\"'<>]*|"
    r"docs\.google\.com/uc\?[^\s\"'<>]*\bid=[A-Za-z0-9_-]+[^\s\"'<>]*"
    r")",
    re.IGNORECASE,
)
FILE_ID_PATTERNS = [
    re.compile(r"/file/d/([A-Za-z0-9_-]+)", re.IGNORECASE),
    re.compile(r"[?&]id=([A-Za-z0-9_-]+)", re.IGNORECASE),
]
VALID_NZB_MARKERS = (b"<?xml", b"<!doctype nzb", b"<nzb")

requests.packages.urllib3.disable_warnings()
SESSION_POOL_SIZE = 50
SESSION_PROXIES = {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"}


@dataclass(frozen=True)
class DownloadedGdFile:
    drive_name: str | None
    payload: bytes
    content_type: str
    suggested_suffix: str



def build_session(pool_size: int = SESSION_POOL_SIZE) -> requests.Session:
    """创建较大的通用连接池，避免多线程下载时频繁丢连接。"""
    pool_size = max(pool_size, 10)
    client = requests.Session()
    client.proxies = dict(SESSION_PROXIES)
    adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size)
    client.mount("http://", adapter)
    client.mount("https://", adapter)
    return client


session = build_session()


def extract_drive_urls(html_text: str) -> list[str]:
    """从 HTML 中提取所有 Google Drive 链接，并按出现顺序去重。"""
    text = html.unescape(html_text)
    candidates = [
        match.group(0)
        for match in DRIVE_URL_RE.finditer(text)
        if "imdb.com" not in match.group(0).lower()
    ]
    return list(dict.fromkeys(candidates))


def extract_drive_file_id(url: str) -> str:
    """从 Google Drive 链接提取 file id。"""
    for pattern in FILE_ID_PATTERNS:
        match = pattern.search(url)
        if match:
            return match.group(1)
    raise RuntimeError(f"无法从链接提取 Google Drive file id: {url}")


def normalize_drive_view_url(url: str, file_id: str) -> str:
    """统一成 Google Drive 查看页链接。"""
    if "/file/d/" in url:
        return url
    return f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"


def decode_js_escapes(text: str) -> str:
    """还原 Google Drive HTML 中常见的 JS 转义。"""
    return (
        text.replace("\\u003d", "=")
        .replace("\\u0026", "&")
        .replace("\\u003c", "<")
        .replace("\\u003e", ">")
        .replace("\\/", "/")
    )


def extract_drive_metadata(view_html: str, file_id: str) -> tuple[str | None, str]:
    """从 Google Drive 查看页提取文件名和直链。"""
    decoded_html = decode_js_escapes(html.unescape(view_html))
    name_match = re.search(
        r'<meta\s+itemprop="name"\s+content="([^"]+)"',
        decoded_html,
        re.IGNORECASE,
    )
    drive_name = html.unescape(name_match.group(1)) if name_match else None

    patterns = [
        re.compile(
            rf"https://drive\.usercontent\.google\.com/(?:uc|download)\?[^\s\"'<>]*\bid={re.escape(file_id)}[^\s\"'<>]*",
            re.IGNORECASE,
        ),
        re.compile(
            rf"https://(?:drive\.google\.com|docs\.google\.com)/uc\?[^\s\"'<>]*\bid={re.escape(file_id)}[^\s\"'<>]*",
            re.IGNORECASE,
        ),
    ]
    for pattern in patterns:
        match = pattern.search(decoded_html)
        if match:
            return drive_name, match.group(0)

    return drive_name, f"https://drive.usercontent.google.com/uc?id={file_id}&export=download"


def build_confirm_download_url(html_text: str) -> str | None:
    """从 Google Drive 告警页提取确认下载链接。"""
    form_match = re.search(
        r'<form[^>]+id="download-form"[^>]+action="([^"]+)"',
        html_text,
        re.IGNORECASE,
    )
    if not form_match:
        return None

    action = html.unescape(form_match.group(1))
    pairs = [
        (html.unescape(name), html.unescape(value))
        for name, value in re.findall(
            r'<input[^>]+name="([^"]+)"[^>]+value="([^"]*)"',
            html_text,
            re.IGNORECASE,
        )
    ]
    if not pairs:
        return None
    return action + "?" + urlencode(pairs)


def infer_download_suffix(drive_name: str | None, payload: bytes, content_type: str) -> str:
    """按文件名、内容或响应头推断下载后缀。"""
    if drive_name:
        name_lower = drive_name.lower()
        if name_lower.endswith(".nzb.gz"):
            return ".nzb.gz"
        suffix = Path(drive_name).suffix
        if re.fullmatch(r"\.[A-Za-z0-9]{1,8}", suffix):
            return suffix

    head = payload[:2048].lstrip().lower()
    if any(marker in head for marker in VALID_NZB_MARKERS):
        return ".nzb"
    if payload.startswith(b"Rar!"):
        return ".rar"
    if payload.startswith(b"PK\x03\x04"):
        return ".zip"
    if "zip" in content_type.lower():
        return ".zip"
    return ".bin"


def ensure_download_payload(payload: bytes, content_type: str) -> None:
    """校验下载结果不是 HTML 页面。"""
    if not payload:
        raise RuntimeError("下载结果为空文件")

    head = payload[:2048].lstrip().lower()
    if head.startswith(b"<!doctype html") or head.startswith(b"<html") or "text/html" in content_type.lower():
        raise RuntimeError("下载结果是 HTML 页面，不是有效文件")


@retry(stop_max_attempt_number=5, wait_random_min=15000, wait_random_max=20000)
def get_binary_response(url: str, referer: str | None = None) -> requests.Response:
    """访问 Google Drive 相关链接。"""
    headers = dict(DEFAULT_HEADERS)
    if referer:
        headers["Referer"] = referer

    response = session.get(url, timeout=90, verify=False, headers=headers)
    if response.status_code != 200:
        raise RuntimeError(f"下载失败，HTTP {response.status_code}")
    return response


def maybe_confirm_drive_warning(response: requests.Response, referer: str) -> tuple[bytes, str, str | None]:
    """处理 Google Drive 大文件告警页。"""
    text = response.text
    if "Virus scan warning" not in text and "can't scan this file for viruses" not in text:
        return response.content, response.headers.get("Content-Type", ""), None

    confirm_url = build_confirm_download_url(text)
    if not confirm_url:
        raise RuntimeError("Google Drive 告警页缺少确认下载表单")

    confirmed = get_binary_response(confirm_url, referer=referer)
    name_match = re.search(
        r'<span class="uc-name-size"><a [^>]+>([^<]+)</a>',
        text,
        re.IGNORECASE,
    )
    warning_name = html.unescape(name_match.group(1)).strip() if name_match else None
    return confirmed.content, confirmed.headers.get("Content-Type", ""), warning_name


def download_gd_url(gd_url: str) -> DownloadedGdFile:
    """下载单个 Google Drive 地址，并返回文件内容与建议后缀。"""
    file_id = extract_drive_file_id(gd_url)
    view_url = normalize_drive_view_url(gd_url, file_id)
    view_response = get_binary_response(view_url)
    drive_name, download_url = extract_drive_metadata(view_response.text, file_id)

    download_response = get_binary_response(download_url, referer=view_url)
    payload, content_type, warning_name = maybe_confirm_drive_warning(download_response, view_url)
    if warning_name:
        drive_name = warning_name
    ensure_download_payload(payload, content_type)

    return DownloadedGdFile(
        drive_name=drive_name,
        payload=payload,
        content_type=content_type,
        suggested_suffix=infer_download_suffix(drive_name, payload, content_type),
    )
