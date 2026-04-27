"""
抓取 onk 站点发布信息

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2026, hxz393. 保留所有权利。
"""
import datetime
import logging
import os
import re
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
from retrying import retry

from my_module import (
    normalize_release_title_for_filename,
    read_file_to_list,
    read_json_to_dict,
    sanitize_filename,
    update_json_config,
    write_list_to_file,
)
from scrapy_gd_downloader import download_gd_url, extract_drive_urls
from scrapy_redis import (
    deserialize_payload,
    drain_queue,
    get_redis_client,
    recover_processing_queue,
    serialize_payload,
)

CONFIG_PATH = 'config/scrapy_onk.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # 配置文件

GROUP_DICT = CONFIG['group_dict']  # 栏目 ID 字典
OUTPUT_DIR = CONFIG['output_dir']  # 输出目录
ONK_URL = CONFIG['onk_url']  # ONK 站点地址
ONK_COOKIE = CONFIG['onk_cookie']  # 用户甜甜
REQUEST_HEAD = CONFIG['request_head']  # 请求头
END_TIME = CONFIG.get('end_time', '2026-03-25')  # 截止日期
THREAD_NUMBER = CONFIG.get('thread_number', 20)  # 详情页并发数
REQUEST_HEAD["Cookie"] = ONK_COOKIE  # 请求头加入认证

START_URL = ONK_URL + "/forums/{fid}/page-{page}?order=post_date&direction=desc"
REDIS_PENDING_KEY = CONFIG.get('redis_pending_key', 'onk_pending')  # 待处理队列
REDIS_PROCESSING_KEY = CONFIG.get('redis_processing_key', 'onk_processing')  # 处理中队列
REDIS_SEEN_KEY = CONFIG.get('redis_seen_key', 'onk_seen')  # 已入队项目集合
REDIS_SCAN_COMPLETE_KEY = CONFIG.get('redis_scan_complete_key', 'onk_scan_complete')  # 列表扫描完成标记

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()


def build_session(pool_size: int) -> requests.Session:
    """创建连接池容量与当前并发相匹配的 Session。"""
    pool_size = max(pool_size, 10)
    client = requests.Session()
    adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size)
    client.mount("http://", adapter)
    client.mount("https://", adapter)
    return client


session = build_session(THREAD_NUMBER)


def get_previous_day(date_str: str) -> str:
    """返回 ``date_str`` 的前一天，格式保持 ``YYYY-mm-dd``。"""
    date_value = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    previous_day = date_value - datetime.timedelta(days=1)
    return previous_day.strftime("%Y-%m-%d")


def get_current_end_time() -> str:
    """读取当前配置中的截止日期，避免同进程多次运行时使用过期值。"""
    return read_json_to_dict(CONFIG_PATH).get("end_time", END_TIME)


def get_yesterday_date_str(reference_date: datetime.date | None = None) -> str:
    """返回当天前一天的日期字符串，格式为 ``YYYY-mm-dd``。"""
    if reference_date is None:
        reference_date = datetime.date.today()
    return get_previous_day(reference_date.strftime("%Y-%m-%d"))


def finalize_onk_run(redis_client=None) -> None:
    """扫描和下载都完成后，回写下一轮截止日期并清理运行状态。"""
    if redis_client is None:
        redis_client = get_redis_client()

    if redis_client.get(REDIS_SCAN_COMPLETE_KEY) != "1":
        logger.info("ONK 列表扫描尚未完成，暂不回写 end_time")
        return

    pending_count = redis_client.llen(REDIS_PENDING_KEY)
    if pending_count:
        logger.info("ONK 队列仍有未完成任务，暂不回写 end_time")
        return

    processing_count = redis_client.llen(REDIS_PROCESSING_KEY)
    if processing_count:
        logger.warning(f"ONK 待处理已空，但处理中仍有 {processing_count} 条，已保留处理中队列，请直接重跑")
        return

    next_end_time = get_yesterday_date_str()
    update_json_config(CONFIG_PATH, "end_time", next_end_time)
    redis_client.delete(
        REDIS_PENDING_KEY,
        REDIS_PROCESSING_KEY,
        REDIS_SEEN_KEY,
        REDIS_SCAN_COMPLETE_KEY,
    )
    logger.info(f"ONK 已更新 end_time 为 {next_end_time}")


def scrapy_onk() -> None:
    """
    先顺序扫描列表并入队，再从 Redis 队列中抓取详情并下载。
    """
    logger.info("抓取 onk 站点发布信息")
    redis_client = get_redis_client()
    stop_date = datetime.datetime.strptime(get_current_end_time(), "%Y-%m-%d")
    try:
        recover_onk_processing_when_pending_is_empty(redis_client)
        enqueue_onk_posts(stop_date, redis_client=redis_client)
        drain_onk_queue(redis_client=redis_client)
    finally:
        finalize_onk_run(redis_client=redis_client)


def build_onk_file_name(result_item: dict) -> str:
    """按当前 ONK 规则生成输出文件名。"""
    name = normalize_release_title_for_filename(result_item["title"])
    name = sanitize_filename(name)
    return f"{name}({result_item['label']})[{result_item['imdb_id']}].onk"


def build_onk_file_path(result_item: dict) -> str:
    """按当前 ONK 规则生成输出文件路径。"""
    return os.path.join(OUTPUT_DIR, build_onk_file_name(result_item))


def write_onk_item(result_item: dict) -> str:
    """写出单个 ``.onk`` 文件并返回路径。"""
    path = build_onk_file_path(result_item)
    links = [result_item["url"], str(result_item["post_date"])]
    write_list_to_file(path, links)
    return path


def write_to_disk(result_list: list) -> None:
    """写入到磁盘"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for result_item in result_list:
        write_onk_item(result_item)


def serialize_onk_post(result_item: dict) -> str:
    """将 ONK 列表项序列化为 Redis 队列任务。"""
    return serialize_payload(
        {
            "title": result_item["title"],
            "label": result_item["label"],
            "url": result_item["url"],
            "imdb_id": result_item["imdb_id"],
            "post_date": str(result_item["post_date"]),
            "file_path": build_onk_file_path(result_item),
        }
    )


def enqueue_onk_page_results(result_list: list[dict], redis_client) -> int:
    """将单页帖子统一写入 Redis 待处理队列。"""
    enqueued_count = 0
    for result_item in result_list:
        if not redis_client.sadd(REDIS_SEEN_KEY, result_item["url"]):
            continue
        redis_client.rpush(REDIS_PENDING_KEY, serialize_onk_post(result_item))
        enqueued_count += 1

    return enqueued_count


def enqueue_onk_posts(stop_date: datetime.datetime, redis_client=None) -> None:
    """顺序扫描所有栏目，把新帖子统一写入 Redis 待处理队列。"""
    if redis_client is None:
        redis_client = get_redis_client()

    if redis_client.get(REDIS_SCAN_COMPLETE_KEY) == "1":
        logger.info("ONK 列表扫描已完成，跳过入队阶段")
        return

    for group_name, group_id in GROUP_DICT.items():
        start_page = 1
        while True:
            logger.info(f"爬取栏目 {group_name} ，爬取页面 {start_page} …")
            results, stop = parse_forum_page(group_id, start_page, stop_date)
            if not results:
                logger.info(f"栏目 {group_name} 没有爬取结果")
                break

            enqueued_count = enqueue_onk_page_results(results, redis_client)
            logger.info(f"共 {len(results)} 个帖子，入队 {enqueued_count} 个")
            logger.info("-" * 255)
            if stop:
                break
            start_page += 1
        logger.info("-" * 255)

    redis_client.set(REDIS_SCAN_COMPLETE_KEY, "1")
    logger.info("ONK 列表扫描完成")
def extract_unique_imdb_id(text: str, file_path: str) -> str | None:
    """从帖子正文提取 IMDb 编号。多个编号时取正文中最先出现的一个。"""
    ids = re.findall(r"\btt\d+\b", text)
    unique_ids = list(dict.fromkeys(ids))
    if len(unique_ids) == 1:
        return unique_ids[0]
    if len(unique_ids) > 1:
        return unique_ids[0]

    logger.warning(f"未找到 {os.path.basename(file_path)} 的 IMDB ID")
    return ""


def rename_onk_file(file_path: str, imdb_id: str | None) -> str:
    """按 IMDb 编号修正 ``[].onk`` 文件名。"""
    if "[].onk" not in file_path or imdb_id is None:
        return file_path

    tag = imdb_id or "无"
    new_file_path = file_path.replace("[].onk", f"[{tag}].onk")
    if new_file_path == file_path:
        return file_path

    if os.path.exists(new_file_path):
        os.remove(file_path)
    else:
        os.rename(file_path, new_file_path)

    if imdb_id:
        logger.info(f"{file_path} -> {new_file_path}")
    else:
        logger.warning(f"无法获取 {os.path.basename(file_path)} 的 imdb 编号")
    return new_file_path


def build_artifact_base_name(onk_path: Path, artifact_index: int, artifact_total: int) -> str:
    """按帖子内下载链接顺序生成稳定的输出基名。"""
    if artifact_total <= 1:
        return onk_path.stem

    width = max(2, len(str(artifact_total)))
    return f"{onk_path.stem}.{artifact_index:0{width}d}"


def find_existing_download(parent_dir: Path, artifact_base_name: str) -> Path | None:
    """按输出基名查找已存在的下载结果。"""
    for candidate in sorted(parent_dir.iterdir()):
        if not candidate.is_file() or ".part." in candidate.name:
            continue
        if candidate.suffix.lower() == ".onk":
            continue
        if candidate.stat().st_size <= 0:
            continue

        candidate_base = candidate.name[:-7] if candidate.name.lower().endswith(".nzb.gz") else candidate.stem
        if candidate_base.casefold() == artifact_base_name.casefold():
            return candidate
    return None


def find_completed_onk_outputs(onk_path: Path) -> list[Path]:
    """查找与当前 ``.onk`` 对应的已完成下载结果，兼容多链接编号输出。"""
    stem = onk_path.stem.casefold()
    prefix = f"{onk_path.stem}.".casefold()
    matches = []

    for candidate in sorted(onk_path.parent.iterdir()):
        if not candidate.is_file() or ".part." in candidate.name:
            continue
        if candidate.suffix.lower() == ".onk":
            continue
        if candidate.stat().st_size <= 0:
            continue

        candidate_base = candidate.name[:-7] if candidate.name.lower().endswith(".nzb.gz") else candidate.stem
        candidate_base_folded = candidate_base.casefold()
        if candidate_base_folded == stem or candidate_base_folded.startswith(prefix):
            matches.append(candidate)

    return matches


def build_onk_url_index() -> dict[str, str]:
    """扫描当前输出目录，建立帖子 URL 到 ``.onk`` 路径的映射。"""
    output_dir = Path(OUTPUT_DIR)
    if not output_dir.exists():
        return {}

    url_index: dict[str, str] = {}

    for candidate in sorted(output_dir.iterdir()):
        if not candidate.is_file() or candidate.suffix.lower() != ".onk":
            continue
        try:
            links = read_file_to_list(str(candidate))
        except OSError:
            continue
        if links:
            url_index.setdefault(links[0].strip(), str(candidate))

    return url_index


def find_onk_file_by_url(url: str, url_index: dict[str, str] | None = None) -> str | None:
    """按帖子 URL 反查当前输出目录里的 ``.onk`` 文件。"""
    if url_index is None:
        url_index = build_onk_url_index()
    return url_index.get(url)


def ensure_onk_file(result_item: dict) -> str:
    """确保当前队列任务对应的 ``.onk`` 文件已经存在。"""
    file_path = result_item.get("file_path") or build_onk_file_path(result_item)
    if os.path.exists(file_path):
        return file_path

    resolved_path = find_onk_file_by_url(result_item["url"])
    if resolved_path is not None:
        return resolved_path

    return write_onk_item(result_item)


def raise_onk_stage_error(file_path: str, url: str, reason: str) -> None:
    """记录醒目的错误日志并抛出异常，留待人工排查后移走 ``.onk``。"""
    logger.error("=" * 120)
    logger.error(f"ONK 任务失败：{reason} | file={os.path.basename(file_path)} | url={url}")
    logger.error("=" * 120)
    raise RuntimeError(reason)


def resolve_onk_file_path(result_item: dict) -> str:
    """优先使用任务内路径；若文件已被改名，则按 URL 回查当前 ``.onk`` 文件。"""
    file_path = result_item.get("file_path", "")
    if file_path and os.path.exists(file_path):
        return file_path

    resolved_path = find_onk_file_by_url(result_item["url"])
    if resolved_path is not None:
        return resolved_path

    raise FileNotFoundError(f"未找到对应的 ONK 文件: {result_item['url']}")


def parse_forum_page(group_id, start_page, stop_time):
    """ 获取某个栏目的所有帖子 """
    url = START_URL.format(fid=group_id, page=start_page)
    resp = get_onk_response(url)
    soup = BeautifulSoup(resp.content, "lxml")

    # 每一条帖子
    items = soup.select('div.structItem.structItem--thread')
    if not items:
        logger.error("没有找到帖子！样式更新了？")
        return [], False

    results = []
    stop = False

    for item in items:
        data = {}
        # 1. 标签（可选）
        label_span = item.select_one('a.labelLink span')
        data["label"] = label_span.get_text(strip=True) if label_span else ""

        # 2. 标题
        title_a = item.select_one('.structItem-title > a[href^="/threads/"]')
        if not title_a:
            continue  # 标题都没有，直接跳过

        # 3. 链接
        data["title"] = title_a.get_text(strip=True)
        data["url"] = ONK_URL + title_a["href"]

        # 4. IMDB 编号（可选）
        imdb_a = item.select_one('span.imdb a[href*="imdb.com/title"]')
        if imdb_a:
            match = re.search(r'(tt\d+)', imdb_a["href"])
            data["imdb_id"] = match.group(1) if match else None
        else:
            data["imdb_id"] = ""

        # 5. 发帖时间（取 structItem-startDate 下的 time.u-dt）
        time_tag = item.select_one('.structItem-startDate time.u-dt')
        post_date = None
        if time_tag and time_tag.has_attr("datetime"):
            date_str = time_tag["datetime"].split("T")[0]
            post_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
            data["post_date"] = post_date
        else:
            data["post_date"] = None

        # 判断停止时间
        if post_date:
            if post_date >= stop_time:
                results.append(data)
            elif start_page != 1:  # 判断是否达到停止条件
                stop = True
        else:
            results.append(data)

    return results, stop


@retry(stop_max_attempt_number=5, wait_random_min=15000, wait_random_max=20000)
def get_onk_response(url: str) -> requests.Response:
    """请求流程"""
    logger.info(f"访问 {url}")
    response = session.get(url, timeout=30, verify=False, headers=REQUEST_HEAD)
    if response.status_code != 200:
        logger.error(f"请求失败，重试 {response.status_code}：{url}")
        raise Exception(f"请求失败")

    return response


def recover_onk_processing_when_pending_is_empty(redis_client) -> int:
    """启动时若待处理为空但处理中有残留，则回退到待处理并继续运行。"""
    if redis_client.llen(REDIS_PENDING_KEY) or not redis_client.llen(REDIS_PROCESSING_KEY):
        return 0

    recovered_count = recover_processing_queue(
        redis_client,
        processing_key=REDIS_PROCESSING_KEY,
        pending_key=REDIS_PENDING_KEY,
        logger=logger,
        queue_label="ONK",
    )
    logger.warning(f"ONK 检测到待处理为空但处理中残留 {recovered_count} 条，已回退到待处理队列并继续运行")
    return recovered_count


def drain_onk_queue(redis_client=None) -> None:
    """从 Redis 队列中取帖子，使用多线程访问详情页并下载同名文件。"""
    if redis_client is None:
        redis_client = get_redis_client()

    drain_queue(
        redis_client,
        pending_key=REDIS_PENDING_KEY,
        processing_key=REDIS_PROCESSING_KEY,
        max_workers=THREAD_NUMBER,
        worker=visit_onk_url,
        deserialize=deserialize_payload,
        logger=logger,
        queue_label="ONK",
        identify_item=lambda info: info["url"],
        recover_processing_on_start=False,
        keep_failed_in_processing=True,
    )


def download_drive_artifact(drive_url: str, onk_path: str, artifact_index: int = 1, artifact_total: int = 1) -> str:
    """下载单个 GD 地址，并按当前 ONK 规则落盘。"""
    onk_file = Path(onk_path)
    artifact_base_name = build_artifact_base_name(onk_file, artifact_index, artifact_total)
    existing_output = find_existing_download(onk_file.parent, artifact_base_name)
    if existing_output is not None:
        logger.info(f"已存在 {existing_output.name}，跳过 {artifact_base_name}")
        return str(existing_output)

    download_result = download_gd_url(drive_url)
    output_path = onk_file.with_name(f"{artifact_base_name}{download_result.suggested_suffix}")
    output_path.write_bytes(download_result.payload)
    logger.info(f"{onk_file.name} -> {output_path.name}")
    return str(output_path)


def visit_onk_url(result_item: dict):
    """访问详情页"""
    file_path = ensure_onk_file(result_item)
    if result_item.get("label") != "NZB":
        return file_path

    completed_outputs = find_completed_onk_outputs(Path(file_path))
    if completed_outputs:
        if len(completed_outputs) == 1:
            return str(completed_outputs[0])
        return [str(path) for path in completed_outputs]

    url = result_item["url"]
    response = get_onk_response(url)
    soup = BeautifulSoup(response.text, 'lxml')

    main_div = soup.find('div', class_='message-cell message-cell--main')
    if not main_div:
        raise_onk_stage_error(file_path, url, "无法获取帖子内容")

    imdb_id = result_item.get("imdb_id", "")
    if not imdb_id:
        text = main_div.get_text("\n", strip=True)
        imdb_id = extract_unique_imdb_id(text, file_path)
    new_file_path = rename_onk_file(file_path, imdb_id)

    drive_urls = extract_drive_urls(str(main_div))
    if not drive_urls:
        raise_onk_stage_error(new_file_path, url, "帖子里没有找到 Google Drive 链接")

    output_paths = [
        download_drive_artifact(drive_url, new_file_path, index, len(drive_urls))
        for index, drive_url in enumerate(drive_urls, start=1)
    ]
    logger.info("=" * 250)
    if len(output_paths) == 1:
        return output_paths[0]
    return output_paths
