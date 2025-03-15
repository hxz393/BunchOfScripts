"""
抓取 ru 站点搜索关键字，抓取结果列表中的磁链，保存到文件。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import concurrent.futures
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

import redis
import requests
from lxml import etree
from retrying import retry

from my_module import read_json_to_dict, sanitize_filename

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

CONFIG_PATH = 'config/scrapy_ru.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # 配置文件

SEARCH_URL = CONFIG['search_url']  # 基本搜索地址
FORUM_URL = CONFIG['forum_url']  # 基本论坛地址
REQUEST_HEAD = CONFIG['request_head']  # 请求头
USER_COOKIE = CONFIG['user_cookie']  # 用户甜甜
RU_DIC_TAG = CONFIG['ru_dic_tag']  # 翻译字典
RU_DIC_GP = CONFIG['ru_dic_gp']  # 翻译字典
REDIS_HOST = CONFIG['redis_host']  # Redis 主机 IP，没有密码
REDIS_SET_KEY = CONFIG['redis_set_key']  # Redis 集合键
THREAD_NUMBER = CONFIG['thread_number']  # 线程数
MIRROR_PATH = CONFIG['mirror_path']  # 镜像文件夹路径

REQUEST_HEAD["Cookie"] = USER_COOKIE  # 请求头加入认证


def scrapy_ru_magnet(key_word: str, target: str, date_order: str = "2", cache: bool = True) -> None:
    """
    将磁链信息保存到来源目录中。

    :param key_word: 关键词
    :param target: 保存目录
    :param date_order: 参数控制按时间新旧排序，2 代表新的在前
    :param cache: 是否验证 Redis 缓存
    :return: 无
    """
    if not key_word:
        return
    logger.info(f"开始搜索：{key_word}")
    # 对于英语和俄语不要转码，其他要转码
    url = f'{SEARCH_URL}{key_word}'
    data = {"nm": key_word.encode("cp1251", "xmlcharrefreplace").decode("cp1251"), "s": date_order}
    logger.debug(data)

    # 获取搜索结果，如果没有获取到，检测别名后返回
    topic_infos = []
    get_all_links(url, topic_infos, data)
    if not topic_infos:
        if cache:
            get_aka_name(key_word, target)
        return

    final_infos = []
    new_urls = []
    redis_client = redis.Redis(host=REDIS_HOST, decode_responses=True)
    if not cache:
        for d in topic_infos:
            d["name"] = os.path.join(target, d["name"])
            final_infos.append(d)
    else:
        # 连接并查询本地 Redis，返回查询结果
        query_urls = [d['url'] for d in topic_infos]
        redis_query_result_raw = bulk_query_ids(redis_client, REDIS_SET_KEY, query_urls)
        redis_query_result = {url: exists for url, exists in zip(query_urls, redis_query_result_raw)}
        # 处理 RDS 查询结果
        for d in topic_infos:
            # RDS 中存在的地址不做查询
            if redis_query_result[d['url']]:
                continue
            new_urls.append(d['url'])
            d["name"] = os.path.join(target, d["name"])
            final_infos.append(d)
        # 当所有链接都在 RDS 中时，检测别名后返回
        if not final_infos:
            get_aka_name(key_word, target)
            logger.info(f"{key_word} 没有新链接需要处理")
            return

    # 多线程访问帖子具体内容，获取其中磁链
    logger.info(f"开始获取信息：{key_word}")
    failed_urls = []  # 用于保存失败的链接
    with ThreadPoolExecutor(max_workers=THREAD_NUMBER) as executor:
        future_to_topic = {executor.submit(scrapy_magnet, topic_info): topic_info for topic_info in final_infos}
        for future in concurrent.futures.as_completed(future_to_topic):
            topic_info = future_to_topic[future]
            try:
                result = future.result()
                # 当返回值为 False 时，记录失败链接
                if result is False:
                    failed_urls.append(topic_info["url"])
            except Exception as exc:
                logger.error(f"[get_magnet] 线程执行出现异常: {exc}")

    # 最后储存本次爬取链接和关键字，打印失败链接
    if cache:
        bulk_insert_ids(redis_client, REDIS_SET_KEY, new_urls)
        get_aka_name(key_word, target)
    if failed_urls:
        logger.warning("以下链接处理失败：")
        for url in failed_urls:
            logger.warning(url)
    else:
        logger.info("所有链接均处理成功。")


def get_all_links(url: str, topic_infos: list, data: dict = None) -> None:
    """
    获取搜索结果中的所有帖子地址。

    :param data: post 数据
    :param url: 搜索地址
    :param topic_infos: 函数修改传入列表，得到 [{'url': 'https://...', 'name': '...'}]
    :return: 无
    """
    # 请求搜索地址，总该使用 POST
    r = requests.post(url=url, headers=REQUEST_HEAD, data=data, timeout=15, verify=False, allow_redirects=True)
    if r.status_code != 200:
        logger.error(f"搜索失败！{r.status_code}")
        return

    # 检测搜索结果数量
    search_counts = int(re.search(r'Результатов поиска: (\d+)', r.text).group(1))
    if search_counts == 0:
        logger.info("没有搜索结果！")
        return
    elif search_counts > 490:
        # 如果要强制下载，将返回注释掉，data 的 s 参数(date_order)设置为 1
        logger.warning("搜索结果过多！")
    logger.info(f"搜索结果：{search_counts} 条")

    # 找到所有种子行，路径为 //tr[@class="tCenter hl-tr"]
    tree = etree.HTML(r.text)
    row_elements = tree.xpath('//tr[@class="tCenter hl-tr"]')
    if not row_elements:
        logger.error("没找到种子行！")
        return

    # 遍历每一行
    for row in row_elements:
        # 找栏目
        group_element = row.xpath('.//td[@class="row1 f-name-col"]//a')
        if not group_element:
            logger.error("没有找到栏目")
            return
        group_title_org = group_element[0].xpath('string(.)').strip()

        # 找标题及链接
        title_element = row.xpath('.//td[@class="row4 med tLeft t-title-col tt"]//a')
        if not title_element:
            logger.error("没有找到标题")
            return
        # 取第一个匹配到的 <a> 标签
        a_tag = title_element[0]
        title_text_org = a_tag.xpath('string(.)').strip()
        topic_link = f"{FORUM_URL}{a_tag.xpath('@href')[0]}"

        # 找文件大小和下载地址
        dl_element = row.xpath('.//td[@class="row4 small nowrap tor-size"]//a')
        if not dl_element:
            continue
        size_text = dl_element[0].xpath('string(.)').replace("\xa0", "").replace("↓", "").strip()  # 例如 "272.1 MB"

        # 修剪文件名
        file_name = f"{title_text_org}「{group_title_org}」({size_text}).log"
        file_name = replace_ru(file_name)
        file_name = file_name.replace("/", "｜").replace("\\", "｜")
        file_name = re.sub(r'\s*｜\s*', '｜', file_name)
        file_name = russian_delete(file_name)
        file_name = sanitize_filename(file_name)
        file_name = truncate_filename(file_name)

        # 得到想要数据，打印出来，并插入到传入的列表 topic_infos
        info = {'url': topic_link, 'name': file_name}
        logger.debug(f'[{group_title_org}]{title_text_org}')
        topic_infos.append(info)

    # 查找下一页链接，找到了递归自身
    next_link = tree.xpath('//a[@class="pg" and text()="След."]')
    if next_link:
        # 如果能找到此链接，说明还有下一页
        href_value = f"{FORUM_URL}{next_link[0].get('href')}"
        logger.info(f"开始下一页链接: {href_value}")
        get_all_links(href_value, topic_infos)


def replace_ru(file_name: str) -> str:
    """
    修改原贴标题。替换俄语到中文。分两次替换

    :param file_name: 原文件名
    :return: 新文件名
    """
    # 替换栏目名
    pattern_group = r'「([^」]+)」'
    matches_gp = re.findall(pattern_group, file_name)
    for match in matches_gp:
        replaced = replace_using_dict(match, RU_DIC_GP)
        file_name = file_name.replace(f"「{match}」", f"「{replaced}」")

    # 替换标题标签，通常是国家和类型
    pattern_description = r'\[([^\]]+)\]'
    matches_dp = re.findall(pattern_description, file_name)
    for match in matches_dp:
        replaced = replace_using_dict(match, RU_DIC_TAG)
        file_name = file_name.replace(f"[{match}]", f"[{replaced}]")
    return file_name


def replace_using_dict(text: str, lookup: dict) -> str:
    """
    用给定的字典对 text 中出现的键进行替换

    :param text: 文本内容
    :param lookup: 翻译字典
    :return: 翻译后的文本
    """
    for k, v in lookup.items():
        # 使用正则表达式，并且设置 IGNORECASE，实现不区分大小写替换
        pattern = re.compile(re.escape(k), re.IGNORECASE)
        text = pattern.sub(v, text)
    return text


def truncate_filename(filename: str, max_length: int = 250) -> str:
    """
    将文件名裁剪到不超过 max_length 长度，以防创建文件失败。按以下顺序：
      1) 若不超过 max_length，直接返回
      2) 若超长，删除最后一个 ']' 与其后首个 '「' 之间的内容（保留 ']' 和 '「'）
      3) 若仍超长，删除第一个 '(' 与其对应的 ')' 之间的内容（含括号）
      4) 若仍超长，从 '「' 向前开始删除（保留从 '「' 开始到最后的所有字符），硬截断到 max_length 长度

    :param filename: 原文件名
    :param max_length: 保留长度
    :return: 新文件名
    """
    # 长度不超限，直接返回
    if len(filename) <= max_length:
        return filename

    # 删除最后一个 ']' 与其后第一个 '「' 之间的内容。通常是字幕SUB、音频DVO等信息
    last_right_bracket = filename.rfind(']')
    if last_right_bracket != -1:
        # 在 last_right_bracket 之后查找 '「'
        bracket_index = filename.find('「', last_right_bracket + 1)
        # 确保能找到，并且位置在 ']' 之后
        if bracket_index != -1 and bracket_index > last_right_bracket:
            # 删除中间部分
            filename = (
                    filename[:last_right_bracket + 1] +  # 保留到包含这个 ']'
                    filename[bracket_index:]  # 从 '「' 开始继续保留
            )
    if len(filename) <= max_length:
        return filename

    # 删除第一个 '(' 与其对应的 ')' 之间的内容（含括号本身）。通常是导演名和别名信息
    first_left_paren = filename.find('(')
    if first_left_paren != -1:
        first_right_paren = filename.find(')', first_left_paren + 1)
        if first_right_paren != -1:
            filename = filename[:first_left_paren] + filename[first_right_paren + 1:]
    if len(filename) <= max_length:
        return filename

    # 从 '「' 向前开始删除，保留 '「' 及后面的内容
    bracket_index = filename.find('「')
    if bracket_index == -1:
        # 如果不存在 '「'，无法“从 '「' 向前”删除，则做硬截断
        filename = filename[:max_length]
    else:
        # 先把从 '「' 开始到结尾的“尾巴”记下来
        tail = filename[bracket_index:]  # '「' 及其后面的所有字符
        tail_len = len(tail)

        if tail_len >= max_length:
            # 如果仅仅 tail 都已经超过 max_length，只能硬截断
            filename = tail[:max_length]
        else:
            # 前面最多可留多少
            front_len = max_length - tail_len
            # 保留开头 front_len 个字符 + tail
            filename = filename[:front_len] + tail
    # 最终若依旧超过 max_length，再做一次硬截断
    if len(filename) > max_length:
        filename = filename[:max_length]

    return filename


def get_aka_name(key_word: str, target: str) -> None:
    """
    从文件名中，获取导演别名

    :param key_word: 本次搜索关键字
    :param target: 信息储存目录
    :return: 无
    """
    # 先将本次关键字储存
    key_word = key_word.replace('"', '')
    Path(os.path.join(target, key_word)).touch()
    # 建立镜像文件夹
    Path(os.path.join(MIRROR_PATH, key_word)).mkdir(parents=True, exist_ok=True)

    # 获取文件列表和别名列表
    aka_names = set()
    file_names = os.listdir(target)
    for name in file_names:
        aka = find_corresponding_name(name, key_word)
        # 除了正则匹配结果，还要求本地不存在名字命名的空文件
        if aka and aka.lower() not in [f.lower() for f in file_names]:
            aka_names.add(aka)

    # 没有别名则返回，有就打印出来
    if not aka_names:
        return
    logger.info("其他可能的名字：")
    for i in aka_names:
        logger.info(i)


def find_corresponding_name(filename: str, given_name: str) -> Optional[str]:
    """
    根据给定的导演名，从文件名中提取对应的另一种语言名称。
    支持括号内多个名称对，名称对以逗号分隔。

    :param filename: 文件名
    :param given_name: 搜索名
    :return: 如果匹配则返回对应的名称，否则返回 None
    """
    # 使用零宽断言，匹配括号内由逗号分隔的多个名称对
    pattern = r'(?<=\(|,)\s*([^,()]+?)\s*｜\s*([^,()]+?)\s*(?=,|\))'
    matches = re.findall(pattern, filename)
    given_name = given_name.replace('"', '')

    for name1, name2 in matches:
        name1 = name1.strip()
        name2 = name2.strip()
        if name1.casefold() == given_name.casefold():
            return name2
        if name2.casefold() == given_name.casefold():
            return name1
    return None


def bulk_insert_ids(r: redis.Redis, set_key: str, ids: list) -> None:
    """
    批量将 ID 插入到 Redis 的 Set 中。

    :param r: redis 客户端
    :param set_key: 保存集合名
    :param ids: 插入 id 列表
    :return: 无
    """
    pipe = r.pipeline()
    for i in ids:
        pipe.sadd(set_key, i)
    pipe.execute()


def bulk_query_ids(r: redis.Redis, set_key: str, ids: list) -> list:
    """
    批量查询给定的 IDs 是否存在于 Redis 的 Set 中。

    :param r: redis 客户端
    :param set_key: 保存集合名
    :param ids: 插入 id 列表
    :return: 返回一个布尔值列表，对应每个 ID 是否存在。
    """
    try:
        # 一次性传入所有要查询的 id
        results = r.smismember(set_key, *ids)
    except redis.exceptions.ResponseError:
        # 如果 SMISMEMBER 命令不可用，使用 pipeline 方式批量执行 SISMEMBER
        pipe = r.pipeline()
        for i in ids:
            pipe.sismember(set_key, i)
        results = pipe.execute()
    return results


@retry(stop_max_attempt_number=5, wait_random_min=30, wait_random_max=300)
def scrapy_magnet(topic_info: dict) -> bool:
    """
    获取帖子内磁力链接。
    topic_info = {'url': 帖子链接, 'name': 文件名}

    :param topic_info: 信息字典
    :return: 成功返回 True
    """
    url = topic_info['url']
    path = topic_info['name']
    logger.debug(f"爬取：{url}")

    r = requests.get(url=url, headers=REQUEST_HEAD, timeout=10, verify=False, allow_redirects=True)
    if r.status_code != 200:
        logger.error(f"链接无法访问: {url}")
        return False

    tree = etree.HTML(r.text)
    dl = tree.xpath('//a[@class="med magnet-link"]')
    dl_alt = tree.xpath('//a[@class="magnet-link"]')
    if not dl or dl_alt:
        logger.error(f"未找到下载链接节点: {url}")
        return False

    link = dl[0].get('href') if dl else dl_alt[0].get('href').strip()
    # 写入文本
    with open(path, "w") as file:
        file.writelines(link)
    return True


def contains_cyrillic(text: str) -> bool:
    """
    判断字符串中是否包含任何西里尔字符 (俄语字符等)

    :param text: 要检查的字符串
    :return: 检查结果，是俄语返回 True
    """
    return bool(re.search(r'[\u0400-\u04FF]', text))


def russian_delete(filename: str) -> str:
    """
    如果文件名前半部分(电影名部分)存在且仅存在一个“｜”分隔符：
      1. 分隔出左右两部分
      2. 判断哪部分是俄语（包含西里尔字符）
      3. 若只有一部分包含西里尔字符，则删除那部分，保留另一部分
    若不满足以上条件则不做处理。

    :param filename: 原始文件名
    :return: 处理后的文件名
    """
    # 1. 找到“电影名部分”与后续信息的分隔位置
    #    寻找最早出现的 '(', '[', '「' 三者之一
    split_chars = ['(', '[', '「']
    # 先假设没有找到，index 设置成长度，表示整个串都属于电影名部分
    cut_index = len(filename)

    for ch in split_chars:
        idx = filename.find(ch)
        if idx != -1 and idx < cut_index:
            cut_index = idx

    # 电影名部分
    movie_part = filename[:cut_index]
    # 剩余部分
    suffix_part = filename[cut_index:]

    # 2. 检查 movie_part 中“｜”的数量
    bar_count = movie_part.count('｜')
    if bar_count == 1:
        # 只有一个｜，则可以认为存在“俄语名｜原名”或“原名｜俄语名”
        left, right = movie_part.split('｜', 1)

        left_cyr = contains_cyrillic(left)
        right_cyr = contains_cyrillic(right)

        # 3. 判断哪侧是俄语名，哪侧是原名
        if left_cyr and not right_cyr:
            # left 是俄语名 -> 删除 left，保留 right
            movie_part = right.strip()
        elif right_cyr and not left_cyr:
            # right 是俄语名 -> 删除 right，保留 left
            movie_part = left.strip()
        else:
            # 两侧都包含或都不包含西里尔字符 -> 不处理
            pass
    else:
        # 0 或多个“｜”，不处理
        pass

    # 4. 拼接回最终文件名
    new_filename = movie_part.strip() + " " + suffix_part
    return new_filename
