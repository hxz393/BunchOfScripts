import datetime
import json
import logging
import os
import re
import shutil

from collections import defaultdict
from multiprocessing import Pool
from typing import List, Union, Optional, Any, Dict, Tuple
from urllib.parse import urlparse

import requests
from lxml import etree
from pymongo import MongoClient
from requests import RequestException
from retrying import retry

from my_module import clean_input
from my_module import read_json_to_dict
from my_module import write_list_to_file

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

# 初始化配置
CONFIG = read_json_to_dict('config/scrapy_bandcamp.json')

USER_COOKIE = CONFIG['scrapy_bandcamp']['user_cookie']  # 帐号 cookie
NEW_ALBUM_TXT = CONFIG['scrapy_bandcamp']['new_album_txt']  # 新专辑来源文本
NEW_ARTIST_TXT = CONFIG['scrapy_bandcamp']['new_artist_txt']  # 新乐队来源文本
MONGO_IP = CONFIG['scrapy_bandcamp']['mongo_ip']  # mongo 数据库地址
MONGO_PORT = CONFIG['scrapy_bandcamp']['mongo_port']  # mongo 数据库端口
REQUEST_HEAD = CONFIG['scrapy_bandcamp']['request_head']  # 请求标头，不含帐号 cookie
LOGIN_URL = CONFIG['scrapy_bandcamp']['login_url']  # 登录地址
UNSUPPORTED_STR = CONFIG['scrapy_bandcamp']['unsupported_str']  # 非法字符串
POOL_NUMBER = CONFIG['scrapy_bandcamp']['pool_number']  # 线程池数量
PROXIES = CONFIG['scrapy_bandcamp']['proxies']  # 本地代理服务器
FULL_PAGE_STYLE = CONFIG['scrapy_bandcamp']['full_page_style']  # 全信息页面风格
SINGLE_PAGE_STYLE = CONFIG['scrapy_bandcamp']['single_page_style']  # 单页信息页面风格

## 处理各种风格名称配置
NAME_STYLE_VA = CONFIG['scrapy_bandcamp']['name_style_va']
NAME_STYLE_0 = CONFIG['scrapy_bandcamp']['name_style_0']  # 艺术家 - 专辑 or 艺术家 ~ 专辑 or 艺术家 – 专辑
NAME_STYLE_1 = CONFIG['scrapy_bandcamp']['name_style_1']  # 标签 艺术家 - 专辑
NAME_STYLE_2 = CONFIG['scrapy_bandcamp']['name_style_2']  # 标签 - 艺术家 - 专辑
NAME_STYLE_3 = CONFIG['scrapy_bandcamp']['name_style_3']  # 艺术家 "专辑"
NAME_STYLE_5 = CONFIG['scrapy_bandcamp']['name_style_5']  # 标签 - 艺术家 "专辑"

MONGO_CLIENT = MongoClient(host=MONGO_IP, port=MONGO_PORT)  # 数据库客户端
BAND_INFO = MONGO_CLIENT.bandcamp.Bandinfo  # 乐队信息
ALBUM_INFO = MONGO_CLIENT.bandcamp.Albuminfo  # 专辑信息
REF_TOKEN = re.findall(r'%5B%22(.+?)%22%2C', USER_COOKIE)[0]  # 参考认证
REQUEST_HEAD["Cookie"] = USER_COOKIE  # 请求标头，更新帐号 cookie


def prune_link(line: str) -> Optional[str]:
    """
    处理输入的链接，将 http:// 替换为 https://， 并移除不需要的 URL 段。

    :param line: 需要处理的链接。
    :type line: str
    :rtype: Optional[str]
    :return: 处理后的链接。
    """
    try:
        return f"https://{urlparse(line).netloc}" if line else None
    except Exception:
        logger.exception(f"处理链接出现错误: {line}")
        return None


def init_txt(path: Union[str, os.PathLike], album_type: int = 0) -> Optional[List[str]]:
    """
    初始化文本文件，对其中的链接进行处理，并排序和去重。

    :type path: Union[str, os.PathLike]
    :param path: 需要处理的文本文件的路径，可以是字符串或 os.PathLike 对象。
    :type album_type: int
    :param album_type: 决定链接处理方式的参数，如果为1，则替换'http://'为'https://'并移除'?from=fannewrel'；否则，使用 prune_link 函数进行处理。默认值为0。
    :rtype: Optional[List[str]]
    :return: 返回处理后的链接列表，如果发生错误则返回 None。
    """
    try:
        with open(path) as file:
            content_raw = file.read()
            content = clean_input(content_raw)

        if album_type == 1:
            content_list = [re.sub(r"\?from=fannewrel", "", re.sub(r"^http://", "https://", i)) for i in content.split("\n")]
        else:
            content_list = [prune_link(i).strip() for i in content.split("\n") if i]

        return_list = sorted(set(content_list))

        backup_path = os.path.normpath(path + '_bak')
        if not os.path.exists(backup_path):
            os.rename(path, backup_path)

        write_list_to_file(path, return_list)

        return return_list
    except Exception:
        logger.exception(f"初始化文本文件时出现错误: {path}")
        return None


@retry(stop_max_attempt_number=3, wait_random_min=100, wait_random_max=1200)
def request_get(url: str, headers: Dict[str, str]) -> Optional[requests.Response]:
    """
    向指定 URL 发起 GET 请求，并返回 response 对象。

    :param url: 需要请求的 URL。
    :type url: str
    :param headers: 请求头信息。
    :type headers: Dict[str, str]
    :rtype: Optional[requests.Response]
    :return: 请求成功返回 response 对象，失败返回 None。
    :raise RequestException: 在发起请求时可能出现的异常。
    """
    try:
        response = requests.get(url=url, headers=headers, timeout=15, verify=False, allow_redirects=False, proxies=PROXIES)
        # response = requests.get(url=url, headers=headers, timeout=15, verify=False, allow_redirects=False)
        # print(url)
        # print(response.status_code)
        if response.status_code in [301, 302, 303]:
            redirect_url = response.headers['Location']
            logger.info(f'请求发生跳转：{redirect_url}')
            # print(response.text)
            if response.headers["location"].startswith('https://bandcamp.com/signup?new_domain'):
                return response
            else:
                headers['Host'] = urlparse(redirect_url).netloc
                headers['Origin'] = redirect_url
                headers['Referer'] = redirect_url
                return request_get(redirect_url, headers)
        return response
    except RequestException:
        logger.exception(f"发送 GET 请求时出现错误: {url}")
        raise


@retry(stop_max_attempt_number=3, wait_random_min=100, wait_random_max=1200)
def request_post(url: str, headers: Dict[str, str], data: Union[str, Dict[str, str]]) -> Optional[requests.Response]:
    """
    向指定 URL 发起 POST 请求，并返回 response 对象。

    :param url: 需要请求的 URL。
    :type url: str
    :param headers: 请求头信息。
    :type headers: Dict[str, str]
    :param data: 需要 post 的数据，可以是字符串或字典类型。
    :type data: Union[str, Dict[str, str]]
    :rtype: Optional[requests.Response]
    :return: 请求成功返回 response 对象，失败返回 None。
    :raise RequestException: 在发起请求时可能出现的异常。
    """
    try:
        response = requests.post(url=url, headers=headers, data=data, timeout=15, verify=False, allow_redirects=True, proxies=PROXIES)
        return response
    except RequestException:
        logger.exception(f"发送 POST 请求时出现错误: {url}")
        raise


def post_action(output_dict: Dict[str, Union[str, List[str]]]) -> Optional[bool]:
    """
    执行收尾动作，包括处理专辑并更新内容。

    :type output_dict: Dict[str, Union[str, List[str]]]
    :param output_dict: 包含需要处理的信息的字典，格式为：{'路径': NEW_ALBUM_TXT, '链接': album_url, '专辑': [album_url1, album_url2]}
    :rtype: Optional[bool]
    :return: 成功返回 True，否则返回 None。
    """

    if not output_dict:
        return None

    try:
        path = output_dict['路径']
        url = output_dict['链接']
        albums = output_dict['专辑']
        target_path = os.path.join(os.path.dirname(path), os.path.splitext(os.path.basename(path))[0] + "_output.txt")
        if albums:
            with open(target_path, "a") as file:
                file.writelines(album + '\n' for album in albums)

        with open(path, "r") as file:
            new_content = [line for line in file if not line.startswith(url)]

        with open(path, "w") as file:
            file.writelines(new_content)

        return True
    except Exception:
        logger.exception(f"修改输出文本时出现错误: {output_dict}，错误信息：")
        return False


def collecting_new_album(album_url: str) -> dict[str, Any] | None:
    """
    收集新专辑信息。

    :type album_url: str
    :param album_url: 新专辑的 url。
    :rtype: dict[str, Any]
    :return: 返回处理完成的字典，失败时返回 None。
    """
    try:
        logger.info(f'开始查询：{album_url}')
        index_url = re.findall(r'https://[^/]+', album_url)[0]
        request_head_album = REQUEST_HEAD.copy()
        request_head_album['Host'] = urlparse(index_url).netloc
        request_head_album['Referer'] = index_url

        response = requests.get(url=album_url, headers=request_head_album, timeout=15, verify=False, allow_redirects=True)
        response_tree = etree.HTML(response.text)

        date_time = datetime.datetime.now()
        page_data = json.loads(response_tree.xpath('//*[@id="pagedata"]/@data-blob')[0])
        label = response_tree.xpath('//head/meta[@property="og:site_name"]/@content')[0]
        artist_name = response_tree.xpath('///*[@id="name-section"]/h3/span/a/text()')[0]
        album_url = response_tree.xpath('//head/meta[@property="og:url"]/@content')[0]
        album_name = response_tree.xpath('//head/meta[@name="title"]/@content')[0].split(', by')[0]
        album_id = int(re.findall(r'item_id=(\d+)', page_data['lo_querystr'])[0])

        db_data = {'Label': label, 'AlbumID': album_id, 'AlbumName': album_name, 'AlbumURL': album_url, 'BandName': artist_name, 'Datetime': date_time}
        key_id = {'AlbumURL': album_url}
        ALBUM_INFO.update_one(key_id, {'$set': db_data}, True)

        logger.info(f'记录完成：{album_url}')
        return {'路径': NEW_ALBUM_TXT, '链接': album_url, '专辑': [album_url]}
    except Exception:
        logger.exception(f"新专辑链接 {album_url} 处理失败：")
        return None


def recording_new_album() -> None:
    """
    记录新专辑。

    :rtype: None
    :return: None。
    """
    try:
        album_url_list = init_txt(NEW_ALBUM_TXT, album_type=1)
        pool = Pool(processes=POOL_NUMBER)
        for album_url in album_url_list:
            if ALBUM_INFO.find_one({'AlbumURL': album_url}):
                post_action({'路径': NEW_ALBUM_TXT, '链接': album_url, '专辑': [album_url]})
            else:
                pool.apply_async(collecting_new_album, args=(album_url,), callback=post_action)
        pool.close()
        pool.join()
    except Exception:
        logger.exception("记录新专辑时发生错误：")


def get_album_list(artist_url: str, retry_count: int = 0) -> Optional[List[str]]:
    """
    获取网页中专辑列表。

    :type artist_url: str
    :param artist_url: 需要请求专辑列表的网页 URL。
    :type retry_count: int
    :param retry_count: 重试次数，默认为0。
    :rtype: Optional[List[str]]
    :return: 一个列表，包含所有专辑的 URL，或者在发生错误时返回 None。
    """
    try:
        index_url = re.findall(r'https://[^/]+', artist_url)[0]
        request_head_music = REQUEST_HEAD.copy()
        request_head_music['Host'] = urlparse(index_url).netloc
        request_head_music['Origin'] = index_url
        request_head_music['Referer'] = index_url

        # response = request_get(artist_url, request_head_music)
        response = request_get(f'{index_url}/music', request_head_music)

        response_tree = etree.HTML(response.text)
        page_style_match = response_tree.xpath('/html/body/@class')
        page_style = page_style_match[0] if page_style_match else None

        if page_style in FULL_PAGE_STYLE:
            label_name_match = response_tree.xpath('//*[@id="band-name-location"]/span[1]/text()') or \
                               response_tree.xpath('//head/meta[@property="og:title"]/@content')
            label_name = label_name_match[0] if label_name_match else None

            album_ids_match = response_tree.xpath('//*[@id="pgBd"]/div[@class="leftMiddleColumns"]/ol[@id="music-grid"]/li/@data-item-id')
            album_ids = [int(album_id.split('-')[-1]) for album_id in album_ids_match] if album_ids_match else None

            album_urls_match = response_tree.xpath('//*[@id="pgBd"]/div[@class="leftMiddleColumns"]/ol[@id="music-grid"]/li/a/@href') or \
                               response_tree.xpath('//*[@id="pgBd"]/div[@class="leftMiddleColumns"]//span[@class="indexpage_list_row"]//div[@class="ipCellLabel1"]/a/@href')
            album_urls = [index_url + part for part in album_urls_match] if album_urls_match else None

            album_names_match = response_tree.xpath('//*[@id="pgBd"]/div[@class="leftMiddleColumns"]/ol[@id="music-grid"]/li/a/p[@class="title"][1]/text()') or \
                                response_tree.xpath('//*[@id="pgBd"]/div[@class="leftMiddleColumns"]//span[@class="indexpage_list_row"]//div[@class="ipCellLabel1"]/a/text()')
            album_names = [name.strip() for name in album_names_match if name.strip()] if album_names_match else None
            # print([label_name, album_ids, album_urls, album_names])

            p_elements = response_tree.xpath('//*[@id="pgBd"]/div[@class="leftMiddleColumns"]/ol[@id="music-grid"]/li/a/p[@class="title"][1]') or \
                         response_tree.xpath('//*[@id="pgBd"]/div[@class="leftMiddleColumns"]//span[@class="indexpage_list_row"]//div[@class="ipCellLabel"]')
            artist_names = [p.xpath('string(./span)').strip() if p.xpath('./span/text()') else label_name for p in p_elements] if p_elements else None

            if not all([label_name, album_urls, album_names, artist_names]):
                # print([label_name, album_urls, album_names, artist_names])
                if retry_count == 0:
                    return get_album_list(f'{index_url}', retry_count + 1)
                elif retry_count == 1:
                    return get_album_list(f'{index_url}/merch', retry_count + 1)
                else:
                    logger.error(f'获取网页中专辑列表尝试全部失败：{index_url}')
                    return

            if not len(album_urls) == len(album_names) == len(artist_names):
                logger.error(f'获取网页中专辑信息没对齐：{index_url}，数据：{[label_name, album_ids, album_urls, album_names, artist_names]}')
                return

            for i in range(len(album_urls)):
                db_data = {'Label': label_name, 'AlbumID': album_ids[i] if album_ids else 0, 'AlbumName': album_names[i], 'AlbumURL': album_urls[i], 'Datetime': datetime.datetime.now(), 'BandName': artist_names[i]}
                key_id = {'AlbumURL': album_urls[i]}
                ALBUM_INFO.update_one(key_id, {'$set': db_data}, True)

            logger.info(f'获取专辑完成：{artist_url}')
            return album_urls
        elif page_style in SINGLE_PAGE_STYLE:
            data_dict_match = response_tree.xpath('//*[@id="pagedata"]/@data-blob')
            data_dict = json.loads(data_dict_match[0]) if data_dict_match else None

            label_name_match = response_tree.xpath('//*[@id="band-name-location"]/span[@class="title"]/text()')
            label_name = label_name_match[0] if label_name_match else None

            artist_name_match = response_tree.xpath('//*[@id="name-section"]/h3/span/a/text()')
            artist_name = artist_name_match[0] if artist_name_match else None

            album_url_match = response_tree.xpath('//head/meta[@property="og:url"]/@content')
            album_url = album_url_match[0] if album_url_match else None

            album_name_match = response_tree.xpath('//head/meta[@name="title"]/@content')
            album_name = album_name_match[0].split(', by')[0] if album_name_match else None

            album_id_match = re.findall(r'item_id=(\d+)', data_dict['lo_querystr']) if data_dict else None
            album_id = int(album_id_match[0]) if album_id_match else None

            if not all([label_name, album_id, album_url, album_name]):
                if retry_count == 0:
                    return get_album_list(f'{index_url}', retry_count + 1)
                elif retry_count == 1:
                    return get_album_list(f'{index_url}/merch', retry_count + 1)
                else:
                    logger.error(f'获取网页中专辑列表尝试全部失败：{index_url}')
                    return

            db_data = {'Label': label_name, 'AlbumID': album_id, 'AlbumName': album_name, 'AlbumURL': album_url, 'BandName': artist_name, 'Datetime': datetime.datetime.now()}
            key_id = {'AlbumURL': album_url}
            ALBUM_INFO.update_one(key_id, {'$set': db_data}, True)

            logger.info(f'获取专辑完成：{artist_url}')
            return [album_url]
        else:
            if retry_count == 0:
                return get_album_list(f'{index_url}', retry_count + 1)
            elif retry_count == 1:
                return get_album_list(f'{index_url}/merch', retry_count + 1)
            else:
                logger.warning(f'**************** 没有解析记录：{artist_url}，样式为：{page_style} ****************')
                return

    except Exception:
        logger.exception(f"获取专辑列表发生错误：{artist_url}，错误信息：")
        return


def get_follow_info(artist_url: str, retry_count: int = 0) -> Optional[Tuple[Union[str, None], Union[requests.Response, None]]]:
    """
    获取艺术家的订阅信息

    :type artist_url: str
    :param artist_url: 艺术家的 URL。

    :type retry_count: int
    :param retry_count: 已重试的次数。

    :rtype: Optional[Tuple[Union[str, None], Union[requests.Response, None]]]
    :return: 返回一个元组，其中包含是否订阅的信息以及响应。
    """

    try:
        index_url = re.findall(r'https://[^/]+', artist_url)[0]
        request_head_artist = REQUEST_HEAD.copy()
        request_head_artist['Host'] = urlparse(artist_url).netloc

        response = request_get(artist_url, request_head_artist)

        response_txt = response.text
        # print(response_txt)
        is_follow_match = re.findall(r'is_following&quot;:(\w+)', response_txt)

        if response.status_code == 303 and not is_follow_match:
            if response.headers["location"].startswith('https://bandcamp.com/signup?new_domain'):
                is_follow = 'deny'
                return is_follow, response

        if not is_follow_match:
            if retry_count == 0:
                return get_follow_info(f'{index_url}/music', retry_count + 1)
            elif retry_count == 1:
                return get_follow_info(f'{index_url}/merch', retry_count + 1)
            else:
                # print(artist_url)
                # print(response_txt)
                return None, response
        else:
            is_follow = is_follow_match[0]
            return is_follow, response

    except Exception:
        logger.exception(f"获取订阅信息时发生意外：{artist_url}，错误信息：")
        return


def follow_band(artist_url: str, follow_post_data: Dict[str, Any]) -> Optional[bool]:
    """
    关注的乐队主页。

    :type artist_url: str
    :param artist_url: 乐队主页网址。
    :type follow_post_data: Dict[str, Any]
    :param follow_post_data: 关注发送数据
    :rtype: Optional[bool]
    :return: 成功返回 True，否则 None
    """
    try:
        index_url = re.findall(r'https://[^/]+', artist_url)[0]
        request_head_follow = REQUEST_HEAD.copy()
        request_head_follow['Host'] = urlparse(index_url).netloc
        request_head_follow['Origin'] = index_url
        request_head_follow['Referer'] = index_url
        follow_url = f'{index_url}/fan_follow_band_cb'

        response = request_post(follow_url, request_head_follow, follow_post_data)

        return True if response.json()['ok'] else None

    except Exception:
        logger.exception(f"关注乐队时发生错误：{artist_url}，错误信息：")
        return


def collecting_new_artist(artist_url: str) -> Optional[Dict[str, Union[str, List[str]]]]:
    """
    收集新的艺术家信息。

    :type artist_url: str
    :param artist_url: 艺术家的 URL。

    :rtype: Optional[Dict[str, Union[str, List[str]]]]
    :return: 返回一个字典，包含艺术家的相关信息，或者在发生错误时返回 None。
    """
    try:
        # album_list = get_album_list(artist_url)
        # print(album_list)
        # return
        return_dict = {'路径': NEW_ARTIST_TXT, '链接': artist_url, '专辑': ['']}
        if BAND_INFO.find_one({'ArtistURL': artist_url}):
            logger.info(f'数据库已有记录，跳过：{artist_url}')
            return return_dict

        is_follow, response = get_follow_info(artist_url)
        if is_follow == 'deny':
            logger.info(f'乐队已注销：{artist_url}')
            return return_dict

        response_txt = response.text
        # print(response_txt)
        artist_match = re.findall(r'artist:\s"([^"]+)"', response_txt) or \
                       re.findall(r'<meta\sproperty="og:site_name"\scontent="([^"]+)">', response_txt) or \
                       re.findall(r'com","name":"([^"]+)","', response_txt) or \
                       re.findall(r'data-band="{&quot;id&quot;:\d+,&quot;name&quot;:&quot;([^"]+)&quot;}"', response_txt) or \
                       re.findall(r'<title>Artists\s\|\s([^"]+)</title>', response_txt)
        artist = artist_match[0] if artist_match else None
        artist_id_match = re.findall(r'"band_id":(\d+)', response_txt) or \
                          re.findall(r'&amp;band_id=(\d+)', response_txt) or \
                          re.findall(r'data-band="{&quot;id&quot;:(\d+)', response_txt)
        artist_id = artist_id_match[0] if artist_id_match else None
        follow_crumb_match = re.findall(r'fan_follow_band_cb&quot;:&quot;(\|fan_follow_band_cb\|\d+\|[^&]+)&', response_txt)
        follow_crumb = follow_crumb_match[0] if follow_crumb_match else None
        follow_post_data = {
            'fan_id': '1568548',
            'action': 'follow',
            'ref_token': REF_TOKEN,
            'band_id': artist_id,
            'crumb': follow_crumb
        }
        if not all([artist, artist_id, follow_crumb]):
            # print(response_txt)
            logger.error(f'正则匹配失败：{artist_url}，缺少关键词：{[artist, artist_id, follow_crumb]}')
            return

        if is_follow == 'true':
            logger.info(f'已经关注：{artist_url}')
        elif not is_follow or is_follow == 'false':
            follow_result = follow_band(artist_url, follow_post_data)
            if not follow_result:
                logger.warning(f'关注失败：{artist_url}')
            else:
                logger.info(f'关注乐队完成：{artist_url}')
        else:
            logger.warning(f'出现异常：{artist_url}，关注信息：{is_follow}')

        album_list = get_album_list(artist_url)
        if not album_list:
            logger.warning(f'获取专辑列表失败，终止：{artist_url}')
            return

        return_dict['专辑'] = album_list
        key_id = {'ArtistID': int(artist_id)}
        db_data = {'ArtistName': artist, 'ArtistID': int(artist_id), 'ArtistURL': artist_url, 'Datetime': datetime.datetime.now()}
        BAND_INFO.update_one(key_id, {'$set': db_data}, True)
        logger.info(f'收集乐队完成：{artist_url}')
        return return_dict
    except Exception:
        logger.exception(f"收集乐队专辑时发生错误：{artist_url}，错误信息：")
        return


def recording_new_artist() -> None:
    """
    抓取新艺术家。

    :rtype: None
    :return: 无返回值。
    """
    try:
        artist_url_list = init_txt(NEW_ARTIST_TXT)
        with Pool(processes=POOL_NUMBER) as pool:
            for artist_url in artist_url_list:
                if artist_url:
                    pool.apply_async(collecting_new_artist, args=(artist_url,), callback=post_action)
            pool.close()
            pool.join()
    except Exception:
        logger.exception('记录新乐队时发生错误：')


# noinspection PyTypeChecker
def get_file_info(source_dir: str) -> Optional[List[dict]]:
    """
    解析文件名中包含信息，按艺术家和专辑分类。

    :param source_dir: 源目录的路径。
    :type source_dir: str
    :rtype: Optional[List[dict]]
    :return: 包含文件信息的字典列表，或者在发生错误时返回 None。
    """
    try:
        file_list = os.listdir(source_dir)
        temp_dict = defaultdict(lambda: {'file_artist': None, 'file_album': None, 'files': []})

        for file_name in file_list:
            parts = file_name.split(' $$ ')
            if len(parts) < 3:
                logger.warning(f"分割文件名字段时失败，“$$” 少于 3：{file_name}")
                continue

            file_artist = parts[0]
            file_album = parts[1]
            key = (file_artist, file_album)

            file_album = re.sub(" \\(\\d{4}\\)$| \\(_date_\\)$", '', file_album)

            temp_dict[key]['file_artist'] = file_artist
            temp_dict[key]['file_album'] = file_album
            temp_dict[key]['files'].append(os.path.join(source_dir, file_name))

        return list(temp_dict.values())
    except Exception:
        logger.exception(f"解析文件名中信息时出现错误: {source_dir}")
        return None


def search_mongo(file_info: Dict[str, str]) -> Optional[Dict[str, str]]:
    """
    在MongoDB中搜索专辑信息。

    :type file_info: Dict[str, str]
    :param file_info: 包含艺术家名、专辑名和文件列表的字典。
    :rtype: Optional[Dict[str, str]]
    :return: 如果搜索成功，则返回一个字典，其中包括MongoDB中找到的专辑信息和文件列表；否则返回None。
    """
    try:
        # 转义所有特殊字符，然后将转义的点号替换为正常的点号以匹配任意字符
        artist_name_pattern = re.compile(re.escape(file_info['file_artist'].replace("_", ".").replace("   ", " . ")).replace("\\.", "."), re.IGNORECASE)
        album_name_pattern = re.compile(re.escape(file_info['file_album'].replace("_", ".").replace("   ", " . ")).replace("\\.", "."), re.IGNORECASE)

        queries = [
            {
                'query': {
                    '$and': [
                        {'BandName': artist_name_pattern},
                        {'AlbumName': album_name_pattern}
                    ]
                },
                'label': 'normal'
            },
            {
                'query': {
                    '$and': [
                        {'Label': artist_name_pattern},
                        {'AlbumName': album_name_pattern}
                    ]
                },
                'label': 'abnormal'
            },
            # {
            #     'query': {'AlbumName': album_name_pattern},
            #     'label': 'only-album'
            # }
        ]

        for query_info in queries:
            logger.debug(f'查询条件：{query_info}')
            query = query_info['query']
            count = ALBUM_INFO.count_documents(query)

            if count > 0:
                mongo_result = ALBUM_INFO.find_one(query)
                mongo_result['files'] = file_info['files']
                return mongo_result

        album_count = ALBUM_INFO.count_documents({"AlbumName": {"$regex": album_name_pattern, "$options": "i"}})
        if album_count == 1:
            mongo_result = ALBUM_INFO.find_one({"AlbumName": {"$regex": album_name_pattern, "$options": "i"}})
            mongo_result['BandName'] = file_info['file_artist']
            return mongo_result

        logger.warning(f"在 mongo 中没搜索到数据：{file_info}")
        return None

    except Exception:
        logger.exception(f"在 Mongo 中搜索专辑信息时出现错误: {file_info}，错误信息：")
        return None


def get_target_name(mongo_result: Dict[str, Union[str, list]]) -> Optional[str]:
    """
    根据 MongoDB 结果获取目标名称。

    :type mongo_result: Dict[str, Union[str, list]]
    :param mongo_result: MongoDB 的查询结果。
    :rtype: Optional[str]
    :return: 艺术家名称或标签名称，如果匹配错误，则返回 None。
    """
    try:
        artist_name = mongo_result['BandName']
        album_name = mongo_result['AlbumName']
        label_name = mongo_result['Label']

        # 艺术家 - 专辑名
        if label_name in NAME_STYLE_0:
            album_name = album_name.replace('–', '-')
            album_name = album_name.replace('~', '-')
            artist_name = album_name.split(' - ')[0].strip()
        # 标签 艺术家 - 专辑名
        elif label_name in NAME_STYLE_1:
            artist_name = re.sub(r'^\S+\s([^-]+)\s-\s.+', r'\1', album_name)
        elif label_name in NAME_STYLE_2:
            artist_name = re.sub(r'^\S+\s-\s([^-]+)\s-\s.+', r'\1', album_name)
        elif label_name in NAME_STYLE_3:
            artist_name = album_name.split(' "')[0].strip()
        elif label_name in NAME_STYLE_5:
            artist_name = re.sub(r'^[^-]+\s-\s([^"]+)\s"[^"]+".+', r'\1', album_name)

        return label_name if artist_name.lower() in NAME_STYLE_VA else artist_name
    except Exception:
        logger.exception(f"匹配艺术家名时出现错误: {mongo_result}，错误信息：")
        return None


def move_file(target_name: str, target_dir: str, file_info: Dict[str, Union[str, list]]) -> bool:
    """
    将文件移动到目标目录，并将目标目录名中在 Windows 上不合法的字符替换掉。

    :type target_name: str
    :param target_name: 目标原始文件名。
    :type target_dir: str
    :param target_dir: 目标目录。
    :type file_info: Dict[str, Union[str, list]]
    :param file_info: 包含源文件路径的信息。
    :rtype: bool
    :return: 如果移动成功返回 True，否则返回 False。
    """
    try:
        illegal_characters = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
        replace_mapping = ['&', '-', '-', '-', '', '', '_', '_', '&']
        for char, replacement in zip(illegal_characters, replace_mapping):
            target_name = target_name.replace(char, replacement)

        target_name = re.sub(UNSUPPORTED_STR, "_", target_name)
        target_name = re.sub("\\.+$", "", target_name).strip()

        target_path = os.path.join(target_dir, target_name)
        os.makedirs(target_path, exist_ok=True)

        for source_path in file_info['files']:
            shutil.move(source_path, target_path)
            logger.debug(f"文件移动完成：{source_path} 到 {target_path}")

        return True
    except Exception:
        logger.exception(f"移动文件时出现错误: {target_name}，错误信息：")
        return False


def process_file_info(file_info: Dict[str, Union[str, int]], target_dir: str) -> Optional[bool]:
    """
    处理文件流程，包括搜索mongo数据库，获取目标名，移动文件。

    :type file_info: Dict[str, Union[str, int]]
    :param file_info: 包括文件的信息
    :type target_dir: str
    :param target_dir: 目标目录
    :rtype: Optional[bool]
    :return: 如果处理成功返回 True，否则返回 None。
    """
    try:
        logger.debug(f"处理文件信息：{file_info}")
        if not file_info:
            return

        mongo_result = search_mongo(file_info)
        logger.debug(f"mongo 数据库返回结果：{mongo_result}")
        if not mongo_result:
            return

        target_name = get_target_name(mongo_result)
        logger.debug(f"匹配艺术家名返回结果：{target_name}")
        if not target_name:
            return

        move_result = move_file(target_name, target_dir, file_info)
        if move_result:
            logger.info(f"专辑 {file_info['file_artist']} - {file_info['file_album']} 处理完成。")
            return True
    except Exception:
        logger.exception("处理文件流程发生错误：")
        return None


def sort_bandcamp_files(source_dir: str, target_dir: str) -> Optional[bool]:
    """
    整理 Bandcamp 的文件。

    :type source_dir: str
    :param source_dir: 源目录的路径。
    :type target_dir: str
    :param target_dir: 目标目录的路径。
    :rtype: Optional[bool]
    :return: 如果整理成功返回 True，否则返回 None。
    """
    try:
        file_info_lists = get_file_info(source_dir)
        with Pool(processes=POOL_NUMBER) as pool:
            for file_info in file_info_lists:
                pool.apply_async(process_file_info, args=(file_info, target_dir))
            pool.close()
            pool.join()

        return True
    except Exception:
        logger.exception("整理文件时发生错误：")
        return None
