"""
将种子或磁力链接添加到服务器 qBittorrent 程序下载

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import logging
import os

import requests
from retrying import retry

from my_module import read_json_to_dict, read_file_to_list
from sort_movie_ops import select_yts_best_torrent

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

CONFIG = read_json_to_dict('config/add_to_qb.json')  # 配置文件

QB_URL = CONFIG['qb_url']  # qb 地址
QB_USER = CONFIG['qb_user']  # qb 用户
QB_PASS = CONFIG['qb_pass']  # qb 密码
QB_SAVE_DIR = CONFIG['qb_save_dir']  # qb 保存目录
MAGNET_PATH = CONFIG['magnet_path']  # 输出目录


def add_to_qb(source: str) -> None:
    """
    获取 JSON 或 LOG 文件中的下载链接，添加到 qb

    :param source: 来源目录
    :return: 无
    """
    # 先登录 QB
    session = requests.Session()
    if not qb_login(session):
        return

    # 遍历文件夹
    done = 0
    for root, dirs, files in os.walk(source):
        # 取当前文件夹的名称作为 director
        director = os.path.basename(root)
        # 子文件夹内只有文件
        for file_name in files:
            # 拼接出完整路径
            file_path = os.path.join(root, file_name)
            # 去掉扩展名，得到文件名字段。处理过长文件名
            file_name_no_ext = os.path.splitext(file_name)[0]
            if len(file_name_no_ext) > 111:
                file_name_no_ext = file_name_no_ext[:111]
            # json 文件来自 ytf
            if file_name.endswith('.json'):
                # 读取 json 文件，获取下载链接
                dl_link = select_yts_best_torrent(read_json_to_dict(file_path))
                # 添加到 qb
                add_magnet_link(session, dl_link, save_path=os.path.join(QB_SAVE_DIR, director, file_name_no_ext).replace("\\", "/"), tags=director, category='ytf')
                done += 1
            # log 文件来自 ru
            elif file_name.endswith('.log'):
                # 读取 log 文件，获取下载链接
                dl_link = read_file_to_list(file_path)[0]
                dl_link = dl_link.replace('\ufeff', '')
                add_magnet_link(session, dl_link, save_path=os.path.join(QB_SAVE_DIR, director, file_name_no_ext).replace("\\", "/"), tags=director, category='ru')
                done += 1
            # 添加种子文件，已经不常用了
            elif file_name.endswith('.torrent'):
                # 添加种子文件
                add_torrent_file(session, file_path, save_path=os.path.join(QB_SAVE_DIR, director, file_name_no_ext).replace("\\", "/"), tags=director, category='ru')
                done += 1
    logger.info(f"共添加 {done} 个任务。")


def qb_login(session: requests.Session) -> bool:
    """
    使用给定的 requests.Session 进行登录。

    :param session: 会话
    :return: 返回登录成功或失败的状态
    """
    login_endpoint = f"{QB_URL}/api/v2/auth/login"
    data = {
        "username": QB_USER,
        "password": QB_PASS
    }
    response = session.post(login_endpoint, data=data)
    # 登录成功时，返回内容通常为 "Ok."
    if response.text == "Ok.":
        logger.info("qBittorrent: 登录成功。")
        return True
    else:
        logger.error(f"qBittorrent: 登录失败，响应内容: {response.text}")
        return False


@retry(stop_max_attempt_number=5, wait_random_min=100, wait_random_max=1200)
def add_magnet_link(session: requests.Session, magnet_link: str, save_path: str = None, tags: str = None, category: str = None) -> None:
    """
    添加磁力链接到 qB，指定可选保存路径。

    :param session: 已登录的 requests.Session
    :param magnet_link: 磁力链接
    :param save_path: 保存目录，默认为 None（由 qb 默认设置）
    :param tags: 任务标签
    :param category: 任务类别

    :return: 无
    """
    add_endpoint = f"{QB_URL}/api/v2/torrents/add"

    data = {
        'urls': f"{magnet_link}",
        'category': 'ytf',
        'tags': tags
    }
    if save_path:
        data['savepath'] = save_path
        data['category'] = category
        data['tags'] = tags

    r = session.post(add_endpoint, data=data)
    if r.status_code == 200:
        if r.text == "Ok.":
            logger.info(f"已添加磁力链接 {tags}: {magnet_link}")
        else:
            logger.error(f"添加磁力链接失败 {tags}: {magnet_link}, status={r.status_code}, resp={r.text}")
    else:
        logger.error(f"添加磁力链接失败 {tags}: {magnet_link}, status={r.status_code}, resp={r.text}")


def add_torrent_file(session: requests.Session, torrent_path: str, save_path: str = None, tags: str = None, category: str = None) -> None:
    """
    添加本地 .torrent 文件到 qBittorrent，指定可选保存路径。

    :param session: 已登录的 requests.Session
    :param torrent_path: 本地种子文件的绝对路径
    :param save_path: 保存目录，默认为 None（由 qb 默认设置）
    :param tags: 任务标签
    :param category: 任务类别

    :return: 无
    """
    add_endpoint = f"{QB_URL}/api/v2/torrents/add"

    # 以 multipart/form-data 格式上传种子
    files = {'torrents': open(torrent_path, 'rb')}
    data = {}
    if save_path:
        data['savepath'] = save_path
        data['category'] = category
        data['tags'] = tags

    r = session.post(add_endpoint, files=files, data=data)
    if r.status_code == 200:
        if r.text == "Ok.":
            logger.info(f"已添加种子文件 {tags}: {torrent_path}")
        else:
            logger.error(f"添加种子文件失败: {tags}, status={r.status_code}, resp={r.text}")
    else:
        logger.error(f"添加种子文件失败: {tags}, status={r.status_code}, resp={r.text}")
