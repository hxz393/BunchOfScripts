"""
启动脚本

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import io
import logging
import sys
import time

from my_module import logging_config

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

logger = logging.getLogger(__name__)

time_now = time.strftime('%Y%m%d%H%M')
logging_config(console_output=True, max_log_size=50, log_file=f"B:/2.脚本/logs-{time_now}.log", default_log_format="%(message)s")


def main(chosen: int) -> None:
    """功能列表

    :param chosen: 选择功能
    :return: 无
    """
    match chosen:
        case 101:
            logger.info("破解网站图片防盗链。启动后通过本地代理端口 10808 访问")
            logger.info("=" * 255)
            from scrapy_pic_1 import scrapy_pic_1
            scrapy_pic_1()
            logger.info("=" * 255)
        case 201:
            logger.info("某游戏网站百度网盘下载地址自动获取")
            logger.info("=" * 255)
            from scrapy_game_1 import scrapy_game_1
            scrapy_game_1()
            logger.info("=" * 255)
        case 301:
            logger.info("BANDCAMP.COM 新专辑处理。链接来源 B:/2.脚本/new_album.txt")
            logger.info("=" * 255)
            from scrapy_bandcamp import recording_new_album
            recording_new_album()
            logger.info("=" * 255)
        case 302:
            logger.info("BANDCAMP.COM 新乐队处理。链接来源 B:/2.脚本/new_artist.txt")
            logger.info("=" * 255)
            from scrapy_bandcamp import recording_new_artist
            recording_new_artist()
            logger.info("=" * 255)
        case 303:
            logger.info("BANDCAMP.COM 下载的音频处理。链接来源 B:/2.脚本/new_artist.txt")
            logger.info("=" * 255)
            from scrapy_bandcamp import sort_bandcamp_files
            source_path = r'B:\0.整理\jd\rss'
            target_path = r'B:\2.脚本'
            sort_bandcamp_files(source_path, target_path)
            logger.info("=" * 255)
        case 401:
            logger.info(r"自动整理已解压下载音乐。成功移动到目标目录，失败移动到 B:\0.整理\手动整理")
            logger.info(r"手动处理完 B:\0.整理\手动整理 下的文件夹，再进行下一步")
            logger.info("=" * 255)
            from sort_discogs import sort_discogs
            source_path = r'B:\0.整理\jd\un'
            target_path = r'B:\2.脚本'
            no_query = False
            sort_discogs(source_path, target_path, no_query)
            logger.info("=" * 255)
        case 402:
            logger.info("整理临时文件夹，修改替换文件夹名中的特殊字符，将改名后的文件夹移动到目标目录")
            logger.info("完成后手动将文件夹从目标目录移回来源目录")
            logger.info("由于有重名目录，可能需要多次运行")
            logger.info("=" * 255)
            from rename_folder_to_common import rename_folder_to_common
            source_path = r'A:\1.临时'
            target_path = r'B:\2.脚本'
            rename_folder_to_common(source_path, target_path)
            logger.info("=" * 255)
        case 403:
            logger.info("根据分隔符分割后，检查并移动重复文件夹到目标目录")
            logger.info("完成后手动将文件夹从目标目录移回来源目录")
            logger.info("由于有重名目录，可能需要多次运行")
            logger.info("=" * 255)
            from move_duplicates import move_duplicates
            source_path = r'A:\1.临时'
            target_path = r'B:\2.脚本'
            move_duplicates(source_path, target_path)
            logger.info("=" * 255)
        case 404:
            logger.info("对比整理来源目录到目标目录，代表不需要后续处理")
            logger.info("done 直接移动到外部归档磁盘，其他目录移动到对应位置")
            logger.info("=" * 255)
            from sort_local import sort_local
            source_path = r'B:\1.临时'
            target_path = r'B:\2.脚本'
            sort_local(source_path, target_path)
            logger.info("=" * 255)
        case 405:
            logger.info("先确认文件夹名无误后，手动移动到来源目录中")
            logger.info("获取来源目录下所有文件夹名，到目标网站搜索，根据搜索结果自动把文件夹移动到不同目录下")
            logger.info("=" * 255)
            from sort_ru import sort_ru
            source_path = r'A:\2.脚本'
            target_path = r'A:\undone\结果目录'
            sort_ru(source_path, target_path)
            logger.info("=" * 255)
        case 501:
            logger.info("整理 MZ 博客专用任务。链接来源：B:/mz_url.txt")
            logger.info("链接需要和目录能对应上，会自动重命名和下载图片")
            logger.info("=" * 255)
            from sort_mz import sort_mz
            source_path = r'B:\0.整理\jd\mz'
            target_path = r'B:\2.脚本'
            sort_mz(source_path, target_path)
            logger.info("=" * 255)
        case 601:
            logger.info(r"抓取 ru 链接，自动将新帖子保存到：B:\0.整理\BT\ru")
            logger.info(r"手动检查 ru 目录，处理后移动到 ru—old")
            logger.info("=" * 255)
            from scrapy_ru import scrapy_ru
            scrapy_ru()
            logger.info("=" * 255)
        case 602:
            logger.info(r"抓取 yts 链接，来源文档一行一个链接，需要自行去重")
            logger.info(r"自动将结果 json 文件保存到：B:\0.整理\BT\yts")
            logger.info(r"手动检查 yts 目录，处理后移动到 yts—old")
            logger.info(r"链接来自 Feedly 阅读器，浏览器控制台提取链接脚本：")
            js = """
const links = Array.from(document.querySelectorAll('a.EntryTitleLink')).map(article => article.href);
console.log(`用于 feedly 阅读器提取链接，总计 ${links.length} 条地址`);
copy(links.join('\n'));
            """
            logger.info(js)
            logger.info("=" * 255)
            from scrapy_yts import scrapy_yts
            source_file = r'config/!00.txt'
            scrapy_yts(source_file)
            logger.info(r"来自 yts 没有导演的种子，试图自行补全")
            from scrapy_yts_fix_imdb import scrapy_yts_fix_imdb
            scrapy_yts_fix_imdb()
            logger.info("=" * 255)
        case 603:
            logger.info(r"对比镜像文件夹，将已整理过的导演种子找出来")
            logger.info(r"镜像文件夹在 E:\视频\电影\Mirror")
            logger.info(r"种子文件夹在 B:\0.整理\BT\ru 和 B:\0.整理\BT\yts")
            logger.info("=" * 255)
            from sort_movie_ops import sort_new_torrents_by_director
            target_path = r'A:\1'
            sort_new_torrents_by_director(target_path)
            logger.info("=" * 255)
        case 604:
            logger.info(r"抓取 ttg 信息，自动将新帖子保存到：B:\0.整理\BT\ttg")
            logger.info(r"手动检查 ttg 目录，处理后移动到 ttg—old")
            logger.info("=" * 255)
            from scrapy_ttg import scrapy_ttg
            scrapy_ttg()
            logger.info("=" * 255)
        case 605:
            logger.info(r"抓取 dhd 信息，自动将新帖子保存到：B:\0.整理\BT\dhd")
            logger.info(r"先更新 cookie")
            logger.info(r"自动转换 dhd 文件，抓取磁力链接")
            logger.info(r"手动检查 dhd 目录，处理后移动到 dhd—old")
            logger.info("=" * 255)
            import asyncio
            from scrapy_dhd import scrapy_dhd_async, dhd_to_log
            asyncio.run(scrapy_dhd_async())
            logger.info("-" * 255)
            dhd_to_log()
            logger.info("=" * 255)
        case 606:
            logger.info(r"抓取 sk 信息，自动将新帖子保存到：B:\0.整理\BT\sk")
            logger.info(r"先更新 sk 和 csfd 站点 cookie")
            logger.info(r"完成后手动更新 end_data")
            logger.info(r"检查 sk 目录，处理后移动到 sk—old")
            logger.info("=" * 255)
            from scrapy_sk import scrapy_sk
            scrapy_sk(start_page=0,end_data="24/12/2025")
            logger.info("=" * 255)
        case 607:
            logger.info(r"抓取 rare 站点信息，自动将新帖子保存到：B:\0.整理\BT\rare")
            logger.info(r"来源文档一行一个链接，可以混合 4 站点")
            logger.info(r"链接来自 Feedly 阅读器，浏览器控制台提取链接脚本参考 yts")
            logger.info("=" * 255)
            from scrapy_rare import scrapy_rare
            source_file = r'config/!00.txt'
            scrapy_rare(source_file)
            logger.info("=" * 255)
        case 608:
            logger.info(r"抓取 mp 站点信息，自动将新帖子保存到：B:\0.整理\BT\rare")
            logger.info(r"半个小时出现 403，手动过验证码，更新 Cookie")
            logger.info(r"开始前手动更新 end_url，找没有更新的发布。结束后记录位置")
            logger.info(r"检查 rare 目录，处理后移动到 rare—old")
            """
12.25
Wake of the Red Witch Dec. 30, 1948
A Big Bold Beautiful Journey Sep. 17, 2025
Backfire Jun. 21, 1988
Princess Arete Aug. 01, 2001
Dikkenek Jun. 21, 2006
            """
            logger.info("=" * 255)
            from scrapy_mp import scrapy_mp
            scrapy_mp(start_page=2, end="princess-arete")
            logger.info("=" * 255)
        case 609:
            logger.info(r"搜索数据库，将已整理过的导演种子找出来")
            logger.info(r"种子文件夹在 B:\0.整理\BT\ 下面")
            logger.info("=" * 255)
            from sort_movie_ops import sort_new_torrents_by_mysql
            target_path = r'A:\1'
            sort_new_torrents_by_mysql(target_path)
            logger.info("=" * 255)
        case 610:
            logger.info(r"抓取 hde 信息，自动将新发布保存到：B:\0.整理\BT\rls")
            logger.info(r"完成后手动更新 end_title")
            logger.info("=" * 255)
            from scrapy_hde import scrapy_hde
            scrapy_hde(start_page=2,end_title="RTE.Sport.Awards.2025.1080p.WEB.H264-CBFM – 4.1 GB")
            logger.info("=" * 255)
        case 611:
            logger.info(r"抓取 rlsbb 信息，自动将新发布保存到：B:\0.整理\BT\rls")
            logger.info(r"完成后手动更新 end_title") #
            logger.info("=" * 255)
            from scrapy_rls import scrapy_rls
            # scrapy_rls(start_page=2, f_mode=True, end_title="The Real Sister 2024 WEB Vietnamese H264-RBB (851MB)")
            # scrapy_rls(start_page=2, f_mode=False, end_title="Popeye The Slayer Man 2025 INTERNAL 1080p BluRay x264-Replica (8.54GB)")
            logger.info("=" * 255)
        case 612:
            logger.info(r"抓取 sc 信息，将新链接丢如 jd 中去抓取")
            """
            https://softcore-index.info/
            12-25
            Aliwan Inn (2025) HD
            Newaza Gal: Straight From Behind (1987)
            Gen’eki Joshidaisei: Kahanshin Focus (1986)
            Kai neko (2016) HD
            """
        case 613:
            logger.info(r"抓取 bds 信息，自动将新发布保存到：B:\0.整理\BT\bds")
            logger.info(r"需要手动更新 cookie")
            logger.info(r"完成后手动更新 end_time")
            """
const titles = Array.from(document.querySelectorAll('div.file-name')).map(div => div.getAttribute('title')).join('\n');   // 一行一个
copy(titles); // 复制115文件列表到剪切板
            """
            from scrapy_bds import scrapy_bds
            scrapy_bds(start_page=1,end_time="2025-12-24") #
            logger.info("=" * 255)
        case 614:
            logger.info(r"抓取 mt 信息，自动将新帖子保存到：B:\0.整理\BT\mt")
            logger.info(r"手动检查 mt 目录，处理后移动到 mt—old")
            logger.info(r"完成后手动更新 start_time 和 end_time")
            logger.info("=" * 255)
            from scrapy_mt import scrapy_mt
            scrapy_mt(start_time="2025-12-17", end_time="2025-12-25")
            logger.info("=" * 255)
        case 615:
            logger.info(r"抓取 onk 信息，自动将新发布保存到：B:\0.整理\BT\onk")
            logger.info(r"完成后手动更新 end_time")
            from scrapy_onk import scrapy_onk
            scrapy_onk(start_page=1, end_time="2026-01-16")
            logger.info("=" * 255)
        case 616:
            logger.info(r"抓取 dlb 信息，自动将新发布保存到：B:\0.整理\BT\dlb")
            logger.info(r"完成后手动更新 end_title")
            from scrapy_dlb import scrapy_dlb
            scrapy_dlb(start_page=200, end_title="2024-01-15")
            logger.info("=" * 255)

        case 701:
            logger.info("整理导演目录，在导演目录生成导演别名和代表链接的空文件")
            logger.info("来源文本首行为导演目录路径，后面三行为导演链接")
            logger.info("=" * 255)
            from sort_movie_director import sort_movie_director
            from sort_movie_ops import get_ids
            from my_module import read_file_to_list
            source_file = r'config/!00.txt'
            get_ids(source_file)
            sort_movie_director(read_file_to_list(source_file)[0])
            logger.info("=" * 255)
        case 702:
            logger.info("从 ru 搜索电影种子信息，储存到目标目录")
            logger.info("先检测第一行是否为导演目录，是的话抓取电影编号的种子")
            logger.info(r"搜索关键字储存在 !00.txt 一行一个")
            logger.info("=" * 255)
            import re, os, shutil
            from scrapy_ru_magnet import scrapy_ru_magnet
            from my_module import read_file_to_list
            from sort_movie_ops import everything_search_filelist, PRE_LOAD_FP
            from get_director_movies import get_director_movies
            source_file = r'config/!00.txt'
            target_path = r"A:\1"
            key_list = read_file_to_list(source_file)

            # 预处理阶段
            pattern = re.compile(r'(tt\d+)')
            if len(key_list) == 1 and os.path.isdir(key_list[0]):
                imdb_ids = get_director_movies(key_list[0])
                if not imdb_ids:
                    return
                imdb_set = set(imdb_ids)

                for fp in PRE_LOAD_FP:
                    folder_name = os.path.basename(fp)
                    m = pattern.search(folder_name)
                    if not m:
                        continue
                    film_id = m.group(1)
                    if film_id in imdb_set:
                        dest = os.path.join(target_path, folder_name)
                        shutil.move(fp, dest)
                logger.info(f"种子收集完成")
                return

            keywords = list({x.lower(): x for x in reversed(key_list)}.values())[::-1]  # 忽略大小写去重
            everything_search_filelist(source_file)
            for key in keywords:
                scrapy_ru_magnet(key, target_path)
                logger.info("-" * 255)
        case 703:
            logger.info("从 ru 搜索电影种子信息，超限版本")
            logger.info("=" * 255)
            from scrapy_ru_magnet import scrapy_ru_magnet
            from my_module import read_file_to_list
            source_file = r'config/!00.txt'
            target_path = r"A:\1"
            order = "1"

            key_list = read_file_to_list(source_file)
            keywords = list({x.lower(): x for x in reversed(key_list)}.values())[::-1]  # 忽略大小写去重
            for key in keywords:
                scrapy_ru_magnet(key, target_path)
                scrapy_ru_magnet(key, target_path, order)
                logger.info("-" * 255)
        case 704:
            logger.info("从 ru 搜索电影名，不走缓存")
            logger.info("=" * 255)
            from scrapy_ru_magnet import scrapy_ru_magnet
            from my_module import read_file_to_list
            source_file = r'config/!00.txt'
            target_path = r"A:\1"
            cache = False

            key_list = read_file_to_list(source_file)
            keywords = list({x.lower(): x for x in reversed(key_list)}.values())[::-1]  # 忽略大小写去重
            for key in keywords:
                scrapy_ru_magnet(key, target_path, cache=cache)
                logger.info("-" * 255)
        case 706:
            logger.info(r"添加种子到盒子，种子信息文件来自：B:\0.整理\Chrome")
            logger.info(r"添加完毕后，需要把 B:\0.整理\Chrome 中目录动手移动到 A:\0c.下载整理")
            logger.info("=" * 255)
            import os
            from add_to_qb import add_to_qb
            from get_director_movies import get_director_movies
            source_path = r'B:\0.整理\Chrome'
            add_to_qb(source_path)
            logger.info("=" * 255)
        case 707:
            logger.info(r"添加种子到 115 离线服务，种子信息文件来自：B:\0.整理\Chrome")
            logger.info(r"添加完毕后，视情况手动处理")
            logger.info("=" * 255)
            from add_to_115 import add_to_115
            source_path = r'B:\0.整理\Chrome'
            add_to_115(source_path)
            logger.info("=" * 255)
        case 708:
            logger.info("整理电影目录，在电影目录生成别名空文件，自动重命名目录")
            logger.info("来源文本首行为电影目录路径，后面三行为电影链接")
            logger.info("=" * 255)
            from sort_movie import sort_movie
            from sort_movie_ops import get_ids
            from my_module import read_file_to_list
            source_file = r'config/!00.txt'
            get_ids(source_file)
            sort_movie(read_file_to_list(source_file)[0])
            logger.info("=" * 255)
        case 710:
            logger.info(r"从盒子 QB 获取下载任务，磁链保存到：B:\0.整理\Chrome")
            logger.info(r"获取文件后，可以添加到 115、PikPak 或夸克离线")
            logger.info("=" * 255)
            from get_qb_downloads import get_qb_downloads
            target_path = r'B:\0.整理\Chrome'
            get_qb_downloads(target_path)
            logger.info("=" * 255)
        case 711:
            logger.info(r"添加种子到 PikPak 离线服务，种子信息文件来自：B:\0.整理\Chrome")
            logger.info(r"离线失败的再通过 115 离线尝试")
            logger.info("=" * 255)
            from add_to_pikpak import add_to_pikpak
            source_path = r'B:\0.整理\Chrome'
            add_to_pikpak(source_path)
            logger.info("=" * 255)
        case 801:
            logger.info(r"自动整理下载目录，将下载完成的种子移动到对应目录内")
            logger.info("来源目录一行一个路径")
            logger.info("=" * 255)
            from my_module import read_file_to_list
            from sort_movie_auto import sort_torrents_auto
            source_file = r'config/!00.txt'
            temp_list = read_file_to_list(source_file)
            for i in temp_list:
                sort_torrents_auto(i)
        case 802:
            logger.info(r"批量自动整理导演目录，完成后移动到 A:\0b.导演别名")
            logger.info("来源目录一行一个导演路径")
            logger.info("需要确保导演路径中存在 tt 编号")
            logger.info(r"如果没有找到豆瓣链接，可以视情况手动移动到 A:\0b.导演别名")
            logger.info("如果没有找到 TMDB 链接，可以手动查询建立 .tmdb 空文件")
            logger.info("=" * 255)
            from my_module import read_file_to_list
            from sort_movie_auto import sort_director_auto
            source_file = r'config/!00.txt'
            temp_list = read_file_to_list(source_file)
            for i in temp_list:
                sort_director_auto(i)
                time.sleep(0.1)
                logger.info("-" * 255)
                time.sleep(0.1)
        case 803:
            logger.info(r"批量搜索导演名，来源 A:\0b.导演别名")
            logger.info(r"如果没有搜索结果，移动到 A:\115")
            logger.info("=" * 255)
            from sort_movie_auto import sort_ru_auto
            source_path = r"A:\0b.导演别名"
            target_path = r"A:\115"
            sort_ru_auto(source_path, target_path)
            logger.info("-" * 255)
        case 805:
            logger.info(r"从 TMDB 获取导演所有电影列表，来源文件一行一个导演路径")
            logger.info("需要确保导演目录中存在 .tmdb 文件")
            logger.info("=" * 255)
            from my_module import read_file_to_list
            from get_director_movies import get_director_movies
            source_file = r'config/!00.txt'
            temp_list = read_file_to_list(source_file)
            for i in temp_list:
                if not i:
                    continue
                get_director_movies(i)
                logger.info("-" * 255)
        case 806:
            logger.info(r"批量自动整理电影目录，来源文件一行一个导演路径")
            logger.info("需要确保电影路径中存在 tt 编号")
            logger.info("=" * 255)
            from my_module import read_file_to_list
            from sort_movie_auto import sort_movie_auto
            source_file = r'config/!00.txt'
            temp_list = read_file_to_list(source_file)
            for i in temp_list:
                if not i:
                    continue
                time.sleep(0.1)
                target_file = f"B:\\2.脚本\\!00-{time_now}.txt"
                sort_movie_auto(i, target_file)
                time.sleep(0.1)
        case 807:
            logger.info(r"依据 imdb id 删除数据库记录")
            logger.info("=" * 255)
            import re
            from sort_movie_mysql import delete_records
            from my_module import read_file_to_list
            source_file = r'config/!00.txt'
            names = read_file_to_list(source_file)
            id_list = [re.search(r"tt\d+", s).group() for s in names]
            delete_records(id_list, "imdb", "movies")
            delete_records(id_list, "imdb", "wanted")
            logger.info("-" * 255)
        case 808:
            logger.info(r"给导演归档，来源文件一行一个导演路径")
            logger.info("=" * 255)
            from my_module import read_file_to_list
            from sort_movie_auto import achieve_director
            source_file = r'config/!00.txt'
            temp_list = read_file_to_list(source_file)
            for i in temp_list:
                if not i:
                    continue
                achieve_director(i)
                time.sleep(0.1)
                logger.warning("-" * 255)
                time.sleep(0.1)
        case 809:
            logger.info(r"提取电影目录的所有电影 id，并去数据库校验")
            logger.info(r"电影目录储存到 config/!00.txt 中")
            logger.info("=" * 255)
            from sort_movie_ops import extract_movie_ids
            from my_module import read_file_to_list
            from sort_movie_mysql import check_movie_ids
            source_file = r'config/!00.txt'
            temp_list = read_file_to_list(source_file)
            id_list = extract_movie_ids(temp_list[0])
            for i in id_list:
                print(i)
            # 归档硬盘，检查是否都记录到数据库
            result_list = check_movie_ids(id_list)
            if not result_list:
                print(f"共 {len(id_list)} 电影，已经全部记录")
                return
            for i in result_list:
                print(i)

        case _:
            logger.warning("请输入有效编号")


def temp(m_path):
    """临时函数"""
    print("修复数据库")
    # 导入函数，初始化变量
    from file_ops import read_file_to_list
    from sort_movie import sort_movie
    lll = read_file_to_list(r"config/!00.txt")
    for ll in lll:
        sort_movie(ll)
        logger.info("-" * 255)


if __name__ == '__main__':
    # yts 临时失败链接储存到下面
    yts_urls = """
ERROR: https://yts.lt/movies/frontline-2025
ERROR: https://yts.lt/movies/the-dam-2025
ERROR: https://yts.lt/movies/perrengue-fashion-2025
ERROR: https://yts.lt/movies/the-lost-chapter-yukis-revenge-2025
ERROR: https://yts.lt/movies/lali-la-que-le-gana-al-tiempo-2025
ERROR: https://yts.lt/movies/predator-badlands-2025
ERROR: https://yts.lt/movies/zamach-na-papieza-2025
ERROR: https://yts.lt/movies/sore-wife-from-the-future-2025
ERROR: https://yts.lt/movies/wake-up-dead-man-a-knives-out-mystery-2025
ERROR: https://yts.lt/movies/verliefd-op-bali-2024
ERROR: https://yts.lt/movies/dashing-through-the-snow-2025
ERROR: https://yts.lt/movies/london-has-fallen-2016
ERROR: https://yts.lt/movies/ohryis-project-2025
ERROR: https://yts.lt/movies/denial-1998
ERROR: https://yts.lt/movies/mike-and-dave-need-wedding-dates-2016
ERROR: https://yts.lt/movies/screamboat-2025
ERROR: https://yts.lt/movies/small-soldiers-1998
ERROR: https://yts.lt/movies/iconic-2024
ERROR: https://yts.lt/movies/vivacious-lady-1938
ERROR: https://yts.lt/movies/the-room-next-door-2024
ERROR: https://yts.lt/movies/the-sundowners-1960
ERROR: https://yts.lt/movies/sometimes-i-think-about-dying-2023
    """
    logger.info(f"开始时间：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
    try:
        # 702 -> ru 标准搜索
        # 703 -> ru 搜索结果超限版
        # 704 -> ru 搜索电影名，不走缓存
        # 706 -> 添加种子到盒子
        # 708 -> 整理单部电影
        # 801 -> 批量预整理，重命名目录，移动种子。要指定目录
        # 802 -> 批量整理导演
        # 806 -> 批量整理电影
        # 807 -> 清理数据库
        # 808 -> 归档导演
        main(616)
        # temp(r"A:\1")
    except Exception:
        logger.exception('Unexpected error!')
    finally:
        logger.info(f"完成时间：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
