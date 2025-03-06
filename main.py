"""
启动脚本

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""

import logging

from my_module import logging_config
from sort_movie_ops import everything_search_filelist

logger = logging.getLogger(__name__)

logging_config(console_output=True)


def main(chosen: int) -> None:
    """功能列表

    :param chosen: 选择功能
    :return: 无
    """
    match chosen:
        case 101:
            print("破解网站图片防盗链。启动后通过本地代理端口 10808 访问")
            print("=" * 255)
            from scrapy_pic_1 import scrapy_pic_1
            scrapy_pic_1()
            print("=" * 255)
        case 201:
            print("某游戏网站百度网盘下载地址自动获取")
            print("=" * 255)
            from scrapy_game_1 import scrapy_game_1
            scrapy_game_1()
            print("=" * 255)
        case 301:
            print("BANDCAMP.COM 新专辑处理。链接来源 B:/2.脚本/new_album.txt")
            print("=" * 255)
            from scrapy_bandcamp import recording_new_album
            recording_new_album()
            print("=" * 255)
        case 302:
            print("BANDCAMP.COM 新乐队处理。链接来源 B:/2.脚本/new_artist.txt")
            print("=" * 255)
            from scrapy_bandcamp import recording_new_artist
            recording_new_artist()
            print("=" * 255)
        case 303:
            print("BANDCAMP.COM 下载的音频处理。链接来源 B:/2.脚本/new_artist.txt")
            print("=" * 255)
            from scrapy_bandcamp import sort_bandcamp_files
            source_path = r'B:\0.整理\jd\rss'
            target_path = r'B:\2.脚本'
            sort_bandcamp_files(source_path, target_path)
            print("=" * 255)
        case 401:
            print(r"自动整理已解压下载音乐。成功移动到目标目录，失败移动到 B:\0.整理\手动整理")
            print(r"手动处理完 B:\0.整理\手动整理 下的文件夹，再进行下一步")
            print("=" * 255)
            from sort_discogs import sort_discogs
            source_path = r'B:\0.整理\jd\un'
            target_path = r'B:\2.脚本'
            no_query = False
            sort_discogs(source_path, target_path, no_query)
            print("=" * 255)
        case 402:
            print("整理临时文件夹，修改替换文件夹名中的特殊字符，将改名后的文件夹移动到目标目录")
            print("完成后手动将文件夹从目标目录移回来源目录")
            print("由于有重名目录，可能需要多次运行")
            print("=" * 255)
            from rename_folder_to_common import rename_folder_to_common
            source_path = r'A:\1.临时'
            target_path = r'B:\2.脚本'
            rename_folder_to_common(source_path, target_path)
            print("=" * 255)
        case 403:
            print("根据分隔符分割后，检查并移动重复文件夹到目标目录")
            print("完成后手动将文件夹从目标目录移回来源目录")
            print("由于有重名目录，可能需要多次运行")
            print("=" * 255)
            from move_duplicates import move_duplicates
            source_path = r'A:\1.临时'
            target_path = r'B:\2.脚本'
            move_duplicates(source_path, target_path)
            print("=" * 255)
        case 404:
            print("对比整理来源目录到目标目录，代表不需要后续处理")
            print("done 直接移动到外部归档磁盘，其他目录移动到对应位置")
            print("=" * 255)
            from sort_local import sort_local
            source_path = r'B:\1.临时'
            target_path = r'B:\2.脚本'
            sort_local(source_path, target_path)
            print("=" * 255)
        case 405:
            print("先确认文件夹名无误后，手动移动到来源目录中")
            print("获取来源目录下所有文件夹名，到目标网站搜索，根据搜索结果自动把文件夹移动到不同目录下")
            print("=" * 255)
            from sort_ru import sort_ru
            source_path = r'A:\2.脚本'
            target_path = r'A:\undone\结果目录'
            sort_ru(source_path, target_path)
            print("=" * 255)
        case 501:
            print("整理 MZ 博客专用任务。链接来源：B:/mz_url.txt")
            print("链接需要和目录能对应上，会自动重命名和下载图片")
            print("=" * 255)
            from sort_mz import sort_mz
            source_path = r'B:\0.整理\jd\mz'
            target_path = r'B:\2.脚本'
            sort_mz(source_path, target_path)
            print("=" * 255)
        case 601:
            print(r"抓取 ru 链接，自动将新帖子保存到：B:\0.整理\BT\种子")
            print("=" * 255)
            from scrapy_ru import scrapy_ru
            scrapy_ru()
            print("=" * 255)
        case 602:
            print(r"抓取 yts 链接，来源文档一行一个链接，需要自行去重")
            print(r"自动将结果 json 文件保存到：B:\0.整理\BT\yts")
            print(r"手动检查 yts 目录，处理后移动到 yts—old")
            print(r"链接来自 Feedly 阅读器，浏览器控制台提取链接脚本：")
            js = """
// 只提取 class 包含 EntryTitleLink 的 <a> 元素
const articles = Array.from(document.querySelectorAll('a.EntryTitleLink'));
const results = articles.map(article => ({title: article.innerText.trim(),link: article.href}));
console.log("提取到的标题和链接：", results);
// 将结果复制到剪贴板（在支持的环境下）
copy(results);
            """
            print(js)
            print("=" * 255)
            from scrapy_yts import scrapy_yts
            source_file = r'config/!00.txt'
            scrapy_yts(source_file)
            print(r"来自 yts 没有导演的种子，试图自行补全")
            from scrapy_yts_fix_imdb import scrapy_yts_fix_imdb
            scrapy_yts_fix_imdb()
            print("=" * 255)
        case 701:
            print("整理导演目录，在导演目录生成导演别名和代表链接的空文件")
            print("来源文本首行为导演目录路径，后面三行为导演链接")
            print("=" * 255)
            from sort_movie_director import sort_movie_director
            from sort_movie_ops import get_ids
            from my_module import read_file_to_list
            source_file = r'config/!00.txt'
            get_ids(source_file)
            sort_movie_director(read_file_to_list(source_file)[0])
            print("=" * 255)
        case 702:
            print("从 ru 搜索电影种子信息，储存到目标目录")
            print(r"搜索关键字储存在 !00.txt 一行一个")
            print("=" * 255)
            from scrapy_ru_magnet import scrapy_ru_magnet
            from my_module import read_file_to_list
            source_file = r'config/!00.txt'
            target_path = r"A:\1"
            key_list = read_file_to_list(source_file)
            keywords = list({x.lower(): x for x in reversed(key_list)}.values())[::-1]  # 忽略大小写去重
            everything_search_filelist(source_file)
            for key in keywords:
                scrapy_ru_magnet(key, target_path)
                print("-" * 255)
        case 703:
            print("从 ru 搜索电影种子信息，超限版本")
            print("=" * 255)
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
                print("-" * 255)
        case 704:
            print("从 ru 搜索电影名，不走缓存")
            print("=" * 255)
            from scrapy_ru_magnet import scrapy_ru_magnet
            from my_module import read_file_to_list
            source_file = r'config/!00.txt'
            target_path = r"A:\1"
            cache = False

            key_list = read_file_to_list(source_file)
            keywords = list({x.lower(): x for x in reversed(key_list)}.values())[::-1]  # 忽略大小写去重
            for key in keywords:
                scrapy_ru_magnet(key, target_path, cache=cache)
                print("-" * 255)
        case 706:
            print(r"添加种子到盒子，种子信息文件来自：B:\0.整理\Chrome")
            print(r"添加完毕后，需要把 B:\0.整理\Chrome 中目录动手移动到 A:\0c.下载整理")
            print("=" * 255)
            from add_to_qb import add_to_qb
            source_path = r'B:\0.整理\Chrome'
            add_to_qb(source_path)
            print("=" * 255)
        case 707:
            print(r"添加种子到 115 离线服务，种子信息文件来自：B:\0.整理\Chrome")
            print(r"添加完毕后，视情况手动处理")
            print("=" * 255)
            from add_to_115 import add_to_115
            source_path = r'B:\0.整理\Chrome'
            add_to_115(source_path)
            print("=" * 255)
        case 708:
            print("整理电影目录，在电影目录生成别名空文件，自动重命名目录")
            print("来源文本首行为电影目录路径，后面三行为电影链接")
            print("=" * 255)
            from sort_movie import sort_movie
            from sort_movie_ops import get_ids
            from my_module import read_file_to_list
            source_file = r'config/!00.txt'
            get_ids(source_file)
            sort_movie(read_file_to_list(source_file)[0])
            print("=" * 255)
        case 709:
            print("整理电影目录，TV 版本")
            print("=" * 255)
            from sort_movie import sort_movie
            from sort_movie_ops import get_ids
            from my_module import read_file_to_list
            source_file = r'config/!00.txt'
            tv = True
            get_ids(source_file)
            sort_movie(read_file_to_list(source_file)[0], tv)
            print("=" * 255)
        case 710:
            print(r"从盒子 QB 获取下载任务，磁链保存到：B:\0.整理\Chrome")
            print(r"获取文件后，可以添加到 115、PikPak 或夸克离线")
            print("=" * 255)
            from get_qb_downloads import get_qb_downloads
            target_path = r'B:\0.整理\Chrome'
            get_qb_downloads(target_path)
            print("=" * 255)
        case 711:
            print(r"添加种子到 PikPak 离线服务，种子信息文件来自：B:\0.整理\Chrome")
            print(r"离线失败的再通过 115 离线尝试")
            print("=" * 255)
            from add_to_pikpak import add_to_pikpak
            source_path = r'B:\0.整理\Chrome'
            add_to_pikpak(source_path)
            print("=" * 255)
        case 801:
            print(r"自动整理下载目录，将下载完成的种子移动到对应目录内")
            print("来源目录一行一个路径")
            print("=" * 255)
            from my_module import read_file_to_list
            from sort_movie_auto import sort_torrents_auto
            source_file = r'config/!00.txt'
            temp_list = read_file_to_list(source_file)
            for i in temp_list:
                sort_torrents_auto(i)
        case 802:
            print(r"批量自动整理导演目录，完成后移动到 A:\0b.导演别名")
            print("来源目录一行一个导演路径")
            print("需要确保导演路径中存在 tt 编号")
            print(r"如果没有找到豆瓣链接，可以视情况手动移动到 A:\0b.导演别名")
            print("如果没有找到 TMDB 链接，可以手动查询建立 .tmdb 空文件")
            print("=" * 255)
            from my_module import read_file_to_list
            from sort_movie_auto import sort_director_auto
            source_file = r'config/!00.txt'
            temp_list = read_file_to_list(source_file)
            for i in temp_list:
                sort_director_auto(i)
                print("-" * 255)
        case 803:
            print(r"批量搜索导演名，来源文件一行一个导演路径")
            print("将会搜索导演所有名字及其别名")
            print(r"如果没有搜索结果，也没有下载文件，移动到 A:\0c.下载整理")
            print("目前进度A-G是安全的")
            print("=" * 255)
            from my_module import read_file_to_list
            from sort_movie_auto import sort_ru_auto
            source_file = r'config/!00.txt'
            temp_list = read_file_to_list(source_file)
            for i in temp_list:
                sort_ru_auto(i)
                print("-" * 255)
        case 804:
            print(r"整理目录中空文件移动到 F:\电影(1)")
            print("=" * 255)
            from sort_movie_auto import sort_aka_files
            source_path_1 = r"A:\0c.下载整理"
            source_path_2 = r"A:\0e.自动整理"
            target_path = r"F:\电影(1)"
            sort_aka_files(source_path_1, target_path)
            print("-" * 255)
            sort_aka_files(source_path_2, target_path)
        case 805:
            print(r"批量自动整理电影目录，来源文件一行一个导演路径")
            print("需要确保电影路径中存在 tt 编号")
            print("=" * 255)
            from my_module import read_file_to_list
            from sort_movie_auto import sort_movie_auto
            source_file = r'config/!00.txt'
            temp_list = read_file_to_list(source_file)
            for i in temp_list:
                if not i:
                    continue
                sort_movie_auto(i)
                print("=" * 255)

        case _:
            print("请输入有效编号")


if __name__ == '__main__':
    # yts 临时失败链接储存到下面
    yts_urls = """
https://yts.mx/movies/for-ellen-2012
https://yts.mx/movies/the-damned-dont-you-wish-that-we-were-dead-2015
https://yts.mx/movies/ill-be-homeless-for-christmas-2012
https://yts.mx/movies/in-sickness-and-in-health-2012
https://yts.mx/movies/what-would-you-do-for-love-2013
https://yts.mx/movies/insect-2018
https://yts.mx/movies/horny-house-of-horror-2010
https://yts.mx/movies/if-you-really-love-me-2012


    """
    try:
        # 702 -> ru 标准搜索
        # 703 -> ru 搜索结果超限版
        # 704 -> ru 搜索电影名，不走缓存
        # 706 -> 添加种子到盒子
        # 708 -> 整理单部电影
        # 801 -> 批量预整理，重命名目录，移动种子。要指定目录
        # 802 -> 批量整理导演
        # 803 -> 批量搜索下载
        # 804 -> 805 前运行，下载整理目录空文件处理
        # 805 -> 批量整理电影
        main(702)
    except Exception:
        logger.exception('Unexpected error!')
    finally:
        print("完成!")
