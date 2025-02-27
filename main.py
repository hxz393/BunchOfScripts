"""
启动脚本

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""

import logging

from my_module import logging_config

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
            print("-" * 255)
            from scrapy_pic_1 import scrapy_pic_1
            scrapy_pic_1()
        case 201:
            print("某游戏网站百度网盘下载地址自动获取")
            print("-" * 255)
            from scrapy_game_1 import scrapy_game_1
            scrapy_game_1()
        case 301:
            print("BANDCAMP.COM 新专辑处理。链接来源 B:/2.脚本/new_album.txt")
            print("-" * 255)
            from scrapy_bandcamp import recording_new_album
            recording_new_album()
        case 302:
            print("BANDCAMP.COM 新乐队处理。链接来源 B:/2.脚本/new_artist.txt")
            print("-" * 255)
            from scrapy_bandcamp import recording_new_artist
            recording_new_artist()
        case 303:
            print("BANDCAMP.COM 下载的音频处理。链接来源 B:/2.脚本/new_artist.txt")
            print("-" * 255)
            from scrapy_bandcamp import sort_bandcamp_files
            source_path = r'B:\0.整理\jd\rss'
            target_path = r'B:\2.脚本'
            sort_bandcamp_files(source_path, target_path)
        case 401:
            print(r"自动整理已解压下载音乐。成功移动到目标目录，失败移动到 B:\0.整理\手动整理")
            print(r"手动处理完 B:\0.整理\手动整理 下的文件夹，再进行下一步")
            print("-" * 255)
            from sort_discogs import sort_discogs
            source_path = r'B:\0.整理\jd\un'
            target_path = r'B:\2.脚本'
            no_query = False
            sort_discogs(source_path, target_path, no_query)
        case 402:
            print("整理临时文件夹，修改替换文件夹名中的特殊字符，将改名后的文件夹移动到目标目录")
            print("完成后手动将文件夹从目标目录移回来源目录")
            print("由于有重名目录，可能需要多次运行")
            print("-" * 255)
            from rename_folder_to_common import rename_folder_to_common
            source_path = r'A:\1.临时'
            target_path = r'B:\2.脚本'
            rename_folder_to_common(source_path, target_path)
        case 403:
            print("根据分隔符分割后，检查并移动重复文件夹到目标目录")
            print("完成后手动将文件夹从目标目录移回来源目录")
            print("由于有重名目录，可能需要多次运行")
            print("-" * 255)
            from move_duplicates import move_duplicates
            source_path = r'A:\1.临时'
            target_path = r'B:\2.脚本'
            move_duplicates(source_path, target_path)
        case 404:
            print("对比整理来源目录到目标目录，代表不需要后续处理")
            print("done 直接移动到外部归档磁盘，其他目录移动到对应位置")
            print("-" * 255)
            from sort_local import sort_local
            source_path = r'B:\1.临时'
            target_path = r'B:\2.脚本'
            sort_local(source_path, target_path)
        case 405:
            print("先确认文件夹名无误后，手动移动到来源目录中")
            print("获取来源目录下所有文件夹名，到目标网站搜索，根据搜索结果自动把文件夹移动到不同目录下")
            print("-" * 255)
            from sort_ru import sort_ru
            source_path = r'A:\2.脚本'
            target_path = r'A:\undone\结果目录'
            sort_ru(source_path, target_path)
        case 501:
            print("整理 MZ 博客专用任务。链接来源：B:/mz_url.txt")
            print("链接需要和目录能对应上，会自动重命名和下载图片")
            print("-" * 255)
            from sort_mz import sort_mz
            source_path = r'B:\0.整理\jd\mz'
            target_path = r'B:\2.脚本'
            sort_mz(source_path, target_path)
        case 601:
            print(r"抓取 ru 链接，自动将新帖子保存到：B:\0.整理\BT\种子")
            print("-" * 255)
            from scrapy_ru import scrapy_ru
            scrapy_ru()
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
            print("-" * 255)
            from scrapy_yts import scrapy_yts
            source_file = r'B:\2.脚本\!00.txt'
            scrapy_yts(source_file)
            print(r"来自 yts 没有导演的种子，试图自行补全")
            from scrapy_yts_fix_imdb import scrapy_yts_fix_imdb
            scrapy_yts_fix_imdb()
        case 701:
            print("整理导演目录，在导演目录生成导演别名和代表链接的空文件")
            print("来源文本首行为导演目录路径，后面三行为导演链接")
            print("-" * 255)
            from sort_movie_director import sort_movie_director
            from sort_movie_ops import get_ids
            from my_module import read_file_to_list
            source_file = r'B:\2.脚本\!00.txt'
            get_ids(source_file)
            sort_movie_director(read_file_to_list(source_file)[0])
        case 702:
            print("从 ru 搜索电影种子信息，储存到目标目录")
            print(r"搜索关键字储存在 B:\2.脚本\!00.txt 第一行")
            print("-" * 255)
            from scrapy_ru_magnet import scrapy_ru_magnet
            from my_module import read_file_to_list
            source_file = r'B:\2.脚本\!00.txt'
            target_path = r"A:\1"
            ascii_encode = False
            scrapy_ru_magnet(read_file_to_list(source_file)[0], target_path, ascii_encode)
        case 703:
            print("从 ru 搜索电影种子信息，转码版本")
            print("不要用于俄语")
            print("-" * 255)
            from scrapy_ru_magnet import scrapy_ru_magnet
            from my_module import read_file_to_list
            source_file = r'B:\2.脚本\!00.txt'
            target_path = r"A:\1"
            ascii_encode = True
            scrapy_ru_magnet(read_file_to_list(source_file)[0], target_path, ascii_encode)
        case 704:
            print("从 ru 搜索电影种子信息，超限版本")
            print("-" * 255)
            from scrapy_ru_magnet import scrapy_ru_magnet
            from my_module import read_file_to_list
            source_file = r'B:\2.脚本\!00.txt'
            target_path = r"A:\1"
            ascii_encode = False
            order = "1"
            scrapy_ru_magnet(read_file_to_list(source_file)[0], target_path, ascii_encode)
            print("-" * 255)
            scrapy_ru_magnet(read_file_to_list(source_file)[0], target_path, ascii_encode, order)
        case 705:
            print("从 ru 搜索电影种子信息，转码超限版本")
            print("-" * 255)
            from scrapy_ru_magnet import scrapy_ru_magnet
            from my_module import read_file_to_list
            source_file = r'B:\2.脚本\!00.txt'
            target_path = r"A:\1"
            ascii_encode = True
            order = "1"
            scrapy_ru_magnet(read_file_to_list(source_file)[0], target_path, ascii_encode)
            print("-" * 255)
            scrapy_ru_magnet(read_file_to_list(source_file)[0], target_path, ascii_encode, order)
        case 706:
            print(r"添加种子到盒子，种子信息文件来自：B:\0.整理\Chrome")
            print(r"添加完毕后，需要把 B:\0.整理\Chrome 中目录动手移动到 A:\0c.下载整理")
            print("-" * 255)
            from add_to_qb import add_to_qb
            source_path = r'B:\0.整理\Chrome'
            add_to_qb(source_path)
        case 707:
            print(r"添加种子到 115 离线服务，种子信息文件来自：B:\0.整理\Chrome")
            print(r"添加完毕后，视情况手动处理")
            print("-" * 255)
            from add_to_115 import add_to_115
            source_path = r'B:\0.整理\Chrome'
            add_to_115(source_path)
        case 708:
            print("整理电影目录，在电影目录生成别名空文件，自动重命名目录")
            print("来源文本首行为电影目录路径，后面三行为电影链接")
            print("-" * 255)
            from sort_movie import sort_movie
            from sort_movie_ops import get_ids
            from my_module import read_file_to_list
            source_file = r'B:\2.脚本\!00.txt'
            get_ids(source_file)
            sort_movie(read_file_to_list(source_file)[0])
        case 709:
            print("整理电影目录，TV 版本")
            print("-" * 255)
            from sort_movie import sort_movie
            from sort_movie_ops import get_ids
            from my_module import read_file_to_list
            source_file = r'B:\2.脚本\!00.txt'
            tv = True
            get_ids(source_file)
            sort_movie(read_file_to_list(source_file)[0], tv)
        case 710:
            print(r"批量自动整理导演目录，完成后移动到 A:\0b.导演别名")
            print("来源目录一行一个导演路径")
            print("需要确保导演路径中存在 tt 编号")
            print("-" * 255)
            from my_module import read_file_to_list
            from sort_movie_auto import sort_director_auto
            source_file = r'B:\2.脚本\!00.txt'
            temp_list = read_file_to_list(source_file)
            for i in temp_list:
                sort_director_auto(i)
                print("-" * 255)
        case 711:
            print(r"批量自动整理电影目录，来源文件一行一个导演路径")
            print("需要确保电影路径中存在 tt 编号")
            print("-" * 255)
            from my_module import read_file_to_list
            from sort_movie_auto import sort_movie_auto
            source_file = r'B:\2.脚本\!00.txt'
            temp_list = read_file_to_list(source_file)
            for i in temp_list:
                sort_movie_auto(i)
        case 712:
            print(r"批量搜索导演名，来源文件一行一个导演路径")
            print("将会搜索导演所有名字及其别名，期待没有搜索结果")
            print("-" * 255)
            from my_module import read_file_to_list
            from sort_movie_auto import sort_ru_auto
            source_file = r'B:\2.脚本\!00.txt'
            temp_list = read_file_to_list(source_file)
            for i in temp_list:
                sort_ru_auto(i)
                print("-" * 255)

        case _:
            print("请输入有效编号")


if __name__ == '__main__':
    # yts 临时失败链接储存到下面
    yts_urls = """



    """
    try:
        # 701 -> 单个整理导演
        # 702 -> 标准 ru 搜索
        # 703 -> 转码 ru 搜索
        # 706 -> 添加种子到盒子
        # 708 -> 单个整理电影
        # 709 -> 单个整理TV
        # 710 -> 批量整理导演
        # 711 -> 批量整理电影
        # 712 -> 批量搜索下载
        main(710)
    except Exception:
        logger.exception('Unexpected error!')
    finally:
        print("完成!")
