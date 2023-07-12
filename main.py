from my_module import *
from my_scripts import *
import logging

logger = logging.getLogger(__name__)

logging_config(console_output=True, log_level='INFO')

if __name__ == '__main__':
    return_dict = {}

    ########## 正常使用 ##############

    try:
        ########## 爬虫 ##############
        # 破解网站图片防盗链。启动后通过本地代理端口 10808 访问。
        # scrapy_pic_1()

        # 某游戏网站百度网盘下载地址获取。
        # scrapy_game_1()

        ########## 整理 ##############
        # 整理下载文件夹，移动到目标目录。
        sort_discogs(source_path=r'B:\0.整理\01', target_path=r'B:\2.脚本', no_query=False)

        # 整理临时文件夹，修改文件夹名中的特殊字符。将修改后的文件夹移动到 target_path，完成后手动将文件夹从 target_path 移回 source_path。
        # return_dict = rename_folder_to_common(source_path=r'B:\1.临时', target_path=r'B:\2.脚本')

        # 根据分隔符分割后，检查并移动重复文件夹。完成后手动将文件夹从 target_dir 移回 source_dir。
        # return_dict = move_duplicates(source_path=r'B:\1.临时', target_path=r'B:\2.脚本')

        # 整理本地目录到完成。剩下文件夹手动检查后，再次运行。处理 target_path 下面的子目录，之后运行 sort_ru 整理。
        # return_dict = sort_local(source_path=r'B:\1.临时', target_path=r'B:\2.脚本')

        # 获取来源目录下所有文件夹名，到目标网站搜索。根据搜索结果把文件夹移动到不同目录下。
        # return_dict = sort_ru(source_path=r'B:\2.脚本', target_path=r'B:\0.整理\结果目录')




        print("完成!")
        pass
    except Exception as e:
        logger.exception(e)
