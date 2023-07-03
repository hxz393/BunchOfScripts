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
        # 破解煎蛋网站图片防盗链。启动后通过本地代理端口 10808 访问。
        # jandan_net()

        # XDGAME百度网盘下载地址获取
        www_xdgame_com()


        ########## 整理 ##############
        # 整理临时文件夹，修改文件夹名中的特殊字符。将修改后的文件夹移动到 target_path，完成后手动将文件夹从 target_path 移回 source_path。
        # return_dict = rename_folder_to_common(source_path=r'B:\1.临时', target_path=r'B:\2.脚本')

        # 根据分隔符分割后，检查并移动重复文件夹。完成后手动将文件夹从 target_dir 移回 source_dir。
        # return_dict = move_duplicates(source_path=r'B:\1.临时', target_path=r'B:\2.脚本')




        if return_dict:
            for k, v in return_dict.items():
                print(f"{k} 移动到 {v}")
        else:
            print('不需要打印')
        pass
    except Exception as e:
        logger.exception(e)
