import my_module
import my_scripts


if __name__ == '__main__':
    logger = my_module.logging_config(console_output=True, log_level='INFO')

    ########## 正常使用  ##############

    try:
        # # 整理临时文件夹，修改文件夹名中的特殊字符。将修改后的文件夹移动到 target_path，完成后手动将文件夹从 target_path 移回 source_path。
        # return_dict = my_scripts.rename_folder_to_common(source_path=r'B:\1.临时', target_path=r'B:\2.脚本')
        # for k, v in return_dict.items():
        #     print(f"{k} 移动到 {v}")


        # # 根据分隔符分割后，检查并移动重复文件夹。完成后手动将文件夹从 target_dir 移回 source_dir。
        # return_dict = my_scripts.move_duplicates(source_path=r'B:\1.临时', target_path=r'B:\2.脚本')
        # for k, v in return_dict.items():
        #     print(f"{k} 移动到 {v}")


        pass
    except Exception as e:
        logger.error(e)



    ############## 性能分析 #########
    来源目录 = r'B:\1.临时'
    目标目录 = r'B:\2.脚本'
    要筛选文件类型列表 = ['.mp3', '.flaC']
    删除文件列表 = ['新建 RTF 文档.rtf', '新建... Microsoft PowerPoint Presentation.pptx', '啊123v']
    文本文件 = r'D:\Software\Programming\Python\BunchOfScripts\my_scripts\resources\new.txt'
    try:
        返回 = my_module.get_file_paths(target_path=来源目录)
        # print(返回)
    except Exception as e:
        logger.exception(e)




