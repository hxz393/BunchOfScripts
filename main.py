import time
import cProfile

import my_comm
import my_scripts


if __name__ == '__main__':
    logger = my_comm.logging_config(console_output=True, log_level='INFO')

    ########## 正常使用  ##############

    # try:
    #     # # 整理临时文件夹，修改文件夹名中的特殊字符。将修改后的文件夹移动到 target_dir，完成后手动将文件夹从 target_dir 移回 source_dir。
    #     # my_scripts.rename_folder_to_common(source_dir=r'B:\1.临时', target_dir=r'B:\2.脚本')
    #
    #     # # 根据分隔符分割后，检查并移动重复文件夹。完成后手动将文件夹从 target_dir 移回 source_dir。
    #     # my_scripts.move_duplicates(source_dir=r'B:\1.临时', target_dir=r'B:\2.脚本')
    #
    #
    # except Exception as e:
    #     logger.error(e)



    ############### 性能分析 #########
    profiler = cProfile.Profile()
    profiler.enable()

    目标目录 = r'B:\1.临时'
    try:
        返回 = my_comm.get_file_paths(target_path=目标目录)
        # print(返回)
    except Exception as e:
        print(e)

    profiler.disable()
    profiler.print_stats()



