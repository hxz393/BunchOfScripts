import time

import my_comm
import my_scripts


if __name__ == '__main__':
    logger = my_comm.logging_config(console_output=True, log_level='INFO')

    ########## 正常使用  ##############

    try:
        my_scripts.create_folders_batch(
            file=r"E:\undone\待下\1.txt",
            target_directory=r"B:\2.脚本\3"
        )
    except Exception as e:
        logger.error(e)



    ############### 测试速度 #########
    start_time = time.time()

    大小 = my_comm.get_target_size(r'B:\0.整理\BT\QB下载')
    print(大小)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f'总用时：{elapsed_time:.2f}秒。')

