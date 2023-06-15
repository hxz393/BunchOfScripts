import my_comm
import time
if __name__ == '__main__':
    ############### 测试速度 #########
    start_time = time.time()

    大小 = my_comm.get_target_size(r'B:\0.整理\BT\QB下载')
    print(大小)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f'总用时：{elapsed_time:.2f}秒。')

