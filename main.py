import common
import time

if __name__ == '__main__':
    start_time = time.time()

    大小 = common.get_target_size(r'B:\1.临时')
    print(大小)


    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f'总用时：{elapsed_time:.2f}秒。')

