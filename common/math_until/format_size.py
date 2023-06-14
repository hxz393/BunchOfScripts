from typing import Union

def format_size(size: Union[int, float], is_disk: bool = False, precision: int = 2) -> str:
    """
    将字节单位的文件或磁盘大小转换为易于理解的格式（KB, MB, GB等）。

    :param size: 文件或磁盘的大小，单位为字节。
    :type size: Union[int, float]
    :param is_disk: 是否是磁盘大小（如果是磁盘大小，则使用1000作为单位换算，否则使用1024）。
    :type is_disk: bool, default False
    :param precision: 转换后的数值的精度（小数点后的位数）。
    :type precision: int, default 2
    :raise TypeError: 如果输入的大小不是浮点数或整数。
    :raise ValueError: 如果输入的大小是负数。
    :return: 格式化后的文件或磁盘大小（字符串格式）。
    """
    # 单位换算列表
    format_list = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
    # 确定单位换算的基数
    unit = 1000.0 if is_disk else 1024.0

    # 检查 size 的类型和值
    if not isinstance(size, (float, int)):
        raise TypeError(f"输入的大小 {size} 类型应为浮点数或整数")
    if size < 0:
        raise ValueError(f"输入的大小 {size} 不应为负数")

    for fmt in format_list:
        # 仅当 size 小于单位时进行下一步，否则结束循环
        if size < unit:
            return f'{round(size, precision)} {fmt}'
        size /= unit  # 更新 size 的值为 size 除以 unit


if __name__ == '__main__':
    try:
        print(format_size(150))  # 输出：'150 Bytes'
        print(format_size(1550))  # 输出：'1.51 KB'
        print(format_size(1049026))  # 输出：'1.0 MB'
        print(format_size(1073741824, is_disk=True))  # 输出：'1.07 GB'
    except Exception as e:
        print(e)
