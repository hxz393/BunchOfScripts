import os
import configparser
from typing import Dict, Any

def config_write(path: str, config: Dict[str, Any]) -> None:
    """
    将配置字典写入配置文件。

    :param path: 配置文件的路径。
    :type path: str
    :param config: 配置字典，其中键为节名，值为包含该节配置项的字典。
    :type config: Dict[str, Any]
    :raises ValueError: 当路径或配置为空时引发。
    :raises Exception: 当尝试写入文件失败时引发。
    """

    # 参数检查
    if not path:
        raise ValueError("路径不能为空")
    if not config:
        raise ValueError("配置不能为空")

    # 创建配置解析器
    config_parser = configparser.ConfigParser()

    # 将配置项添加到配置解析器
    for section, section_config in config.items():
        config_parser[section] = section_config

    # 尝试写入配置文件
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as f:
            config_parser.write(f)
    except Exception as e:
        raise Exception(f"无法写入配置文件 {path}: {str(e)}")


if __name__ == '__main__':
    xxx = "x1x"
    # 使用示例
    config_dict = {
        "section1": {
            "key1": "value1",
            "key2": 1,
        },
        "section2": {
            "keyA": 2.44,
            "keyB": xxx,
        },
    }
    try:
        config_write(r"config/config.ini", config_dict)
    except Exception as e:
        print(e)
