import configparser
from typing import Optional

def config_read(path: str) -> Optional[configparser.ConfigParser]:
    """
    从指定的路径读取配置文件。

    :param path: 配置文件的路径。
    :type path: str
    :return: configparser.ConfigParser 对象，如果文件读取失败，则返回 None。
    :rtype: Optional[configparser.ConfigParser]
    """

    # 创建配置解析器
    config_parser = configparser.ConfigParser()

    # 尝试读取配置文件
    try:
        with open(path, 'r') as f:
            config_parser.read_file(f)
    except FileNotFoundError:
        print(f"未找到配置文件 {path}")
        return None
    except Exception as e:
        print(f"无法读取配置文件 {path}: {str(e)}")
        return None

    return config_parser

if __name__ == '__main__':
    # 使用示例
    try:
        config_parser = config_read(r"config/config.ini")
        if config_parser is not None:
            for section in config_parser.sections():
                print(f"{section}:")
                for key, value in config_parser.items(section):
                    print(f"  {key} = {value}")
    except Exception as e:
        print(e)
