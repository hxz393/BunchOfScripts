import os
import configparser
from typing import Dict, Any, Union

def config_write(target_path: str, config: Dict[str, Union[str, Any]]) -> None:
    """
    将配置字典写入配置文件。

    :param target_path: 配置文件的路径。
    :type target_path: str
    :param config: 配置字典，其中键为节名，值为包含该节配置项的字典。
    :type config: Dict[str, Any]
    :raises ValueError: 当路径或配置为空时引发。
    :raises Exception: 当尝试写入文件失败时引发。
    """
    if not target_path:
        raise ValueError("Path should not be empty.")
    if not config:
        raise ValueError("Config should not be empty.")

    # Create a new ConfigParser object
    config_parser = configparser.ConfigParser()

    # Add sections and options from config dict to ConfigParser object
    for section, section_config in config.items():
        config_parser[section] = {k: str(v) for k, v in section_config.items()}

    try:
        # Create directory if not exists
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        # Write config to file
        with open(target_path, 'w') as f:
            config_parser.write(f)
    except Exception as e:
        raise Exception(f"Failed to write config to file {target_path}: {e}")

if __name__ == '__main__':
    xxx = True
    yyy = None
    config = {
        "DEFAULT": {
            "key3": "value3",
            "keyc": 0,
        },
        "section1": {
            "key1": "value1",
            "key2": 1,
        },
        "section2": {
            "keyA": 2.44,
            "keyB": xxx,
            "keyC": yyy,
        },
    }
    target_path = r"config/config.ini"
    try:
        config_write(target_path=target_path, config=config)
    except Exception as e:
        print(e)

