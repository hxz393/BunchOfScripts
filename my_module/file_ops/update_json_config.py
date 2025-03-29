"""
更新 JSON 配置文件中的某个键的值。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393。保留所有权利。
"""
import json


def update_json_config(file_path: str, key: str, new_value: str) -> None:
    """
    更新 JSON 配置文件中的某个键的值。

    :param file_path: JSON 配置路径
    :param key: 键
    :param new_value: 值
    :return: 无
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        config = json.load(f)  # config 是个 dict

    config[key] = new_value

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
