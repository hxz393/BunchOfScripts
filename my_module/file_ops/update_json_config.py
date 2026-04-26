"""
更新 JSON 配置文件中的某个键的值。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393。保留所有权利。
"""
import json
import os
import threading
from typing import Any

CONFIG_LOCK = threading.Lock()


def update_json_config(file_path: str, key: str | list[str] | tuple[str, ...], new_value: Any) -> None:
    """
    更新 JSON 配置文件中的某个键的值。

    :param file_path: JSON 配置路径
    :param key: 键或键路径。传 ``"foo"`` 更新顶层键，传
        ``["parent", "child"]`` 更新嵌套键。
    :param new_value: 任意可被 JSON 序列化的值，例如字符串、数字、列表或字典
    :return: 无
    """
    key_path = [key] if isinstance(key, str) else list(key)
    if not key_path:
        raise ValueError("key 不能为空")

    temp_file_path = f"{file_path}.tmp"
    with CONFIG_LOCK:
        with open(file_path, 'r', encoding='utf-8') as f:
            config = json.load(f)  # config 是个 dict

        target = config
        for path_key in key_path[:-1]:
            child = target.get(path_key)
            if not isinstance(child, dict):
                child = {}
                target[path_key] = child
            target = child

        target[key_path[-1]] = new_value

        with open(temp_file_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)

        os.replace(temp_file_path, file_path)
