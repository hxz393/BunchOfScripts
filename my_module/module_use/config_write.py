"""
这是一个Python文件，包含一个函数：`config_write`。

`config_write`函数的目标是将配置字典写入配置文件。如果文件成功写入，它将返回True。如果写入失败，它将返回None。

这个函数接受两个参数：
- `target_path`：配置文件的路径，可以是字符串或`pathlib.Path`对象。
- `config`：配置字典，其中键为节名，值为包含该节配置项的字典。

此文件依赖于以下Python库：
- `pathlib`
- `configparser`
- `logging`
- `typing`

函数使用了日志记录器记录任何在写入过程中发生的错误。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""
import configparser
import logging
from pathlib import Path
from typing import Dict, Any, Union, Optional

logger = logging.getLogger(__name__)


def config_write(target_path: Union[str, Path], config: Dict[str, Union[str, Any]]) -> Optional[bool]:
    """
    将配置字典写入配置文件。

    :param target_path: 配置文件的路径。
    :type target_path: Union[str, Path]
    :param config: 配置字典，其中键为节名，值为包含该节配置项的字典。
    :type config: Dict[str, Any]
    :return: 如果文件成功写入，返回 True；如果写入失败，返回 None。
    :rtype: Optional[bool]
    """
    try:
        if not target_path:
            logger.error("Path should not be empty.")
            return None
        if not config:
            logger.error("Config should not be empty.")
            return None

        config_parser = configparser.ConfigParser()

        for section, section_config in config.items():
            config_parser[section] = {k: str(v) for k, v in section_config.items()}

        path = Path(target_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('w', encoding="utf-8") as f:
            config_parser.write(f)

        return True
    except Exception as e:
        logger.error(f"Failed to write config to file {target_path}: {e}")
        return None
