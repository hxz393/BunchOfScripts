import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Union

logger = logging.getLogger(__name__)

def write_dict_to_json(target_path: Union[str, Path], data: Dict[str, Any]) -> Optional[bool]:
    """
    将字典数据写入到 JSON 格式文件。

    :param target_path: Json文件的路径，可以是字符串或 pathlib.Path 对象。
    :type target_path: Union[str, Path]
    :param data: 要写入的字典数据。
    :type data: Dict[str, Any]
    :return: 成功时返回True，失败时返回None。
    :rtype: Optional[bool]
    """
    try:
        target_path = Path(target_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)

        with target_path.open('w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        logger.error(f"An error occurred while writing to the JSON file at '{target_path}': {e}")
        return None
