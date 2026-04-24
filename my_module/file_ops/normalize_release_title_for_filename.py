"""
发布标题到文件名片段的通用规范化函数。
"""
import re
from collections.abc import Iterable


def normalize_release_title_for_filename(
        title: str,
        max_length: int = 220,
        replace_pipe: bool = True,
        replace_placeholder_dot: bool = True,
        extra_cleanup_patterns: Iterable[str] | None = None,
) -> str:
    """
    规范化发布标题，供 ``sanitize_filename`` 前使用。

    :param title: 原始标题
    :param max_length: 标题部分最大长度
    :param replace_pipe: 是否把 ``|`` 替换成全角逗号
    :param replace_placeholder_dot: 是否把 ``{@}`` 替换成 ``.``
    :param extra_cleanup_patterns: 额外的正则清理规则
    :return: 规范化后的标题
    """
    normalized = title
    if replace_pipe:
        normalized = re.sub(r'\s*\|\s*', '，', normalized)
    normalized = re.sub(r'\s*/\s*', '｜', normalized)
    normalized = re.sub(r'\s*\\\s*', '｜', normalized)
    normalized = normalized.replace("\t", " ")
    normalized = re.sub(r'\s+', ' ', normalized)

    if extra_cleanup_patterns:
        for pattern in extra_cleanup_patterns:
            normalized = re.sub(pattern, '', normalized)

    if replace_placeholder_dot:
        normalized = normalized.replace("{@}", ".")

    normalized = normalized.strip()
    if len(normalized) <= max_length:
        return normalized

    return normalized[:max_length]
