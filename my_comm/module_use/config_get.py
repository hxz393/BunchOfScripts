from typing import Union
import configparser

def config_get(config_parser: configparser.ConfigParser, section: str, key: str, default_value: str) -> Union[int, str, float]:
    """
    从配置文件中获取指定的配置项的值。

    :param config_parser: 配置解析器对象。
    :type config_parser: configparser.ConfigParser
    :param section: 配置项所在的区域名。
    :type section: str
    :param key: 配置项的键名。
    :type key: str
    :param default_value: 当指定的配置项不存在时，返回的默认值。
    :type default_value: str
    :return: 返回指定配置项的值，如果该配置项不存在，则返回默认值。
    :rtype: Union[int, str, float]
    """
    def is_float(s: str) -> bool:
        """判断字符串是否可以转化为浮点数。"""
        try:
            float(s)
            return True
        except ValueError:
            return False

    def get_typed_default_value(default_value: str) -> Union[int, str, float]:
        """将默认值字符串转化为对应的数值类型。"""
        if default_value.isdigit():
            return int(default_value)
        elif is_float(default_value):
            return float(default_value)
        else:
            return default_value


    # 尝试获取配置项的值
    value = config_parser.get(section, key, fallback=None)
    if value is None:
        return get_typed_default_value(default_value)

    # 根据默认值的类型，转换配置项的值的类型
    if default_value.isdigit():
        return int(value) if value.isdigit() else get_typed_default_value(default_value)
    elif is_float(default_value):
        return float(value) if is_float(value) else get_typed_default_value(default_value)
    else:
        return value


if __name__ == '__main__':
    # 使用示例
    config_parser = configparser.ConfigParser()
    config_parser.read(r"config/config.ini")
    print(config_get(config_parser, "section1", "key1", 'default'))  # 获取指定配置项的值，如果该配置项不存在，则返回 "default"
    print(type(config_get(config_parser, "section2", "keyb", "2.2")))  # 如果默认值和配置项的值类型不匹配，返回默认值