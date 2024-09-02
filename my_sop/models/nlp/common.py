import re
import unicodedata
from html import unescape


def text_normalize(text, to_lower=True):
    """所有字符串数据标准化"""
    text = unescape(text)  # 网络符号转义："Health D&#39;licious" -> "Health D'licious"
    text = re.sub('[ ]+', ' ', re.sub('[\v\r\n\t]+', ' ', text))  # 去除制表符
    text = unicodedata.normalize("NFKC", text)  # 全角转半角
    text = text.strip()  # 去首位空格并小写
    return text.lower() if to_lower else text
