# 检查是否带有中文字符
import re


def check_contain_chinese(check_str):
    for ch in check_str:
        if u'\u4e00' <= ch <= u'\u9fff':
            return True
    return False


def is_real_name(check_name):
    """
    长度在2-5之间,全中文
    :param check_name:
    :return:
    """
    if not isinstance(check_name,str):
        return False
    if len(check_name) <2 or len(check_name) >5:
        return False
    for ch in check_name:
        if u'\u4e00' > ch or ch > u'\u9fff':
            return False
    return True


def extract_chinese(extract_str):
    """
    提取字符串中的中文
    :param extract_str:
    :return:
    """
    pre = re.compile(u'[\u4e00-\u9fa5]')
    res = re.findall(pre, extract_str)
    return ''.join(res)


def filter_chinese(extract_str):
    """
    提取字符串中的中文
    :param extract_str:
    :return:
    """
    pre = re.compile(u'[\u4e00-\u9fa5]')
    res = re.sub(pre,'', extract_str)
    return ''.join(res)

