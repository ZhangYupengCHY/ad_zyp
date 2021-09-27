#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/5/3 0003 15:54
# @Author  : Zhang YP
# @Email   : 1579922399@qq.com
# @github  :  Aaron Ramsey
# @File    : api_walmart_getitemperformance.py


# -*- coding: utf-8 -*-
"""
Proj: ad_helper
Created on:   2020/7/2 14:36
@Author: RAMSEY

"""
import hashlib
import json
import re
import time
from datetime import datetime

import requests

from retrying import retry
import pandas as pd

from my_toolkit import chinese_check,public_function,sql_write_read

"""
api获取楚晋的亚马逊账号
"""


@public_function.loop_func(update_time=10)
@retry(stop_max_attempt_number=3, wait_fixed=0.5)
def get_cj_amazon_account() -> pd.DataFrame:
    """
    api获取楚晋的亚马逊账号


    """

    def get_sign(random,nowTime):
        """
        获取标注
        Returns
        -------

        """
        secret = 'g9FXGC*/2KG+rk1mjj6WC&exnpGoECAwEAAQ=k!'
        return hashlib.md5((random+nowTime+secret).encode()).hexdigest()


    randomStr = 'ramsey'
    nowTime = str(int(time.time()))
    sign = get_sign(randomStr,nowTime)
    params = {
        'str': randomStr,
        'time':nowTime,
        "sign": sign,
        'org_code':'org_00009'
    }
    # todo 测试环境
    # request_url = r'http://dp.yibai-it.com:33106/chuxun/api/account_list'
    request_url = r'http://121.37.24.230:81/chuxun/api/account_list'
    requests_data = requests.get(url=request_url, params=params)
    if requests_data.status_code != 200:
        status_code = requests_data.status_code
        raise ConnectionError(f'can not connect request brand interface {request_url}.ERROR STATUS CODE: {status_code}')
    response_text = json.loads(requests_data.text)
    responseInfo = response_text['data_list']
    if responseInfo:
        cjAccountInfo = pd.DataFrame(responseInfo)
        cjAccountInfo['account_name_cn'] = cjAccountInfo['account_name'].copy()
        cjAccountInfo['site'] = [public_function.COUNTRY_CN_EN_DICT.get(chinese_check.extract_chinese(account).replace('站', ''), None)
                                 for account in cjAccountInfo['account_name']]
        cjAccountInfo['station'] = [chinese_check.filter_chinese(account) if ~pd.isna(account) else "" for account in
                                    cjAccountInfo['account_name']]
        cjAccountInfo['station'] = [public_function.standardize_station(station) for station in cjAccountInfo['station']]
        cjAccountInfo['account_name'] = cjAccountInfo['station'].str.lower() + '_' + cjAccountInfo['site'].str.lower()
    # 添加更新时间列
    cjAccountInfo['update_datetime'] = datetime.now().replace(microsecond=0)
    # 将请求结果上传到Mysql数据库中
    accountShortNameMysqlTable = 'cj_amazon_account'
    sql_write_read.to_table_replace(cjAccountInfo,accountShortNameMysqlTable)


if __name__ == '__main__':
    get_cj_amazon_account()