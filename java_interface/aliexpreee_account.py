#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/3/31 0031 17:31
# @Author  : Zhang YP
# @Email   : 1579922399@qq.com
# @github  :  Aaron Ramsey
# @File    : query_aliexpress_account.py
"""
接口查询速卖通全部账号
"""
from datetime import datetime


import json
import requests
import pandas as pd


import conn_db,public_function


@public_function.loop_func(update_time=14)
def query_aliexpress_account() -> 'dict':
    """
    通过接口获取接口数据:
        这里特定指定通过接口查询速卖通
    查询方式：
        接口文档地址:
            http://erppub.yibainetwork.com/services/api/aliexpress/accountlist
    """
    page = 1
    allAccountInfo = pd.DataFrame()
    while 1:

        request_url = "http://erppub.yibainetwork.com/services/api/aliexpress/accountlist"
        params={
            "page":page,
        }
        # 通过ali_sku查询erpsku
        response = requests.get(request_url,params=params)
        if response.status_code != 200:
            raise ConnectionError(f'{request_url} status code is {response.status_code}.')
        response = json.loads(response.content)
        accountInfo = pd.DataFrame.from_dict(response['data']['data'])
        if len(accountInfo.index) == 0:
            break
        page+=1
        allAccountInfo = pd.concat([allAccountInfo,accountInfo])
    if len(allAccountInfo.index) >0:
        # 添加当前时间信息
        allAccountInfo['updatetime'] = datetime.now().replace(microsecond=0)
        # 替换数据库中的账号信息
        tableName = "ali_express_account"
        # 首先删除数据库中的记录
        conn_db.to_sql_delete(f"delete from {tableName}")
        # 添加信息
        conn_db.to_sql_append(allAccountInfo,tableName)


if __name__ == '__main__':
    query_aliexpress_account()