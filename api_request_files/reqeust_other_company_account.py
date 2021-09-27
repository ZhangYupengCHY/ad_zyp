#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/6/4 0004 9:18
# @Author  : Zhang YP
# @Email   : 1579922399@qq.com
# @github  :  Aaron Ramsey
# @File    : another_company_account.py


"""
获取其他公司的账号
"""
import os
import zipfile
import pandas as pd
import json
import requests

import process_station,conn_db

anotherCompanyDict = {'cj': '楚晋', 'yunyi': '云翳', 'mxr': '木星人', 'dongyi': "东益", 'jingjia': '景嘉', 'wilson': '恩威逊',
                      'yilj': '伊莱嘉', 'lldz': '郎罗'}


def another_company_account(company_name):
    """
   获取其他公司的账号

    Returns
    -------
        pd.DataFrame()
        columns:id,account_name
    """

    url = r'http://amazon.yibainetwork.com/services/accountsync/run/org_code/%s' % company_name
    response = requests.get(url)
    if response.status_code != 200:
        raise ConnectionError(f'{url}无法连接.')
    data = response.content
    try:
        dataInfo = pd.DataFrame(json.loads(data)['items'])
    except:
        print(data)
    dataAccountDict = process_station.Station.cn_2_en(list(dataInfo['account_name']))
    dataInfo.insert(0,'station' ,[dataAccountDict.get(station,'') for station in dataInfo['account_name']])
    del dataInfo['account_name']
    conn_db.to_sql_append(dataInfo,table=f'{company_name}_station',db='other_company_station')
    return dataInfo


