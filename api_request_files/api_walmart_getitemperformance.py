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
import os
import re
import time
from datetime import datetime
import zipfile
import requests

from retrying import retry
import pandas as pd
import base64
from sqlalchemy import create_engine

"""
查询速卖通中站点的产品信息
"""


@retry(stop_max_attempt_number=3, wait_fixed=0.5)
def get_item_performance(account_name) -> pd.DataFrame:
    """
    查询walmaret账号表现


    """

    def get_sign(timestamp):
        """
        获取标注
        Returns
        -------

        """
        secret = 'YThlM2Y1NjI0ODQyYTQzMGI2MDllZWUzYmQ5NjVjYzY='
        time = {"time": timestamp}
        signStr = json.dumps(time) + secret
        signStr = signStr.replace(' ', '')
        return hashlib.md5(signStr.encode('utf8')).hexdigest()

    nowTime = int(time.time()) + 1
    sign = get_sign(nowTime)
    # 获取输入的账号的简称
    walmartAccountExtendSign = 'walmart-'
    if account_name.startswith(walmartAccountExtendSign):
        shortAccount = account_name.replace(walmartAccountExtendSign,'')
    else:
        shortAccount = account_name
    # 判断输入的简称是否在全部的walmart账号中
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(
        'aduser', 'aduser2021', '127.0.0.1', 3306, 'team_station', 'utf8'))
    sql = 'select * from walmart_account'
    conn = engine.connect()
    df = pd.read_sql(sql, conn)
    allWalmartAccountShortName = df['account']
    if shortAccount not in allWalmartAccountShortName.values:
        raise ValueError(f'输入的账号不存在:{shortAccount}')

    accountName = walmartAccountExtendSign+shortAccount
    params = {
        'time': str(nowTime),
        "sign": sign,
        'file_type': '2',
        'account': account_name,
    }
    request_url = r'http://smallplatformapi.yibainetwork.com/services/walmart/api/getitemperformance'
    requests_data = requests.get(url=request_url, params=params)
    if requests_data.status_code != 200:
        status_code = requests_data.status_code
        raise ConnectionError(f'can not connect request brand interface {request_url}.ERROR STATUS CODE: {status_code}')
    response_text = json.loads(requests_data.text)
    content = base64.b64decode(response_text['data']['file_data'])
    AccountSaveFolder = r"D:\待处理\{}".format(shortAccount)
    if not os.path.exists(AccountSaveFolder):
        os.mkdir(AccountSaveFolder)
    saveBasePath = f'{shortAccount}_temp.zip'
    savePath = os.path.join(AccountSaveFolder,saveBasePath)
    with open(savePath, mode='wb') as f:
        f.write(content)
    unzip_file(savePath)
    # 重命名
    for file in os.listdir(AccountSaveFolder):
        if 'ItemPerformanceReport' in file:
            try:
                os.rename(os.path.join(AccountSaveFolder,file),os.path.join(AccountSaveFolder,shortAccount+'_'+file.replace('ItemPerformanceReport','financial data')))
            except:
                os.remove(os.path.join(AccountSaveFolder,file))
                continue
    # 删除临时压缩文件
    if os.path.exists(savePath):
        os.remove(savePath)

def unzip_file(zipFilePath,saveFolder=None):
    """
    将压缩文件解压到指定文件夹中
    Parameters
    ----------
    zipFilePath :
    saveFolder :

    Returns
    -------

    """
    zip_file = zipfile.ZipFile(zipFilePath)
    zip_list = zip_file.namelist()  # 得到压缩包里所有文件
    if saveFolder is None:
        saveFolder = os.path.dirname(zipFilePath)
    if not os.path.exists(saveFolder):
        os.mkdir(saveFolder)
    for f in zip_list:
        zip_file.extract(f, saveFolder)  # 循环解压文件到指定目录
    zip_file.close()


if __name__ == '__main__':
    get_item_performance('walmart-M76W')

