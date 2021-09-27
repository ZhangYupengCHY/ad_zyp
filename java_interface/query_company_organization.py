#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/2/18 0018 9:14
# @Author  : Zhang YP
# @Email   : 1579922399@qq.com
# @github  :  Aaron Ramsey
# @File    : query_high_quality_erpsku.py


import requests
import json
import time
import hashlib
from datetime import datetime

import pandas as pd

from my_toolkit import sql_write_read, conn_db, public_function

"""
通过java接口,获取公司的组织架构
"""



def query_whole_company_organization():


    def query_company_organization() -> 'dict':
        """
        获取公司的组织架构
        """
        request_url = "http://oa.yibainetwork.com//services/account_user_api/getChangeAccount"
        secret = 'wvNCLiypU6i3'
        timestamp = str(int(time.time()))
        token = hashlib.md5((timestamp + secret).encode()).hexdigest()
        params = {
            'appid': 8,
            'timestamp': timestamp,
            'token': token,
        }
        response = requests.post(request_url, data=params)
        if response.status_code != 200:
            raise ConnectionError(f'{request_url} status code is {response.status_code}.')
        status = json.loads(response.text)['status']
        if status == 2:
            return False
        organizationDF = pd.DataFrame(json.loads(response.text)['list'])
        nowDatatime = datetime.strftime(datetime.now().replace(microsecond=0), format('%Y-%m-%d %H:%M:%S'))
        if len(organizationDF.index) > 0:
            organizationDF['update_time'] = nowDatatime
        else:
            return False
        # 将工号全部规范化
        organizationDF['user_number'] = [public_function.standardize_user_number(num,case='lower') for num in organizationDF['user_number']]

        # 将用户信息上传到数据库中
        tableName = 'company_organization'
        # 更新人员情况:先删除,再添加:不删除,直接添加
        # userList = list(organizationDF['user_number'])
        # userStr = sql_write_read.query_list_to_str(userList)
        # sql = 'delete from %s where user_number in (%s)' % (tableName, userStr)
        # conn_db.to_sql_delete(sql)
        conn_db.to_sql_append(organizationDF, tableName)


        if not organizationDF.empty:
            updateStatus(json.dumps(list(organizationDF['user_number'])), timestamp)
            return True
        else:
            return False

    def updateStatus(user_number, timestamp):
        """
        回传标记更新取值状态
        Returns
        -------

        """
        request_url = "http://oa.yibainetwork.com/services/account_user_api/updateStatus"
        secret = 'wvNCLiypU6i3'
        token = hashlib.md5((timestamp + secret).encode()).hexdigest()
        params = {
            'appid': 8,
            'user_number_list': user_number,
            'timestamp': timestamp,
            'token': token,
        }

        response = requests.post(request_url, data=params)
        if response.status_code != 200:
            raise ConnectionError(f'{request_url} status code is {response.status_code}.')


    while 1:
        continueQuery = query_company_organization()
        if continueQuery:
            print(f'{datetime.now().replace(microsecond=0)}:更新完成北森系统中的销售权限')
        else:
            print(f'{datetime.now().replace(microsecond=0)}:北森系统中暂无可更新的销售权限')
        time.sleep(7200)


if __name__ == '__main__':
    query_whole_company_organization()
