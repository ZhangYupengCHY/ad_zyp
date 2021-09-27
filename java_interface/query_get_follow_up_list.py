"""
获取跟卖信息
"""
import re
import requests
import json
from requests.auth import HTTPBasicAuth
import time
import hashlib
import pandas as pd
import sqlalchemy
from sqlalchemy.dialects import mysql as sqlType
from datetime import datetime
import gc

from my_toolkit import my_api, sql_write_read


# 获取计划系统的token
def api_get_follow_up(url=r'http://rest.java.yibainetwork.com/mrp/yibaiHttpShipment/getFollowUpList', limit=2000):
    def get_info_from_follow(url, params, jsonParams):
        response = requests.post(url, params=params, json=jsonParams)
        if response.status_code == 200:
            return json.loads(response.content)['data_list']

    token = my_api.CompanyApiBase.get_java_token()
    params = {
        'access_token': token
    }
    # 首先通过接口获取总的数据条数
    # 按200每次来限定请求页数
    page = 1
    allInfo = []
    while 1:
        jsonParmas = {
            'page': page,
            'limit': limit
        }
        pageInfo = get_info_from_follow(url, params, jsonParmas)
        if pageInfo:
            allInfo.extend(pageInfo)
            page += 1
        else:
            break
    allInfo = pd.DataFrame(allInfo)
    allInfo['update_time'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
    # 将信息存储到数据库中
    columnsDtypes = {
        'id': sqlType.INTEGER(10),
        'group_id': sqlType.MEDIUMINT(10),
        'from_account_id': sqlType.MEDIUMINT(10),
        'to_account_id': sqlType.MEDIUMINT(10),
        'md5_from': sqlType.VARCHAR(128),
        'md5_to': sqlType.VARCHAR(128),
        'group_name': sqlType.VARCHAR(128),
        'from_account_name': sqlType.VARCHAR(128),
        'from_seller_sku': sqlType.VARCHAR(256),
        'from_fnsku': sqlType.VARCHAR(256),
        'to_account_name': sqlType.VARCHAR(128),
        'to_seller_sku': sqlType.VARCHAR(256),
        'to_fnsku': sqlType.VARCHAR(256),
        'created_uid': sqlType.VARCHAR(128),
        'updated_uid': sqlType.VARCHAR(128),
        'approved_uid': sqlType.VARCHAR(128),
        'created_name': sqlType.VARCHAR(256),
        'updated_name': sqlType.VARCHAR(256),
        'approved_name': sqlType.VARCHAR(256),
        'remark': sqlType.VARCHAR(128),
        'discard_reason': sqlType.VARCHAR(128),
        'from_asin': sqlType.CHAR(10),
        'from_station_code': sqlType.CHAR(2),
        'is_to_stock_sku': sqlType.TINYINT(1),
        'is_freeze': sqlType.TINYINT(1),
        'approval_status': sqlType.TINYINT(1),
        'validate_status': sqlType.TINYINT(1),
        'discard_status': sqlType.TINYINT(1),
        'validate_number': sqlType.TINYINT(1),
        'created_at': sqlType.DATETIME,
        'updated_at': sqlType.DATETIME,
        'approved_at': sqlType.DATETIME,
        'update_time': sqlType.DATETIME,
    }
    sql_write_read.to_local_table_replace(allInfo, 'sku_follow_up', dtype=columnsDtypes)
    del allInfo
    gc.collect()
    time.sleep(24*3600)


if __name__ == '__main__':
    api_get_follow_up()
