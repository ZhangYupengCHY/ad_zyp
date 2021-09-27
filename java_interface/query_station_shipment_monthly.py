# -*- coding: utf-8 -*-
"""
Proj: ad_helper
Created on:   2020/7/3 15:24
@Author: RAMSEY

"""

import django_redis
import re
import os
from datetime import datetime
import time
import gc
import sqlalchemy.dialects.mysql as sqlTypes


import requests
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from retrying import retry
from random import randint

from my_toolkit import public_function
from my_toolkit import commonly_params
from my_toolkit import sql_write_read
from my_toolkit import process_files,process_station

"""
近一个月的所有已完成shipment的sku和FBA数量
步骤
1.多线程通过api请求站点的shipment数据
2.将数据上传至数据库中
"""

@public_function.loop_func(update_time=9)
@retry(stop_max_attempt_number=3, wait_fixed=20)
def query_all_station_shipment_monthly():
    """
    请求全部站点一个月到货情况
    :return: None
    """
    print(f'{datetime.now()}开始请求到货情况.')

    THREAD_NUM = 20
    THREAD_POOL = ThreadPoolExecutor(THREAD_NUM)

    def request_monthly_shipped():
        """
        查询站点近一个月是否有完成shipment(到货)
        Returns:pd.DataFrame or None
            若有数据则返回pd.DataFrame,若没有数据则返回None

        """
        # 1.获取接口数据
        base_url = "http://amazon.yibainetwork.com//services/amazon/amazonfbainboundshipments/status/aid/"
        # 判断站点队列是否为空
        if station_queue.empty():
            return
        account_id = station_queue.dequeue()
        request_url = base_url + f'{account_id}' + '/flush/1'
        response = requests.get(url=request_url)
        if response.status_code != 200:
            return
        response_text = json.loads(response.text)
        status_code = response_text['code']
        if status_code != 200:
            return
        sellersku_shipment_info = response_text['detail']['data']
        if sellersku_shipment_info is None:
            return
        account_id = response_text['detail']['account_id']
        account_name = response_text['detail']['account_name']
        # 2.将接口数据转换成DataFrame
        # todo 核查库存有效性以及如何有效利用一个月到货数量数据
        seller_sku_shipment_data = []
        for shipment_id, shipment_data in sellersku_shipment_info.items():
            one_shipment = pd.DataFrame(shipment_data)
            one_shipment['shipmentid'] = shipment_id
            seller_sku_shipment_data.append(one_shipment)
        seller_sku_shipment_data = pd.concat(seller_sku_shipment_data)
        seller_sku_shipment_data['account_id'] = account_id
        seller_sku_shipment_data['account_name'] = account_name
        station_shipment_monthly_queue.enqueue(seller_sku_shipment_data)

    def thread_request_station():
        """
        多线程请求站点每月到货数据
        :return:
        """
        while 1:
            all_task = []
            for _ in range(THREAD_NUM):
                all_task.append(THREAD_POOL.submit(request_monthly_shipped))
            for future in as_completed(all_task):
                future.result()
            if station_queue.empty():
                break

    global station_queue, station_shipment_monthly_queue
    # 账号id列表(将账号范围设置的大一点,为了防止以后账号急速扩张)
    station_list = list(range(1,10000))
    # station_list = list(range(10))
    # 站号队列
    station_queue = public_function.Queue()
    # 站号返回结果队列
    station_shipment_monthly_queue = public_function.Queue()
    # 将站号数据加载到队列中
    [station_queue.enqueue(station) for station in station_list]
    # 多线程站点请求
    thread_request_station()
    # 全部站点一个月到货信息
    if station_shipment_monthly_queue.empty():
        return
    station_shipment_monthly_queue_items = list(filter(lambda x:x is not None,station_shipment_monthly_queue.items))
    all_station_shipment_info = pd.concat(station_shipment_monthly_queue_items)
    # 将中文字段名修改为英文
    # 修改站点名,主要是将站点中特殊字符串剔除
    all_station_shipment_info['account_name'] = all_station_shipment_info['account_name'].apply(
        lambda x: re.sub('[ 站-]', '', x))
    account_name_set = set(all_station_shipment_info['account_name'])
    site_name_zh_set = set([re.sub('[a-zA-Z0-9]', '', account_name) for account_name in account_name_set])
    # 加载目前存在的站点中文名集合,若不存在,则预警
    exist_sites_zh_set = set(public_function.COUNTRY_CN_EN_DICT.keys())
    if not site_name_zh_set.issubset(exist_sites_zh_set):
        new_site = site_name_zh_set - exist_sites_zh_set
        exist_sites_zh_set_path = os.path.abspath(commonly_params.__file__)
        raise ValueError(
            f'NEW SITE CREATE:"{new_site}".PLEASE CHECK OR EDIT {exist_sites_zh_set_path}中country_zh_en_dict字典.')
    for zh_site, en_site in public_function.COUNTRY_CN_EN_DICT.items():
        all_station_shipment_info['account_name'] = all_station_shipment_info['account_name'].apply(
            lambda x: x.replace(zh_site, '_' + en_site).upper())
    # 添加字段:id,station,site,datetime
    all_station_shipment_info['account_name'] = [process_station.standardStation(account_name) for account_name in all_station_shipment_info['account_name']]
    all_station_shipment_info['station'] = all_station_shipment_info['account_name'].apply(lambda x: x[:-3])
    all_station_shipment_info['site'] = all_station_shipment_info['account_name'].apply(lambda x: x[-2:])
    now_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    all_station_shipment_info['updatetime'] = now_datetime
    all_station_shipment_info['id'] = list(range(len(all_station_shipment_info)))
    # 重命名
    all_station_shipment_info.rename(columns={'account_name': 'account'}, inplace=True)
    # 重新排列列名
    all_station_shipment_info = all_station_shipment_info[
        ['id', 'seller_sku', 'qty', 'shipment_status', 'closed_at', 'shipmentid', 'account_id', 'account', 'station',
         'site', 'updatetime']]
    # 只需要到货为非0的信息
    all_station_shipment_info.query("qty != '0'",inplace=True)
    # 将全部站点的信息上传到mysql数据库
    station_shipment_monthly_db_name = 'station_shipment_monthly'
    columnsDtypes = {
        'id':sqlTypes.INTEGER(10),
        'seller_sku':sqlTypes.VARCHAR(255),
        'qty':sqlTypes.MEDIUMINT(8),
        'shipment_status':sqlTypes.VARCHAR(128),
        'closed_at':sqlTypes.DATETIME,
        'shipmentid':sqlTypes.VARCHAR(128),
        'account_id':sqlTypes.MEDIUMINT(8),
        'account':sqlTypes.VARCHAR(255),
        'station':sqlTypes.VARCHAR(255),
        'site':sqlTypes.CHAR(2),
        'updatetime':sqlTypes.DATETIME,
    }
    sql_write_read.to_table_replace(all_station_shipment_info, station_shipment_monthly_db_name,dtype=columnsDtypes,chunksize=1000,
        method='multi')
    # createIndex = """CREATE index `account` on `%s`(`account`)"""% station_shipment_monthly_db_name
    # sql_write_read.commit_sql(createIndex)
    # 上传到redis中
    sql_write_read.Redis_Store(db=0).refresh_df(all_station_shipment_info,station_shipment_monthly_db_name)
    print('===============================================')
    print(f"{datetime.now()}完成请求站点的30天站点到货数据.")
    print('===============================================')
    # 删掉变量
    del all_station_shipment_info
    gc.collect()


if __name__ == '__main__':
    query_all_station_shipment_monthly()
