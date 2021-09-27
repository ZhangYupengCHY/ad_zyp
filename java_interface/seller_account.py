#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/3/24 0024 15:15
# @Author  : Zhang YP
# @Email   : 1579922399@qq.com
# @github  :  Aaron Ramsey
# @File    : seller_account.py

"""
更新全部销售负责的站点
"""



import requests
import json
import time
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import gc

import pandas as pd


from my_toolkit import sql_write_read,public_function


"""
通过java接口,获取销售负责的站点
"""


@public_function.loop_func(update_time=9)
def update_all_sellers_account(monthly_update_day=10):
    """
    更新全部销售负责的站点(每月10号更新一次):来源是通过接口获取的销售的初始业绩表
    Returns
    -------

    """

    def query_seller_account() -> 'dict':
        """
        获取销售负责的账号
        """
        # 测试环境
        # request_url = "http://192.168.71.128:82/services/account_user_api/getAmazonUserData"
        # 线上环境
        request_url = "http://oa.yibainetwork.com/services/account_user_api/getAmazonUserData"
        secret = 'wvNCLiypU6i3'
        timestamp = str(int(time.time()))
        token = hashlib.md5((timestamp + secret).encode()).hexdigest()
        if sellerWorkNumQueue.empty():
            return
        user_code = sellerWorkNumQueue.dequeue()
        if len(user_code) == 0:
            return
        params = {
            'appid': 8,
            'user_code': user_code,
            'timestamp': timestamp,
            'token': token,
        }
        try:
            response = requests.post(request_url, data=params, headers={'Connection':'close'},timeout=(10,60))
        except Exception as e:
            print(e)
            return
        if response.status_code != 200:
            print(f'{request_url} status code is {response.status_code}.')
            return
        status = json.loads(response.text)['status']
        if status == 2:
            print(f'{user_code}:{json.loads(response.text)}')
            return
        sellerAccountInfo = pd.DataFrame(json.loads(response.text)['data'])
        if len(sellerAccountInfo.index) == 0:
            return
        else:
            sellerAccountInfo['user_num'] = user_code
            sellerMangerStationQueue.enqueue(sellerAccountInfo)
            print(f'成功:{user_code}')
            return

    def thread_request_station():
        """
        多线程请求站点每月到货数据
        :return:
        """
        while 1:
            all_task = []
            for _ in range(THREAD_NUM):
                all_task.append(ThreadPool.submit(query_seller_account))
            for future in as_completed(all_task):
                future.result()
            if sellerWorkNumQueue.empty():
                print('全部销售的请求完.')
                break

    def query_all_work_num():
        """
        获取全部人员的工号:从nickname数据库中提取全部人员的工号
        Returns
        -------
        """
        _connMysql = sql_write_read.QueryMySQL()
        nickNameTable = 'nickname'
        nickNameSql = 'select work_number from %s' % nickNameTable
        workNumInfo = _connMysql.read_table(nickNameTable, nickNameSql, columns=['work_number'])
        _connMysql.close()
        return set(workNumInfo['work_number'])

    # 每月的十号更新完
    if datetime.now().day != monthly_update_day:
        return

    THREAD_NUM = 8
    ThreadPool = ThreadPoolExecutor(THREAD_NUM)
    global sellerMangerStationQueue,sellerWorkNumQueue
    allWorkNum = query_all_work_num()
    # # 加上大写
    # allWorkNumUpper = set([num.upper() for num in allWorkNum])
    # allWorkNum.update(allWorkNumUpper)
    # 请求的员工工号
    sellerWorkNumQueue = public_function.Queue()
    sellerWorkNumQueue.enqueue_items(allWorkNum)
    # 员工返回的结果
    sellerMangerStationQueue = public_function.Queue()
    # 多线程请求站点数据
    thread_request_station()
    if sellerMangerStationQueue.items is None:
        return
    sellerMangerStationInfo = pd.concat(sellerMangerStationQueue.items,ignore_index=True)
    sellerMangerStationInfo.rename(columns={'id':'account_id'},inplace=True)
    sellerMangerStationInfo.drop_duplicates(inplace=True)
    sellerMangerStationInfo['update_time'] = datetime.now().replace(microsecond=0)
    # 将初始业绩表中的工号统一化
    sellerMangerStationInfo['user_num'] = [public_function.standardize_user_number(num) for num in sellerMangerStationInfo['user_num']]
    sellerMangerStationInfo['job_number'] = sellerMangerStationInfo['user_num']
    # 更新销售负责的站点数据库
    # 将用户信息上传到数据库中
    tableName = 'erp_seller_account'
    # 更新掉有初始业绩表的销售
    allSeller = set(sellerMangerStationInfo['job_number'])
    allSellerStr = sql_write_read.query_list_to_str(allSeller)
    try:
        deleteSql = 'delete from `%s` where `job_number` in (%s)'%(tableName,allSellerStr)
        # 先删除站点数据
        sql_write_read.commit_sql(deleteSql)
        sql_write_read.to_table_append(sellerMangerStationInfo,tableName)
        print(f'更新成功:销售负责的站点. {datetime.now().replace(microsecond=0)}')
    except Exception as e:
        print(e)
        print(f'更新失败:销售负责的站点. {datetime.now().replace(microsecond=0)}')

    del allWorkNum,sellerMangerStationInfo,sellerMangerStationQueue,sellerWorkNumQueue
    gc.collect()


if __name__ == '__main__':
    update_all_sellers_account()

