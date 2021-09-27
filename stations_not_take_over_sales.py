"""
将广告组没有接手的站点的销售额等信息存储到redis
"""



#!/usr/bin/env python
# encoding: utf-8
import gc

"""
-----------------
@author: Arron Ramsey
@license: (C) Copyright 2009-2020, Node Supply Chain Manager Corporation Limited.
@contact: zyp1579922399@gmail.com
@file: query_yibai_sku.py
@time: 2020/7/8 9:44
@desc:
-----------------
"""
from datetime import datetime, timedelta
import re
import pandas as pd
import requests
import time
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
from my_toolkit import public_function, sql_write_read
from java_interface import account_id_index


THREAD_NUM = 4
ThreadPool = ThreadPoolExecutor(THREAD_NUM)

"""
通过接口,查询亚马逊站点销售额信息(时间范围内的)
"""


@public_function.loop_func(update_time=14)
def upload_shop_sales_info_2_redis():
    """
    将店铺销售额信息以及没有被广告组接手的店铺销售额信息上传到redis中和mysql中.

    每天更新一次即可
    :return:
    """

    # todo MD5加密,固定的写法
    def encrypt_MD5(params):
        m = hashlib.md5()
        m.update(params.encode("utf-8"))
        return m.hexdigest()

    def request_amazon_shop_sales(date_start, date_end):
        """
        通过接口请求亚马逊店铺的销售额(一定时间范围内)

        开始时间与结束时间不得超过三天
        Parameters
        ----------
        date_start :str
            开始时间,包含此时间
        date_end :str
            结束时间,不包含此时间
        Returns
        -------
            list

        """

        # 输入检验
        if (not isinstance(date_start, str)) or (not isinstance(date_end, str)):
            return
        try:
            date_start_datetype = datetime.strptime(date_start, '%Y-%m-%d').date()
            date_end_datetype = datetime.strptime(date_end, '%Y-%m-%d').date()
        except:
            raise TypeError('输入的格式必须是%Y-%m-%d:例如 2020-05-20.')
        date_interval = (date_end_datetype - date_start_datetype).days
        if date_interval <= 0:
            raise ValueError(f'输入的date_end必须大于输入的date_start.')
        elif date_interval > 3:
            raise ValueError(f'输入的date_end与输入date_start的时间间隔必须在3天之内(包含3天).')

        def natural_key(s):
            return [int(s) if s.isdigit() else s for s in re.split(r'(\d+)', s)]

        params = {
            "date_start": date_start,
            "date_end": date_end,
            "t": int(time.time()),
            "key": "amazon_ad",
            "secret": "b57c#N1!"
        }

        l = sorted(params.items(), key=lambda x: natural_key(x[0]))
        # l = sorted(params.items(), key=lambda x: x[0])
        query = map(lambda x: "{}={}".format(x[0], x[1]), l)
        query_string = '&'.join(list(query))
        sign = encrypt_MD5(query_string).upper()
        params.update({"sign": sign})
        del (params['secret'])

        resp = requests.get("http://salesworkbench.yibainetwork.com:8001/api/v2/statistics/amazon/AccountDailyStatics", params=params)
        # resp = requests.get("http://192.168.94.100:8001/api/v2/statistics/amazon/AccountDailyStatics", params=params)

        return json.loads(resp.text)['data']

    def amazon_shop_sales(date_start, date_end):
        """
        计算亚马逊店铺的销售额(一定时间范围内)
        包含列为:account_id,paytime,amazon_fulfill_channel,order_sum,usd_total_price

        开始时间与结束时间不得超过三天
        Parameters
        ----------
        date_start :str
            开始时间,包含此时间
        date_end :str
            结束时间,不包含此时间
        Returns
        -------
            pd.DataFrame or None
        """
        # 输入检验
        if (not isinstance(date_start, str)) or (not isinstance(date_end, str)):
            return
        try:
            date_start_datetype = datetime.strptime(date_start, '%Y-%m-%d').date()
            date_end_datetype = datetime.strptime(date_end, '%Y-%m-%d').date() - timedelta(days=1)
        except:
            raise TypeError('输入的格式必须是%Y-%m-%d:例如 2020-05-20.')
        date_interval = (date_end_datetype - date_start_datetype).days
        if date_interval < 0:
            raise ValueError(f'输入的date_end必须大于输入的date_start.')
        elif date_interval < 4:
            return pd.DataFrame(request_amazon_shop_sales(date_start, date_end))

        else:
            # 若请求时间大于三天,必须分段请求
            query_date_queue = public_function.Queue()
            query_date_queue.enqueue_items(list(pd.date_range(date_start_datetype, date_end_datetype).date))

            # 四线程请求
            all_result = []
            while 1:
                all_task = []
                if query_date_queue.empty():
                    break
                for _ in range(THREAD_NUM):
                    if query_date_queue.size() >= 3:
                        # 请求站点列表
                        request_date_range = query_date_queue.dequeue_items(3)
                        request_start_date = datetime.strftime(request_date_range[0], '%Y-%m-%d')
                        request_end_date = datetime.strftime(request_date_range[-1] + timedelta(days=1), '%Y-%m-%d')
                        all_task.append(
                            ThreadPool.submit(request_amazon_shop_sales, request_start_date, request_end_date))
                    elif (query_date_queue.size() < 3) and (not query_date_queue.empty()):
                        # 请求站点列表
                        request_date_range = query_date_queue.dequeue_items(query_date_queue.size())
                        request_start_date = datetime.strftime(request_date_range[0], '%Y-%m-%d')
                        request_end_date = datetime.strftime(request_date_range[-1] + timedelta(days=1), '%Y-%m-%d')
                        all_task.append(
                            ThreadPool.submit(request_amazon_shop_sales, request_start_date, request_end_date))
                    else:
                        pass
                for future in as_completed(all_task):
                    result = future.result()
                    if result is not None and result:
                        all_result.extend(result)

        return pd.DataFrame(all_result)

    def stations_not_take_over(only_station_info, account_index):
        """
        没有被广告组接手的站点

        Parameters
        ----------
        account_index : pd.DataFrame
            公司账号id与名称索引
        only_station_info : pd.DataFrame
            广告组接手站点信息
        Returns
        -------

            pd.DataFrame
        """
        # 输入检测
        if (not isinstance(only_station_info, pd.DataFrame)) or (not isinstance(account_index, pd.DataFrame)):
            raise ValueError('only_station_info或account_index 输入的值不是pd.DataFrame.')
        if (len(only_station_info.index) == 0) or (len(account_index.index) == 0):
            raise ValueError('only_station_info或account_index 输入的值为空.')
        station_column_name_only_station_info = 'station'
        station_column_name_account_index = 'account_name'
        if station_column_name_only_station_info not in only_station_info.columns:
            raise ValueError(f'only station info中账号信息列:{station_column_name_only_station_info}不存在.')
        if station_column_name_account_index not in account_index.columns:
            raise ValueError(f'account_index中账号信息列:{station_column_name_account_index}不存在.')
        # 初始化
        # 将需要拼接的账号列转换为小写--账号名
        only_station_info_stations = set(
            [public_function.standardize_station(station_name) for station_name in only_station_info[station_column_name_only_station_info]])
        account_index[station_column_name_account_index] = [public_function.standardize_station(station_name) for station_name in
                                                            account_index[station_column_name_account_index]]

        # 将account index的账号列名设置为索引:加快查询速度
        account_index = account_index.set_index(station_column_name_account_index, drop=False)

        not_taken_account = account_index[~account_index.index.isin(only_station_info_stations)]

        if len(not_taken_account.index) == 0:
            return pd.DataFrame(columns=account_index.columns)
        else:
            return account_index[
                [index not in only_station_info_stations for index in account_index.index]].reset_index(
                drop=True)

    def shop_sales_add_account_name(shop_sales, account_index):
        """
        广告组没有接手的站点销售额情况

        Parameters
        ----------
        shop_sales : pd.DataFrame
            全部站点销售额信息
        account_index :pd.DataFrame
            广告组没有接手的站点id
        Returns
        -------

            pd.DataFrame
            广告组没有接手的站点销售额信息
        """
        # 输入性判断
        if (not isinstance(shop_sales, pd.DataFrame)) or (not isinstance(account_index, pd.DataFrame)):
            raise TypeError('shop_sales的输入类型不是pd.DataFrame,或是stations的输入类型不是pd.DataFrame.')
        if (len(shop_sales.index) == 0) or (len(account_index.index) == 0):
            raise ValueError('shop_sales或是未接手站点信息为空.')
        # 将内连接的列设置为索引
        shop_sales_account_id_column = 'account_id'
        account_index_account_id_column = 'id'
        shop_sales = shop_sales.set_index(shop_sales_account_id_column, drop=False)
        account_index = account_index.set_index(account_index_account_id_column, drop=True)
        return pd.merge(shop_sales, account_index, how='inner', left_index=True, right_index=True).reset_index(
            drop=True)

    THREAD_NUM = 4
    ThreadPool = ThreadPoolExecutor(THREAD_NUM)

    # 计算需要店铺的起始时间
    now_date = datetime.now().date()
    now_date_str = datetime.strftime(now_date,'%Y-%m-%d')
    days_interval = 31
    start_date = now_date-timedelta(days=days_interval)
    start_date_str = datetime.strftime(start_date, '%Y-%m-%d')
    # 获取全部的店铺销售额等信息
    all_shop_sales = amazon_shop_sales(start_date_str,now_date_str)
    if all_shop_sales is None or len(all_shop_sales.index) == 0:
        return
    #添加更新列
    all_shop_sales['update_time'] = now_date_str
    # 获取only_station_info
    _conn_mysql = sql_write_read.QueryMySQL()
    only_station_info = _conn_mysql.read_table('only_station_info')
        # 关闭连接
    _conn_mysql.close()
    # 获取账号索引
    account_index = account_id_index.station_id_index()
    # 得到没有被接手的站点id
    stations_adgroup_not_take_over = stations_not_take_over(only_station_info, account_index)
    # 匹配得到站点名
    shop_sales_adgroup_not_take_over = shop_sales_add_account_name(all_shop_sales, stations_adgroup_not_take_over)

    # 删除掉不和规则的站点(不是以下划线+两位国家简称代号结尾)
    shop_sales_adgroup_not_take_over = shop_sales_adgroup_not_take_over[
        shop_sales_adgroup_not_take_over['account_name'].str.contains('_[a-z]{2}$',case=False,regex=True)]
    shop_sales_adgroup_not_take_over['account_name'] = [account_name.lower() for account_name in shop_sales_adgroup_not_take_over['account_name']]

    # 匹配得到店铺的负责人
    # 添加新站点的广告专员
    accountSet  =  pd.DataFrame([set(shop_sales_adgroup_not_take_over['account_name'])]).T
    accountSet.columns=['account_name']
    accountSet['account_name'] = [account.replace(' ','_').replace('-','_') for account in accountSet['account_name']]
    accountSet['station'] = [station[:-3].lower() for station in accountSet['account_name']]
    only_station_info['account'] = [station[:-3].lower() for station in
                                            only_station_info['station']]

    only_station_info_manager = only_station_info[['account', 'ad_manger']].drop_duplicates(subset=['account'],keep='first')


    # 按之前该账号的站点之前有的被接手的分给该人,则将该账号分给汪维或是张立滨,或是账号没有打广告的,则另外计算
    accountDistribution = pd.merge(accountSet, only_station_info_manager, how='left', left_on=['station'],right_on=['account'])
    # 未接收的店铺先分给人工智能1
    backupManagerList = ['人工智能1','人工智能2','人工智能3','人工智能4','人工智能5','人工智能6']
    backupManager = random.choice(backupManagerList)
    accountDistribution['ad_manger'].fillna(value=backupManager,inplace=True)
    accountDistribution  = accountDistribution[['account_name','ad_manger']]
    acceptTime = datetime.now().strftime('%Y-%m-%d')
    acceptDateTime = datetime.now().strftime('%y-%m-%d %H:%M:%S')
    operatorTimeStr = (datetime.now()-timedelta(days=10)).strftime('%y-%m-%d %H:%M:%S')
    accountDistribution['accept_time'] = acceptTime
    accountDistribution['operator_time'] = operatorTimeStr
    accountDistribution['update_time'] = acceptDateTime
    accountDistribution['company'] = '易佰'
    accountDistribution.rename(columns={'account_name':'station','ad_manger':'ad_manger'},inplace=True)
    accountDistribution.drop_duplicates(inplace=True)
    accountDistributionManger = accountDistribution[~pd.isna(accountDistribution['ad_manger'])]
    # 将信息上传到only_station_info中
    sql_write_read.to_table_append(accountDistributionManger,'only_station_info')
    # 将信息上传到redis中
    ## 全部的redis key
    _conn_redis = public_function.Redis_Store(db=0,decode_responses=False)
    all_redis_key = _conn_redis.keys()
    ## 将站点销售信息上传到redis中,替换到历史的
    shop_sales_redis_sign_key = 'shop_sales'
    ## 首先上传全部站点销售信息
    all_shop_sales_sign_key = shop_sales_redis_sign_key+'_all'
    if all_shop_sales is not None and len(all_shop_sales.index)!=0:
        # 删除历史键
        [_conn_redis.delete(key) for key in all_redis_key if all_shop_sales_sign_key in key.decode()]
        #上传到mysql数据库
        all_shop_sales_mysql_name = 'shop_sales_monthly'
        sql_write_read.to_local_table_replace(all_shop_sales,all_shop_sales_mysql_name)
    ## 上传广告组没有接手站点销售额信息
    all_shop_sales_not_take_over_sign_key = shop_sales_redis_sign_key+'_not_take_over'
    if shop_sales_adgroup_not_take_over is not None and len(shop_sales_adgroup_not_take_over.index)!=0:
        # 删除历史键
        [_conn_redis.delete(key) for key in all_redis_key if all_shop_sales_not_take_over_sign_key in key.decode()]
        # 添加新的键
        all_shop_sales_not_take_over_new_key = all_shop_sales_not_take_over_sign_key + f'_{now_date_str}'
        # 上传到redis中
        _conn_redis.redis_upload_df(all_shop_sales_not_take_over_new_key,shop_sales_adgroup_not_take_over)
        # 上传到mysql数据库
        shop_sales_not_take_over_mysql_name = 'shop_sales_monthly_not_take_over'
        sql_write_read.to_local_table_replace(shop_sales_adgroup_not_take_over,shop_sales_not_take_over_mysql_name)
    ## 关闭redis连接
    _conn_redis.close()
    del account_index
    del all_shop_sales
    del only_station_info
    del shop_sales_adgroup_not_take_over
    del stations_adgroup_not_take_over
    gc.collect()

    print(f"{datetime.now()}:完成更新站点销售额.")


if __name__ == '__main__':
    upload_shop_sales_info_2_redis()