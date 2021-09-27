#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/7/22 0022 14:57
# @Author  : Zhang YP
# @Email   : 1579922399@qq.com
# @github  :  Aaron Ramsey
# @File    : ad_sku_have_ordered.py

import os
from datetime import datetime, timedelta
import warnings
import re
import time
import shutil

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed,ProcessPoolExecutor

import change_rate
from my_toolkit import public_function
from my_toolkit import process_files
from my_toolkit import commonly_params
from my_toolkit import init_station_report
from my_toolkit import sql_write_read

THREAD_POOL = ThreadPoolExecutor(16)
# PROCESS_POOL = ProcessPoolExecutor(8)

"""
    将站点的搜索词报告上传到服务器中:每两个月更新一次
"""

warnings.filterwarnings(action='ignore')
EXCHANGE_RATE = change_rate.change_current()


def find_new_station(date_range=1) -> list:
    """
    找到需要更新站点列表
    Args:
        date_range (int) default 1:
        全部站点中需要
    Returns:list
        需要计算的站点列表

    """
    # 初始化redis
    try:
        redis_conn = public_function.Redis_Store(db=2)
    except:
        redis_conn.close()
        raise ConnectionError('Can not connect redis.')
    # 获取站点中五表时间
    five_files_redis_sign = commonly_params.five_files_redis_sign
    all_redis_keys = redis_conn.keys()
    redis_conn.close()
    five_files_redis_keys = [key for key in all_redis_keys if five_files_redis_sign in key]
    # 每个redis键的最后14位为报表上传时间,站点信息在'FIVE_FILES_KEYS_SAVE:02_AU_AC_20200718105127'
    # redis键由:20位标识符('FIVE_FILES_KEYS_SAVE')+站点+2位报表名称+14位时间标识符组成
    # 从今日向前取date_range天数的站点
    now_date = datetime.today().date()
    start_date = now_date - timedelta(days=date_range)
    return list(set([key[21:-18] for key in five_files_redis_keys if
                     (datetime.strptime(key[-14:], '%Y%m%d%H%M%S').date() >= start_date) & (
                             datetime.strptime(key[-14:],
                                               '%Y%m%d%H%M%S').date() < now_date)]))


def load_station_report(station_name, report_type='cp'):
    """
    加载站点广告报表
    Args:

        station_name: str
            站点名
        report_type: str
            报表类型
    Returns:pd.DataFrame
        返回的站点报表数据

    """
    redis_conn = public_function.Redis_Store(db=2)
    five_files_redis_sign = commonly_params.five_files_redis_sign
    all_redis_keys = redis_conn.keys()
    station_report_key = [key for key in all_redis_keys if
                          (five_files_redis_sign in key) & (station_name.upper() in key) & (
                                  report_type.upper() == key[-17:-15].upper())]
    if len(station_report_key) > 1:
        # print(f'{station_name}_{report_type} have multiple redis key')
        # 选择时间最大的键
        station_report_key_time_dict = {key: key[-14:] for key in station_report_key}
        station_report_key = [key for key, time in station_report_key_time_dict.items() if
                              time == max(station_report_key_time_dict.values())][0]
    elif len(station_report_key) == 1:
        station_report_key = station_report_key[0]
    else:
        raise ValueError(f'{station_name}_{report_type} have none redis key.')
    station_report_pkl_path = redis_conn.get(station_report_key)
    redis_conn.close()
    return process_files.read_pickle_2_df(station_report_pkl_path)


@public_function.run_time
def upload_all_st(date_range=1000):
    """
    获取广告出单sku相关字段主函数.
        默认为每天对前一天的新的站点数据进行处理
        从广告报表以及sku、erp sku和asin对应关系表中取出
            :账号名,广告日期,sku,erp sku,asin,（后期加上类目）,广告出单量,广告竞价,广告花费,广告销售额,acos
        存储到广告组服务器表中.
    步骤:
        1.找到需要处理的站点列表
        2.加载站点全部数据:广告报表和sku/erp sku/asin对应关系表
        3.初始化数据来源
        4.将所需字段输出到广告组服务器中

    Args:
        date_range (int) default 1:
            处理时间段内的站点数据
    Return: None

    """
    global all_stations_queue, all_stations_queue_data
    # step1.找到需要处理的站点列表
    # 计算时间段内站点信息
    all_new_station = find_new_station(date_range=date_range)
    if not all_new_station:
        start_date = datetime.today().date() - timedelta(days=date_range)
        yesterday = datetime.today().date() - - timedelta(days=1)
        if yesterday != start_date:
            print('**********************************')
            print(f'{start_date}-{yesterday}没有新的站点.')
            print('**********************************')
        else:
            print('**********************************')
            print(f'{yesterday}没有新的站点.')
            print('**********************************')
        return
    # step2.加载站点数据
    # 加载站点的cp数据
    i = 0

    # 处理有订单的站点

    def st_info(station):
        """
        初始化站点数据:
            通过sellersku广告报表,ac报表,同时得到需要的列
        :param station:str
            站点名
        :return: pd.DataFrame
        """

        # 1.1加载st数据
        try:
            station_campaign_data = load_station_report(station,report_type='st')
        except:
            return
        # 核查广告报表数据
        public_function.type_verify(station_campaign_data, pd.DataFrame)
        # 1.2初始化广告报表数据
        report_type = 'st'
        station_st_data = init_station_report.init_report(station_campaign_data, report_type)

        if station_st_data is None:
            return

        try:
            station_data_columns = station_st_data.columns
            if not set(['Start Date', 'End Date']).issubset(set(station_data_columns)):
                if 'Date' in station_data_columns:
                    station_st_data['Start Date'] = station_st_data['Date']
                    station_st_data['End Date'] = station_st_data['Date']
                else:
                    return
            station_st_data['station'] = station
            station_st_data['update_time'] = datetime.today().replace(microsecond=0)
            station_st_data = station_st_data[st_info_columns]
        except Exception as e:
            print('*' * 20)
            print(e)
            print(station)
            print(station_st_data.columns)
            print('*' * 20)
            return

        # 上传st报表
        st_info_table = 'station_st_report'
        sql_write_read.to_table_append(station_st_data, st_info_table)
        print(f'完成上传:{station}')


    # 加载st信息中的列名
    st_info_columns_sql = "select * from station_st_report limit 1"
    st_info_data = sql_write_read.read_table(st_info_columns_sql)
    st_info_columns = list(st_info_data.columns)

    for station in all_new_station:
        st_info(station)


if __name__ == '__main__':
    upload_all_st()
