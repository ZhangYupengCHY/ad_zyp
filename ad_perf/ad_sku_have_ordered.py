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
import gc
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


# PROCESS_POOL = ProcessPoolExecutor(8)

"""
近7天近14天近30天做上广告出单了的sku及竞价、广告数据生成一张报表
    包含字段:
        账号名,广告日期,sku,erp sku,asin,（后期加上类目）,广告出单量,广告竞价,广告花费,广告销售额,acos
    字段来源:
        账号名:文件路径中提取
        广告日期:广告报表中Ad Group列提取
        sku:广告报表中SKU列
        erp sku:通过sku与erp sku对应表(通过读取数据库存储在redis中,直接在redis中取)
        asin:active listing表中asin1列(通过sku匹配)
        类目:通过接口获取
        广告出单量:广告报表中Orders列
        广告竞价:广告报表中Max Bid列
        广告花费:广告报表中Spend列
        广告销售额:广告报表中Sales列
        acos:广告报表中ACoS列

"""


@public_function.loop_func(update_time=4)
@public_function.run_time
def ad_sku_have_ordered(date_range=30):
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

    THREAD_POOL = ThreadPoolExecutor(16)
    warnings.filterwarnings(action='ignore')
    EXCHANGE_RATE = change_rate.change_current()


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

    # 处理有订单的站点

    def init_station_data():
        """
        初始化站点数据:
            通过sellersku广告报表,ac报表,同时得到需要的列
        :param station:str
            站点名 
        :return: pd.DataFrame
        """
        try:
            # 从队列中加载station
            if all_stations_queue.empty():
                return
            else:
                station = all_stations_queue.dequeue()
            site = station[-2:].upper()
            # 1.1加载campaign数据
            station_campaign_data = load_station_report(station)
            # 核查广告报表数据
            public_function.type_verify(station_campaign_data, pd.DataFrame)
            # 1.2初始化广告报表数据
            report_type = 'cp'
            station_campaign_data = init_station_report.init_report(station_campaign_data, report_type)
            init_station_report.change_currency(station_campaign_data, 'cp', site, currency='dollar')

            sku_data = station_campaign_data[(station_campaign_data['Record Type'] == 'Ad')]
            if sku_data.empty:
                return
            # 得到sku的订单时间
            # Ad Group列最后6位为时间,倒数第7为下划线
            invalid_upload_time_sku_sign = ''
            sku_data['sku_upload_time'] = [
                invalid_upload_time_sku_sign if len(ad_group) < 7 else ad_group[-6:] if (ad_group[-7] == '_') & (
                    ad_group[-6:].isdigit()) else invalid_upload_time_sku_sign for ad_group in
                sku_data['Ad Group']]
            # 添加asin列: 从ad_group_name去提取
            sku_data['group_name_asin'] = [
                re.findall(' B0[a-zA-Z0-9]{8}', ad_group, re.I)[-1].strip(' ') if re.findall(' B0[a-zA-Z0-9]{8}',
                                                                                             ad_group,
                                                                                             re.I) else '' for ad_group
                in sku_data['Ad Group']]

            # 筛出掉没有上传时间的sku
            sku_data = sku_data[
                sku_data['sku_upload_time'] != invalid_upload_time_sku_sign]
            if sku_data.empty:
                return
            # 将sku上传时间列转换为日期格式
            sku_data['sku_upload_time'] = sku_data['sku_upload_time'].apply(
                lambda x: datetime.strptime(x, '%y%m%d'))

            # 添加上架天数列
            now_datetime = datetime.today()
            sku_data['ad_upload_days'] = sku_data['sku_upload_time'].apply(
                lambda x: (now_datetime - x).days)

            # 添加账号列和上传日期
            sku_data['station'] = station

            # 添加ctr列
            sku_data['ctr'] = [str(round(clicks * 100 / impressions, 2)) + '%' if impressions > 0 else '0.00%' for
                               clicks, impressions in zip(sku_data['Clicks'], sku_data['Impressions'])]

            # 提取需要的列:账号名,广告日期,sku,（后期加上类目）,广告出单量,广告竞价,广告花费,广告销售额,acos,广告列中抓取的asin
            extract_columns = ['station', 'SKU', 'ad_upload_days', 'sku_upload_time', 'Max Bid', 'Impressions', 'ctr',
                               'Clicks', 'Orders', 'Spend', 'Sales', 'ACoS', 'group_name_asin']

            # 筛选掉sku中有花费>1或是点击大于10
            sku_data = sku_data[(sku_data['Clicks'] >= 10) | (sku_data['Sales'] >= 0.1) | (sku_data['Spend'] >= 0.1)]

            # 筛选:筛选出广告报表需要列
            sku_data = sku_data[extract_columns]

            # 2.1 加载active_listing报表
            active_listing_data = load_station_report(station, report_type='ac')
            # 2.2 初始化active_listing数据
            report_type = 'ac'
            active_listing_data = init_station_report.init_report(active_listing_data, report_type)
            # 若必须的列不存在,则用空值充填
            need_columns_active_listing = ['asin1', 'fulfillment-channel']
            for column in need_columns_active_listing:
                if column not in active_listing_data.columns:
                    active_listing_data[column] = ''

            # 判断需要列是否存在
            active_listing_need_columns = ['seller-sku', 'asin1', 'open-date', 'price', 'fulfillment-channel']
            if not set(active_listing_need_columns).issubset(set(active_listing_data.columns)):
                lose_columns = set(active_listing_need_columns) - set(active_listing_data.columns)
                print('******************************************************************')
                print(f'{station} active listing lose {lose_columns} column')
                print('******************************************************************')
                return
            # 将open-date转换为日期格式
            active_listing_data = public_function.column_to_datetime_active(active_listing_data, site, 'open-date')

            # 将cp表和ac表合并添加asin列和open-date列
            sku_data = pd.merge(sku_data, active_listing_data[active_listing_need_columns],
                                how='left',
                                left_on='SKU', right_on='seller-sku')

            del sku_data['seller-sku']

            sku_data['open_days'] = [
                (datetime.today() - date_from_ac).days if pd.notna(date_from_ac) else open_days_from_cp
                for
                date_from_ac, open_days_from_cp in zip(sku_data['open-date'],
                                                       sku_data['ad_upload_days'])]

            # 添加平均上传点击列
            sku_data['average_clicks'] = [round(clicks / open_days, 2) if open_days > 0 else 0 for
                                          clicks, open_days in zip(sku_data['Clicks'], sku_data['open_days'])]

            # 添加客单价
            sku_data['price'] = sku_data['price'].apply(
                lambda x: round(x * EXCHANGE_RATE[site], 2) if pd.notna(x) else 0)

            if not sku_data.empty:
                # print(f'完成站点信息初始化:{station}.')
                all_stations_queue_data.enqueue(sku_data)
                return
            else:
                return
        except:
            return

    # 多线程请求
    def thread_query():
        while 1:
            all_task = []
            for _ in range(16):
                all_task.append(THREAD_POOL.submit(init_station_data))
            for future in as_completed(all_task):
                future.result()
            if all_stations_queue.empty():
                break

    # for station in all_new_station:
    #     try:
    #         station_data_merge_cp_ac = init_station_data(station)
    #     except:
    #         public_function.print_color('='*20)
    #         public_function.print_color(f'{station}有问题!')
    #         public_function.print_color('=' * 20)
    #         continue
    #     i += 1
    #     print(f'完成第{i}个: {station}')
    #     # i += 1
    #     # if i == 100:
    #     #     break
    #     if station_data_merge_cp_ac is not None:
    #         all_stations_data_merge_cp_ac.append(station_data_merge_cp_ac)

    # 多线程实现
    # 全部站点
    all_stations_queue = public_function.Queue()
    # 添加站点信息到队列中
    all_stations_queue.enqueue_items(all_new_station)
    # 全部站点返回结果
    all_stations_queue_data = public_function.Queue()
    # 多线程请求
    thread_query()
    # 请求结果
    all_stations_data_merge_cp_ac = all_stations_queue_data.items
    if len(all_stations_data_merge_cp_ac) == 0:
        return
    else:
        all_stations_data_merge_cp_ac = pd.concat(all_stations_data_merge_cp_ac, ignore_index=True)

    # 将open-date,sku_upload_time列转换为日期格式
    datetime_columns = ['open-date', 'sku_upload_time']
    for column in datetime_columns:
        all_stations_data_merge_cp_ac[column] = all_stations_data_merge_cp_ac[column].apply(
            lambda x: x.date() if pd.notna(x) else None)

    # 添加upload_datetime列
    now_datetime = datetime.today()
    all_stations_data_merge_cp_ac['update_datetime'] = now_datetime

    # 将acos转换为浮点型数据
    del all_stations_data_merge_cp_ac['ACoS']

    all_stations_data_merge_cp_ac['acos'] = [round(spend / sale, 4) if sale != 0 else 0 for spend, sale in
                                             zip(all_stations_data_merge_cp_ac['Spend'],
                                                 all_stations_data_merge_cp_ac['Sales'])]

    # 修改列名
    all_stations_data_merge_cp_ac.rename(
        columns={'SKU': 'seller_sku', 'Max Bid': 'max_bid', 'Orders': 'orders', 'Spend': 'spend',
                 'Sales': 'sales', 'asin1': 'active_listing_match_asin',
                 'open_days': 'open-days', 'sku_upload_time': 'ad-open-date'},
        inplace=True)

    # result.1. 得到有订单的sku信息
    # 站点订单 > 1
    have_ordered_ad_sku_data = all_stations_data_merge_cp_ac[
        (all_stations_data_merge_cp_ac['orders'] > 0) & (all_stations_data_merge_cp_ac['ad_upload_days'] <= 30)]
    # 添加id列
    have_ordered_ad_sku_data['id'] = range(1, have_ordered_ad_sku_data.shape[0] + 1)
    # 添加erp_sku列
    sellsersku_list = list(have_ordered_ad_sku_data['seller_sku'])
    sellersku_erpsku_tied_info = sql_write_read.query_sku_tied(sellsersku_list)
    have_ordered_ad_sku_data = pd.merge(have_ordered_ad_sku_data, sellersku_erpsku_tied_info, how='left',on='seller_sku')
    # 添加类目
    category_info = sql_write_read.query_category(sellsersku_list)
    have_ordered_ad_sku_data = pd.merge(have_ordered_ad_sku_data, category_info, how='left',
                                             on='seller_sku')
    # 调整列的位置
    have_ordered_export_columns = ['id', 'station', 'seller_sku', 'erp_sku', 'active_listing_match_asin',
                                   'group_name_asin', 'open-date', 'open-days', 'ad-open-date', 'ad_upload_days',
                                   'fulfillment-channel', 'max_bid', 'orders', 'spend',
                                   'sales', 'acos', 'price', 'linelist_cn_name_degree_1', 'linelist_cn_name_degree_2',
                                   'linelist_cn_name_degree_3', 'update_datetime']
    have_ordered_ad_sku_data = have_ordered_ad_sku_data.reindex(columns=have_ordered_export_columns)
    # 按照订单降序排列
    have_ordered_ad_sku_data.sort_values(by=['station', 'orders'], inplace=True, ascending=False)

    # 上传到数据库中
    if len(have_ordered_ad_sku_data) > 0:
        table_name = 'station_have_ordered_sku'
        sql_write_read.to_table_replace(have_ordered_ad_sku_data, table_name)

    # result.2 .
    # 站点订单为0, clicks大于100或average_clicks(clicks / upload_days)>3, open-days <= 45
    have_potential_sku_data_case_1 = all_stations_data_merge_cp_ac[(all_stations_data_merge_cp_ac['orders'] == 0) & (
            (all_stations_data_merge_cp_ac['Clicks'] > 100) | (
            all_stations_data_merge_cp_ac['average_clicks'] > 3)) & (all_stations_data_merge_cp_ac[
                                                                         'open-days'] <= 45)]
    # 添加id列
    have_potential_sku_data_case_1['id'] = range(1, have_potential_sku_data_case_1.shape[0] + 1)

    # 添加erp_sku列
    have_potential_sellsersku_list = list(have_potential_sku_data_case_1['seller_sku'])
    have_potential_sellersku_erpsku_tied_info = sql_write_read.query_sku_tied(have_potential_sellsersku_list)
    have_potential_sku_data_case_1 = pd.merge(have_potential_sku_data_case_1, have_potential_sellersku_erpsku_tied_info, how='left',on='seller_sku')
    # 添加类目
    have_potential_category_info = sql_write_read.query_category(have_potential_sellsersku_list)
    have_potential_sku_data_case_1 = pd.merge(have_potential_sku_data_case_1, have_potential_category_info, how='left',
                                             on='seller_sku')

    # 调整列的位置
    have_potential_sku_data_case_1_export_columns = ['id', 'station', 'seller_sku', 'erp_sku',
                                                     'active_listing_match_asin',
                                                     'group_name_asin', 'open-date', 'open-days', 'ad-open-date',
                                                     'ad_upload_days', 'fulfillment-channel', 'max_bid',
                                                     'Impressions', 'Clicks', 'spend', 'sales', 'price', 'acos', 'ctr',
                                                     'average_clicks', 'linelist_cn_name_degree_1',
                                                     'linelist_cn_name_degree_2', 'linelist_cn_name_degree_3',
                                                     'update_datetime']
    have_potential_sku_data_case_1 = have_potential_sku_data_case_1.reindex(columns=have_potential_sku_data_case_1_export_columns)

    # # # # 上传到数据库中
    if len(have_potential_sku_data_case_1) > 0:
        table_name_potential = 'station_potential_sku'
        sql_write_read.to_table_replace(have_potential_sku_data_case_1, table_name_potential)

    # result.3 .
    # bid需要降低的sku

    have_potential_sku_data_case_2 = all_stations_data_merge_cp_ac[
        ((all_stations_data_merge_cp_ac['orders'] == 0) & (all_stations_data_merge_cp_ac['Clicks'] > 100)) |
        ((all_stations_data_merge_cp_ac['spend'] > 10) & (all_stations_data_merge_cp_ac['orders'] == 0)) |
        ((all_stations_data_merge_cp_ac['Clicks'] > 300) & (all_stations_data_merge_cp_ac['acos'] > 0.3))]

    # 添加站点负责人列
    # 加载站点负责人信息
    only_station_info_query_sql = "select station,ad_manger from only_station_info"
    only_station_info = sql_write_read.read_table(only_station_info_query_sql)
    only_station_info.rename(columns={'ad_manger': "ad_manager"}, inplace=True)
    only_station_info['station'] = only_station_info['station'].apply(lambda x: x.upper())

    have_potential_sku_data_case_2 = pd.merge(have_potential_sku_data_case_2, only_station_info, how='left',
                                              on='station')

    # 添加 id 列
    have_potential_sku_data_case_2['id'] = range(1, have_potential_sku_data_case_2.shape[0] + 1)

    # 添加erp_sku列
    have_potential_sellsersku_list_2 = list(have_potential_sku_data_case_2['seller_sku'])
    have_potential_sellersku_erpsku_tied_info_2 = sql_write_read.query_sku_tied(have_potential_sellsersku_list_2)
    have_potential_sku_data_case_2 = pd.merge(have_potential_sku_data_case_2, have_potential_sellersku_erpsku_tied_info_2, how='left',on='seller_sku')
    # 添加类目
    have_potential_category_info_2 = sql_write_read.query_category(have_potential_sellsersku_list_2)
    have_potential_sku_data_case_2 = pd.merge(have_potential_sku_data_case_2, have_potential_category_info_2, how='left',
                                             on='seller_sku')

    # 调整列的位置
    have_potential_sku_data_case_2_export_columns = ['id', 'station', 'ad_manager', 'seller_sku', 'erp_sku',
                                                     'active_listing_match_asin',
                                                     'group_name_asin', 'open-date', 'open-days', 'ad-open-date',
                                                     'ad_upload_days', 'fulfillment-channel', 'max_bid',
                                                     'Impressions', 'Clicks', 'spend', 'sales', 'orders', 'price',
                                                     'acos', 'ctr',
                                                     'average_clicks', 'linelist_cn_name_degree_1',
                                                     'linelist_cn_name_degree_2', 'linelist_cn_name_degree_3',
                                                     'update_datetime']
    have_potential_sku_data_case_2 = have_potential_sku_data_case_2.reindex(
        columns=have_potential_sku_data_case_2_export_columns)

    # # # # 上传到数据库中
    if len(have_potential_sku_data_case_2) > 0:
        table_name_potential_case_2 = 'station_potential_sku_case_2'
        sql_write_read.to_table_replace(have_potential_sku_data_case_2, table_name_potential_case_2)


    del all_stations_data_merge_cp_ac
    del have_ordered_ad_sku_data
    del have_potential_sku_data_case_2
    del have_potential_sku_data_case_1
    gc.collect()

    print('***************************')
    print('\033[0;31m 完成上传有订单和有潜力的sku \033[0m')
    print('***************************')


if __name__ == '__main__':
    ad_sku_have_ordered()
