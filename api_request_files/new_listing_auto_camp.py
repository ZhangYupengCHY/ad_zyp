# -*- coding: utf-8 -*-
"""
Proj: ad_helper
Created on:   2020/3/1 10:50
@Author: RAMSEY

Standard:
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""

import rsa, json, requests, os, redis, zipfile, shutil, time, re, warnings
import numpy as np
# from retry import retry
import pandas as pd
# import Crypto.PublicKey.RSA
import base64, pymysql
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from mongo_con import MongoDb

THREAD_POOL = ThreadPoolExecutor(6)
PROCESS_POOL = ProcessPoolExecutor(2)
redis_pool_6 = redis.ConnectionPool(host='127.0.0.1', port=6379, db=6, decode_responses=True)
redis_pool_7 = redis.ConnectionPool(host='127.0.0.1', port=6379, db=7, decode_responses=True)
red = redis.StrictRedis(connection_pool=redis_pool_7)
red_station_status = redis.StrictRedis(connection_pool=redis_pool_6)
warnings.filterwarnings("ignore")

exchange_rate = {'CA': 0.7519, 'DE': 1.0981, 'FR': 1.0981, 'IT': 1.0981, 'SP': 1.0981, 'JP': 0.009302,
                 'UK': 1.2445, 'MX': 0.05147, 'IN': 0.01412, 'US': 1, 'ES': 1.0981, 'AU': 0.6766}


# 读取active_data
def read_active(path):
    dfcolumns = pd.read_csv(path,
                            nrows=1, encoding="ISO-8859-1", sep=',')
    columns_name = dfcolumns.columns
    if 'item_id' in columns_name:
        file_data = pd.read_csv(path, encoding="ISO-8859-1", sep=',')

    else:
        try:
            file_data = pd.read_csv(path, encoding="ISO-8859-1", sep=',', header=None,
                                    skiprows=1)
            file_data = file_data.ix[:, 2:16]
            file_data.columns = columns_name[0:15]
        except:
            file_data = pd.read_csv(path, encoding="gbk", sep=',')
    return file_data


# 读取单个文件数据(若为excel,则读取单个sheet)
def read_files(files_path: 'full_path', sheet_name='Sheet1'):
    split_file_path = os.path.splitext(files_path)
    if len(split_file_path) > 1:
        file_type = split_file_path[-1].lower()
        if file_type in ['.csv', '.txt']:
            try:
                file_data = pd.read_csv(files_path, error_bad_lines=False)
                return file_data
            except Exception as e:
                file_data = pd.read_csv(files_path, encoding="ISO-8859-1")
                return file_data
            except Exception as e:
                print(f"文件无法被正确读取: {files_path}")
        if file_type in ['.xlsx', '.xls']:
            try:
                if sheet_name == 'Sheet1':
                    file_data = pd.read_excel(files_path)
                    return file_data
                else:
                    read_excel = pd.ExcelFile(files_path)
                    sheet_names = read_excel.sheet_names
                    if sheet_name not in sheet_names:
                        print(f'{files_path}中没有{sheet_name}.')
                        return
                    else:
                        file_data = read_excel.parse(sheet_name)
                        return file_data
            except Exception as e:
                print(f"文件无法被正确读取:{files_path}")
        else:
            print(f'文件不为文本格式,请检查文件:{files_path}')
    else:
        print(f'请检查文件是否为有效路径:{files_path}')


# 获取站点广告与AC报表
def get_camp_active_data(process_station, stations_folder=r'F:\api_request_all_files', camp_30_sign_word='30天',
                         active_listing_sign_word='Active+Listings', all_order_sign_word='All Orders'):
    # 定位需要站点的路径
    # print(f'START:{process_station}')
    date_folder = [file_name for file_name in os.listdir(stations_folder) if re.findall('^\d', file_name)]
    if not date_folder:
        print(f'文件夹:{stations_folder}下没有日期文件夹.')
        return
    new_date_folder_path = os.path.join(stations_folder, max(date_folder))
    stations_list = os.listdir(new_date_folder_path)
    if process_station not in stations_list:
        print(f'文件夹{new_date_folder_path}没有站点{process_station}.')
        return
    station_path = os.path.join(new_date_folder_path, process_station)
    station_files = os.listdir(station_path)
    if not station_files:
        print(f'{station_path}没有文件.')
        return
    # 获取广告报表
    camp_30 = [file for file in station_files if camp_30_sign_word in file]
    if not camp_30:
        print(f'{station_path}没有30天广告报表.')
        return
    if len(camp_30) >= 1:
        if len(camp_30) > 1:
            print(f'{station_path}含有多个广告报表,取最新的一个.')
        camp_30_basename = camp_30[0]
    camp_30_path = os.path.join(station_path, camp_30_basename)
    camp_30_data = read_files(camp_30_path, sheet_name='Sponsored Products Campaigns')
    if camp_30_data.empty:
        print(f'{camp_30_path}为空表.')
        return
    # 获取all order
    all_order = [file for file in station_files if all_order_sign_word in file]
    if not all_order:
        print(f'{station_path}没有订单报表.')
        return
    if len(all_order) >= 1:
        if len(all_order) > 1:
            print(f'{station_path}含有多个订单报表,取最新的一个.')
        all_order_basename = all_order[0]
    all_order_path = os.path.join(station_path, all_order_basename)
    all_order_data = read_files(all_order_path)
    if all_order_data.empty:
        print(f'{all_order_path}为空表.')
        return

    # 获取active_listing  active_listing_sign_word
    active_listing = [file for file in station_files if active_listing_sign_word in file]
    if not active_listing:
        print(f'{station_path}没有active_listing报表.')
        return
    if len(active_listing) >= 1:
        if len(active_listing) > 1:
            print(f'{station_path}含有多个active_listing报表,取最新的一个.')
        active_listing_basename = active_listing[0]
    active_listing_path = os.path.join(station_path, active_listing_basename)
    active_listing_data = read_active(active_listing_path)
    if active_listing_data.empty:
        print(f'{active_listing_path}为空表.')
        return
    return [camp_30_data, active_listing_data, all_order_data]


def process_auto_new(stations_folder=r'F:\api_request_all_files', camp_30_sign_word='30天',
                     active_listing_sign_word='Active+Listings'):
    # 充填空的sku,并获取sku
    def get_cmap_sku(camp_data):
        camp_data.columns = [column.strip(' ') for column in camp_data.columns]
        if not set(['Record Type', 'Ad Group', 'SKU']).issubset(set(camp_data.columns)):
            lose_column = set(['Record Type', 'Ad Group', 'SKU']) - set(camp_data.columns)
            print(f'{process_station}:camp表缺失{lose_column}')
            return
        ad_info = camp_data[camp_data['Record Type'] == 'Ad']
        ad_group_list = ad_info['Ad Group']
        sku_list = ad_info['SKU']
        camp_sku_set = set([sku if pd.notna(sku) else ad_group.split(' ')[0] for ad_group, sku in
                            zip(ad_group_list, sku_list)])
        # 删除某些元素
        if 'ad' in camp_sku_set:
            camp_sku_set.remove('ad')
        return camp_sku_set

    def get_active_listing_info(active_listing_data):
        active_listing_data.columns = [column.strip(' ').lower() for column in active_listing_data.columns]
        if not set(['seller_sku', 'asin1', 'price', 'fulfillment_channel', 'open_date']).issubset(
                active_listing_data.columns):
            print(f'{process_station}:active_listing缺失seller_sku/asin/price/fulfillment_channel')
        # active_listing_data['seller_sku'] = active_listing_data['seller_sku'].apply(lambda x: x.lower())
        active_listing_sku_set_asin = active_listing_data[
            ['seller_sku', 'asin1', 'price', 'fulfillment_channel', 'open_date']]

        return active_listing_sku_set_asin

    def get_all_order_sku(all_order_data, site):
        sales_channel = {'it': 'Amazon.it', 'de': 'Amazon.de', 'es': 'Amazon.es', 'fr': 'Amazon.fr',
                         'uk': 'Amazon.co.uk', 'jp': 'Amazon.co.jp', 'us': 'Amazon.com', 'ca': 'Amazon.ca',
                         'mx': 'Amazon.com.mx', 'in': 'Amazon.in', 'au': 'Amazon.com.au'}
        all_order_data.columns = [column.strip(' ') for column in all_order_data.columns]
        if not set(['sales_channel', 'order_status', 'sku']).issubset(set(all_order_data.columns)):
            lose_column = set(['sales_channel', 'order_status', 'sku']) - set(all_order_data.columns)
            print(f'{process_station}:all_order表缺失{lose_column}')
            return
        site_sales_channel = sales_channel[site]
        all_order_sku = all_order_data[(all_order_data['sales_channel'] == site_sales_channel) & (
                all_order_data['order_status'] != 'Cancelled')]['sku']
        all_order_sku_set = set(all_order_sku)
        return all_order_sku_set

    def new_listing_upload_format(station_name, new_sku_list, active_listing_info, new_ao_listing, max_bid=0.4):
        export_columns = ['Campaign Name', 'Campaign Daily Budget', 'Campaign Start Date', 'Campaign End Date',
                          'Campaign Targeting Type', 'Ad Group Name', 'Max Bid', 'SKU', 'Keyword',
                          'Product Targeting ID',
                          'Match Type', 'Campaign Status', 'Ad Group Status', 'Status', 'Bidding strategy', 'Price',
                          'Fulfillment_channel', 'Start Date']
        # 竞价为空，bid = price * 15% * 3%
        # auto_new_data.reset_index(drop=True, inplace=True)
        station_name = station_name.upper()
        # start_date = datetime.now().strftime('%Y%m%d')
        file_date = datetime.now().strftime('%Y%m%d')[2:]
        camp_name_listing = f"AUTO-{station_name}-by-SP_Bulk-New"
        camp_name_ao = f"AUTO-{station_name}-by-SP_Bulk"
        country = station_name[-2:]
        listing_sku_upload_data = pd.DataFrame(columns=export_columns)
        ao_sku_upload_data = pd.DataFrame(columns=export_columns)

        def trans_sku_into_upload(sku, camp_name):
            asin = active_listing_info['asin1'][active_listing_info['seller_sku'] == sku].values[0]
            price = active_listing_info['price'][active_listing_info['seller_sku'] == sku].values[0]
            fulfillment = active_listing_info['fulfillment_channel'][active_listing_info['seller_sku'] == sku].values[0]
            start_date = active_listing_info['open_date'][active_listing_info['seller_sku'] == sku].values[0]
            if 'def' in fulfillment.lower():
                fulfillment = 'fbm'
            else:
                fulfillment = 'fba'
            bid = round(min(float(price) * 0.15 * 0.03, max_bid / exchange_rate[country]), 2)
            empty_list = [np.nan] * len(export_columns)
            processed_auto_new_data = pd.DataFrame([empty_list, empty_list], columns=export_columns)
            processed_auto_new_data['Campaign Name'] = camp_name
            processed_auto_new_data['Ad Group Name'] = "%s %s_%s" % (sku, asin, file_date)
            processed_auto_new_data.ix[0, 'Max Bid'] = bid
            processed_auto_new_data.ix[1, 'SKU'] = sku
            processed_auto_new_data.ix[0, 'Ad Group Status'] = 'enabled'
            processed_auto_new_data.ix[1, 'Status'] = 'enabled'
            # 添加客单价,发货方式,上架时间
            processed_auto_new_data['Price'] = price
            processed_auto_new_data['Fulfillment_channel'] = fulfillment
            processed_auto_new_data['Start Date'] = start_date
            return processed_auto_new_data

        if new_sku_list:
            listing_sku_upload_data = pd.concat([trans_sku_into_upload(sku, camp_name_listing) for sku in new_sku_list])
            listing_sku_upload_data['new_add_type'] = 'listing'
        if new_ao_listing:
            ao_sku_upload_data = pd.concat([trans_sku_into_upload(sku, camp_name_ao) for sku in new_ao_listing])
            ao_sku_upload_data['new_add_type'] = 'ao'
        # 添加新增类型并将两种类型合并

        if (not listing_sku_upload_data.empty) & (not ao_sku_upload_data.empty):
            all_sku_upload_data = pd.concat([ao_sku_upload_data, listing_sku_upload_data])
        elif not listing_sku_upload_data.empty:
            all_sku_upload_data = listing_sku_upload_data
        elif not ao_sku_upload_data.empty:
            all_sku_upload_data = ao_sku_upload_data
        else:
            return
        # 筛选出客单价10美金的SKU
        all_sku_upload_data = all_sku_upload_data[all_sku_upload_data['Price'] >= 10/(exchange_rate[country])]
        return all_sku_upload_data

    def process_time():
        with open(r"F:\api_request_all_files\cost_time.xlsx", 'a+') as f:
            f.write(f'{process_station}\t')
            f.write(f'{finish_time - start_time}\n')

    try:
        # 处理的站点
        start_time = datetime.now()
        all_process_stations = red.lrange('complete_files_station', 0, -1)
        if not all_process_stations:
            return
        process_station = red.lpop('complete_files_station')
        print(f'START:{process_station}')

        # 获取camp,active_listing

        [camp_data, active_listing_data, all_order_data] = get_camp_active_data(process_station,
                                                                                stations_folder=stations_folder,
                                                                                camp_30_sign_word=camp_30_sign_word,
                                                                                active_listing_sign_word=active_listing_sign_word,
                                                                                all_order_sign_word='All Orders')
        camp_sku_set = get_cmap_sku(camp_data)

        # 生成上传表
        active_listing_info = get_active_listing_info(active_listing_data)
        active_listing_sku_set = set(active_listing_info['seller_sku'])
        site = process_station[-2:].lower()
        all_order_sku_set = get_all_order_sku(all_order_data, site)
        new_sku = active_listing_sku_set - camp_sku_set
        new_sku_num = len(new_sku)
        # listing新增sku
        new_listing_sku = new_sku - all_order_sku_set
        # ao新增sku
        ao_listing_sku = new_sku & all_order_sku_set
        new_listing_upload_data = new_listing_upload_format(process_station, new_listing_sku, active_listing_info,
                                                            ao_listing_sku)
        fba_num = len(new_listing_upload_data[(pd.notna(new_listing_upload_data['SKU'])) & (
                new_listing_upload_data['Fulfillment_channel'] == 'fba')])
        fbm_num = len(new_listing_upload_data[(pd.notna(new_listing_upload_data['SKU'])) & (
                new_listing_upload_data['Fulfillment_channel'] == 'fbm')])
        now_datetime = datetime.now().strftime('%Y%m%d')
        # 1.将结果写入到csv中
        new_listing_upload_data.to_csv(
            f"F:/upload_sheet/new_listing_upload/{process_station}_{now_datetime}_fba{fba_num} fbm{fbm_num}.csv",
            index=False)

        # 加载全部的站点名和广告专员
        def db_download_station_names(db='team_station', table='only_station_info', ip='127.0.0.1', port=3306,
                                      user_name='marmot', password='marmot123') -> pd.DataFrame:
            """
            加载广告组接手的站点名
            :return: 所有站点名
            """
            conn = pymysql.connect(
                host=ip,
                user=user_name,
                password=password,
                database=db,
                port=port,
                charset='UTF8')
            # 创建游标
            cursor = conn.cursor()
            # 写sql
            sql = """SELECT station,ad_manger FROM {} """.format(table)
            # 执行sql语句
            cursor.execute(sql)
            stations_name_n_manger = cursor.fetchall()
            stations_name_n_manger = pd.DataFrame([list(station) for station in stations_name_n_manger],
                                                  columns=['station_name', 'manger'])
            stations_name_n_manger = stations_name_n_manger[
                (stations_name_n_manger['manger'] != '汪磊') & (stations_name_n_manger['manger'] != '王艳')]
            stations_name_n_manger.drop_duplicates(inplace=True)
            conn.commit()
            cursor.close()
            conn.close()
            return stations_name_n_manger

        # 2.将FBA新增大于50的存储到一个文件夹加下
        fba_above_50_folder = os.path.join("F:/upload_sheet/new_listing_upload", f'{now_datetime}_fba_above_50')
        if not os.path.exists(fba_above_50_folder):
            os.mkdir(fba_above_50_folder)
        if fba_num >= 30:
            # 得到站点对应的站点名
            station_manager = db_download_station_names()
            manager = station_manager['manger'][station_manager['station_name'] == process_station.lower()]
            if len(manager) == 0:
                manager = 'Nobody'
            else:
                manager = manager.values[0]
            manager_folder = os.path.join(fba_above_50_folder, manager)
            if not os.path.exists(manager_folder):
                os.mkdir(manager_folder)
            new_listing_upload_data_path = os.path.join(manager_folder,
                                                        f"{process_station}_{now_datetime}_fba{fba_num} fbm{fbm_num}.csv")
            new_listing_upload_data.to_csv(new_listing_upload_data_path, index=False)

        finish_time = datetime.now()
        print(f'{process_station}: 自动生成新增上传表成功.')
        print(f'{process_station}: 花费{finish_time - start_time} S')
        process_time()
        # 3.将df存储到mongodb中
        new_listing_mongo = MongoDb('ad_team_station', 'new_listing_auto_add')
        new_listing_log_mongo = MongoDb('ad_team_station', 'new_listing_auto_add_log')
        new_listing_upload_data['station'] = process_station
        new_listing_upload_data_list = new_listing_upload_data.to_dict(orient='records')
        new_listing_mongo.mongo_insert(new_listing_upload_data_list)
        now_datetime2 = datetime.now()
        log_file = [
            {'station_name': process_station, 'update_time': now_datetime2, 'add_num': new_sku_num, 'fba_num': fba_num,
             'fbm_num': fbm_num}]
        new_listing_log_mongo.mongo_insert(log_file)
    except Exception as e:
        print(e)
        print(f'{process_station}: 自动生成新增上传表失败.')


# # 线程池
# def thread_read_file():
#     all_task = []
#     for one_page in range(1):
#         all_task.append(THREAD_POOL.submit(process_auto_new))
#     for future in as_completed(all_task):
#         future.result()


def process_read_file():
    all_task = []
    for one_page in range(4):
        all_task.append(PROCESS_POOL.submit(process_auto_new))
    for future in as_completed(all_task):
        future.result()


if __name__ == "__main__":
    # 删除库中原本的上传表数据
    new_listing_del_mongo = MongoDb('ad_team_station', 'new_listing_auto_add')
    new_listing_del_mongo.mongo_del({})
    now_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'START AT: {now_datetime}')
    with open(r"F:\api_request_all_files\cost_time.xlsx", 'a+') as f:
        f.write("站点\t")
        f.write("运行时间(秒)\n")
    while 1:
        process_read_file()
        # time.sleep(36000)
