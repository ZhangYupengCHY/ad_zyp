# -*- coding: utf-8 -*-
"""
Proj: AD-Helper1
Created on:   2019/12/12 22:40
@Author: RAMSEY

Standard:
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""

import redis, pymysql, json
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import sys, os, uuid, shutil, time, gc
from datetime import datetime, timedelta
import pandas as pd
from search_sku_or_asin import unzip_dir, get_camp_file_dir, get_activelisting_file_dir, \
    read_active_listing, \
    process_campaign_data, standard_camp_data, db_upload_sku_info, sql_to_redis, db_download_station_names, \
    db_insert_station_name

sys.path.append(r"D:\AD-Helper1\ad_helper\recommend\my_toolkit")
from read_campaign import read_campaign
from init_campaign import init_campaign
import glob

redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=1, decode_responses=True)
red = redis.StrictRedis(connection_pool=redis_pool)
# 处理uuid
redis_pool2 = redis.ConnectionPool(host='127.0.0.1', port=6379, db=2, decode_responses=True)
red2 = redis.StrictRedis(connection_pool=redis_pool2)

# THREAD_POOL = ThreadPoolExecutor(4)
PROCESS_POOL = ProcessPoolExecutor(4)
station_zipped_folder = r"C:\Users\Administrator\Desktop\station_folder"
station_temp_folder = r"D:\AD-Helper1\ad_helper\recommend\search_sku_asin\temp"


def db_upload_repeat_sku(repeat_sku_info, station_name, campaign_targeting_type='manual', db='team_station',
                         ip='192.168.129.240',
                         user_name='marmot',
                         password='marmot123', port=3306):
    """
    将手动与自动广告上传到数据库中
    :param repeat_sku_info:重复广告数据
    :param station_name:站点名
    :return: None
    """
    now_time = str(datetime.now())[0:19]
    repeat_sku_info['update_time'] = now_time
    account = station_name[:-3]
    site = station_name[-2:]
    all_list = []
    repeat_sku_info.reset_index(drop=True, inplace=True)
    df = repeat_sku_info.astype(object).replace(np.nan, 'None')
    df = np.array(df)
    len_df = df.shape[0]
    for i in range(len_df):
        temp_tuple = df[i]
        a_emp_tuple = tuple(temp_tuple)
        all_list.append(a_emp_tuple)

    # 创建连接
    conn = pymysql.connect(
        host=ip,
        user=user_name,
        password=password,
        database=db,
        port=port,
        charset='UTF8')

    table_name = "station_" + campaign_targeting_type + "_repeat_sku"

    # 创建游标
    cursor = conn.cursor()

    insert_sql = """insert into {} (account,site,record_id, record_type, campaign_id, campaign,
       campaign_daily_budget, campaign_start_date,
       campaign_end_date, campaign_targeting_type, ad_group, max_bid,
       keyword_or_product_targeting, product_targeting_id, match_type,
       sku, campaign_status, ad_group_status, status, impressions,
       clicks, spend, orders, sales, acos,update_time) \
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""".format(
        table_name)
    delete_sql = """DELETE from {} where account = {} and site = {} """.format(table_name, "'%s'" % account,
                                                                               "'%s'" % site)

    try:
        cursor.execute(delete_sql)
        if all_list:
            cursor.executemany(insert_sql, all_list)
        conn.commit()
        # print(f"成功! 更新{station_name} {campaign_targeting_type}重复sku.")
    except Exception as e:
        conn.rollback()
        print(e)
        print(f"失败! 更新{station_name} {campaign_targeting_type}重复sku.")


# 将店铺广告报表中开启状态下空sku信息录入到数据库中
def db_upload_camp_no_sku_name_num(no_sku_name_info: list, station_name, db='team_station',
                                   ip='192.168.129.240',
                                   user_name='marmot',
                                   password='marmot123', port=3306):
    """
    将手动与自动广告上传到数据库中
    :param new_listing_no_sku_info:新品未打广告信息
    :param station_name:账号站点
    :return: None
    """
    no_sku_name_info = tuple(no_sku_name_info)
    station_name = station_name.lower()
    account = station_name[:-3]
    site = station_name[-2:]
    # 创建连接
    conn = pymysql.connect(
        host=ip,
        user=user_name,
        password=password,
        database=db,
        port=port,
        charset='UTF8')

    table_name = 'station_camp_sku_no_name'

    # 创建游标
    cursor = conn.cursor()
    insert_sql = """insert into {} (account,site,all_ad_num, all_manual_ad_num, manual_ad_no_sku_name_num, all_auto_ad_num,
       auto_ad_no_sku_name_num,update_time) values (%s,%s,%s,%s,%s,%s,%s,%s)""".format(
        table_name)
    delete_sql = """DELETE from {} where account = {} and site = {} """.format(table_name, "'%s'" % account,
                                                                               "'%s'" % site)

    try:
        cursor.execute(delete_sql)
        cursor.execute(insert_sql, no_sku_name_info)
        conn.commit()
        # print(f"成功! 更新{station_name} sku没有名字信息")
    except Exception as e:
        conn.rollback()
        print(e)
        print(f"失败! 更新{station_name} sku没有名字信息")


# 得到店铺广告报表中开启状态下空sku信息
def get_camp_no_sku_name_num(camp_data, station_name):
    station_name = station_name.lower()
    account = station_name[:-3]
    site = station_name[-2:]

    # 全部开启sku数量
    whole_enabled_sku_num = len(set(camp_data['SKU'][camp_data['Record Type'] == 'Ad']))
    # 全部开启手动广告sku数量与缺失sku数量
    whole_enabled_manual_sku = set(
        camp_data['SKU'][(camp_data['Record Type'] == 'Ad') & (camp_data['Campaign Targeting Type'] == 'Manual')])
    whole_enabled_manual_sku_num = len(whole_enabled_manual_sku)
    manual_no_sku_num = len([sku for sku in whole_enabled_manual_sku if 'no_sku' in sku])
    # 全部开启自动广告sku数量与缺失sku数量
    whole_enabled_auto_sku = set(
        camp_data['SKU'][(camp_data['Record Type'] == 'Ad') & (camp_data['Campaign Targeting Type'] == 'Auto')])
    whole_enabled_auto_sku_num = len(whole_enabled_auto_sku)
    auto_no_sku_num = len([sku for sku in whole_enabled_auto_sku if 'no_sku' in sku])
    now_time = datetime.now()
    whole_info = [account, site, whole_enabled_sku_num, whole_enabled_manual_sku_num, manual_no_sku_num,
                  whole_enabled_auto_sku_num, auto_no_sku_num, now_time]
    # 上传到数据库中
    db_upload_camp_no_sku_name_num(whole_info, station_name)


def get_n_db_upload_repeat_sku(camp_data_ori, station_name):
    """
    获得广告报表中在售的重复的sku，并按类型入库
    其中auto手动广告按sku分组,自动广告manual按sku,kws,广告类型分组
    列
    all_camp_columns=['account','site','Record ID', 'Record Type', 'Campaign ID', 'Campaign',
       'Campaign Daily Budget', 'Portfolio ID', 'Campaign Start Date',
       'Campaign End Date', 'Campaign Targeting Type', 'Ad Group', 'Max Bid',
       'Keyword or Product Targeting', 'Product Targeting ID', 'Match Type',
       'SKU', 'Campaign Status', 'Ad Group Status', 'Status', 'Impressions',
       'Clicks', 'Spend', 'Orders', 'Total Units', 'Sales', 'ACoS',
       'Bidding strategy', 'Placement Type', 'Increase bids by placement']
    need_columns=['account','site','Record ID', 'Record Type', 'Campaign ID', 'Campaign',
       'Campaign Daily Budget', 'Portfolio ID', 'Campaign Start Date',
       'Campaign End Date', 'Campaign Targeting Type', 'Ad Group', 'Max Bid',
       'Keyword or Product Targeting', 'Product Targeting ID', 'Match Type',
       'SKU', 'Campaign Status', 'Ad Group Status', 'Status', 'Impressions',
       'Clicks', 'Spend', 'Orders', 'Total Units', 'Sales', 'ACoS']
    :param camp_data_ori:原始广告数据
    :param station_name:账号站点
    :return:None
    """
    camp_data = camp_data_ori.copy()
    camp_data = camp_data.applymap(lambda x: None if x == " " else x)
    need_columns = ['account', 'site', 'Record ID', 'Record Type', 'Campaign ID', 'Campaign',
                    'Campaign Daily Budget', 'Campaign Start Date',
                    'Campaign End Date', 'Campaign Targeting Type', 'Ad Group', 'Max Bid',
                    'Keyword or Product Targeting', 'Product Targeting ID', 'Match Type',
                    'SKU', 'Campaign Status', 'Ad Group Status', 'Status', 'Impressions',
                    'Clicks', 'Spend', 'Orders', 'Sales', 'ACoS']
    # 初始化1. 将广告类型和出价补全
    #        广告类型和出价都是向下充填
    camp_data['Campaign Targeting Type'].fillna(method='ffill', inplace=True)
    camp_data['Campaign Daily Budget'].fillna(method='ffill', inplace=True)
    camp_data['Campaign Start Date'].fillna(method='ffill', inplace=True)
    camp_data['Campaign End Date'].fillna(method='ffill', inplace=True)
    camp_data['Max Bid'].fillna(method='ffill', inplace=True)
    #       2. 选择在售的广告
    # camp_data[['Campaign Status', 'Ad Group Status', 'Status']] = camp_data[
    #     ['Campaign Status', 'Ad Group Status', 'Status']].applymap(lambda x: x.lower() if ~pd.isna(x) else x)
    camp_data = camp_data[
        (camp_data['Campaign Status'] == 'enabled') & (camp_data['Ad Group Status'] == 'enabled') & (
                camp_data['Status'] == 'enabled')]
    #       3.留下广告和kws一级
    camp_data = camp_data[camp_data['Record Type'].isin(['Ad', 'Keyword', 'Product Targeting'])]
    # enable_camp_data['add_sku'] = enable_camp_data['SKU'].copy()
    # enable_camp_data['add_sku'].fillna(method='ffill',inplace=True)
    account = station_name[:-3]
    site = station_name[-2:]
    col_name = camp_data.columns.tolist()
    col_name.insert(0, 'account')
    col_name.insert(1, 'site')
    camp_data = camp_data.reindex(columns=col_name)
    exist_col = camp_data.columns.tolist()
    camp_data['account'] = account
    camp_data['site'] = site

    if not set(need_columns).issubset(set(exist_col)):
        lost_col = set(need_columns) - set(exist_col)
        print(f"{station_name}广告报表缺失{lost_col}列")

    camp_data = camp_data[need_columns]

    # 填充AD行中sku为空
    no_sku_list = list(camp_data['SKU'][camp_data['Record Type'] == 'Ad'][
                           pd.isna(camp_data['SKU'][camp_data['Record Type'] == 'Ad'])])
    if no_sku_list:
        no_sku_list = ["no_sku_" + str(i) for i in range(len(no_sku_list))]
        camp_data.ix[list(camp_data['SKU'][camp_data['Record Type'] == 'Ad'][pd.isna(
            camp_data['SKU'][camp_data['Record Type'] == 'Ad'])].index), 'SKU'] = no_sku_list

    # 得到店铺广告报表中开启状态下空sku信息并上传到数据库中
    get_camp_no_sku_name_num(camp_data, station_name)

    auto_camp_data = camp_data[camp_data['Campaign Targeting Type'] == 'Auto']
    manual_camp_data = camp_data[camp_data['Campaign Targeting Type'] == 'Manual']

    # 筛选出重复的自动sku
    auto_sku_series = auto_camp_data['SKU'].value_counts(dropna=True)
    auto_repeat_sku_set = set(auto_sku_series[auto_sku_series > 1].index)
    auto_camp_data['SKU'].fillna(method='ffill', inplace=True)
    auto_repeat_sku_data = auto_camp_data[auto_camp_data['SKU'].isin(auto_repeat_sku_set)]
    auto_repeat_sku_data = auto_repeat_sku_data[auto_repeat_sku_data['Record Type'] == 'Ad']
    db_upload_repeat_sku(auto_repeat_sku_data, station_name, campaign_targeting_type='auto')

    # # 自动广告的呈现方式
    # auto_repeat_sku_data = auto_repeat_sku_data.groupby('SKU').agg(
    #     {'Keyword or Product Targeting': '/'.join, 'Impressions': 'sum', 'Clicks': 'sum', 'Spend': 'sum',
    #              'Orders': 'sum', 'Total Units':'sum','Sales':'sum'}).reset_index()

    # 筛选出重复的手动sku
    manual_camp_data['SKU'].fillna(method='ffill', inplace=True)
    manual_camp_data = manual_camp_data[manual_camp_data['Record Type'] == 'Keyword']
    manual_camp_data['aid_repeat_column'] = manual_camp_data['SKU'] + "&&" + manual_camp_data[
        'Keyword or Product Targeting'] + "&&" + manual_camp_data['Match Type']
    manual_sku_series = manual_camp_data['aid_repeat_column'].value_counts(dropna=True)
    manual_repeat_sku_set = set(manual_sku_series[manual_sku_series > 1].index)
    manual_repeat_sku_data = manual_camp_data[manual_camp_data['aid_repeat_column'].isin(manual_repeat_sku_set)]
    del manual_repeat_sku_data['aid_repeat_column']
    db_upload_repeat_sku(manual_repeat_sku_data, station_name, campaign_targeting_type='manual')


# 从服务器的team_station.station_uuid_index 上获得站点旧的uuid,并更新掉旧的uuid
def db_download_station_old_uuid(station_name, station_new_uuid, db='team_station', ip='192.168.129.240',
                                 user_name='marmot',
                                 password='marmot123', port=3306, table='station_uuid_index') -> list:
    conn = pymysql.connect(
        host=ip,
        user=user_name,
        password=password,
        database=db,
        port=port,
        charset='UTF8')

    # 创建游标
    cursor = conn.cursor()
    # table_name
    table_name = table

    # 规范站点名
    station_name = station_name.lower()
    station_name = station_name.replace("-", '_').replace(" ", "_")

    try:
        # 查询旧的uuid
        select_sql = """SELECT * FROM {} where station = {}""".format(table_name, "'%s'" % station_name)
        # 执行sql
        cursor.execute(select_sql)
        all_result = cursor.fetchall()
        station_uuid_index = list(all_result)
        if station_uuid_index:
            station_old_uuid = json.loads(station_uuid_index[0][1])['uuid']
        else:
            station_old_uuid = []
        # 将旧的uuid替换掉
        delete_sql = """DELETE FROM {} where station = {}""".format(table_name, "'%s'" % station_name)
        insert_sql = """INSERT INTO {} (station,uuid) values(%s,%s)""".format(table_name)
        station_new_uuid_json = json.dumps({"uuid": station_new_uuid})
        station_new_uuid_tuple = tuple([station_name, station_new_uuid_json])
        cursor.execute(delete_sql)
        cursor.execute(insert_sql, station_new_uuid_tuple)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(e)
        print(f"从服务器上team_station.station_uuid_index更新{station_name}uuid出错。")
        conn.rollback()
        conn.close()
        return []
    return station_old_uuid


# 从服务器上传sku,asin,手动广告的关键词的uuid
def db_upload_uuid_key_index(key_name, key_info, uuid_station_num, db='team_station', ip='192.168.129.240',
                             user_name='marmot',
                             password='marmot123', port=3306):
    """
    :param uuid_station_num: 每个站点的uuid的编号
    """

    # 加载所有的对应需要新增的关键列表
    key_info = key_info[~pd.isna(key_info[key_name])]
    key_info = key_info[~key_info[key_name].isin(['', ' '])]

    key_list = list(key_info[key_name])
    if not key_list:
        return
    # 首先加载数据库中关键词与uuid信息
    exist_uuid = db_download_uuid_key_index(key_name, key_list)
    if exist_uuid.empty:
        import_uuid_index = key_info
    else:
        # 首先将数据库中的uuid信息转换为list 在数据库中uuid是以json的格式存储的'{'uuid':[1,2,3]}'
        import_uuid_index = key_info.copy()
        exist_uuid['uuid1'] = exist_uuid['uuid'].apply(lambda x: json.loads(x)['uuid'])
        del exist_uuid['uuid']
        # 删掉本站点的uuid
        uuid_station_num = str(uuid_station_num)[0:4]
        # print(f'站点编号{uuid_station_num}')
        filter_station_old_uuid = []
        for key_uuids in exist_uuid['uuid1']:
            one_sku_all_uuid = [key_uuid for key_uuid in key_uuids if uuid_station_num != str(key_uuid)[10:14]]
            filter_station_old_uuid.append(one_sku_all_uuid)
        exist_uuid['uuid1'] = filter_station_old_uuid

        import_uuid_index = pd.merge(import_uuid_index, exist_uuid, on=key_name)
        [import_uuid.extend(exist) for import_uuid, exist in zip(import_uuid_index['uuid'], import_uuid_index['uuid1'])]

    import_uuid_index['uuid'] = import_uuid_index['uuid'].apply(lambda x: json.dumps({'uuid': list(set(x))}))

    import_uuid_index = import_uuid_index[[key_name, 'uuid']]

    all_list = []
    import_uuid_index.reset_index(drop=True, inplace=True)
    df = import_uuid_index.astype(object).replace(np.nan, 'None')
    df = np.array(df)
    len_df = df.shape[0]
    for i in range(len_df):
        temp_tuple = df[i]
        a_emp_tuple = tuple(temp_tuple)
        all_list.append(a_emp_tuple)

    # 创建连接
    conn = pymysql.connect(
        host=ip,
        user=user_name,
        password=password,
        database=db,
        port=port,
        charset='UTF8')

    # 创建游标
    cursor = conn.cursor()

    # 上传到sql中
    key_list_str = ",".join(list(map(lambda x: "'%s'" % x, key_list)))
    table_name = "station_" + key_name + "_uuid_index"
    delete_sql = """DELETE FROM {} where {} in ({}) """.format(table_name, "%s" % key_name, key_list_str)
    insert_sql = """INSERT INTO {} ({},uuid) values (%s,%s)""".format(table_name, "%s" % key_name)
    try:
        cursor.execute(delete_sql)
        cursor.executemany(insert_sql, all_list)
        conn.commit()
        # print(f'{key_name}上传uuid成功')
    except Exception as e:
        conn.rollback()
        print(f'{key_name}上传uuid失败')
        print(e)


# 从服务器加载sku,asin,手动广告的关键词的uuid
def db_download_uuid_key_index(key_name, key_list, db='team_station', ip='192.168.129.240',
                               user_name='marmot',
                               password='marmot123', port=3306):
    conn = pymysql.connect(
        host=ip,
        user=user_name,
        password=password,
        database=db,
        port=port,
        charset='UTF8')

    # 创建游标
    cursor = conn.cursor()
    # table_name
    table_name = "station_" + key_name + "_uuid_index"
    # 写sql
    if not key_list:
        return pd.DataFrame([])
    key_list_str = ",".join(list(map(lambda x: "'%s'" % x, key_list)))
    select_sql = """SELECT * FROM {} where {} in ({}) """.format(table_name, "%s" % key_name, key_list_str)
    # 执行sql语
    cursor.execute(select_sql)
    all_result = cursor.fetchall()
    all_result = pd.DataFrame([list(j) for j in all_result], columns=[f'{key_name}', 'uuid'])
    conn.commit()
    cursor.close()
    conn.close()
    return all_result


# 将新品未打广告信息上传到数据库中
def db_upload_new_listing_no_sku(new_listing_no_sku_info: list, station_name, db='team_station',
                                 ip='192.168.129.240',
                                 user_name='marmot',
                                 password='marmot123', port=3306):
    """
    将手动与自动广告上传到数据库中
    :param new_listing_no_sku_info:新品未打广告信息
    :param station_name:账号站点
    :return: None
    """
    new_listing_no_sku_tuple = tuple(new_listing_no_sku_info)
    station_name = station_name.lower()
    account = station_name[:-3]
    site = station_name[-2:]
    # 创建连接
    conn = pymysql.connect(
        host=ip,
        user=user_name,
        password=password,
        database=db,
        port=port,
        charset='UTF8')

    table_name = 'station_new_listing_no_sku'

    # 创建游标
    cursor = conn.cursor()

    insert_sql = """insert into {} (account,site,new_listing_no_sku_30days, new_listing_no_sku_all_days, active_sku_num,
            camp_enable_sku_num,update_time) values (%s,%s,%s,%s,%s,%s,%s)""".format(
        table_name)
    delete_sql = """DELETE from {} where account = {} and site = {} """.format(table_name, "'%s'" % account,
                                                                               "'%s'" % site)

    try:
        cursor.execute(delete_sql)
        cursor.execute(insert_sql, new_listing_no_sku_tuple)
        conn.commit()
        # print(f"成功! 更新{station_name} 新品未打广告信息")
    except Exception as e:
        conn.rollback()
        print(e)
        print(f"失败! 更新{station_name} 新品未打广告信息")


# 新品未做广告数量(active_listing中新品分为30天以及表中全部sku)
def new_listing_no_sku(station_name, camp_ori_data, active_data):
    enable_camp_sku = set(camp_ori_data['SKU'][(camp_ori_data['Campaign Status'] == 'enabled') & (
            camp_ori_data['Ad Group Status'] == 'enabled') & (camp_ori_data['Status'] == 'enabled')])
    station_active_all_sku_quantity = len(active_data)
    station_camp_sku_quantity = len(enable_camp_sku)
    new_listing_no_sku_all = active_data[~active_data['seller-sku'].isin(enable_camp_sku)]
    if new_listing_no_sku_all.empty:
        new_listing_no_sku_all_quantity = 0
    else:
        new_listing_no_sku_all_quantity = len(new_listing_no_sku_all)
    now_date = datetime.now()
    days_before_30 = now_date.date() - timedelta(days=30)
    new_listing_no_sku_all['open-date'] = pd.to_datetime(new_listing_no_sku_all['open-date'], dayfirst=True)
    new_listing_no_sku_30days = new_listing_no_sku_all[new_listing_no_sku_all['open-date'] >= days_before_30]
    if new_listing_no_sku_30days.empty:
        new_listing_no_sku_30days_quantity = 0
    else:
        new_listing_no_sku_30days_quantity = len(new_listing_no_sku_30days)
    # 站点新品未做广告数据包括:账号,站点,30点未做广告数量,全部时间未做广告数量,active中广告数量,camp中广告数量,更新时间
    station_now_listing_no_sku_info = [station_name[:-3], station_name[-2:], new_listing_no_sku_30days_quantity,
                                       new_listing_no_sku_all_quantity, station_active_all_sku_quantity,
                                       station_camp_sku_quantity, now_date]
    # 将信息上传到数据库中
    db_upload_new_listing_no_sku(station_now_listing_no_sku_info, station_name)


def read_process_upload():
    station_dir = red.lpop('camp_h5')
    # process_station_num = 0
    if station_dir:
        print("开始处理:" + os.path.basename(station_dir))
        try:
            cam_dir = glob.glob(station_dir + "/*h5")
            active_listing_dir = glob.glob(station_dir + "/*Active*")
            camp_ori_data = pd.read_hdf(cam_dir[0], key='df')
            station_name = os.path.basename(station_dir).upper()

            # r1. 将重复广告入库
            get_n_db_upload_repeat_sku(camp_ori_data, station_name)

            try:
                active_data = read_active_listing(active_listing_dir[0], station_name,
                                                  need_columns=['seller-sku', 'asin1', 'open-date'])
                # r2. 将新品未打广告的上传到数据中
                new_listing_no_sku(station_name, camp_ori_data, active_data)
                active_data = active_data[['seller-sku', 'asin1']]
            except:
                active_data = read_active_listing(active_listing_dir[0], station_name,
                                                  need_columns=['seller-sku', 'asin1'])

            # 得到站点sku数据
            camp_all_sku_data = process_campaign_data(station_name, camp_ori_data, active_data,
                                                      export_columns=['account', 'site', 'Campaign', 'Ad Group',
                                                                      'ad_group_bid', 'SKU', 'asin',
                                                                      'Keyword or Product Targeting',
                                                                      'Max Bid', 'Campaign Targeting Type',
                                                                      'Match Type', 'negative_keyword',
                                                                      'negative_asin', 'target_asin',
                                                                      'Campaign Status',
                                                                      'Ad Group Status', 'Status',
                                                                      'Impressions',
                                                                      'Clicks', 'Spend', 'Orders', 'Sales',
                                                                      'ACoS'])
            if camp_all_sku_data.empty:
                return
            standard_camp_data(camp_all_sku_data,
                               columns=['Impressions', 'Clicks', 'Spend', 'Orders', 'Sales', 'ACoS'])

            # 新建标识列(uuid)(时间戳+站点id编号+行id)
            # 获得站点id编号
            all_station_name = db_download_station_names(db='team_station', table='only_station_name_index',
                                                         ip='192.168.129.240', port=3306,
                                                         user_name='marmot', password='marmot123')
            if station_name.lower() in list(all_station_name['station_name']):
                station_id = \
                    all_station_name['station_id'][all_station_name['station_name'] == station_name.lower()].values[0]
            else:
                new_id = max(all_station_name['station_id']) + 1
                new_station = (new_id, station_name.lower())
                db_insert_station_name(new_station, db='team_station', table='only_station_name_index',
                                       ip='192.168.129.240',
                                       port=3306,
                                       user_name='marmot', password='marmot123')
                station_id = new_id
            station_id = (int(2e3) + station_id) * int(1e7)
            # 获得时间戳
            now_time = int(time.time())
            # 新的uuid
            time_e = int(1e11)
            new_uuid = [now_time * time_e + station_id + int(2e6) + i for i in range(len(camp_all_sku_data))]
            camp_all_sku_data['uuid'] = new_uuid
            # 旧的uuid
            old_uuid = db_download_station_old_uuid(station_name, new_uuid, db='team_station', ip='192.168.129.240',
                                                    user_name='marmot',
                                                    password='marmot123', port=3306, table='station_uuid_index')
            # print(f"{station_name}旧的uuid长度为{len(old_uuid)}")
            # r2 更新mysql中站点sku数据

            db_upload_sku_info(camp_all_sku_data, new_uuid, old_uuid, db='team_station', table_name='sku_kws_info',
                               ip='192.168.129.240',
                               user_name='marmot',
                               password='marmot123', port=3306)

            # r3 更新redis中站点uuid数据
            # sql_to_redis(camp_all_sku_data)
            shutil.rmtree(station_dir)
            # process_station_num += 1
            # print("{} redis更新完成...".format(station_name))
            # 加载sku,asin,keyword的uuid索引到对应的数据库中
            sku_uuid_info = camp_all_sku_data.groupby(['SKU']).agg({'uuid': lambda x: x.tolist()}).reset_index()
            try:
                db_upload_uuid_key_index('SKU', sku_uuid_info, station_id)
            except Exception as e:
                print(e)
                pass
            asin_uuid_info = camp_all_sku_data.groupby(['asin']).agg({'uuid': lambda x: x.tolist()}).reset_index()
            try:
                db_upload_uuid_key_index('asin', asin_uuid_info, station_id)
            except Exception as e:
                print(e)
                pass
            manual_info = camp_all_sku_data[camp_all_sku_data['Campaign_Targeting_Type'] == 'Manual']
            if manual_info.empty:
                pass
            else:
                kws_uuid_info = manual_info.groupby(['Keyword_or_Product_Targeting']).agg(
                    {'uuid': lambda x: x.tolist()}).reset_index()
                kws_uuid_info.rename(columns={'Keyword_or_Product_Targeting': 'keyword'}, inplace=True)
                try:
                    db_upload_uuid_key_index('keyword', kws_uuid_info, station_id)
                except Exception as e:
                    # print(e)
                    pass

            print(f"{station_name}全部完成.")
        except Exception as err:
            print(err)
            # red.rpush('camp_h5', station_dir)

    else:
        # print("暂时没有站点需要写入到数据库中，休息1分钟...")
        # print("暂时没有站点需要写入到数据库中，休息1分钟...!!!")
        time.sleep(60)
        while datetime.now().hour == 1:
            print("进入到1点，早上9点再更新...")
            time.sleep(28800)


def process_read_file():
    while 1:
        all_task = []
        for one_page in range(4):
            all_task.append(PROCESS_POOL.submit(read_process_upload))
        for future in as_completed(all_task):
            future.result()


if __name__ == "__main__":
    process_read_file()
