import os
from datetime import datetime, timedelta
import time
import pymysql
import shutil
import zipfile
import re
import numpy as np
import warnings
import pandas as pd
import redis
import uuid

import sys

sys.path.append('E:/ad_zyp/price_dist/my_toolkit')
from read_campaign import read_campaign
from init_campaign import init_campaign

"""
读取五表文件夹下的站点的所有压缩包，解压后得到所有站点的广告报表，得到广告报表中sku与aisn信息，用于查找
sku与asin相关信息
字段包括：
账号,站点,广告系列名,广告组名,广告组竞价,SKU,ASIN,关键词，
匹配类型,negative_keyword,negative_asin,定向asin,广告组的三种状态
"""
warnings.filterwarnings("ignore")
station_folder = r"F:\station_folder"
station_zipped_folder = "E:/ad_zyp/store_ad_info/temp"

pool = redis.ConnectionPool(host='127.0.0.1', port=6379, decode_responses=True)
red = redis.StrictRedis(connection_pool=pool)
# 处理uuid
redis_pool2 = redis.ConnectionPool(host='127.0.0.1', port=6379, db=2, decode_responses=True)
red2 = redis.StrictRedis(connection_pool=redis_pool2)


# 解压文件包
def unzip_dir(zip_dir):
    z = zipfile.ZipFile(zip_dir, "r")
    # 打印zip文件中的文件列表
    file_name_list = z.namelist()
    writer_folder = os.path.join(station_zipped_folder, os.path.basename(zip_dir)[:-4])
    if os.path.exists(writer_folder):
        shutil.rmtree(writer_folder)
    os.mkdir(writer_folder)
    file_name_list = [file for file in file_name_list if file.find('/') == -1]
    for filename in file_name_list:
        content = z.read(filename)
        with open(writer_folder + '/' + filename, 'wb') as f:
            f.write(content)


def trans_active_columns(input_df):
    """
    翻译activelisting中的表头
    :param input_df:activelisting数据
    :return:翻译好表头后的数据
    """
    active_df = input_df.copy()
    all_columns = active_df.columns
    if 'ASIN 1' in all_columns:
        report = active_df[['出品者SKU', '価格', 'ASIN 1', '商品名', '出品日', 'フルフィルメント・チャンネル']]
        report.rename(columns={'出品者SKU': 'seller-sku',
                               '価格': 'price',
                               'ASIN 1': 'asin1',
                               '商品名': 'item-name',
                               '出品日': 'open-date',
                               'フルフィルメント・チャンネル': 'fulfillment-channel',
                               }, inplace=True)
    elif 'ASIN1' in all_columns:
        report = active_df[['卖家 SKU', '价格', 'ASIN1', '商品名称', "开售日期", "配送渠道"]]
        report.rename(columns={'卖家 SKU': 'seller-sku',
                               '价格': 'price',
                               'ASIN1': 'asin1',
                               '商品名称': 'item-name',
                               "开售日期": "open-date",
                               "配送渠道": "fulfillment-channel"
                               }, inplace=True)
    elif 'asin1' in all_columns:
        report = active_df[['seller-sku', 'asin1', 'price', 'item-name', 'open-date', 'fulfillment-channel']]
    else:
        report = 0

    return report


def read_active_listing(avtivelisting_path: 'full_dir', station_name, need_columns=['seller-sku', 'asin1']):
    """
    读取activelisting表，得到其中几列（默认为sellersku与asin）.
    :param avtivelisting_path: 在售商品报表路径
    :param station_name:站点名
    :param need_columns:需要列
    :return:sellersku与asin1对应表
    """
    # 读取activelisting表
    empty_activelisting = pd.DataFrame([], columns=need_columns)
    station_name = station_name.upper()
    site = station_name[-2:]
    if not avtivelisting_path:
        return empty_activelisting
    try:
        activelisting_data = pd.read_table(avtivelisting_path)
    except:
        with open(avtivelisting_path, 'rb') as f:
            data = f.readlines()
        # activelisting没有数据
        activelisting_data = [line.decode('utf-8', errors='ignore').strip('\n').strip('\r').split('\t') for line in
                              data]
        activelisting_data = pd.DataFrame(activelisting_data[1:], columns=activelisting_data[0])
    # 翻译日本表头
    if site == 'JP':
        activelisting_data = trans_active_columns(activelisting_data)
    # 判断表数据是否为空
    if activelisting_data.empty:
        return empty_activelisting
    # 判断需要的列是否存在
    active_listing_columns = activelisting_data.columns
    active_listing_columns = [column.replace(" ", '').lower() for column in active_listing_columns]
    if not set(need_columns).issubset(set(active_listing_columns)):
        lost_columns = set(need_columns) - set(active_listing_columns)
        print("error3: {}的active_listing表缺失{}列...".format(station_name, lost_columns))
        print("请检查{} {}active_listing表...".format(station_zipped_folder, station_name))
        return empty_activelisting
    # 得到需要列数据，并做去空值，去重复项
    activelisting_data = activelisting_data[need_columns]
    activelisting_data = activelisting_data.applymap(lambda x: None if x in ['', ' '] else x)
    activelisting_data.dropna(inplace=True)
    activelisting_data.drop_duplicates(subset='seller-sku', keep='first', inplace=True)
    activelisting_data.reset_index(drop=True, inplace=True)
    return activelisting_data


# 读取广告报表
def my_read_campaign(file_dir: 'full_dir', file_site, need_columns='all'):
    """
    读取广告报表
    :param file_dir:广告报表的全路径
    :param file_site:广告报表的国家(用于翻译)
    :param need_columns:需要的列(默认为全列)
    :return:需要的广告报表数据列
    """
    # 读取excel内容
    file_data = read_campaign(file_dir, file_site)
    file_data = init_campaign(file_data, file_site.upper(), 'empty')
    if file_data.empty:
        print("error2: {}的广告报表的数据为空".format(os.path.dirname(file_dir)))
    # 选择全部列
    if need_columns.lower() == 'all':
        return file_data
    # 选择部分列
    if set(need_columns).issubset(set(file_data.columns)):
        file_data = file_data[need_columns]
        return file_data
    else:
        lost_columns = set(need_columns) - set(file_data.columns)
        print("error3: {}的广告报表的缺少{}列".format(os.path.dirname(file_dir), lost_columns))


def sql_to_redis(all_df):
    if all_df.empty:
        return

    for one_asin, asin_group in all_df.groupby('asin'):
        # 根据name删除redis中的任意数据类型
        if pd.isna(one_asin):
            continue
        if one_asin in [' ', '*', '']:
            continue
        # if red.exists(one_asin):
        #     key_type = red.type(one_asin)
        #     # red.delete(one_asin)
        #     if key_type == 'list':
        #         val_result = red.lrange(one_asin, 0, -1)
        #         for one_value in val_result:
        #             if re.search(account_site, one_value):
        #                 red.lrem(one_asin, 0, one_value)
        red.rpush(one_asin, *list(asin_group['uuid']))

    for one_sku, asin_group in all_df.groupby('SKU'):
        # 根据name删除redis中的任意数据类型
        if pd.isna(one_sku):
            continue
        if one_sku in [' ', '*', '']:
            continue
        one_sku = str(one_sku)
        # if red.exists(one_sku):
        #     key_type = red.type(one_sku)
        #     # red.delete(one_asin)
        #     if key_type == 'list':
        #         val_result = red.lrange(one_sku, 0, -1)
        #         for one_value in val_result:
        #             if re.search(account_site, one_value):
        #                 red.lrem(one_sku, 0, one_value)
        red.rpush(one_sku, *list(asin_group['uuid']))

    for one_kw, asin_group in all_df.groupby('Keyword_or_Product_Targeting'):
        # 根据name删除redis中的任意数据类型
        if pd.isna(one_kw):
            continue
        if one_kw in [' ', '*', '']:
            continue
        # if red.exists(one_kw):
        #     key_type = red.type(one_kw)
        #     # red.delete(one_asin)
        #     if key_type == 'list':
        #         val_result = red.lrange(one_kw, 0, -1)
        #         for one_value in val_result:
        #             if re.search(account_site, one_value):
        #                 red.lrem(one_kw, 0, one_value)
        red.rpush(one_kw, *list(asin_group['uuid']))


# 获得文件夹下的广告报表路径
def get_camp_file_dir(ad_file_path: 'full_path', match_keyword='bulk') -> 'full_path':
    """
    得到文件夹下的广告报表
    :param ad_file_path: 广告报表的父目录全路径
    :param match_keyword: 匹配广告报表的关键词
    :return: 广告报表的全路径
    """
    file_lists = os.listdir(ad_file_path)
    ad_file_basename = [file for file in file_lists if match_keyword in file]
    if len(ad_file_basename) == 0:
        print("error1: {}中没有广告报表,请查看.".format(os.path.basename(ad_file_path)))
    elif len(ad_file_basename) >= 2:
        print("error1: {}中存在多个广告报表,请查看.".format(os.path.basename(ad_file_path)))
    return os.path.join(ad_file_path, ad_file_basename[0])


# 获得文件夹下的在售商品报表(activelisting)路径
def get_activelisting_file_dir(ad_file_path: 'full_path', match_keyword='Active+Listings') -> 'full_path':
    """
    得到文件夹下的在售商品报表
    :param ad_file_path: 在售商品报表的父目录全路径
    :param match_keyword: 匹配在售商品报表的关键词
    :return: 在售商品报表的全路径,错误返回空值
    """
    file_lists = os.listdir(ad_file_path)
    ad_file_basename = [file for file in file_lists if match_keyword in file]
    if len(ad_file_basename) == 0:
        print("error1: {}中没有activelisting,请查看.".format(os.path.basename(ad_file_path)))
        return []
    elif len(ad_file_basename) >= 2:
        print("error1: {}中存在多个activelisting,请查看.".format(os.path.basename(ad_file_path)))
    return os.path.join(ad_file_path, ad_file_basename[0])


def concat_sku(one_grouped_data, empty_data):
    """
    将一组聚合后的数据进行处理后形成以一个sku为一行的数据
    :param one_grouped_data: 一组聚合后的数据
    :param empty_data: 空的数组
    :return: 多行单个sku行
    """
    ad_group_bid = one_grouped_data['Max Bid'][one_grouped_data['Record Type'] == 'Ad Group'].values[0]
    ad_group_bid = str(ad_group_bid).replace(",", ".")
    one_grouped_data['Max Bid'].fillna(ad_group_bid, inplace=True)
    one_grouped_data['Max Bid'] = one_grouped_data['Max Bid'].apply(
        lambda x: ad_group_bid if x == ' ' else str(x).replace(",", "."))
    # 获取asin
    asin = re.findall('B0.{8}', one_grouped_data['Ad Group'].values[0])
    if not asin:
        asin = ''
    else:
        asin = asin[0]
    all_sku = set(one_grouped_data['SKU'][one_grouped_data['Record Type'] == 'Ad'].dropna())
    # empty_data = pd.DataFrame(columns=all_columns)
    if one_grouped_data.empty:
        print("没有数据")
        return empty_data
    camp_grouped_data = []
    for sku in all_sku:
        # 一个sku数据
        one_sku_data = one_grouped_data[~pd.isna(one_grouped_data['Keyword or Product Targeting'])]
        # one_sku_data = one_sku_data.query('Keyword or Product Targeting !=" "')
        one_sku_data = one_sku_data[one_sku_data['Keyword or Product Targeting'] != ' ']
        # [one_sku_data['Keyword or Product Targeting'] != ' ']
        one_sku_data['SKU'] = sku
        one_sku_data['asin'] = asin
        one_sku_data['ad_group_bid'] = ad_group_bid
        if one_sku_data.empty:
            continue
        else:
            camp_grouped_data.append(one_sku_data)
    if len(camp_grouped_data) > 0:
        return pd.concat(camp_grouped_data)
    else:
        return empty_data


# 对广告报表数据进行处理,得到账号,站点,广告系列名,广告组名,广告组竞价,SKU,ASIN,关键词，
# 匹配类型,negative_keyword,negative_asin,定向asin,广告组的三种状态
def process_campaign_data(station_name, camp_data: "pd.DataFrame", active_data,
                          export_columns=['account', 'site', 'Campaign', 'Ad Group', 'ad_group_bid', 'SKU', 'asin',
                                          'Keyword or Product Targeting',
                                          'Max Bid', 'Campaign Targeting Type', 'Match Type', 'negative_keyword',
                                          'negative_asin', 'target_asin', 'Campaign Status',
                                          'Ad Group Status', 'Status']) -> 'pd.DataFrame':
    """
    通过处理广告报表数据，得到想要的列
    '账号','站点','广告系列','广告组','广告组竞价','sku','asin'，'keyword','关键词出价','匹配类型','negative_keyword'
    'negative_asin','广告类型','广告系列状态','广告组状态','sku状态'
    :param station_name: 站点名
    :param camp_data: 广告报表数据
    :param active_data: 在售商品报表数据
    :param export_columns: 计算需要的列
    :return: 需要的列
    """
    # 首先判断计算列在不在
    need_columns = ['Campaign Targeting Type', 'Ad Group', 'Campaign', 'Max Bid', 'SKU',
                    'Keyword or Product Targeting', 'Match Type', 'Campaign Status', 'Ad Group Status', 'Status']
    if not set(need_columns).issubset(set(camp_data.columns)):
        lost_columns = set(need_columns) - set(camp_data.columns)
        print("error3: 广告报表{}列缺失".format(lost_columns))
    # 首先向下充填Campaign Targeting Type
    try:
        camp_data['Campaign Targeting Type'] = camp_data['Campaign Targeting Type'].apply(
            lambda x: np.NAN if x == ' ' else x)
    except:
        pass
    camp_data['Campaign Targeting Type'].fillna(method='ffill', inplace=True)
    # 去除ad_group为空的列
    camp_data = camp_data[~pd.isna(camp_data['Ad Group'])]
    # 去除ad_group为" "的列空字符串的列
    camp_data = camp_data[camp_data['Ad Group'] != ' ']
    # 将站点内的广告数据 按照广告大组，广告小组以及广告方式进行分组
    camp_data_grouped = camp_data.groupby(['Campaign', 'Ad Group', 'Campaign Targeting Type'])
    empty_dataframe = pd.DataFrame(columns=camp_data.columns)
    # 组内数据处理
    camp_grouped_data = [concat_sku(grouped_data, empty_dataframe) for camp_key, grouped_data in camp_data_grouped]
    try:
        camp_grouped_data = pd.concat(camp_grouped_data)
    except:
        print("{}的广告数据有问题... 来源{}".format(station_name, station_folder))
        return pd.DataFrame([])

    # 创建定向ASIN，negative_asin，negative_keyword列
    camp_grouped_data['negative_keyword'] = ''
    camp_grouped_data['negative_asin'] = ''
    camp_grouped_data['target_asin'] = ''
    # negative_keyword赋值
    camp_grouped_data['negative_keyword'][(camp_grouped_data['Record Type'] == 'Keyword') & (
        camp_grouped_data['Match Type'].str.contains('negative'))] = \
        camp_grouped_data['Keyword or Product Targeting'][(camp_grouped_data['Record Type'] == 'Keyword') & (
            camp_grouped_data['Match Type'].str.contains('negative'))]

    # negative_asin Rcord Type=Product Targeting 和match type = Negative Targeting Expression
    camp_grouped_data['negative_asin'][(camp_grouped_data['Record Type'] == 'Product Targeting') & (
            camp_grouped_data['Match Type'] == 'Negative Targeting Expression')] = \
        camp_grouped_data['Keyword or Product Targeting'][
            (camp_grouped_data['Record Type'] == 'Product Targeting') & (
                    camp_grouped_data['Match Type'] == 'Negative Targeting Expression')]

    # target_asin
    camp_grouped_data['target_asin'][(camp_grouped_data['Record Type'] == 'Product Targeting') & (
            camp_grouped_data['Match Type'] == 'Targeting Expression')] = \
        camp_grouped_data['Keyword or Product Targeting'][
            (camp_grouped_data['Record Type'] == 'Product Targeting') & (
                    camp_grouped_data['Match Type'] == 'Targeting Expression')]

    # 处理'Keyword or Product Targeting'列的包含定向ASIN，negative_asin，negative_keyword
    camp_grouped_data['Keyword or Product Targeting'][(camp_grouped_data['Record Type'] == 'Product Targeting') & (
            camp_grouped_data['Match Type'] == 'Targeting Expression')] = ''
    camp_grouped_data['Keyword or Product Targeting'][(camp_grouped_data['Record Type'] == 'Keyword') & (
        camp_grouped_data['Match Type'].str.contains('negative'))] = ''
    camp_grouped_data['Keyword or Product Targeting'][(camp_grouped_data['Record Type'] == 'Product Targeting') & (
            camp_grouped_data['Match Type'] == 'Negative Targeting Expression')] = ''
    camp_grouped_data.reset_index(drop=True, inplace=True)

    camp_grouped_data['account'] = station_name[:-3]
    camp_grouped_data['site'] = station_name[-2:]
    # 筛选出自己需要的列
    if set(export_columns).issubset(set(camp_grouped_data.columns)):
        camp_grouped_data = camp_grouped_data[export_columns]
    else:
        lost_column = set(export_columns) - set(camp_grouped_data.columns)
        print("{}缺失{}列".format(station_name, lost_column))
        common_columns = set(export_columns) & set(camp_grouped_data.columns)
        camp_grouped_data = camp_grouped_data[common_columns]
    # # 去除camp_grouped_data中的逗号
    # camp_grouped_data = camp_grouped_data.applymap(
    #     lambda x: x.replace(',', '.') if isinstance(x, str) else x)
    # 重新命名
    camp_grouped_data.rename(columns={'Status': 'Sku_Status'}, inplace=True)
    camp_grouped_data.rename(columns={column: column.replace(' ', '_') for column in camp_grouped_data.columns},
                             inplace=True)
    if active_data.empty:
        return camp_grouped_data

    new_camp_grouped_data = pd.merge(camp_grouped_data, active_data, left_on='SKU', right_on='seller-sku', how='left',
                                     sort=False)
    new_camp_grouped_data['asin'] = [active_asin if not pd.isna(active_asin) else camp_asin for camp_asin, active_asin
                                     in zip(new_camp_grouped_data['asin'], new_camp_grouped_data['asin1'])]
    del new_camp_grouped_data['seller-sku']
    del new_camp_grouped_data['asin1']

    return new_camp_grouped_data


# 按照站点更新数据库中的sku信息
def db_upload_sku_info(sku_data, new_uuid, old_uuid, db='team_station', table_name='sku_kws_info', ip='127.0.0.1',
                       user_name='marmot',
                       password='marmot123', port=3306):
    """
    将每个站点的sku的表现存储到服务器数据库表中
    :param sku_data:每个站点下sku信息
    :param new_uuid:每个站点下新的uuid
    :param old_uuid:每个站点下老的uuid
    :param db:数据库名
    :param ip:服务器ip
    :param user_name:账号
    :param password:密码
    :param port:端口
    :return:None
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
    # 站号
    account = sku_data['account'].values[0]
    site = sku_data['site'].values[0]
    now_datetime = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
    sku_data['update_time'] = now_datetime

    sku_data = sku_data[
        ['uuid', 'account', 'site', 'Campaign', 'Ad_Group', 'ad_group_bid', 'SKU', 'asin',
         'Keyword_or_Product_Targeting',
         'Max_Bid', 'Campaign_Targeting_Type', 'Match_Type', 'negative_keyword', 'negative_asin', 'target_asin',
         'Campaign_Status', 'Ad_Group_Status', 'Sku_Status', 'update_time', 'Impressions', 'Clicks', 'Spend', 'Orders',
         'Sales', 'ACoS']]
    # 将数据变成可进行读入数据库的dict格式
    all_list = []
    sku_data.reset_index(drop=True, inplace=True)
    df = sku_data.astype(object).replace(np.nan, 'None')
    df = np.array(df)
    len_df = df.shape[0]
    for i in range(len_df):
        temp_tuple = df[i]
        a_emp_tuple = tuple(temp_tuple)
        all_list.append(a_emp_tuple)
    # 写sql
    table_name = table_name
    insert_sql = """insert into {} (uuid,account,site,Campaign,Ad_Group,ad_group_bid,SKU,asin,Keyword_or_Product_Targeting,Max_Bid,Campaign_Targeting_Type,Match_Type,\
                    negative_keyword,negative_asin,target_asin,Campaign_Status,Ad_Group_Status,Sku_Status,update_time,Impressions,Clicks,Spend,Orders,Sales,ACoS) \
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""".format(
        table_name)
    # 执行sql语句
    try:
        if old_uuid:
            # 将list转换成mysql 能识别的格式 (字符串之间的引号)
            old_uuid_str = ",".join(list(map(lambda x: "'%s'" % x, old_uuid)))
            delete_sql = "DELETE from {} where uuid in ({})".format(table_name, old_uuid_str)
            cursor.execute(delete_sql)
        cursor.executemany(insert_sql, all_list)
        conn.commit()
        # print('{}_{} mysql更新完成...'.format(account, site))
    except Exception as e:
        conn.rollback()
        print('{}_{} mysql更新失败...'.format(account, site))
        print(e)
    cursor.close()
    conn.close()
    try:
        station_name = account + '_' + site
        red2.ltrim(station_name, 1, 0)
        red2.rpush(station_name, *new_uuid)
    except Exception as e:
        print(e)
        red2.rpush(station_name, *new_uuid)
        print("更新uuid失败,只执行了添加新的uuid到旧的uuid中...")


# 将导入到数据库中的数字格式规范化
def standard_camp_data(camp_data, columns=['Impressions', 'Clicks', 'Spend', 'Orders', 'Sales', 'ACoS']):
    """
    对基本的数据进行处理
    :param camp_data: 广告数据源
    :param columns: 处理列
    :return:None
    """
    camp_data[columns] = camp_data[columns].astype("str")
    camp_data['ACoS'] = camp_data['ACoS'].apply(lambda x: x.replace(",", "."))
    # camp_data[['Impressions', 'Clicks', 'Orders']] = camp_data[['Impressions', 'Clicks', 'Orders']].applymap(
    #     lambda x: x.replace(",00", "").replace(",",''))
    camp_data[['Impressions', 'Clicks', 'Orders']] = camp_data[
        ['Impressions', 'Clicks', 'Orders']].applymap(lambda x: int(float(x.replace(",", ''))))

    camp_data[['Spend', 'Sales']] = camp_data[['Spend', 'Sales']].applymap(
        lambda x: float(x.replace(",", '')) / 100 if ',' in x else x)

    camp_data['ACoS'] = camp_data['ACoS'].apply(lambda x: str(round(float(x), 2)) + '%' if "%" not in x else x)


def main_process_search_sku_info():
    """
    读取五表压缩文件夹,得到更新的站点名
    :return:所有更新的站点
    """
    processed_station_info = set()
    # while 1:
    # 目前文件夹下的所有压缩文件
    station_zipped_list = os.listdir(station_folder)
    # 将目前文件夹的今日的所有站点和站点压缩文件组合成一个唯一的字符串
    now_date = datetime.now().date() - timedelta(days=2)
    stations_dir = [os.path.join(station_folder, station) for station in station_zipped_list]
    # station_info = set(
    #     [station_dir + '_' + time.ctime(os.path.getmtime(station_dir)) for station_dir in
    #      stations_dir])
    station_info = set(
        [station_dir + '_' + time.ctime(os.path.getmtime(station_dir)) for station_dir in stations_dir if
         datetime.strptime(time.ctime(os.path.getmtime(station_dir)), '%a %b %d %H:%M:%S %Y').date() >= now_date])
    # 需要处理的今天站点信息
    needed_process_station = station_info - processed_station_info
    # 需要处理站点的压缩包路径
    needed_process_station_zip_namelist = [station[:-25] for station in
                                           needed_process_station]
    if len(needed_process_station_zip_namelist) > 0:
        if os.path.exists(station_zipped_folder):
            shutil.rmtree(station_zipped_folder)
        os.mkdir(station_zipped_folder)
        process_station_num = 0
        print("此次更新{}个站点".format(len(needed_process_station_zip_namelist)))
        for one_station_zipped in needed_process_station_zip_namelist:
            # 站号_站点
            try:
                station_name = os.path.basename(os.path.splitext(one_station_zipped)[0])
                if station_name == 'desktop':
                    continue
                # 国家
                site = station_name[-2:]
                print("{} 开始更新...".format(station_name))
                # 首先解压文件
                unzip_dir(one_station_zipped)
                # 得到解压文件夹下的广告报表
                ad_dir = os.path.join(station_zipped_folder, station_name)
                ad_path = get_camp_file_dir(ad_dir)
                camp_ori_data = my_read_campaign(ad_path, site)
                if camp_ori_data.empty:
                    continue
                active_path = get_activelisting_file_dir(ad_dir)
                active_data = read_active_listing(active_path, station_name)
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

                standard_camp_data(camp_all_sku_data,
                                   columns=['Impressions', 'Clicks', 'Spend', 'Orders', 'Sales', 'ACoS'])
                # 新建标识列(uuid)

                indentify_list = [str(uuid.uuid1()) + '_' + station_name for i in range(len(camp_all_sku_data))]
                camp_all_sku_data['uuid'] = indentify_list
                # 更新站点sku数据
                db_upload_sku_info(camp_all_sku_data, db='team_station', table_name='sku_info',
                                   ip='127.0.0.1',
                                   user_name='marmot', password='marmot123', port=3306)
                sql_to_redis(camp_all_sku_data)
                shutil.rmtree(os.path.dirname(ad_path))
                process_station_num += 1
                print("{} 更新完成...".format(station_name))
            except Exception as e:
                print(e)
                continue
        print("此时更新完成{}个站点".format(process_station_num))
        processed_station_info.update(needed_process_station)
        time.sleep(60)
        # if datetime.now().hour == 23:
        #     processed_station_info = set()
        #     print("进入11点，开始休眠10个小时...")
        #     time.sleep(36000)


# 从服务器上加载站点编号
def db_download_station_names(db='team_station', table='only_station_name', ip='127.0.0.1', port=3306,
                              user_name='marmot', password='marmot123') -> list:
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
    sql = """SELECT id,station FROM {} """.format(table)
    # 执行sql语句
    cursor.execute(sql)
    station_names = cursor.fetchall()
    station_names = pd.DataFrame([list(j) for j in station_names], columns=['station_id', 'station_name'])
    conn.commit()
    cursor.close()
    conn.close()
    # print("STEP1: 完成下载站点名信息...")
    # print("===================================================")
    return station_names


# 添加站点名
def db_insert_station_name(new_station, db='team_station', table='only_station_name', ip='127.0.0.1', port=3306,
                           user_name='marmot', password='marmot123'):
    """
    插入新的广告站点
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

    # 将list数据转换为dict
    dict_insert_station = tuple(new_station)
    # 写sql
    insert_sql = """insert into {} (id,station) values (%s,%s) """.format(table)
    # 执行sql语句
    try:
        cursor.execute(insert_sql, dict_insert_station)
        conn.commit()
        cursor.close()
        conn.close()
        # print("STEP2: 更新站点名信息...")
        # print("===================================================")
    except Exception as e:
        conn.rollback()
        print(e)


if __name__ == "__main__":
    t1 = datetime.now()
    main_process_search_sku_info()
    t2 = datetime.now()
    print("花费{}".format(t2 - t1))
