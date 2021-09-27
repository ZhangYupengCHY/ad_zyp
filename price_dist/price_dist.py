import os
import pandas as pd
from datetime import datetime, timedelta
import pymysql
import re
import numpy as np
import warnings
import zipfile
import time
import shutil
import rsa
import base64
import requests
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import sys

sys.path.append('E:/ad_zyp/price_dist/my_toolkit')
from read_campaign import read_campaign
from init_activing import clean_active_listing
from init_campaign import init_campaign
from format_data import format_num
from init_report_data import init_report
from my_toolkit import init_station_report
import my_toolkit.public_function as public_function
import my_toolkit.process_files as process_files
import my_toolkit.sql_write_read as sql_write_read
import my_toolkit.change_rate as change_rate

"""
通过对广告的客单价进行区间统计其中统计的区间为[0,10,30,50,100,200,300,400,500,1000]
对区间内sku就行统计
1. sku总数:客单价区间内sku汇总统计 (all_listing表)
2. sku的出单率(A. 本区间内的出单率 B.本价格的出单率)
3. 广告情况:1. 客单价区间:广告总花费,广告销售额,转化率,店铺花费,店铺销售额,转化率
            2. FBA/FBM:广告总花费,广告销售额,转化率,店铺花费,店铺销售额,转化率
            3.上架时间:广告总花费,广告销售额,转化率,店铺花费,店铺销售额,转化率




"""
warnings.filterwarnings("ignore")
# 汇率
exchange_rate = change_rate.change_current()

bid_exchange = {'CA': 1, 'DE': 1, 'FR': 1, 'IT': 1, 'SP': 1, 'JP': 0.009302,
                'UK': 1, 'MX': 0.05147, 'IN': 0.01412, 'US': 1, 'ES': 1, 'AU': 0.6766}

# 本币
ad_group_least_bid = {'CA': 0.02, 'DE': 0.02, 'FR': 0.02, 'IT': 0.02, 'SP': 0.02, 'JP': 2,
                      'UK': 0.02, 'MX': 0.1, 'IN': 0.1, 'US': 0.02, 'ES': 0.02, 'AU': 0.1}

acos_ideal = {'CA': 0.14, 'DE': 0.15, 'FR': 0.15, 'IT': 0.15, 'SP': 0.15, 'JP': 0.15,
              'UK': 0.18, 'MX': 0.15, 'IN': 0.18, 'US': 0.18, 'ES': 0.15, 'AU': 0.15}
# 本币
cpc_max = {'CA': 0.4, 'DE': 0.35, 'FR': 0.35, 'IT': 0.3, 'SP': 0.3, 'JP': 25,
           'UK': 0.4, 'MX': 2.5, 'IN': 4.5, 'US': 0.5, 'ES': 0.3, 'AU': 0.4}

# 站点节点
site_web = {'US': 'Amazon.com', 'CA': 'Amazon.ca', 'FR': 'Amazon.fr', 'UK': 'Amazon.co.uk', 'DE': 'Amazon.de',
            'ES': 'Amazon.es', 'IT': 'Amazon.it', 'JP': 'Amazon.jp', 'MX': 'Amazon.com.mx', 'IN': 'Amazon.in',
            'AU': 'Amazon.com.au'}
# 站点的最小出价
ad_group_max_bid_lower_limit_dict = {'US': 0.02, 'CA': 0.02, 'MX': 0.1, 'UK': 0.02, 'DE': 0.02, 'FR': 0.02, 'IT': 0.02,
                                     'ES': 0.02, 'JP': 2, 'AU': 0.02, 'IN': 1, 'AE': 0.24}

campaign_budget_dict = {'CA': 200, 'DE': 200, 'FR': 200, 'IT': 200, 'SP': 200, 'JP': 20000,
                        'UK': 200, 'MX': 3800, 'IN': 14000, 'US': 200, 'ES': 200, 'AU': 200}

# 文件存储的位置
# 测试的文件夹
station_folder = 'F:/station_folder'
# station_folder = 'E:/AD_WEB/file_dir/station_folder'
station_zipped_folder = 'E:/AD_WEB/file_dir/temp'
ad_perf = "E:/AD_WEB/file_dir/ad_performance"


# 上传重复广告
def db_upload_repeat_sku(repeat_sku_info, station_name, campaign_targeting_type='manual', db='team_station',
                         ip='127.0.0.1',
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
                                   ip='127.0.0.1',
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


# 上传重复广告主函数
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

    # 上传重复广告的数量到数据库中
    total_sku_num_sql = "select account, site, COUNT(distinct account, site, sku) as station_total " \
                        "from station_auto_repeat_sku where orders=0 GROUP BY account, site ORDER BY station_total DESC"

    each_sku_num_sql = "SELECT account, site, sku, count(1) AS sku_total FROM station_auto_repeat_sku " \
                       "GROUP BY account, site, sku ORDER BY sku_total DESC"

    auto_ad_no_order_sql = "SELECT account, site, sku, count( 1 ) AS no_order_total FROM station_auto_repeat_sku " \
                           "where orders=0 GROUP BY account, site, sku ORDER BY no_order_total DESC"

    def request_sql(sql):
        sql_df = sql_write_read.read_local_db(sql)
        sql_df['station'] = sql_df['account'] + "_" + sql_df['site']
        sql_df['station'] = sql_df['station'].str.lower()
        sql_df['station'] = sql_df['station'].str.replace('-', '_')
        now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if "station_total" in sql:
            # 获取各账号站点的广告接收人
            ad_manger_sql = "select ad_manger, station from only_station_info"
            ad_manger_df = sql_write_read.read_table(ad_manger_sql)
            # 匹配广告接收人
            sql_df = sql_df.merge(ad_manger_df, on='station', how='left')
            sql_df['update_time'] = now_time
            sql_write_read.to_local_table_replace(sql_df, 'same_ad_station_total_new')
            # 更新到redis中
            sql_write_read.Redis_Store(db=0).refresh_df(sql_df[['station', 'station_total']],'same_ad_station_total_new')
            return
        elif "sku_total" in sql:
            sql_df['update_time'] = now_time
            sql_write_read.to_local_table_replace(sql_df, 'same_ad_sku_total_new')
            return
        elif "no_order_total" in sql:
            sql_df['update_time'] = now_time
            sql_write_read.to_local_table_replace(sql_df, 'same_ad_no_order_total_new')
            return
        else:
            return

    for sql in [total_sku_num_sql, each_sku_num_sql, auto_ad_no_order_sql]:
        request_sql(sql)


# 1. sku总数:区间内sku汇总统计 (all_listing表)
def sku_fulfillment_opendate_price_dist(activelisting_report_dirname,
                                        need_columns=['seller-sku', 'price', 'open-date', 'fulfillment-channel'],
                                        sep='\t',
                                        price_range=list(np.arange(0, 100001, 100))) -> list:
    """
    找到activelisting下不同客单价的区间分布
    :param activelisting_report_dirname: activelisting最新表的路径
    :param sep: 数据之间的分隔符
    :param need_columns: 计算需要的列(price客单价,sku列) sku:sellersku  price:price
    :param price_range: 客单价的区间分布
    :return:sku的发货方式/上架时间   sku的客单价的分布
            price sku
            0-10    1
            10-30   2
            ...     ...
            1000-   0
    """
    activelisting_data = process_files.read_pickle_2_df(activelisting_report_dirname)
    if site == 'JP':
        activelisting_data = active_total_columns(activelisting_data)
    if 'fulfillment-channel' not in activelisting_data.columns:
        print("{}  activelisting表没有fulfillment-channel列...".format(activelisting_report_dirname))
    try:
        activelisting_data = activelisting_data[need_columns]
    except Exception as e:
        print(activelisting_report_dirname, e)
    activelisting_data.dropna(inplace=True)
    # 处理日本站标题问题
    sku_column_name = need_columns[0]
    price_column_name = need_columns[1]
    # 1. 获得fulfillment和opendate数据
    open_date_column = need_columns[2]
    fulfillment_column = need_columns[3]
    sku_fulfillment_opendate = activelisting_data[[sku_column_name, open_date_column, fulfillment_column]]
    # 1.1上架时间(天)
    sku_fulfillment_opendate[open_date_column] = sku_fulfillment_opendate[open_date_column].apply(lambda x: str(x[:10]))
    sku_fulfillment_opendate['on_dates'] = [(datetime.now() - start_date).days for
                                            start_date in
                                            pd.to_datetime(sku_fulfillment_opendate[open_date_column], dayfirst=True)]

    # 1.2 发货方式（FBM/FBA）
    sku_fulfillment_opendate['ship_type'] = ['FBM' if fulfillment_type.upper() == 'DEFAULT' else 'FBA' for
                                             fulfillment_type in sku_fulfillment_opendate[fulfillment_column]]
    del sku_fulfillment_opendate[open_date_column]
    del sku_fulfillment_opendate[fulfillment_column]
    # 2. 提取sku与客单价数据
    sku_dist_data = activelisting_data[[sku_column_name, price_column_name]]
    # 获得该站点的国家，将price转换成美元
    # site = os.path.dirname(activelisting_report_dirname)[-13:-11].upper()
    sku_dist_data[price_column_name] = sku_dist_data[price_column_name].astype('float')
    # 将price 进行转换
    sku_dist_data[price_column_name] = sku_dist_data[price_column_name].apply(
        lambda x: exchange_rate[site] * x)
    # 将activelisting的客单价下的sku按照区间聚合数据
    sku_dist = sku_dist_data.groupby(pd.cut(sku_dist_data[price_column_name], price_range)).agg(
        {price_column_name: 'count', sku_column_name: lambda x: list(x)})
    price_interval = pd.Series(sku_dist.index).apply(lambda x: str(x.left) + '-' + str(x.right))
    sku_dist['price_interval'] = list(price_interval)
    sku_dist['price_interval'] = sku_dist['price_interval'].str.replace('{}'.format(price_range[0]), '0')
    sku_dist['price_interval'] = sku_dist['price_interval'].str.replace('-{}'.format(price_range[-1]), '')
    sku_dist.rename(columns={price_column_name: 'sku总数'}, inplace=True)
    sku_dist = sku_dist[['price_interval', 'sku总数', sku_column_name]]
    sku_dist.reset_index(drop=True, inplace=True)

    return [sku_fulfillment_opendate, sku_dist, sku_dist_data]


def init_allorder(allorder: pd.DataFrame, order_dir):
    """
    对allorder处理:
        1. 剔除quantity为0的order
        2. 对sales-channel进行筛选，筛选本站点的数据
        3. 总价/数量 = 单价
    :param allorder:所有的订单数据
    :param order_dir:订单报表路径
    :return:初始化后的订单数据
    """
    t_allorder = allorder.copy()
    # 筛选订单报表中有出单的order
    t_allorder = t_allorder[t_allorder['quantity'] > 0]
    # 筛选自己站点的订单
    if site in ('IN', 'AU', 'JP'):
        pass
    elif 'sales-channel' in allorder.columns:
        t_allorder = t_allorder[t_allorder['sales-channel'] == site_web[site]]
    else:
        print('{} 的all_order表的sales-channel列不存在...'.format(order_dir))
        return []
    # 计算sku单价
    t_allorder = t_allorder[t_allorder['item-price'] != '']
    t_allorder['item-price'] = t_allorder['item-price'].astype('float')
    t_allorder['item-price'] = t_allorder['item-price'] / t_allorder['quantity']
    # 进行汇率换算
    t_allorder['item-price'] = t_allorder['item-price'] * exchange_rate[site]
    t_allorder.reset_index(drop=True, inplace=True)
    return t_allorder


# 2.客单价区间内各个sku出单率情况
def sku_order_price_dist(allorder_dir, sku_dist_data, price_range, need_columns=['sku', 'quantity']):
    """
    找到出单下sku的每个价格区间的出单率（区间出单/总出单），以及出单率（区间出单/区间总sku）
    :param allorder_dir:allorder的路径
    :param need_columns:计算所需的列 sku列和订单个数列
    :param sku_dist_data:sku价格区间分布数据
    :param price_range:价格区间分布数据
    :return: 区间的订单率（区间出单数/总出单数）price_order_rate，以及出单率（区间出单sku/区间总sku）sku_order_rate
    """
    new_sku_dist_data = sku_dist_data.copy()
    # 得到allorder下的sku列表
    allorder = process_files.read_pickle_2_df(allorder_dir)
    allorder.columns = [columns.strip(' ') for columns in allorder.columns]
    # 删除空白数据
    allorder.dropna(subset=[allorder.columns[3]], inplace=True)
    # 求区间的出单率（区间出单/总出单）sku_price_rate，以及出单率（区间出单/区间总sku）sku_order_rate
    sku_column = need_columns[0]
    order_num_column = need_columns[1]
    if 'quantity' in allorder.columns:
        allorder = allorder[allorder[order_num_column] != '']
        allorder[order_num_column] = allorder[order_num_column].astype('int')
    elif 'quantity-purchased' in allorder.columns:
        allorder.rename(columns={'quantity-purchased': 'quantity'}, inplace=True)
        allorder = allorder[allorder[order_num_column] != '']
        allorder[order_num_column] = allorder[order_num_column].astype('int')
    else:
        print('{}不存在quantity列...'.format(allorder_dir))
    # 对allorder进行初始化处理
    allorder = init_allorder(allorder, allorder_dir)
    # 得到出单sku订单分价格区间下销售额以及销售比例
    # 复制order
    new_allorder = allorder.copy()
    new_allorder = new_allorder[['quantity', 'item-price']]
    new_allorder['出单SKU销售额'] = new_allorder['item-price'] * new_allorder['quantity']
    # del new_allorder['quantity']
    # 得到分区sku的销售额与销售额比例
    new_allorder = new_allorder.groupby(pd.cut(new_allorder['item-price'], price_range)).agg(
        {'出单SKU销售额': 'sum'}).reset_index()
    del new_allorder['item-price']
    new_allorder_sales = int(sum(new_allorder['出单SKU销售额']))
    if new_allorder_sales <= 0:
        new_allorder['sales_rate'] = '0.0%'
    else:
        new_allorder['sales_rate'] = new_allorder['出单SKU销售额'] / new_allorder_sales
        new_allorder['sales_rate'] = new_allorder['sales_rate'].apply(
            lambda x: str(round(x * 100, 1)) + '%')
    new_allorder.loc[-1, :] = [new_allorder_sales, '100%']
    new_allorder['出单SKU销售额'] = new_allorder['出单SKU销售额'].apply(lambda x: int(x))

    # 将allorder的订单按照sku汇总
    allorder = allorder.groupby('sku').agg({'quantity': 'sum'}).reset_index()
    # 订单列表下的全部sku
    allorder_sku = set(allorder[sku_column])
    new_sku_dist_data['have_order_sku'] = [set(allorder_sku) & set(sku) for sku in new_sku_dist_data['seller-sku']]
    order_sku_quantity_list = []
    for sku_list in new_sku_dist_data['have_order_sku']:
        if sku_list:
            order_list = []
            for sku in sku_list:
                order = allorder[order_num_column][allorder[sku_column] == sku]
                order = [int(i) for i in order]
                order_list.extend(order)
        else:
            order_list = []
        order_sku_quantity_list.append(order_list)

    # sku的名字和出单数量
    new_sku_dist_data['have_order_sku_limit'] = new_sku_dist_data['have_order_sku'].copy()
    for i in range(len(new_sku_dist_data)):
        sku_name_order_dict = {sku: order for sku, order in
                               zip(new_sku_dist_data['have_order_sku'][i], order_sku_quantity_list[i])}
        order_sku_name_orders_dict = sorted([(value, sku) for (sku, value) in sku_name_order_dict.items()],
                                            reverse=True)
        sku_list = [sku_tupe[1] for sku_tupe in order_sku_name_orders_dict]
        order_list = [sku_tupe[0] for sku_tupe in order_sku_name_orders_dict]
        order_list = [int(order) if isinstance(order, str) else order for order in order_list]
        above_ten_order_sku = {sku: value for value, sku in zip(order_list, sku_list) if value >= 10}
        if len(above_ten_order_sku) <= 5:
            all_sku = sku_list[0:5]
            all_order = order_list[0:5]
            above_ten_order_sku = {sku: order for sku, order in zip(all_sku, all_order)}
        if len(above_ten_order_sku) >= 10:
            all_sku = sku_list[0:10]
            all_order = order_list[0:10]
            above_ten_order_sku = {sku: order for sku, order in zip(all_sku, all_order)}
        new_sku_dist_data['have_order_sku_limit'][i] = above_ten_order_sku

    # 所有满足条件的sku
    all_sku = {}
    {all_sku.update(sku_list) for sku_list in new_sku_dist_data['have_order_sku_limit'] if len(sku_list) > 0}
    all_sku = sorted([(value, sku) for (sku, value) in all_sku.items()], reverse=True)

    sku_list = [sku_list[1] for sku_list in all_sku]
    order_list = [sku_list[0] for sku_list in all_sku]

    all_sku = {sku: value for value, sku in zip(order_list, sku_list) if value >= 10}
    if len(all_sku) <= 5:
        all_sku = {sku: value for value, sku in zip(order_list, sku_list) if value >= 5}
    if len(all_sku) >= 10:
        all_sku = {sku: value for value, sku in zip(order_list[0:10], sku_list[0:10])}

    # sku出单数
    new_sku_dist_data['order_sku_quantity'] = [sum(sku_order_list) for sku_order_list in order_sku_quantity_list]
    # sku出单个数
    new_sku_dist_data['order_sku_num'] = [len(sku_order_list) for sku_order_list in order_sku_quantity_list]

    allorder_num = new_sku_dist_data['order_sku_quantity'].sum()

    # 汇总sku_dist表
    new_sku_dist_data.loc[-1, :] = ['汇总', sum(new_sku_dist_data['sku总数']), '', '', all_sku,
                                    sum(new_sku_dist_data['order_sku_quantity']),
                                    sum(new_sku_dist_data['order_sku_num'])]

    # sku出单率（区间出单sku/区间总sku）sku_order_rate
    new_sku_dist_data['sku_order_rate'] = [order_sku_num / all_sku_list if all_sku_list != 0 else 0 for
                                           order_sku_num, all_sku_list in
                                           zip(new_sku_dist_data['order_sku_num'], new_sku_dist_data['sku总数'])]
    # 区间订单率（区间出单数/总出单数）price_order_rate
    new_sku_dist_data['price_order_rate'] = [price_order / allorder_num if allorder_num != 0 else 0 for price_order in
                                             new_sku_dist_data['order_sku_quantity']]

    # 输出项添加两列
    new_sku_dist_data = pd.concat([new_sku_dist_data, new_allorder], axis=1)
    return new_sku_dist_data


# 汇总统计出单sku与actlisting不出单的sku的表现
def different_sku_perf(ordered_sku, all_sku, camp_data):
    """
    汇总统计出单与没有出单下的sku的整体表现
    :param ordered_sku_list: 出单sku列表
    :param all_sku: campaign的sku列表
    :param camp_data: 广告数据
    :return:汇总后的不同sku的表现
    """
    # 不出单的sku
    not_ordered_sku = all_sku - ordered_sku
    # 得到出单sku明细
    ordered_sku_data = camp_data[camp_data['SKU'].isin(ordered_sku)]
    # 汇总出单sku
    ordered_sku_grouped = ordered_sku_data.sum()
    # 得到不出单sku明细
    dont_ordered_sku_data = camp_data[camp_data['SKU'].isin(not_ordered_sku)]
    # 汇总不出单sku
    dont_ordered_sku_grouped = dont_ordered_sku_data.sum()
    ordered_sku_grouped['Units Ordered'] = ordered_sku_grouped['Units Ordered'] + dont_ordered_sku_grouped[
        'Units Ordered']
    ordered_sku_grouped['Ordered Product Sales'] = ordered_sku_grouped['Ordered Product Sales'] + \
                                                   dont_ordered_sku_grouped['Ordered Product Sales']
    dont_ordered_sku_grouped['Ordered Product Sales'], dont_ordered_sku_grouped['Units Ordered'] = 0, 0
    # 汇总出单与不出单数据
    ordered_or_not_sku_perf = pd.concat([ordered_sku_grouped, dont_ordered_sku_grouped], axis=1).T
    ordered_or_not_sku_perf['SKU'] = ['出单sku', '不出单sku']
    ordered_or_not_sku_perf['ACoS'] = [spend / sale if sale > 0 else 0 for spend, sale in
                                       zip(ordered_or_not_sku_perf['Spend'], ordered_or_not_sku_perf['Sales'])]
    ordered_or_not_sku_perf['cpc'] = [spend / click if int(click) > 0 else 0 for spend, click in
                                      zip(ordered_or_not_sku_perf['Spend'], ordered_or_not_sku_perf['Clicks'])]
    ordered_or_not_sku_perf['cr'] = [order / click if int(order) > 0 else 0 for click, order in
                                     zip(ordered_or_not_sku_perf['Clicks'], ordered_or_not_sku_perf['Orders'])]
    if ordered_or_not_sku_perf['Ordered Product Sales'][0] != 0:
        ordered_or_not_sku_perf['PROM_RATIO'] = [
            ordered_or_not_sku_perf['Spend'][0] / ordered_or_not_sku_perf['Ordered Product Sales'][0],
            ordered_or_not_sku_perf['Spend'][1] / ordered_or_not_sku_perf['Ordered Product Sales'][0]]
        ordered_or_not_sku_perf['SALES_RATIO'] = [
            (ordered_or_not_sku_perf['Sales'][0] / ordered_or_not_sku_perf['Ordered Product Sales'][0]),
            (ordered_or_not_sku_perf['Sales'][1] / ordered_or_not_sku_perf['Ordered Product Sales'][0])]
    else:
        ordered_or_not_sku_perf['PROM_RATIO'], ordered_or_not_sku_perf['SALES_RATIO'] = [0, 0], [0, 0]
    # ordered_or_not_sku_perf['PROM_RATIO'] = [spend / shop_sale if int(shop_sale) > 0 else 0 for spend, shop_sale in
    #                                          zip(ordered_or_not_sku_perf['Spend'],
    #                                              ordered_or_not_sku_perf['Ordered Product Sales'])]
    # ordered_or_not_sku_perf['SALES_RATIO'] = [ad_sale / shop_sale if int(shop_sale) > 0 else 0 for ad_sale, shop_sale in
    #                                           zip(ordered_or_not_sku_perf['Sales'],
    #                                               ordered_or_not_sku_perf['Ordered Product Sales'])]

    ordered_or_not_sku_perf['是否出单sku'] = ['出单sku', '不出单sku']

    ordered_or_not_sku_perf = ordered_or_not_sku_perf[
        ['是否出单sku', 'Impressions', 'Clicks', 'Spend', 'Orders', 'Sales', 'Units Ordered', 'Ordered Product Sales',
         'ACoS',
         'cpc',
         'cr', 'PROM_RATIO', 'SALES_RATIO']]
    return ordered_or_not_sku_perf


# 找到六个所需要的文件组
# {'campaign': 'Bulk', 'search': 'SearchTerm', 'business': 'Business',
#                          'active_listing': 'active_listing', 'all_listing': 'all_listing','allorders':'allorders'}
def gather_file(folder_dir):
    file_typical_word = {'campaign': 'bulk', 'business': 'Business',
                         'active_listing': 'Active+Listings', 'Search': 'Search'}
    if os.path.exists(folder_dir):
        all_files = os.listdir(folder_dir)
        all_need_files = {}
        for key, value in file_typical_word.items():
            try:
                all_need_files[key] = [file for file in all_files if value in file][0]
            except:
                pass
        all_need_files['allorders'] = \
            [file for file in all_files if ((os.path.splitext(file)[1] == '.txt') & ('Listings' not in file))][0]
        # 打印识别的5表的文件名
        # print(f'{os.path.basename(folder_dir)}:{all_need_files}')
        return all_need_files
    else:
        return {key: [] for key in file_typical_word.keys()}


#
# # 站点对应负责人
# def db_download_station_names(db='team_station', table='only_station_info', ip='wuhan.yibai-it.com', port=33061,
#                               user_name='zhangyupeng', password='zhangyupeng') -> list:
#     """
#     加载广告组接手的站点名
#     :return: 所有站点名
#     """
#     conn = pymysql.connect(
#         host=ip,
#         user=user_name,
#         password=password,
#         database=db,
#         port=port,
#         charset='UTF8')
#     # 创建游标
#     cursor = conn.cursor()
#     # 写sql
#     sql = """SELECT station FROM {} """.format(table)
#     # 执行sql语句
#     cursor.execute(sql)
#     station_names = cursor.fetchall()
#     station_names = list(set([j[0] for j in station_names]))
#     conn.commit()
#     cursor.close()
#     conn.close()
#     print("STEP1: 完成下载站点名信息...")
#     print("===================================================")
#     return station_names


# 读取一天广告站点的数据
def read_one_campaign(campaign_dirname: 'dir',
                      need_columns='all') -> pd.DataFrame:
    """
    读取广告站点数据
    :param campaign_dirname: 广告站点的路径
    :param site: 广告站点的国家
    :param need_columns: 需要的列名
    :return: 广告站点所需要的数据
    """
    # 读取excel内容
    file_data = process_files.read_pickle_2_df(campaign_dirname)
    file_data = init_campaign(file_data, site.upper(), 'empty')
    if 'Total units' in file_data.columns:
        file_data.rename(columns={'Total units': 'Total Units'}, inplace=True)
    if need_columns != 'all':
        file_data = file_data[need_columns]
    # 去除sku列为空
    file_data = file_data[file_data['SKU'] != ' ']
    file_data[['Spend', 'Sales']] = file_data[['Spend', 'Sales']].applymap(
        lambda x: x.replace(",", ".") if isinstance(x, str) else x)
    file_data[['Spend', 'Sales']] = file_data[['Spend', 'Sales']].applymap(lambda x: float(x) * exchange_rate[site])
    file_data = file_data.groupby(['SKU'])[
        'Impressions', 'Clicks', 'Spend', 'Orders', 'Total Units', 'Sales'].sum().reset_index()
    file_data.reset_index(inplace=True, drop=True)
    return file_data


# 得到广告报表sku的列表
def get_camp_sku_list(campaign_dirname: 'dir'):
    file_data = process_files.read_pickle_2_df(campaign_dirname)
    file_data = init_campaign(file_data, site.upper(), 'empty')
    file_data = file_data[~pd.isna(file_data['SKU'])]
    file_data[['Campaign Status', 'Ad Group Status', 'Status']] = file_data[
        ['Campaign Status', 'Ad Group Status', 'Status']].applymap(lambda x: x.lower() if isinstance(x, str) else x)

    # 广告报表的全部sku
    camp_all_sku_set = set(file_data['SKU'])
    camp_all_sku_set.discard(' ')
    camp_all_sku_set.discard('')
    # 广告报表全部开启的sku
    camp_all_enabled_sku_set = set(file_data['SKU'][
                                       (file_data['Campaign Status'] == 'enabled') & (
                                               file_data['Ad Group Status'] == 'enabled') & (
                                               file_data['Status'] == 'enabled')])
    camp_all_enabled_sku_set.discard(' ')
    camp_all_enabled_sku_set.discard('')

    return [camp_all_sku_set, camp_all_enabled_sku_set]


# 读取业务报表br
def read_business(br_dirname: 'dir', need_columns='all') -> pd.DataFrame:
    """
    读取站点的业务报表数据
    :param br_dirname: 业务报表的路径
    :param need_columns: 所需要的列
    :return: 业务报表数据
    """
    r_data = process_files.read_pickle_2_df(br_dirname)
    r_data = init_station_report.init_report(r_data,'br')
    if need_columns != 'all':
        if set(need_columns).issubset(set(r_data.columns)):
            r_data = r_data[need_columns]
            r_data.dropna(inplace=True)
            # 将sales列转换成美元
            sales_column_name = r_data.columns[-1]
            # try:
            #     r_data[sales_column_name] = r_data[sales_column_name].apply(
            #         lambda x: float(''.join(re.findall('\d+[0-9.]', x))) * exchange_rate[site])
            # except:
            #     r_data[sales_column_name] = r_data[sales_column_name].apply(
            #         lambda x: float(re.findall('\d+\.?\d*', x)[0]) * exchange_rate[site])
            r_data[sales_column_name] = r_data[sales_column_name].apply(
                lambda x: (float("".join(re.findall('\d', x))) * exchange_rate[site]) / 100)
            if site == 'JP':
                r_data[sales_column_name] = r_data[sales_column_name] * 100
            # 订单列转化成整型 第二列
            r_data['Units Ordered'] = r_data['Units Ordered'].apply(
                lambda x: int(x.replace(',', '')) if isinstance(x, str) else x)
            r_data = r_data.groupby(['SKU'])['Units Ordered', 'Ordered Product Sales'].sum().reset_index()
            r_data.reset_index(inplace=True, drop=True)
        else:
            lost_columns = set(need_columns) - set(r_data.columns)
            print('error3: {} 表下br表缺失{}列...'.format(os.path.dirname(br_dirname), lost_columns))
            return pd.DataFrame([])
    return r_data


# 读取获得br表数据
def get_business_info(br_dirname: 'dir', need_columns='all'):
    """
    获得站点的br数据
    :param br_dirname:业务报表的路径
    :param site:站点国家
    :param need_columns:所需要的列
    :return:br数据
    """
    ori_br_data = read_business(br_dirname, need_columns=need_columns)
    if ori_br_data.empty:
        return pd.DataFrame([])
    return ori_br_data


# 得到客单价下的sku表现: 广告总花费、广告总销售、转化率、店铺总花费等
def price_range_perf(ad_shop_data, sku_price,
                     price_range=[-0.0001, 10, 30, 50, 100, 200, 300, 400, 500, 1000, 100000000]):
    """
    得到客单价下的sku表现: 广告总花费、广告总销售、转化率、店铺总花费
    :param ad_shop_data: 广告和店铺数据
    :return: 指标
    """
    # 得到每个广告的sku的客单价
    ad_shop_data = pd.merge(ad_shop_data, sku_price, left_on='SKU', right_on='seller-sku', how='left')
    ad_shop_data.fillna(999999, inplace=True)
    price_dist_perf_data = ad_shop_data.groupby(pd.cut(ad_shop_data['price'], price_range)).agg(
        {'Impressions': 'sum', 'Clicks': 'sum', 'Spend': 'sum', 'Orders': 'sum', 'Sales': 'sum', 'Units Ordered': 'sum',
         'Ordered Product Sales': 'sum'}).reset_index()
    price_dist_perf_data.rename(columns={'price': 'price_range'}, inplace=True)

    price_dist_perf_data['ACoS'] = [spend / sales if sales != 0 else 0 for spend, sales in
                                    zip(price_dist_perf_data['Spend'], price_dist_perf_data['Sales'])]
    price_dist_perf_data['cpc'] = [spend / click if click != 0 else 0 for spend, click in
                                   zip(price_dist_perf_data['Spend'], price_dist_perf_data['Clicks'])]
    price_dist_perf_data['cr'] = [order / click if click != 0 else 0 for order, click in
                                  zip(price_dist_perf_data['Orders'], price_dist_perf_data['Clicks'])]
    price_dist_perf_data['prom_ratio'] = [spend / shop_sale if shop_sale != 0 else 0 for spend, shop_sale in
                                          zip(price_dist_perf_data['Spend'],
                                              price_dist_perf_data['Ordered Product Sales'])]
    price_dist_perf_data['sales_ratio'] = [ad_sale / shop_sale if shop_sale != 0 else 0 for ad_sale, shop_sale in
                                           zip(price_dist_perf_data['Sales'],
                                               price_dist_perf_data['Ordered Product Sales'])]
    price_dist_perf_data['price_range'] = price_dist_perf_data['price_range'].apply(
        lambda x: str(x.left) + '-' + str(x.right))

    return price_dist_perf_data


# 通过接口将新增和更新数据上传的erp上
class AdApiFileUpload(object):
    """
    广告组广告上传的接口:
        分为：新建和更新
        新建的接口为:(线下) 'http://192.168.2.160:80/services/api/advertise/Creatrreportinterface'
                参数： account_id和upload_file
        更新接口: url:http://192.168.2.168:80/services/api/advertise/updatereport
                参数:account_id和data

    """

    # __base_url = 'http://192.168.2.160:80'  # 线下环境
    __base_url = 'http://120.78.243.154'  # 线上环境
    __create_url = '/services/api/advertise/Creatrreportinterface'
    __update_url = '/services/api/advertise/updatereport'
    __key_path = r"E:\ad_zyp\price_dist\public.key"

    def __init__(self):
        pass

    def __token__(self):
        # 通过接口上传数据
        with open(self.__key_path, 'r') as fp:
            public_key = fp.read()
        # pkcs8格式
        key = public_key
        password = "Kr51wGeDyBM39Q0REVkXn4lW7ZqCxdPLS8NO6iIfubTJcvsjt2YpmAgzHFUoah"
        pubkey = rsa.PublicKey.load_pkcs1_openssl_pem(key)
        password = password.encode('utf8')
        crypt_password = rsa.encrypt(password, pubkey)
        token = base64.b64encode(crypt_password).decode()
        return token

    # 通过接口上传新建的文件
    def requests_upload_create(self, **kwargs):
        """
        通过上传需要新增的站点对应的站点的id和站点的路径，将文件通过api上传到服务器中
        :param kwargs: 'account_id'、'file_path'
        :return:
        """
        token = self.__token__()
        account_id = int(kwargs['account_id'])
        file_path = kwargs['file_path']
        # 读取文件
        with open(file_path, 'rb') as f:
            content = f.read()
        # print(content)

        upload_create_url = self.__base_url + self.__create_url
        # 普通参数
        upload_param = {
            'token': token,
            'account_id': account_id,
            'upload_file': open(file_path, 'rb')
        }
        # print(f'站点id {account_id} 开始新增上传.')
        files = {'upload_file': open(file_path, 'rb')}

        response = requests.post(upload_create_url, data=upload_param, files=files)
        status_code = response.status_code
        # print(f'status_code: {response.status_code}')
        if status_code == 200:
            content = json.loads(response.content)
            info = content['info']
            if info == '上传成功':
                upload_time = content['record_info']['upload_date']
                return upload_time
            else:
                return
        else:
            return

    # 通过接口上传更新的文件
    def requests_upload_update(self, **kwargs):
        """
        通过接口参数上传站点的更新文件
        :param kwargs: account_id和file_path
        :return:
        """
        token = self.__token__()
        # 更新接口的account_id 是站点名 账号+下划线+站点
        account_id = kwargs['account_id']
        file_path = kwargs['file_path']
        with open(file_path, 'rb') as f:
            content = f.read()
        # url
        update_url = self.__base_url + self.__update_url
        # 参数
        upload_param = {
            'token': token,
            'account_id': account_id,
            'data': content
        }
        print(f'站点 {account_id} 开始更新上传.')
        response = requests.post(update_url, data=upload_param)
        status_code = response.status_code
        print(f'status_code: {response.status_code}')
        if status_code == 200:
            print(response.text)
        else:
            print(f'状态:fail.')


def standard_export_file(price_data, fulfillment_data,
                         dates_data, ordered_data):
    """
    将四种输出项汇总成一个df
    :param price_data: 价格区间汇总项
    :param fulfillment_data: 发货方式汇总项
    :param dates_data: 上架时间汇总项
    :param ordered_data: 是否有订单项
    :return: 汇总表格
    """
    column_name = ['分类汇总项', 'Impressions', 'Clicks', 'Spend', 'Orders', 'Sales', '店铺订单', '店铺销售额', 'ACoS', 'cpc', 'cr',
                   '推广占比', '广销比']
    fisrt_df = pd.DataFrame([list(dates_data.iloc[-1, :])], columns=column_name)
    # 空白行
    empty_row = pd.DataFrame([[''] * len(column_name)], columns=column_name)
    p_first_row = ['客单价区间']
    p_first_row.extend([''] * (len(column_name) - 1))
    price_first_row = pd.DataFrame([p_first_row], columns=column_name)
    f_first_row = ['发货方式']
    f_first_row.extend([''] * (len(column_name) - 1))
    filfullment_first_row = pd.DataFrame([f_first_row],
                                         columns=column_name)
    d_on_first_row = ['上线时间']
    d_on_first_row.extend([''] * (len(column_name) - 1))
    dates_on_first_row = pd.DataFrame([d_on_first_row], columns=column_name)
    h_first_row = ['订单类别']
    h_first_row.extend([''] * (len(column_name) - 1))
    have_ordered_type_first_row = pd.DataFrame([h_first_row],
                                               columns=column_name)

    price_data.columns = column_name
    fulfillment_data.columns = column_name
    # dates_data = dates_data.loc[0:3, :]
    dates_data.columns = column_name
    ordered_data.columns = column_name

    # 汇总
    all_export_files = pd.concat(
        [fisrt_df, empty_row, price_first_row, price_data, empty_row, filfullment_first_row, fulfillment_data,
         empty_row, dates_on_first_row, dates_data, empty_row, have_ordered_type_first_row, ordered_data])

    all_export_files.reset_index(inplace=True, drop=True)

    # 将'Spend','Sales','店铺销售额'保留整数
    all_export_files[['Spend', 'Sales', '店铺销售额']] = all_export_files[['Spend', 'Sales', '店铺销售额']].applymap(
        lambda x: int(x) if isinstance(x, float) else x)

    # acos,cr百分号保留一位小数
    all_export_files[['ACoS', 'cr']] = all_export_files[['ACoS', 'cr']].applymap(
        lambda x: str(round(x * 100, 1)) + '%' if isinstance(x, float) else x)

    # cpc保留两位有效数字
    all_export_files['cpc'] = all_export_files['cpc'].apply(lambda x: round(float(x), 2) if isinstance(x, float) else x)

    # 推广占比,广销比百分号保留一位有效数字
    all_export_files[['推广占比', '广销比']] = all_export_files[['推广占比', '广销比']].applymap(
        lambda x: str(round(float(x) * 100, 1)) + '%' if isinstance(x, float) else x)

    # 除去百分号小数
    # all_export_files[['ACoS', 'cr', '推广占比', '广销比']] = all_export_files[['ACoS', 'cr', '推广占比', '广销比']].applymap(
    #     lambda x: x.replace('.0', '') if '%' in x else x)

    return all_export_files


# 读取单个文件数据(若为excel,则读取单个sheet)
def read_files(files_path: 'full_path', sheet_name='Sheet1'):
    split_file_path = os.path.splitext(files_path)
    if len(split_file_path) > 1:
        file_type = split_file_path[-1].lower()
        if file_type in ['.csv', '.txt']:
            try:
                file_data = pd.read_csv(files_path, error_bad_lines=False, warn_bad_lines=False)
                if file_data.shape[1] == 1:
                    file_data = pd.read_csv(files_path, sep='\t', error_bad_lines=False, warn_bad_lines=False)
                return file_data
            except Exception as e:
                file_data = pd.read_csv(files_path, encoding="ISO-8859-1", error_bad_lines=False, warn_bad_lines=False)
                if file_data.shape[1] == 1:
                    file_data = pd.read_csv(files_path, sep='\t', encoding="ISO-8859-1", error_bad_lines=False,
                                            warn_bad_lines=False)
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


# 连接数据库得到站点平均cpc,acos,返回station,acos,cpc,站点负责人四列
def db_download_station_names(db='team_station', table='only_station_info', ip='127.0.0.1', port=3306,
                              user_name='marmot', password='marmot123'):
    """
    加载广告组接手的站点以及对应的平均数据
    :return: 站点平均cpc和acos
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
    sql = """SELECT station,acos,cpc,ad_manger FROM {} """.format(table)
    # 执行sql语句
    cursor.execute(sql)
    stations_avg = cursor.fetchall()
    stations_avg = pd.DataFrame([list(station) for station in stations_avg],
                                columns=['station', 'acos', 'cpc', 'ad_manger'])
    stations_avg.drop_duplicates(inplace=True)
    conn.commit()
    cursor.close()
    conn.close()
    return stations_avg


# processed_files 1.处理精否/否定asin
def negative_exact(station_name, camp_data, st_data, active_listing_data):
    """
    描述:
        用户搜索的内容中,点击或是曝光产生不了收益，这些词或是ASIN我们可以认为是无效的,于是可以将
        这部分用户搜索词或是ASIN停掉,不再产生花费。
    逻辑:
        否定分为:处理精否(关键词keyword)和否定ASIN(ASIN)
        否定关键词主逻辑：
            # 条件1： click>40,order=0;
            # 条件2： spend>站点平均cpc*40,order=0;
            # 条件3： acos>站点平均acos*3,cr<2%;
            # 条件4：acos>站点平均acos*3,cr<3%,click>40*(order+1);
            # 条件5: acos>站点平均acos*3,spend>站点平均cpc*40*(order+1)
        否定ASIN主逻辑:
            # 通过click去否定
    :param station_name:站点名
    :param camp_data:广告报表
    :param st_data:搜索词报表
    :param active_listing_data:用于sku和asin之间的关系
    :param stations_folder:站点文件夹存储的路径
    :return:
    """

    # 按照条件处理需要精否的keywords,生成精否过程表
    def get_negative_exact_kws(station_name, st_data, camp_data):
        if st_data is None:
            return
        if st_data.empty:
            return
        # 保留关键词,剔除掉B0,b0开头(ASIN否定)
        site = station_name[-2:].upper()
        stations_info = db_download_station_names()
        station_acos = stations_info['acos'][stations_info['station'] == station_name.lower()]
        if station_acos.empty:
            station_acos = None
            print(f'站点不存在:站点{station_name}无法在only_station_info中找到.')
            return
        else:
            station_acos = station_acos.values[0]
            station_acos = float(station_acos.replace('%', '')) / 100
            # 防止站点acos过高，设置一个上限
            station_acos = min(station_acos, acos_ideal[site] * 2)

        station_cpc = stations_info['cpc'][stations_info['station'] == station_name.lower()]
        if station_cpc.empty:
            station_cpc = None
            print(f'站点不存在:站点{station_name}无法在only_station_info中找到.')
            return
        else:
            station_cpc = station_cpc.values[0]
            # 防止站点cpc过高，设置一个上限
            station_cpc = min(station_cpc, cpc_max[site])

        # 筛选出需要的列
        # clicks,spend,7 day total orders,advertising cost of sales(acos)
        need_columns = ['Campaign Name', 'Ad Group Name', 'Customer Search Term', 'Match Type',
                        'Impressions', 'Clicks', 'Spend', '7 Day Total Orders (#)', '7 Day Total Sales',
                        'Advertising Cost of Sales (ACoS)', '7 Day Conversion Rate']
        if not set(need_columns).issubset(set(st_data.columns)):
            print(f'缺失列: 站点 {station_name} st表缺失{set(need_columns) - set(st_data.columns)}列。')
            return
        # 将acos和cr转化成数值型
        st_data[['Advertising Cost of Sales (ACoS)', '7 Day Conversion Rate']] = st_data[
            ['Advertising Cost of Sales (ACoS)', '7 Day Conversion Rate']].applymap(
            lambda x: x if not isinstance(x, str) else float(x) if '%' not in x else float(x.replace('%', '')) / 100)

        # st表中的关键词搜索
        st_keywords_data = st_data[~st_data['Customer Search Term'].str.startswith(('B0', 'b0'))]
        # 查找满足条件的关键词

        # 条件1： click>40,order=0
        negative_exact_kws_1 = st_keywords_data[need_columns][
            (st_keywords_data['Clicks'] > 40) & (st_keywords_data['7 Day Total Orders (#)'] == 0)]
        # 条件2： spend>站点平均cpc*40,order=0
        if station_cpc is not None:
            negative_exact_kws_2 = st_keywords_data[need_columns][
                (st_keywords_data['Spend'] > station_cpc * 40) & (st_keywords_data['7 Day Total Orders (#)'] == 0)]
        else:
            negative_exact_kws_2 = None
        # 条件3： acos>站点平均acos*3,cr<2%;
        if station_acos is not None:
            negative_exact_kws_3 = st_keywords_data[need_columns][
                (st_keywords_data['Advertising Cost of Sales (ACoS)'] > station_acos * 3) & (
                        st_keywords_data['7 Day Conversion Rate'] < 0.02)]
        else:
            negative_exact_kws_3 = None
        # 条件4：acos>站点平均acos*3,cr<3%,click>40*(order+1);
        if station_acos is not None:
            negative_exact_kws_4 = st_keywords_data[need_columns][
                (st_keywords_data['Advertising Cost of Sales (ACoS)'] > station_acos * 3) & (
                        st_keywords_data['7 Day Conversion Rate'] < 0.03) &
                (st_keywords_data['Clicks'] > (st_keywords_data['7 Day Total Orders (#)'] + 1) * 40)]
        else:
            negative_exact_kws_4 = None
        # 条件5: acos>站点平均acos*3,spend>站点平均cpc*40*(order+1)
        if station_acos is not None:
            negative_exact_kws_5 = st_keywords_data[need_columns][
                (st_keywords_data['Advertising Cost of Sales (ACoS)'] > station_acos * 3) &
                (st_keywords_data['Spend'] > (st_keywords_data['7 Day Total Orders (#)'] + 1) * 40 * station_cpc)]
        else:
            negative_exact_kws_5 = None
        all_negative_exact_kws = [kws for kws in [negative_exact_kws_1, negative_exact_kws_2, negative_exact_kws_3,
                                                  negative_exact_kws_4, negative_exact_kws_5] if kws is not None]
        if all_negative_exact_kws:
            all_negative_exact_kws = pd.concat(all_negative_exact_kws)

        # st表中的ASIN搜索
        st_asin_data = st_data[st_data['Customer Search Term'].str.startswith(('B0', 'b0'))]
        # 查找满足条件的asin
        negative_exact_asin = st_asin_data[need_columns][
            (st_asin_data['Clicks'] > 40) & (st_asin_data['7 Day Total Orders (#)'] == 0)]

        # 将asin否定关键词和keyword否定关键词合并处理
        all_negative_exact = pd.concat([all_negative_exact_kws, negative_exact_asin])

        all_negative_exact.drop_duplicates(inplace=True)
        # 输出精否临时表
        now_date = datetime.now().date()
        # 删除camp表中重复的精否词
        if (camp_data is not None) & (not camp_data.empty):
            # 处理camp表的列名
            camp_data.columns = [column.strip(" ") for column in camp_data.columns]
            camp_negative_exact_data = camp_data[
                (camp_data['Match Type'].str.contains('negative', case=False)) & (
                        camp_data['Campaign Status'] == 'enabled') &
                (camp_data['Ad Group Status'] == 'enabled') & (camp_data['Status'] == 'enabled')]
            if not camp_negative_exact_data.empty:
                for index, row in all_negative_exact.iterrows():
                    st_camp_name = row['Campaign Name']
                    st_group_name = row['Ad Group Name']
                    st_kws = row['Customer Search Term']
                    camp_negative_exact_haved = camp_negative_exact_data[
                        (camp_negative_exact_data['Campaign'] == st_camp_name)
                        & (camp_negative_exact_data['Ad Group'] == st_group_name)
                        & (camp_negative_exact_data['Keyword or Product Targeting'] == st_kws)]
                    if not camp_negative_exact_haved.empty:
                        all_negative_exact.drop(index=index, inplace=True)
        all_negative_exact.reset_index(drop=True, inplace=True)
        # 通过camp表 添加Max bid和Status两列
        for index in all_negative_exact.index:
            max_bid = camp_data['Max Bid'][
                (camp_data['Campaign'] == all_negative_exact.loc[index, 'Campaign Name']) &
                (camp_data['Ad Group'] == all_negative_exact.loc[index, 'Ad Group Name']) &
                (camp_data['Keyword or Product Targeting'] == all_negative_exact.loc[
                    index, 'Customer Search Term'])]
            if not max_bid.empty:
                all_negative_exact.loc[index, 'Max bid'] = max_bid.values[0]
            else:
                all_negative_exact.loc[index, 'Max bid'] = None

            status = camp_data['Status'][(camp_data['Campaign'] == all_negative_exact.loc[index, 'Campaign Name']) &
                                         (camp_data['Ad Group'] == all_negative_exact.loc[index, 'Ad Group Name']) &
                                         (camp_data['Keyword or Product Targeting'] == all_negative_exact.loc[
                                             index, 'Customer Search Term'])]
            if not status.empty:
                all_negative_exact.loc[index, 'Status'] = status.values[0]
            else:
                camp_da_group_status = camp_data['Ad Group Status'][
                    (camp_data['Campaign'] == all_negative_exact.loc[index, 'Campaign Name']) &
                    (camp_data['Ad Group'] == all_negative_exact.loc[index, 'Ad Group Name'])]
                if not camp_da_group_status.empty:
                    all_negative_exact.loc[index, 'Status'] = camp_da_group_status.values[0]

                else:
                    all_negative_exact.loc[index, 'Status'] = None

            export_columns = ['Campaign Name', 'Ad Group Name', 'Customer Search Term', 'Max bid', 'Match Type',
                              'Impressions', 'Clicks', 'Spend', '7 Day Total Orders (#)', '7 Day Total Sales',
                              'Advertising Cost of Sales (ACoS)', '7 Day Conversion Rate', 'Status']
            all_negative_exact = all_negative_exact[export_columns]
            all_negative_exact.drop_duplicates(inplace=True)

        # 输出精否逻辑
        def negative_exact_logic(avg_cpc, avg_acos):
            '''
            logic1:click>40,order=0
            logic2:spend>站点平均cpc*40,order=0
            logic3:acos>站点平均acos*3,cr<2%
            logic4:acos>站点平均acos*3,cr<3%,click>40*(order+1)
            logic5:acos>站点平均acos*3,spend>站点平均cpc*(order+1)
            '''
            negative_exact_logic = pd.DataFrame([['click>40 & order=0', f'spend>{round(avg_cpc * 40, 2)} & order=0',
                                                  f'acos>{round(avg_acos * 3, 2)} & cr<2%',
                                                  f'acos>{round(avg_acos * 3, 2)} & cr<3% & click>40*(order+1)',
                                                  f'acos>{round(avg_acos * 3, 2)} & sp\
                                                                        end>{round(avg_cpc, 2)}*(order+1)']],
                                                columns=['logic1', 'logic2', 'logic3', 'logic4', 'logic5'])
            return negative_exact_logic
            # negative_exact_logic.to_excel(file_save_path, index=False, sheet_name='精否条件')

        # 输出精否过程表和精否条件
        def export_negative_exact_logic_n_kws(all_negative_exact_kws, negative_exact_logic, file_save_path):
            if (all_negative_exact_kws.empty) or (all_negative_exact_kws is None):
                return
            write = pd.ExcelWriter(file_save_path)
            all_negative_exact_kws.to_excel(write, sheet_name='精否过程表', index=False)
            negative_exact_logic.to_excel(write, sheet_name='精否条件', index=False)
            write.save()

        # 输出精否过程表中精否条件
        # negative_exact_logic_info = negative_exact_logic(station_cpc, station_acos)
        # export_negative_exact_logic_n_kws(all_negative_exact, negative_exact_logic_info, file_save_path)
        return all_negative_exact

    # 生成精否表
    def negative_kws_file(station_name, negative_exact_kws_data, camp_data, active_listing_data):
        if negative_exact_kws_data is None:
            return
        if negative_exact_kws_data.empty:
            return
        std_columns = ['Campaign Name', 'Campaign Daily Budget', 'Campaign Start Date', 'Campaign End Date',
                       'Campaign Targeting Type', 'Ad Group Name',
                       'Max Bid', 'SKU', 'Keyword', 'Product Targeting ID', 'Match Type', 'Campaign Status',
                       'Ad Group Status', 'Status', 'Bidding strategy']
        negative_exact_kws_upload_format = pd.DataFrame(columns=std_columns)
        for index, row in negative_exact_kws_data.iterrows():
            negative_exact_kw_upload_format_temp = pd.DataFrame([[None] * len(std_columns)], columns=std_columns)
            negative_exact_kw_upload_format_temp['Campaign Name'] = row['Campaign Name']
            negative_exact_kw_upload_format_temp['Ad Group Name'] = row['Ad Group Name']
            negative_exact_kw_upload_format_temp['Keyword'] = row['Customer Search Term']
            negative_exact_kws_upload_format = negative_exact_kws_upload_format.append(
                negative_exact_kw_upload_format_temp)
        negative_exact_kws_upload_format['Match Type'] = 'negative exact'
        negative_exact_kws_upload_format[['Campaign Status', 'Ad Group Status', 'Status']] = 'enabled'

        # 由于ad group name列存在命名不规范的情况(ad group name命名规范为sku asin),于是重新调整ad group的命名

        # 分开处理否定关键词和否定ASIN,由于否定ASIN需要打在定向ASIN下面打,于是改变否定ASIN的输出格式
        camp_data_sku_info = camp_data[['Campaign', 'Ad Group', 'SKU']][pd.notnull(camp_data['SKU'])]
        active_listing_data_sku_asin = active_listing_data[['seller-sku', 'asin1']]
        # 得到正确的SKU,添加SKU列
        negative_exact_kws_upload_format = pd.merge(negative_exact_kws_upload_format, camp_data_sku_info,
                                                    left_on=['Campaign Name', 'Ad Group Name'],
                                                    right_on=['Campaign', 'Ad Group'], how='left')
        # 得到正确的ASIN,添加ASIN列
        negative_exact_kws_upload_format = pd.merge(negative_exact_kws_upload_format, active_listing_data_sku_asin,
                                                    left_on=['SKU_y'], right_on=['seller-sku'], how='left')
        # negative_exact_kws_upload_format['Ad Group Name'] = negative_exact_kws_upload_format['SKU_y'] + ' ' + \
        #                                                     negative_exact_kws_upload_format['asin1']
        negative_exact_kws_upload_format.rename(columns={'SKU_x': 'SKU'}, inplace=True)
        negative_exact_kws_upload_format = negative_exact_kws_upload_format[
            pd.notnull(negative_exact_kws_upload_format['Ad Group Name'])]
        negative_exact_kws_upload_format = negative_exact_kws_upload_format[std_columns]

        if not negative_exact_kws_upload_format.empty:

            # 精否词数据
            negative_exact_kw = negative_exact_kws_upload_format[
                ~negative_exact_kws_upload_format['Keyword'].str.contains('b0', case=False)]

            # 否定ASIN
            negative_asin_all_rows = negative_exact_kws_upload_format[
                negative_exact_kws_upload_format['Keyword'].str.contains('b0', case=False)]

        else:
            return

        # 每个否定kw行添加ad group
        if not negative_exact_kw.empty:

            def build_negative_kw_format(negative_kw_row: pd.Series, campaign):
                """
                为每一个否定kw行添加对应的ad group行
                :param negative_kw_row:
                :return:
                """

                camp_ad_group_data = campaign[campaign['Record Type'] == 'Ad Group']
                campaign_name = negative_kw_row['Campaign Name']
                ad_group_name = negative_kw_row['Ad Group Name']
                sku = ad_group_name.split(' ')[0]
                ad_group_row = camp_ad_group_data[(camp_ad_group_data['Campaign'] == campaign_name) & (
                        camp_ad_group_data['Ad Group'] == ad_group_name)]
                if not ad_group_row.empty:
                    ad_group_bid = ad_group_row['Max Bid'].values[0]
                    ad_group_row = [campaign_name, None, None, None, None, ad_group_name, ad_group_bid, None,
                                    None, None, None, 'enabled', 'enabled', None, None]
                    # sku_row =  [campaign_name, None, None, None, None, ad_group_name, None, sku,
                    #                 None, None, None, 'enabled', 'enabled', 'enabled', None]
                else:
                    # defalut ad group row
                    site = station_name[-2:].upper()
                    min_bid = ad_group_max_bid_lower_limit_dict[site]
                    ad_group_row = ['MANUAL-ST-EXACT-by-SP_Bulk', None, None, None, None, ad_group_name, min_bid, None,
                                    None, None, None, 'enabled', 'enabled', None, None]
                    # sku_row = ['MANUAL-ST-EXACT-by-SP_Bulk', None, None, None, None, ad_group_name, None, sku,
                    #                 None, None, None, 'enabled', 'enabled', 'enabled', None]
                ad_group_row = pd.Series(ad_group_row, index=negative_kw_row.index)
                # sku_row = pd.Series(sku_row, index=negative_kw_row.index)
                # one_negative_kw_group = pd.concat([ad_group_row,sku_row,negative_kw_row],axis=1).T
                one_negative_kw_group = pd.concat([ad_group_row, negative_kw_row], axis=1).T
                return one_negative_kw_group

            negative_exact_kw = list(
                map(build_negative_kw_format, [negative_kw_row for _, negative_kw_row in
                                               negative_exact_kw.iterrows()],
                    [camp_data] * len(negative_exact_kw)))

            negative_exact_kw = pd.concat(negative_exact_kw)

        if not negative_asin_all_rows.empty:

            # 否定asin中添加ad行,sku行和定向asin行
            def bulid_negative_asin_format(negative_asin_row: 'pd.Series', station_site,
                                           unknown_station_ad_group_max_bid_lower_limit=0.02):
                """
                否定asin为一行,添加对应的ad gruop行,sku行以及创造一个自身对应的定向asin
                ,同时出价给站点的最小出价
                :param negative_asin_row: 一个否定asin行
                :return: 充填完全的一个asin
                """
                station_site_list = ad_group_max_bid_lower_limit_dict.keys()

                if station_site.upper() in station_site_list:
                    station_ad_group_max_bid_lower_limit = ad_group_max_bid_lower_limit_dict[station_site]

                else:
                    print(f"UNKNOWN SITE: {station_site} 未知.")
                    print("广告组出价暂时给0.02,请及时添加新站点信息")
                    station_ad_group_max_bid_lower_limit = unknown_station_ad_group_max_bid_lower_limit

                # campaign name为固定值
                campaign_name = 'Negative Targeting Expression-SP_Bulk'
                ad_group_name = negative_asin_row['Ad Group Name']
                negative_asin_row_index = negative_asin_row.index
                empty_row = [None] * len(negative_asin_row)
                # find asin
                asin = re.findall('[Bb]0.{8}', ad_group_name)
                # 若没有asin,则返回
                if not asin:
                    return pd.Series(empty_row, index=negative_asin_row_index)
                ad_group_asin_expression = f'ASIN="{asin[-1].upper()}"'
                sku = ad_group_name.split(' ')[0]
                # ad_group_name是sku加asin
                ad_group_name = f'{sku} {asin[-1]}'
                empty_row = [None] * len(negative_asin_row)
                # first row(ad 行)
                ad_group_row = pd.Series(empty_row, index=negative_asin_row_index)
                ad_group_row['Max Bid'] = station_ad_group_max_bid_lower_limit
                ad_group_row['Campaign Status'] = 'enabled'
                ad_group_row['Ad Group Status'] = 'enabled'
                # second row(sku 行)
                sku_row = pd.Series(empty_row, index=negative_asin_row_index)
                sku_row['SKU'] = sku
                sku_row['Campaign Status'], sku_row['Ad Group Status'], sku_row[
                    'Status'] = 'enabled', 'enabled', 'enabled'
                # third row(创造的定向asin行)
                create_asin_row = pd.Series(empty_row, index=negative_asin_row_index)
                create_asin_row['Max Bid'] = station_ad_group_max_bid_lower_limit
                create_asin_row['Keyword'], create_asin_row[
                    'Product Targeting ID'] = ad_group_asin_expression, ad_group_asin_expression
                create_asin_row['Match Type'] = 'Targeting Expression'
                create_asin_row['Campaign Status'], create_asin_row['Ad Group Status'], create_asin_row[
                    'Status'] = 'enabled', 'enabled', 'enabled'
                # fourth row(第四行为否定asin行)
                negative_asin = negative_asin_row['Keyword']
                negative_asin_expression = f'ASIN="{negative_asin.upper()}"'
                negative_asin_row['Keyword'], negative_asin_row[
                    'Product Targeting ID'] = negative_asin_expression, negative_asin_expression
                negative_asin_row['Match Type'] = 'Negative Targeting Expression'

                # 合并四行
                one_negative_asin_df = pd.concat([ad_group_row, sku_row, create_asin_row, negative_asin_row], axis=1).T
                # 添加Campaign 行和ad group行
                one_negative_asin_df['Campaign Name'] = campaign_name
                one_negative_asin_df['Ad Group Name'] = ad_group_name
                return one_negative_asin_df

                # # 否定ASIN的Campaign Name是固定写法"Negative Targeting Expression-SP_Bulk"

            station_site = station_name[-2:].upper()
            # 循环否定asin中的每一行
            all_format_negative_asin = list(
                map(bulid_negative_asin_format, [negative_asin_row for _, negative_asin_row in
                                                 negative_asin_all_rows.iterrows()],
                    [station_site] * len(negative_asin_all_rows)))
            all_format_negative_asin = pd.concat(all_format_negative_asin)
            all_negative_data = pd.concat([negative_exact_kw, all_format_negative_asin])

        else:
            all_negative_data = negative_exact_kw
        all_negative_data.drop_duplicates(inplace=True)
        all_negative_data.reset_index(drop=True, inplace=True)

        return all_negative_data

    # 生成精否过程表
    negative_exact_kws_temp = get_negative_exact_kws(station_name, st_data, camp_data)

    # 生成精否表
    all_negative_data = negative_kws_file(station_name, negative_exact_kws_temp, camp_data, active_listing_data)

    # 由于erp上传必须要ad group行,于是添加ad group行

    return all_negative_data


# processed_files 2.处理新品自动新增(AO新增和active listing新增)
def process_auto_new(process_station, active_listing_data, all_order_data, camp_data):
    # 充填空的sku,并获取sku
    def get_camp_sku(camp_data):
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
        if not set(['seller-sku', 'asin1', 'price', 'fulfillment-channel', 'open-date']).issubset(
                active_listing_data.columns):
            print(f'{process_station}:active_listing缺失seller_sku/asin/price/fulfillment_channel')
        # active_listing_data['seller_sku'] = active_listing_data['seller_sku'].apply(lambda x: x.lower())
        active_listing_sku_set_asin = active_listing_data[
            ['seller-sku', 'asin1', 'price', 'fulfillment-channel', 'open-date']]

        return active_listing_sku_set_asin

    def get_all_order_sku(all_order_data, site):
        sales_channel = {'it': 'Amazon.it', 'de': 'Amazon.de', 'es': 'Amazon.es', 'fr': 'Amazon.fr',
                         'uk': 'Amazon.co.uk', 'jp': 'Amazon.co.jp', 'us': 'Amazon.com', 'ca': 'Amazon.ca',
                         'mx': 'Amazon.com.mx', 'in': 'Amazon.in', 'au': 'Amazon.com.au'}
        all_order_data.columns = [column.strip(' ') for column in all_order_data.columns]
        if 'order-status' not in all_order_data.columns:
            all_order_data['order-status'] = 'Shipped'
        if not set(['sales-channel', 'order-status', 'sku']).issubset(set(all_order_data.columns)):
            lose_column = set(['sales-channel', 'order-status', 'sku']) - set(all_order_data.columns)
            print(f'{process_station}:all_order表缺失{lose_column}')
            return
        site_sales_channel = sales_channel[site]
        all_order_sku = all_order_data[(all_order_data['sales-channel'] == site_sales_channel) & (
                all_order_data['order-status'] != 'Cancelled')]['sku']
        all_order_sku_set = set(all_order_sku)
        return all_order_sku_set

    def new_listing_upload_format(station_name, new_sku_list, active_listing_info, new_ao_listing, camp_data,
                                  max_bid=0.3):
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

        def get_new_active_listing_camp_name(station_name, camp_data, max_active_listing_group_num=5000,
                                             recent_camp_date_distance=90):
            """
            active_listing新增中广告大组的命名规则:
                CASE 1.原先不存在的active_listing新增(广告大组名以AUTO开头,包含NEW)中,
                        则新建一个广告大组名以今天日期结尾（AUTO-站点-by-SP_Bulk-New_200521）
                CASE 2.原先存在的active_listing新增，若不存在已经命名的日期结尾的，
                        则新建一个广告大组名以今天日期结尾（AUTO-站点-by-SP_Bulk-New_200521）
                CASE 3.原先存在的active_listing新增，若存在已经命名的日期结尾的，但是最新的日期广告大组中的的ad group个数小于5000,并且时间间隔小于90天
                        则添加到该组中
                CASE 4.原先存在的active_listing新增，若存在已经命名的日期结尾的，但是最新的日期广告大组中的的ad group个数大于5000或是间隔大于90天
                        则新建一个广告大组名以今天日期结尾（AUTO-站点-by-SP_Bulk-New_200521）
            :param station_name:站点名
            :param camp_data:广告报表
            :return:新增的active_listing中广告大组的命名
            """
            if (camp_data is None) or (camp_data.empty):
                return
            if 'Campaign' not in camp_data.columns:
                return
            now_date = datetime.now().date().strftime('%y%m%d')
            # 首先判断是否存在active_listing新增大组
            exist_active_listing_camp_name_info = camp_data['Campaign'][
                (camp_data['Campaign'].str.contains('new', case=False)) & (
                    camp_data['Campaign'].str.startswith('AUTO')) & (camp_data['Record Type'] == 'Ad Group')]
            station_name = station_name.upper()
            # case 1:原先不存在的active_listing新增
            if exist_active_listing_camp_name_info.empty:
                return f'AUTO-{station_name}-by-SP_Bulk-New_{now_date}'
            # case 2:原先存在的active_listing新增,但是不存在已经命名的日期结尾的
            exist_active_listing_camp_name_set = set(exist_active_listing_camp_name_info)
            # 提取active_listing中的数字
            exist_active_listing_camp_name_date = [int(re.sub('[^0-9]', '', camp_name)) for camp_name in
                                                   exist_active_listing_camp_name_set if
                                                   re.sub('[^0-9]', '', camp_name)]
            if not exist_active_listing_camp_name_date:
                return f'AUTO-{station_name}-by-SP_Bulk-New_{now_date}'
            # case 3:原先存在的active_listing新增，若存在已经命名的日期结尾的，但是最新的日期广告大组中的的ad group个数小于5000,并且时间间隔小于90天
            nearest_date = max(exist_active_listing_camp_name_date)
            exist_recent_active_listing_camp_name = \
                [name for name in exist_active_listing_camp_name_set if str(nearest_date) in name][0]
            exist_recent_active_listing_camp_num = len(exist_active_listing_camp_name_info[
                                                           exist_active_listing_camp_name_info == exist_recent_active_listing_camp_name])
            # 计算时间间隔
            nearest_date = str(nearest_date)
            if len(nearest_date) == 6:
                date_distance = (int(now_date[-6:-4]) - int(nearest_date[-6:-4])) * 365 + (
                        int(now_date[-4:-2]) - int(nearest_date[-4:-2])) * 30 + (
                                        int(now_date[-2:]) - int(nearest_date[-2:]))
            else:
                return f'AUTO-{station_name}-by-SP_Bulk-New_{now_date}'

            if (exist_recent_active_listing_camp_num < max_active_listing_group_num) & (
                    date_distance < recent_camp_date_distance):
                return exist_recent_active_listing_camp_name
            # case 4:原先存在的active_listing新增，若存在已经命名的日期结尾的，但是最新的日期广告大组中的的ad group个数大于5000或是间隔大于90天
            else:
                return f'AUTO-{station_name}-by-SP_Bulk-New_{now_date}'

        def trans_sku_into_upload(sku, camp_name):
            asin = active_listing_info['asin1'][active_listing_info['seller-sku'] == sku].values[0]
            price = active_listing_info['price'][active_listing_info['seller-sku'] == sku].values[0]
            fulfillment = active_listing_info['fulfillment-channel'][active_listing_info['seller-sku'] == sku].values[0]
            start_date = active_listing_info['open-date'][active_listing_info['seller-sku'] == sku].values[0]
            if 'def' in fulfillment.lower():
                fulfillment = 'fbm'
            else:
                fulfillment = 'fba'
            bid = round(min(float(price) * 0.1 * 0.03, max_bid / bid_exchange[country]), 2)
            empty_list = [np.nan] * len(export_columns)
            processed_auto_new_data = pd.DataFrame([empty_list, empty_list], columns=export_columns)
            processed_auto_new_data['Campaign Name'] = camp_name
            processed_auto_new_data['Ad Group Name'] = "%s %s_%s" % (sku, asin, file_date)
            processed_auto_new_data.loc[0, 'Max Bid'] = bid
            processed_auto_new_data.loc[1, 'SKU'] = sku
            processed_auto_new_data.loc[0, 'Ad Group Status'] = 'enabled'
            processed_auto_new_data.loc[1, 'Status'] = 'enabled'
            # 添加客单价,发货方式,上架时间
            processed_auto_new_data['Price'] = price
            processed_auto_new_data['Fulfillment-channel'] = fulfillment
            processed_auto_new_data['Start Date'] = start_date
            return processed_auto_new_data

        if new_sku_list:
            new_active_listing_camp_name = get_new_active_listing_camp_name(station_name, camp_data)
            listing_sku_upload_data = pd.concat([trans_sku_into_upload(sku, camp_name_listing) for sku in new_sku_list])
            listing_sku_upload_data['new_add_type'] = 'listing'
            listing_sku_upload_data['Campaign Name'] = new_active_listing_camp_name
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
        all_sku_upload_data = all_sku_upload_data[all_sku_upload_data['Price'] >= 10 / (exchange_rate[country])]
        return all_sku_upload_data

    # 广告报表
    camp_data['Campaign Targeting Type'] = camp_data['Campaign Targeting Type'].fillna(method='ffill')
    camp_data['Campaign Targeting Type'] = camp_data['Campaign Targeting Type'].str.lower()
    camp_sku_set = get_camp_sku(camp_data[camp_data['Campaign Targeting Type'] == 'auto'])

    # 生成上传表
    active_listing_info = get_active_listing_info(active_listing_data)
    active_listing_sku_set = set(active_listing_info['seller-sku'])
    site = process_station[-2:].lower()
    all_order_sku_set = get_all_order_sku(all_order_data, site)
    new_sku = active_listing_sku_set - camp_sku_set
    new_sku_num = len(new_sku)
    # listing新增sku
    new_listing_sku = new_sku - all_order_sku_set
    # ao新增sku
    ao_listing_sku = new_sku & all_order_sku_set
    new_listing_upload_data = new_listing_upload_format(process_station, new_listing_sku, active_listing_info,
                                                        ao_listing_sku, camp_data)
    if new_listing_upload_data is None:
        return
    fba_num = len(new_listing_upload_data[(pd.notna(new_listing_upload_data['SKU'])) & (
            new_listing_upload_data['Fulfillment-channel'] == 'fba')])
    fbm_num = len(new_listing_upload_data[(pd.notna(new_listing_upload_data['SKU'])) & (
            new_listing_upload_data['Fulfillment-channel'] == 'fbm')])
    now_datetime = datetime.now().strftime('%Y%m%d')

    # 2.将FBA新增大于30的存储到一个文件夹加下
    # manager_folder = os.path.join(files_save_dirname, 'new_listing_auto_create(FBA_ABOVE_30)')
    if fba_num >= 30:
        # # 得到站点对应的站点名
        # if not os.path.exists(manager_folder):
        #     os.makedirs(manager_folder)
        # new_listing_upload_data_path = os.path.join(manager_folder,
        #                                             f"{process_station}_{now_datetime}_fba{fba_num} fbm{fbm_num}.csv")
        # new_listing_upload_data.to_csv(new_listing_upload_data_path, index=False)
        # print(f'{process_station}: 自动广告新增上传表完成.')
        std_columns = ['Campaign Name', 'Campaign Daily Budget', 'Campaign Start Date', 'Campaign End Date',
                       'Campaign Targeting Type', 'Ad Group Name', 'Max Bid', 'SKU', 'Keyword',
                       'Product Targeting ID', 'Match Type', 'Campaign Status', 'Ad Group Status', 'Status',
                       'Bidding strategy']
        new_listing_upload_data = new_listing_upload_data[std_columns]
        new_listing_upload_data.drop_duplicates(inplace=True)
        return new_listing_upload_data


# processed_files 3.关键词新增和ASIN新增
def process_st_new(station_name, st_data, camp_data, active_listing_data):
    """
    逻辑:
        1.出单和出单词的初步处理：
            1. 排除B0  2. 7天成交订单 > 0
        2. 去重: SKU + 关键词 + 匹配方式 已经做过的关键词去重、
        3. 在售: 即active_listing中要有这个SKU
        4.出价逻辑
        以下出价为广泛匹配出价，而精准出价为广泛出价+0.01，其中要是对应的Match Type出价才可以
        自动组出价：
        1. 出单且acos小于指定acos
            1.1 acos小于指定acos*0.1
                1.1.1 出单数为1: 关键词cpc+0.02
                1.1.2 出单数(1,5): 所在自动组出价+0.03
                1.1.3 出单数大于等于5: 所在自动组出价+0.04
            1.2 acos小于指定acos*0.3
                1.2.1 出单数为1: 关键词cpc+0.01
                1.2.2 出单数(1,5): 所在自动组出价+0.01
                1.2.3 出单数大于等于5: 所在自动组出价+0.02
            1.3 acos小于指定acos
                1.3.1 出单数为1: 关键词cpc
                1.3.2 出单数(1,5): 所在自动组出价
                1.3.3 出单数大于等于5: 所在自动组出价+0.01
        2.出单且acos大于指定acos
                关键词cpc*(指定acos/关键词acos)
        手动组出价:
        1. 出单且acos小于指定acos
            1.1 acos小于指定acos*0.1
                1.1.1 出单数为1: 关键词出价+0.02
                1.1.2 出单数(1,5): 关键词出价+0.03
                1.1.3 出单数大于等于5: 关键词出价+0.04
            1.2 acos小于指定acos*0.3
                1.2.1 出单数为1: 关键词出价+0.01
                1.2.2 出单数(1,5): 关键词出价+0.01
                1.2.3 出单数大于等于5: 关键词出价+0.02
            1.3 acos小于指定acos
                1.3.1 出单数为1: 关键词出价
                1.3.2 出单数(1,5): 关键词出价
                1.3.3 出单数大于等于5: 关键词出价+0.01
        2.出单且acos大于指定acos
            关键词cpc*(指定acos/关键词acos)


    步骤:
        1.初始化:判断st_data,以及camp_data是否正常
    :param st_data:ST原始数据
    :param camp_data:广告报表原始数据
    :return:ST新增的上传表
    """

    # 货币转换
    def currency_trans(currency) -> 'digit':
        """
        将货币装换成数字
        逻辑:
            通过判断倒数第三位是否是,(逗号)或是.(点号)来判断是小数还是整数
        :param currency:需要转换的货币
        :return: 整型或浮点型货币
        """
        if pd.isnull(currency):
            return
        if not isinstance(currency, str):
            return
        else:
            currency = currency.strip(' ')
            currency_temp = re.findall('\d.*', currency)
            if len(currency_temp) == 1:
                currency_temp = currency_temp[-1]
                if currency_temp[-3] in [',', '.']:
                    # 该数字为包含两位小数的数字
                    return float(re.sub('[,.]', '', currency_temp)) / 100
                else:
                    # 该数字不包含两位小数的数字
                    return int(re.sub('[,.]', '', currency_temp))
            if not currency_temp:
                return
            if len(currency_temp) > 1:
                return

    def detect_process_data_error(st_data, camp_data):
        """
        检测st_data和camp_data:若为空或是不是DataFrame或是为空
        :param datas: 检测st_data和camp_data等
        :return: false or true
        """
        datas = [st_data, camp_data]
        for data in datas:
            if (data is None) or (not isinstance(data, pd.DataFrame)) or (data.empty):
                return False
        return True

    def init_st_data(st_ori_data):
        """
        描述:
            从原始的st数据中筛选出需要的st数据
        逻辑:
            1. 排除Customer Search Term列中开头为B0且为10位数(BO开头不是关键词)  2. 7天成交订单 > 0
        :param st_ori_data:原始的st data
        :return: 筛选后的st data
        """
        # 1.判断数据是否正确
        if (st_ori_data is None) or (not isinstance(st_ori_data, pd.DataFrame)) or (st_ori_data.empty):
            return
        # 2.判断数据的列是否存在
        st_ori_data.columns = [col.strip(' ') for col in st_ori_data.columns]
        columns = st_ori_data.columns
        need_columns = {'Customer Search Term', '7 Day Total Sales'}
        st_ori_data.columns = ['7 Day Total Sales' if '7 Day Total Sales' in col else col for col in
                               st_ori_data.columns]
        if not need_columns.issubset(columns):
            print(f'lost columns:{station_name} 的st表缺少 {need_columns - set(columns)}')
            return
        # 筛除掉开头为B0且为10位数的搜索词
        # st_data_temp = st_ori_data[~st_ori_data['Customer Search Term'].str.contains('B0',case=False)]
        # 删除掉7天成交量订单大于0
        st_data_temp = st_ori_data[st_ori_data['7 Day Total Sales'] > 0]
        st_data_temp['Match Type'] = st_data_temp['Match Type'].apply(lambda x: x.lower())

        return st_data_temp

    def get_one_to_one_camp_data(camp_ori_data):
        """
        描述：
            1.从原始的camp中筛选出一个ad_group只有一个sku的数据，
            2. 将max bid向下充填
        逻辑:
            1.将campaign/ad_group汇总 如果sku个数大于1就剔除掉
        :param camp_ori_data: 原始的广告报表
        :return: 处理好之后的广告报表
        """
        # 1.判断st是否有效
        if (camp_ori_data is None) or (not isinstance(camp_ori_data, pd.DataFrame)) or (camp_ori_data.empty):
            print(f'camp data error: {station_name}')
            return
        # 2.判断数据的列是否存在
        columns = camp_ori_data.columns
        need_columns = {'Record Type', 'Campaign', 'Ad Group', 'SKU', 'Max Bid'}
        if not need_columns.issubset(columns):
            print(f'lost columns:{station_name} 的camp表缺少 {need_columns - columns}')
            return
        # 3. 筛选出ad行
        camp_ori_data['Campaign Targeting Type'].fillna(method='ffill', inplace=True)
        camp_ori_data['Match Type'] = camp_ori_data['Match Type'].str.lower()
        camp_ori_data_ad_row = camp_ori_data[camp_ori_data['Record Type'] == 'Ad']
        # 4.先找到一对一的camp,ad_group
        ad_group_type = camp_ori_data_ad_row.groupby(['Campaign', 'Ad Group']).agg({'SKU': 'count'}).reset_index()
        ad_group_one_to_one = ad_group_type[ad_group_type['SKU'] == 1]
        # 5.筛选出camp中一对一的数据
        ad_group_one_to_one['camp_ad_group'] = ad_group_one_to_one['Campaign'] + ad_group_one_to_one['Ad Group']
        ad_group_one_to_one_set = set(ad_group_one_to_one['camp_ad_group'])
        camp_ori_data['camp_ad_group'] = camp_ori_data['Campaign'] + camp_ori_data['Ad Group']
        camp_data_one_to_one_temp = camp_ori_data[camp_ori_data['camp_ad_group'].isin(ad_group_one_to_one_set)]
        camp_data_one_to_one = camp_data_one_to_one_temp.copy()
        del camp_data_one_to_one['camp_ad_group']

        # 向下充填max bid
        camp_data_one_to_one['Max Bid'].fillna(method='ffill', inplace=True)
        # 填充sku
        one_group_data = []
        ad_group_grouped = camp_data_one_to_one.groupby(['Campaign', 'Ad Group'])
        for group, data in ad_group_grouped:
            sku = data['SKU'][data['Record Type'] == 'Ad'].values[0]
            data['SKU'].fillna(value=sku, inplace=True)
            one_group_data.append(data)
        camp_data_one_to_one = pd.concat(one_group_data)

        return camp_data_one_to_one

    def new_st_upload_format(station_name, st_n_camp_data, camp_data, active_listing_data, init_acos):
        """
        描述:
            st 新增
        逻辑：
            对st和camp合并后的数据进行按照acos和出单的情况进行分类上传
            按照camp name /ad group/sku分类汇总，得到sku下的全部kws的bid
        :param station_name:
        :param st_data:
        :return:
        """
        export_columns = ['Campaign Name', 'Campaign Daily Budget', 'Campaign Start Date', 'Campaign End Date',
                          'Campaign Targeting Type', 'Ad Group Name', 'Max Bid', 'SKU', 'Keyword',
                          'Product Targeting ID',
                          'Match Type', 'Campaign Status', 'Ad Group Status', 'Status', 'Bidding strategy']

        station_name = station_name.upper()
        site = station_name[-2:]
        bid_exchange_rate = bid_exchange[site]
        bid_exchange_unit = 0.01 / bid_exchange_rate

        # 生成用于去重的辅助列 (优化速度 将一对多的广告组单独处理)
        camp_data_temp = camp_data.copy()
        camp_data_temp['ad_group_temp'] = camp_data_temp['Campaign'] + camp_data_temp['Ad Group']
        camp_ad_group_temp_grouped = camp_data_temp.groupby(['ad_group_temp'])
        ad_group_one_to_more_sku = []
        ad_group_one_to_one_sku = []
        for ad_group_temp_name, grouped_data in camp_ad_group_temp_grouped:
            sku_num = [sku for sku in grouped_data['SKU'] if pd.notnull(sku)]
            if len(sku_num) > 1:
                ad_group_one_to_more_sku.append(ad_group_temp_name)
            if len(sku_num) == 1:
                ad_group_one_to_one_sku.append(ad_group_temp_name)
        # ad group对应一个sku
        ad_group_one_to_one_sku_data = camp_data_temp[camp_data_temp['ad_group_temp'].isin(ad_group_one_to_one_sku)]
        ad_group_one_to_one_sku_data['SKU'].fillna(method='ffill', inplace=True)
        ad_group_one_to_one_sku_data_kws_degree = ad_group_one_to_one_sku_data[
            ad_group_one_to_one_sku_data['Record Type'].isin(['Product Targeting', 'Keyword'])]
        ad_group_one_to_one_sku_kws_sign = set(ad_group_one_to_one_sku_data_kws_degree['SKU'] +
                                               ad_group_one_to_one_sku_data_kws_degree['Keyword or Product Targeting'] +
                                               ad_group_one_to_one_sku_data_kws_degree['Match Type'])
        # ad group对应多个sku
        ad_group_one_to_more_sku_data = camp_data_temp[camp_data_temp['ad_group_temp'].isin(ad_group_one_to_more_sku)]

        ad_group_set = set(ad_group_one_to_more_sku_data['ad_group_temp'].values)
        all_kws_repeat_sign = []
        for ad_group in ad_group_set:
            if pd.isnull(ad_group):
                continue
            one_ad_group_data = ad_group_one_to_more_sku_data[
                ad_group_one_to_more_sku_data['ad_group_temp'] == ad_group]
            one_ad_group_sku = set(one_ad_group_data['SKU'].values)
            one_ad_group_sku = [sku for sku in one_ad_group_sku if pd.notnull(sku)]
            if len(one_ad_group_sku) == 0:
                continue
            one_ad_group_kws = [kw for kw in one_ad_group_data['Keyword or Product Targeting'] if pd.notnull(kw)]
            one_ad_group_match_type = [match_type for match_type in one_ad_group_data['Match Type'] if
                                       pd.notnull(match_type)]
            if len(one_ad_group_sku) == 1:
                one_ad_group_repeat = [f'{one_ad_group_sku[0]}{kw}{match_type}' for kw, match_type in
                                       zip(one_ad_group_kws, one_ad_group_match_type)]
            else:
                one_ad_group_repeat = []
                for one_sku in one_ad_group_sku:
                    if pd.isna(one_sku):
                        continue
                    one_sku_repeat = []
                    for kw, match_type in zip(one_ad_group_kws, one_ad_group_match_type):
                        one_kw_repeat_sign = f'{one_sku}{kw}{match_type}'
                        one_sku_repeat.append(one_kw_repeat_sign)
                    one_ad_group_repeat.extend(one_sku_repeat)
            all_kws_repeat_sign.extend(one_ad_group_repeat)
        ad_group_one_to_more_sku_kws_sign = set(all_kws_repeat_sign)
        camp_data_temp_set = ad_group_one_to_more_sku_kws_sign | ad_group_one_to_one_sku_kws_sign
        camp_data_temp_set = set(map(lambda x: x.lower(), camp_data_temp_set))

        # 按照camp name /ad group/sku汇总
        # 计算广泛广告
        #  这里不能用camp name去分组，可以用Match Type_x来分组
        # 先处理ST报表中的关键词新增
        new_keyword_data = st_n_camp_data[~st_n_camp_data['Customer Search Term'].str.contains('b0', case=False)]
        new_asin_data = st_n_camp_data[st_n_camp_data['Customer Search Term'].str.contains('b0', case=False)]

        # A.关键词新增聚合
        grouped_data_keyword = new_keyword_data.groupby(['Ad Group Name', 'SKU', 'Match Type_x'])
        # B.ASIN新增聚合
        grouped_data_asin = new_asin_data.groupby(['Ad Group Name', 'SKU', 'Match Type_x'])

        def calc_one_sku(one_grouped_data, active_listing_data, init_acos, match_type='broad', new_add_type='keyword'):
            empty_list = [np.nan] * len(export_columns)
            sku_name = one_grouped_data['SKU'].values[0]

            # 判断SKU是否在售
            asin = active_listing_data['asin1'][active_listing_data['seller-sku'] == sku_name]
            if len(asin) > 0:
                asin = asin.values[0]
                ad_group_name = f'{sku_name} {asin}'
            else:
                return pd.DataFrame(columns=export_columns)
            kws = set(one_grouped_data['Customer Search Term'])
            bid_list = []
            not_repeat_kw = []
            # 给sku_bid
            sku_bid = ad_group_least_bid[site]
            for kw in kws:
                # 检测keyword中的重复广告
                if new_add_type == 'keyword':
                    if match_type == 'broad':
                        repeat_detect = sku_name + kw + 'broad'
                    else:
                        repeat_detect = sku_name + kw + 'exact'
                # 检测asin中的重复广告
                if new_add_type == 'asin':
                    repeat_detect = f'{sku_name}asin="{kw}"Targeting Expression'
                # 去掉重复广告
                if repeat_detect.lower() in camp_data_temp_set:
                    continue
                kw_acos = one_grouped_data['Advertising Cost of Sales (ACoS)'][
                    one_grouped_data['Customer Search Term'] == kw].values[0]
                if isinstance(kw_acos, str):
                    if '%' in kw_acos:
                        kw_acos = float(kw_acos.replace('%', '')) / 100
                    else:
                        kw_acos = float(kw_acos)
                order = \
                    one_grouped_data['7 Day Total Orders (#)'][one_grouped_data['Customer Search Term'] == kw].values[0]
                if isinstance(order, str):
                    order = int(order)
                cpc = one_grouped_data['Cost Per Click (CPC)'][one_grouped_data['Customer Search Term'] == kw].values[0]
                if isinstance(cpc, str):
                    cpc = float(cpc)
                bid_kw = one_grouped_data['Targeting'][one_grouped_data['Customer Search Term'] == kw].values[0]
                targeting_kw = one_grouped_data['Max Bid'][one_grouped_data['Keyword or Product Targeting'] == bid_kw]
                if len(targeting_kw) > 0:
                    ad_group_bid = \
                        one_grouped_data['Max Bid'][one_grouped_data['Keyword or Product Targeting'] == bid_kw].values[
                            0]
                    group_type = one_grouped_data['Campaign Targeting Type'].values[0]
                    if group_type.lower() == 'manual':
                        camp_type = 'manual'
                    else:
                        camp_type = 'auto'
                else:
                    try:
                        ad_group_bid = float(one_grouped_data['Max Bid'].values[0])
                    except:
                        ad_group_bid = one_grouped_data['Max Bid'].values[0]
                        ad_group_bid = float(ad_group_bid.replace(',', '.'))
                    camp_type = 'auto'
                if isinstance(ad_group_bid, str):
                    ad_group_bid = currency_trans(ad_group_bid)
                if pd.isnull(cpc):
                    cpc = ad_group_bid
                # logic
                if match_type == 'exact':
                    add_bid = bid_exchange_unit
                else:
                    add_bid = 0
                if kw_acos < init_acos * 0.1:
                    if order == 1:
                        if camp_type == 'manual':
                            bid = ad_group_bid + 2 * bid_exchange_unit
                        else:
                            bid = cpc + 2 * bid_exchange_unit
                    elif (order > 1) & (order < 5):
                        bid = ad_group_bid + 3 * bid_exchange_unit
                    else:
                        bid = ad_group_bid + 4 * bid_exchange_unit
                elif kw_acos < init_acos * 0.3:
                    if order == 1:
                        if camp_type == 'manual':
                            bid = ad_group_bid + bid_exchange_unit
                        else:
                            bid = cpc + bid_exchange_unit
                    elif (order > 1) & (order < 5):
                        bid = ad_group_bid + bid_exchange_unit
                    else:
                        bid = ad_group_bid + 2 * bid_exchange_unit
                elif kw_acos <= init_acos:
                    if order == 1:
                        if camp_type == 'manual':
                            bid = ad_group_bid
                        else:
                            bid = cpc
                    elif (order > 1) & (order < 5):
                        bid = ad_group_bid
                    else:
                        bid = ad_group_bid + bid_exchange_unit
                elif kw_acos > init_acos:
                    bid = cpc * (init_acos / kw_acos)
                # excat 加上0.01
                bid += add_bid
                if site == 'JP':
                    bid = int(bid)
                else:
                    bid = round(bid, 2)
                bid_list.append(bid)
                not_repeat_kw.append(kw)

            if not bid_list:
                return pd.DataFrame(columns=export_columns)
            data_len = len(bid_list)
            row = data_len + 2
            processed_st_new_data = pd.DataFrame([empty_list] * row, columns=export_columns)
            if match_type == 'broad':
                processed_st_new_data['Campaign Name'] = f'MANUAL-{station_name}-by-SP_Bulk'
            elif match_type == 'exact':
                processed_st_new_data['Campaign Name'] = f'MANUAL-ST-EXACT-by-SP_Bulk'
            else:
                print(f'ST match_type: {station_name}站点的匹配方式不对...')

            processed_st_new_data.loc[1, 'SKU'] = sku_name
            # 重新给ad group name 判断ad group 中有没有asin
            processed_st_new_data['Ad Group Name'] = ad_group_name
            processed_st_new_data.loc[0, 'Max Bid'] = sku_bid
            processed_st_new_data['Campaign Status'] = 'enabled'
            processed_st_new_data['Ad Group Status'] = 'enabled'
            processed_st_new_data.loc[1:, 'Status'] = 'enabled'
            processed_st_new_data.loc[2:, 'Keyword'] = not_repeat_kw
            processed_st_new_data.loc[2:, 'Max Bid'] = bid_list
            processed_st_new_data.loc[2:, 'Match Type'] = match_type

            return processed_st_new_data

        # 1.计算关键词新增中的广泛出价广告
        broad_data = [calc_one_sku(one_grouped_data, active_listing_data, init_acos) for sku_index, one_grouped_data in
                      grouped_data_keyword]
        broad_data = [data for data in broad_data if data is not None]
        if broad_data:
            broad_data = pd.concat(broad_data)

            # 第一行
            camp_name = f'MANUAL-{station_name}-by-SP_Bulk'
            if site in ['CA', 'DE', 'FR', 'IT', 'SP', 'UK', 'US', 'ES']:
                daily_budge = 200
            else:
                daily_budge = int(200 / bid_exchange_rate)
            broad_data_first_row = pd.DataFrame(
                [[camp_name, daily_budge, None, None, 'Manual', None, None, None, None, None,
                  None, 'enabled', None, None, 'Dynamic bidding (down only)']], columns=export_columns)
            broad_data = pd.concat([broad_data_first_row, broad_data])
        else:
            broad_data = pd.DataFrame()

        # 2.计算关键词新增中的精准出价广告
        exact_data = [calc_one_sku(one_grouped_data, active_listing_data, init_acos, match_type='exact') for
                      sku_index, one_grouped_data in
                      grouped_data_keyword]
        exact_data = [data for data in exact_data if data is not None]
        if exact_data:
            exact_data = pd.concat(exact_data)

            # 第一行
            exact_camp_name = 'MANUAL-ST-EXACT-by-SP_Bulk'
            if site in ['CA', 'DE', 'FR', 'IT', 'SP', 'UK', 'US', 'ES']:
                daily_budge = 200
            else:
                daily_budge = int(200 / bid_exchange_rate)
            exact_data_first_row = pd.DataFrame(
                [[exact_camp_name, daily_budge, None, None, 'Manual', None, None, None, None, None,
                  None, 'enabled', None, None, 'Dynamic bidding (down only)']], columns=export_columns)
            exact_data = pd.concat([exact_data_first_row, exact_data])
        else:
            exact_data = pd.DataFrame()

        # 3.计算ASIN
        asin_data = [
            calc_one_sku(one_grouped_data, active_listing_data, init_acos, match_type='broad', new_add_type='asin') for
            sku_index, one_grouped_data in grouped_data_asin]
        asin_data = [data for data in asin_data if data is not None]
        if asin_data:
            asin_data = pd.concat(asin_data)
            # 计算ASIN的第一行
            asin_camp_name = 'Negative Targeting Expression-SP_Bulk'
            if site in ['CA', 'DE', 'FR', 'IT', 'SP', 'UK', 'US', 'ES']:
                daily_budge = 200
            else:
                daily_budge = int(200 / bid_exchange_rate)
            asin_data_first_row = pd.DataFrame(
                [[asin_camp_name, daily_budge, None, None, 'Manual', None, None, None, None, None,
                  None, 'enabled', None, None, 'Dynamic bidding (down only)']], columns=export_columns)
            asin_data = pd.concat([asin_data_first_row, asin_data])
            asin_data['Keyword'] = asin_data['Keyword'].fillna(value='')
            # # 修改ASIN新增中的Campaign Name,Keyword,Product Targeting ID和Match Type四列
            # 否定ASIN的Campaign Name是固定写法"Negative Targeting Expression-SP_Bulk"
            asin_campaign_name = "Negative Targeting Expression-SP_Bulk"
            asin_data['Campaign Name'] = asin_campaign_name
            asin_data['Keyword'] = [f'asin="{asin.upper()}"' if (pd.notnull(asin) & (asin not in ['', ' '])) else asin
                                    for
                                    asin in asin_data['Keyword']]
            asin_data['Product Targeting ID'] = asin_data['Keyword']
            asin_data['Match Type'] = ['Targeting Expression' if (pd.notnull(asin) & (asin not in ['', ' '])) else asin
                                       for
                                       asin in asin_data['Keyword']]
        else:
            asin_data = pd.DataFrame()
        # all_st_data = asin_data
        all_st_data = pd.concat([broad_data, exact_data, asin_data])
        if all_st_data.empty:
            # print(f'st 新增为空: {station_name}')
            return
        all_st_data = all_st_data[export_columns]

        all_st_data.drop_duplicates(inplace=True)

        return all_st_data

    def get_init_acos(station_name, camp_ori_data):
        """
        描述:
            通过广告报表中acos和销售额的表现,得到用于计算ST新增中的指定acos
        逻辑:
            1.acos>15% : init_acos = 15%
            2.acos<15%
                1. sales > 1000美元 : init_acos = acos - 1%
                2. sales < 1000美元
                    1. acos > 11% :init_acos = acos - 1%
                    1. acos < 11% :init_acos = 10%
        :param station_name:站点名
        :param camp_ori_data:站点的广告报表原始数据
        :return:指定的acos(init_acos)
        """
        # 1.判断st是否有效
        if (camp_ori_data is None) or (not isinstance(camp_ori_data, pd.DataFrame)) or (camp_ori_data.empty):
            print(f'camp data error: {station_name}')
            return
        # 2.判断列是否存在
        columns = camp_ori_data.columns
        need_columns = {'Spend', 'Sales'}
        if not need_columns.issubset(columns):
            print(f'lost columns:{station_name} 的camp表缺少 {need_columns - columns}')
            return
        site = station_name[-2:].upper()
        for column in ['Spend', 'Sales']:
            if camp_ori_data[column].dtype not in [np.float64, np.int64]:
                camp_ori_data[column] = camp_ori_data[column].apply(lambda x: currency_trans(x))
        # camp表中包含五个层级的数据，于是需要除以5
        station_spend = sum(camp_ori_data['Spend']) * exchange_rate[site] / 5
        station_sales = sum(camp_ori_data['Sales']) * exchange_rate[site] / 5
        station_acos = station_spend / station_sales

        # 逻辑
        if station_acos > 0.15:
            return 0.15
        elif station_sales > 1000:
            return station_acos - 0.01
        elif station_acos > 0.11:
            return station_acos - 0.01
        else:
            return 0.1

    # 1.判断st和camp数据的有效性
    result = detect_process_data_error(st_data, camp_data)
    if not result:
        return
    # 2.初始化st 和camp
    new_st_data = init_st_data(st_data)
    new_camp_data = get_one_to_one_camp_data(camp_data)
    # 3.st表和camp表通过 camp ad group来连接
    st_n_camp_data = pd.merge(new_st_data, new_camp_data[
        ['Campaign', 'Ad Group', 'Max Bid', 'Keyword or Product Targeting', 'Match Type', 'SKU',
         'Campaign Targeting Type']],
                              left_on=['Campaign Name', 'Ad Group Name'],
                              right_on=['Campaign', 'Ad Group'], how='right')
    st_n_camp_data = st_n_camp_data[pd.notnull(st_n_camp_data['Campaign Name'])]
    # 4.ST自动新增
    init_acos = get_init_acos(station_name, camp_data)
    upload_data = new_st_upload_format(station_name, st_n_camp_data, camp_data, active_listing_data, init_acos)

    # # 输出成excel
    #
    # processed_folder_name = 'processed_files'
    # file_save_folder = os.path.join(stations_folder, station_name, processed_folder_name)
    # if not os.path.exists(file_save_folder):
    #     os.makedirs(file_save_folder)
    # now_date = datetime.now().strftime('%y.%m.%d')
    # file_basename = f'{now_date} {station_name.upper()} 关键词新增和ASIN新增.xlsx'
    # file_save_path = os.path.join(file_save_folder, file_basename)
    # # print(file_save_path)
    # upload_data.drop_duplicates(inplace=True)
    # upload_data.reset_index(drop=True, inplace=True)
    # upload_data.to_excel(file_save_path, index=False, sheet_name='关键词新增和ASIN新增')
    if upload_data is not None:
        upload_data.drop_duplicates(inplace=True)
        upload_data.reset_index(drop=True, inplace=True)
    return upload_data


# 处理自动上传的新建祝函数
def process_new_add_upload_amazon(station_dir):
    """
    描述:
        将每日更新的站点数据生成新品自动新增（AO新增自动、Listing新增自动、ASIN定向、否定ASIN、ST提取手动(关键词新增)
        精确否定词）通过接口上传到亚马逊的后台。
    逻辑:
        首先是读取最新的站点数据，然后处理站点数据，生成新品自动新增的数据，然后在通过接口上传到亚马逊后台
    :param station_dir: 站点路径
    :return: 上传
    """

    # 1.获得campaign_data, active_data, search_data, order_data
    def get_station_all_files(station_dir: 'path') -> 'four_report_data':
        """
        读取站点的全部数据，生成五表的字典
        :param station_dir:
        :return:
        """
        site = os.path.basename(station_dir)[-2:].upper()
        account = os.path.basename(station_dir)[0:-3].upper()
        station_name = f'{account}_{site}'
        # 找到五个目标文件所需要的文件组
        gather_files_basename = gather_file(station_dir)
        # 将五个文件的的basename添加dirname
        gather_files_fulldir = {report_name: os.path.join(station_dir, report_last_file) for
                                report_name, report_last_file
                                in
                                gather_files_basename.items()}
        haved_files_dict = gather_files_fulldir.keys()

        need_report_key_name_set = ['campaign', 'active_listing', 'Search', 'allorders']
        # 判断文件是否齐全
        if not set(need_report_key_name_set).issubset(haved_files_dict):
            lost_report = set(need_report_key_name_set) - haved_files_dict
            print(f"LOST REPORT:{station_name} 缺失 {lost_report}报表")
            return None, None, None, None
        # 需要的四表
        [campaign_path, active_listing_path, search_path, order_path] = [gather_files_fulldir[report_key_name] for
                                                                         report_key_name in need_report_key_name_set]
        [active_data, search_data, order_data] = [read_files(path) for path in
                                                  [active_listing_path, search_path, order_path]]
        campaign_data = read_files(campaign_path, sheet_name='Sponsored Products Campaigns')

        # 解决active_listing中表头有中文的现象
        active_data = clean_active_listing(active_data)

        init_report(active_data)
        init_report(search_data)
        init_report(order_data)
        init_report(campaign_data)

        return campaign_data, active_data, search_data, order_data

    # 从四表中,得到站点新增的数据,分别为
    # 2.得到新增的五种类别(AO新增自动、Listing新增自动、ASIN定向、否定ASIN、ST提取手动(关键词新增)、精确否定词)的新增表
    def all_new_add_data(station_name, camp_data, active_listing_data, st_data, all_order_data):
        # 1.得到精否/否定asin
        negative_all_data = negative_exact(station_name, camp_data, st_data, active_listing_data)

        # 2.得到新品自动新增(AO新增和active listing新增)数据
        auto_new_data = process_auto_new(station_name, active_listing_data, all_order_data, camp_data)
        # 3.得到关键词新增和ASIN新增
        st_new_and_asin_data = process_st_new(station_name, st_data, camp_data, active_listing_data)
        # 全部的上传新增数据
        all_add_data = []
        for data in [negative_all_data, auto_new_data, st_new_and_asin_data]:
            if (data is None) or (data.empty):
                continue
            else:
                all_add_data.append(data)

        if not all_add_data:
            return
        else:
            all_add_data = pd.concat(all_add_data)
            all_add_data.reset_index(drop=True, inplace=True)

            return all_add_data

    def get_account_id(station_name, account_id_path=r'E:\ad_zyp\price_dist\账号对应ID.xlsx'):
        if not os.path.exists(account_id_path):
            print(f"account_id对应表不存在")
            return None
        account_id_data = pd.read_excel(account_id_path, sheet_name='yibai_amazon_account')
        account_id_data = account_id_data[['account_id', '广告站点']]
        account_id_data.dropna(inplace=True)
        station_name = station_name.lower()
        account = station_name[:-3]
        site = station_name[-2:]
        station_name = account + '-' + site
        account_id_data['广告站点'] = account_id_data['广告站点'].apply(lambda x: x.lower())
        account_id = account_id_data['account_id'][account_id_data['广告站点'] == station_name]
        if len(account_id) == 0:
            print(f'无法站到站点的id: 站点{station_name}无法寻找到对应的ID，情查看“{account_id_path}”后添加.')
            return None
        else:
            account_id = account_id.values[0]
            return int(account_id)

    # api上传新增日志
    # 按照站点更新数据库中的sku信息
    def db_upload_sku_info(upload_log: 'list', db='team_station', table_name='api_upload_create_campaign_log',
                           ip='127.0.0.1',
                           user_name='marmot',
                           password='marmot123', port=3306):
        """
        日志为包含6种信息的列表 ['account','station','site','upload_datetime','manager','data_len']
        将每个站点的sku的表现存储到服务器数据库表中
        :param upload_log:上传的日志 ['account','station','site','upload_datetime','manager','data_len']
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
        # 将数据变成可进行读入数据库的dict格式
        upload_log = tuple(upload_log)

        insert_sql = """insert into {} (station,account,site,upload_datetime,manager, \
        data_len) values (%s,%s,%s,%s,%s,%s)""".format(
            table_name)
        # 执行sql语句
        try:
            cursor.execute(insert_sql, upload_log)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(e)
        conn.close()
        cursor.close()

    # 定时刷新网页
    def refresh_url(url, time_interval=60, start_time=0, end_time=24,
                    chromedriver_path=r"C:\Python37\Scripts\chromedriver.exe"):
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        driver = webdriver.Chrome(executable_path=chromedriver_path, options=chrome_options)
        driver.get(url)
        i = 1
        flag = 0
        while 1:
            time.sleep(time_interval)
            try:
                driver.refresh()
                # print('fresh successful.')
                flag = 1
                break
            except:
                pass
            i += 1
            if i >= 4:
                break
        driver.close()
        if flag == 1:
            return True
        else:
            return False

    # 将新建或是更新的广告数据添加Record ID列、Record Type列、Portfolio ID列后上传到erp
    def format_to_erp(station_name, new_st_data, camp_data):
        """
        其他的新增表的格式都是与st新增差不多，这里取名new_st_data指的就是新增

        将st新增中的表中每一条记录添加campaign中对应的三列：Record ID/Record Type/Portfolio ID
        1.Record ID列:为每一列的ID（唯一）
            其中通过st新增表和camp表的campign name/ad group/sku/match type/Keyword列来匹配
            若存在，则可以得到Record ID,
            若不存在则Record ID为空
        2.Record Type列:为每一列的层级
            第一级:Campaign
            第二级:Ad Group
            第三级:Ad
            第四级:Keyword/Product Targeting
        3.Portfolio ID列:
            恒为空
        :param station_name:站点名
        :param new_st_data:st新增
        :param camp_data:广告报表
        :return:
        """
        if (not isinstance(new_st_data, pd.DataFrame)) or (new_st_data is None) or (new_st_data.empty):
            return
        if (not isinstance(camp_data, pd.DataFrame)) or (camp_data is None) or (camp_data.empty):
            return

        # 处理st新增报表，按照campaign-ad_group为唯一来创建组
        def build_new_st_group(new_st_data_ori: pd.DataFrame) -> pd.DataFrame:
            """
            对ST新增的词，按照campaign-ad_group为唯一来创建组，为erp上传的格式。
            1.删除每个广告组的首行
            2.按照'Campaign Name','Ad Group Name'进行分组
            3.添加每组的首行
            :param new_st_data_ori: 原始的dataframe表
            :return: 处理后的dataframe
            """
            # 判断st报表的有效性
            if (not isinstance(new_st_data_ori, pd.DataFrame)) or (new_st_data_ori is None) or (new_st_data_ori.empty):
                return
            # 1.删除每个广告组的首行
            st_columns = new_st_data_ori.columns
            new_st_data_ori.reset_index(drop=True, inplace=True)
            new_st_data_ori['Campaign Status'] = 'enabled'
            new_st_data_ori['Ad Group Status'] = 'enabled'
            # 得到广告大组的预算
            first_row_campaign_budget = new_st_data_ori.ix[0, 'Campaign Daily Budget']
            if pd.isnull(first_row_campaign_budget) or (first_row_campaign_budget == ''):
                site = station_name[-2:]
                campaign_budget = campaign_budget_dict[site.upper()]
            else:
                campaign_budget = first_row_campaign_budget
            if not isinstance(campaign_budget, (int, float)):
                campaign_budget = int(campaign_budget)
            # 判断'Product Targeting ID'是否存在
            if 'Product Targeting ID' not in st_columns:
                columns_list = list(new_st_data_ori.columns)
                columns_list.insert(10, 'Product Targeting ID')
                new_st_data_ori['Product Targeting ID'] = ''
                # 按照新的columns_list来排序
                new_st_data_ori = new_st_data_ori[columns_list]
                st_columns = new_st_data_ori.columns
            new_st_data_no_campaign = new_st_data_ori[pd.isnull(new_st_data_ori['Campaign Daily Budget'])]
            if new_st_data_no_campaign.empty:
                return
            # 2.按照Campaign Name和Ad Group Name来进行分组以及添加每个组的首行
            new_st_data_no_campaign_grouped = new_st_data_no_campaign.groupby(['Campaign Name', 'Ad Group Name'])
            new_st_data_list = []
            for campaign_n_ad_group, one_campaign_n_ad_group_data in new_st_data_no_campaign_grouped:
                campaign_name = one_campaign_n_ad_group_data['Campaign Name'].values[0]
                one_campaign_n_ad_group_data.fillna(value='', inplace=True)
                one_campaign_n_ad_group_data.drop_duplicates(inplace=True)
                campaign_first_row = pd.DataFrame(
                    [[campaign_name, campaign_budget, None, None, None, None, None, None, None, None, None,
                      'enabled', None, None, None]], columns=st_columns)

                # 精否没有ad group 需要手动添加ad group行 通过Max Bid列为空来识别是否拥有ad group行
                ad_group_row_sign = one_campaign_n_ad_group_data[pd.notnull(one_campaign_n_ad_group_data['Max Bid'])]
                if ad_group_row_sign.empty:
                    ad_group_name = one_campaign_n_ad_group_data['Ad Group Name'].values[0]
                    ad_grop_row = pd.DataFrame(
                        [[campaign_name, None, None, None, None, ad_group_name, None, None, None, None, None,
                          'enabled', 'enabled', None, None]], columns=st_columns)
                    campaign_first_row = pd.concat([campaign_first_row, ad_grop_row])

                one_campaign_n_ad_group_data_complete = pd.concat([campaign_first_row, one_campaign_n_ad_group_data])
                # Campaign Daily Budget来排序Campaig第一层级，Status来排序Ad group第二层级,SKU来排序Ad第三层级,Keyword来排序第四层级
                one_campaign_n_ad_group_data_complete['Status'].fillna(value='', inplace=True)
                one_campaign_n_ad_group_data_complete.sort_values(
                    by=['Campaign Daily Budget', 'Status', 'Keyword', 'SKU'],
                    ascending=[True, True, True, True], inplace=True)
                new_st_data_list.append(one_campaign_n_ad_group_data_complete)
            new_st_data = pd.concat(new_st_data_list)
            new_st_data.reset_index(inplace=True, drop=True)
            return new_st_data

        # 添加Record Type列
        def add_record_type(format_new_st):
            """
            根据特征为format_new_st 添加record_type列:
            1.当Campaign Daily Budget有值，则为第一层级 Campaign
            2.当Ad Group Status有值，但是Status为空,则为第二层级 Ad Group
            3.当SKU有值，则为第三层级 Ad
            4.Keyword有值，则为第四层级
                将Campaign Targeting Type列填充之后
                当Campaign Targeting Type中为Manual 则 Keyword
                当Campaign Targeting Type中为Auto 则 Product Targeting
            :param format_new_st:需要处理的ST新增的erp上传表
            :return:添加了record type列之后的上传表
            """
            if (not isinstance(format_new_st, pd.DataFrame)) or (format_new_st is None) or (format_new_st.empty):
                return
            format_new_st['Record Type'] = ''
            # 添加第一层级 Campaign
            format_new_st['Record Type'] = ['Campaign' if pd.notnull(budget) else record_type for budget, record_type in
                                            zip(format_new_st['Campaign Daily Budget'], format_new_st['Record Type'])]
            # 添加第二层级
            # Status列之前用''填充过空值
            format_new_st['Record Type'] = ['Ad Group' if pd.notnull(ad_status) & (status == '') else record_type for
                                            ad_status, status, record_type in
                                            zip(format_new_st['Ad Group Status'], format_new_st['Status'],
                                                format_new_st['Record Type'])]
            # 添加第三层级
            format_new_st['Record Type'] = ['Ad' if sku != '' else record_type for sku, record_type in
                                            zip(format_new_st['SKU'], format_new_st['Record Type'])]
            # 添加第四层级
            keyword_sign = ['broad', 'exact', 'negative exact']
            format_new_st['Record Type'] = [
                record_type if keyword == '' else 'Keyword' if match_type.lower() in keyword_sign else 'Product Targeting'
                for
                keyword, match_type, record_type in
                zip(format_new_st['Keyword'], format_new_st['Match Type'], format_new_st['Record Type'])]

            # 由于精否中存在ad group 的bid为空的情况，于是用广告表的Max Bid来充填
            ad_group_bid_null = format_new_st[
                pd.isnull(format_new_st['Max Bid_x']) & (format_new_st['Record Type'] == 'Ad Group')]
            if not ad_group_bid_null.empty:
                format_new_st['Max Bid_x'] = [
                    camp_max_bid if (record_type == 'Ad Group') & pd.isnull(st_max_bid) else st_max_bid for
                    st_max_bid, camp_max_bid, record_type in
                    zip(format_new_st['Max Bid_x'], format_new_st['Max Bid_y'], format_new_st['Record Type'])]

            format_new_st.rename(columns={'Max Bid_x': 'Max Bid'}, inplace=True)
            return format_new_st

        new_st_data = build_new_st_group(new_st_data)

        # 创建辅助列来合并
        new_st_data[['Campaign Name', 'Ad Group Name', 'SKU', 'Match Type', 'Keyword']] = new_st_data[
            ['Campaign Name', 'Ad Group Name', 'SKU', 'Match Type', 'Keyword']].fillna(value='')
        camp_data[['Campaign', 'Ad Group', 'SKU', 'Match Type', 'Keyword or Product Targeting']] = camp_data[
            ['Campaign', 'Ad Group', 'SKU', 'Match Type', 'Keyword or Product Targeting']].fillna(value='')
        new_st_data['aux'] = new_st_data['Campaign Name'] + new_st_data['Ad Group Name'] + new_st_data[
            'SKU'] + new_st_data['Match Type'] + new_st_data['Keyword']
        camp_data['aux'] = camp_data['Campaign'] + camp_data['Ad Group'] + camp_data['SKU'] + camp_data[
            'Match Type'] + camp_data['Keyword or Product Targeting']
        camp_match_data = camp_data[['Record ID', 'Portfolio ID', 'Max Bid', 'aux']]
        camp_match_data.drop_duplicates(inplace=True)

        format_new_st = pd.merge(new_st_data, camp_match_data,
                                 on='aux', how='left', sort=False)
        format_new_st.rename(columns={'Campaign Name': 'Campaign', 'Ad Group Name': 'Ad Group'}, inplace=True)

        # 添加record_type列
        format_new_st = add_record_type(format_new_st)

        # 添加bid+列
        format_new_st['Bid+'] = ['off' if (ad_type == 'Campaign') & (pd.isnull(record_id)) else '' for
                                 record_id, ad_type in
                                 zip(format_new_st['Record ID'], format_new_st['Record Type'])]
        # 填写广告组类型列('Campaign Targeting Type')
        # 先删掉原表中的'Campaign Targeting Type'列,然后再通过内敛，添加Campaign Targeting Type
        del format_new_st['Campaign Targeting Type']
        camp_data['Campaign Targeting Type'] = [camp_type if record_type.lower() == 'campaign' else '' for
                                                record_type, camp_type in
                                                zip(camp_data['Record Type'], camp_data['Campaign Targeting Type'])]
        camp_type = camp_data[['Record ID', 'Campaign Targeting Type']][
            (pd.notnull(camp_data['Campaign Targeting Type'])) & (camp_data['Campaign Targeting Type'] != '')]
        format_new_st = pd.merge(format_new_st, camp_type, on='Record ID', how='left')

        format_new_st = format_new_st[
            (format_new_st['Portfolio ID'] == '') | (pd.isnull(format_new_st['Portfolio ID']))]

        # 调整输出的列顺序
        format_new_st = format_new_st[
            ['Record ID', 'Record Type', 'Campaign', 'Campaign Daily Budget', 'Portfolio ID', 'Campaign Start Date',
             'Campaign End Date', 'Campaign Targeting Type', 'Ad Group', 'Max Bid', 'Keyword', 'Product Targeting ID',
             'Match Type', 'SKU', 'Campaign Status', 'Ad Group Status', 'Status', 'Bid+']]

        return format_new_st

    try:
        campaign_data, active_listing_data, search_data, order_data = get_station_all_files(station_dir)
    except Exception as e:
        print(f'READ FILE ERROR:{station_dir} 下有表有问题')
        print(e)
        return
    # 1. 验证四表的有效性
    error_files_flag = 0
    for report in [campaign_data, active_listing_data, search_data, order_data]:
        if (report is None) or (report.empty):
            print(f'READ FILE ERROR:{station_dir} 下{report}有问题')
            error_files_flag += 1
        if error_files_flag != 0:
            return
    # 2.得到新增的五种类别表
    site = os.path.basename(station_dir)[-2:].upper()
    account = os.path.basename(station_dir)[0:-3].upper()
    station_name = f'{account}_{site}'
    create_camp_folder = r"E:/AD_WEB/file_dir/create_camp"
    if not os.path.exists(create_camp_folder):
        os.makedirs(create_camp_folder)
    now_date = datetime.now().date().strftime('%Y-%m-%d')
    add_data_save_path = os.path.join(create_camp_folder, f'{station_name}_{now_date}_all_create_data.xlsx')
    all_campaign_add_data = all_new_add_data(station_name, campaign_data, active_listing_data, search_data, order_data)

    # 添加Record ID列、Record Type列、Portfolio ID列后上传到erp
    all_campaign_add_data = format_to_erp(station_name, all_campaign_add_data, campaign_data)

    # 3.存储为xlsx
    if all_campaign_add_data is None:
        print(f"无新增新品自动新增数据:{station_name}")
        return
    if all_campaign_add_data.empty:
        print(f"无新增新品自动新增数据:{station_name}")
        return
    all_campaign_add_data.to_excel(add_data_save_path, index=False)
    # 4.通过接口上传
    # 获得账号对应的ID
    account_id = get_account_id(station_name)
    if account_id is not None:
        # 上传新增
        requests_upload = AdApiFileUpload()
        upload_create_dict = {
            'account_id': account_id,
            'file_path': add_data_save_path
        }
        # erp上传到亚马逊上
        upload_time = requests_upload.requests_upload_create(**upload_create_dict)
        # 刷新页面上传
        erp_upload_refresh_url = "http://120.78.243.154/services/advertising/generatereport/generatereport"
        is_refresh = refresh_url(erp_upload_refresh_url, time_interval=120)
        # print(is_refresh)
        # upload_status = False
        if not is_refresh:
            raise ValueError(f'无法刷新页面:{erp_upload_refresh_url}')
        if not upload_time is None:
            # 将上传的参数写入到日志数据库中
            # 日志包括['account','station','site','upload_datetime','manager','data_len']
            # 数据表长度
            data_len = len(all_campaign_add_data)
            # 获得负责人信息
            station_info = db_download_station_names()
            manager_name = station_info['ad_manger'][station_info['station'] == station_name.lower()]
            if list(manager_name):
                manager_name = manager_name.values[0]
            else:
                manager_name = 'None'
            upload_log = [station_name.lower(), account.lower(), site.lower(), upload_time, manager_name, data_len]
            db_upload_sku_info(upload_log)
            print(f'新增上传完成:{station_name}')
            return
        # 将上传的信息上传到数据库中
        else:
            print(f'新增上传失败:{station_name}')


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


# 循环运行
def loop_station():
    processed_stations = dict()
    while 1:
        completed_report_stations_updatetime_info = public_function.get_station_updatetime(date_before=1)
        # 需要处理的站点时间集合
        need_process_station = {station: updatetime for station, updatetime in
                                completed_report_stations_updatetime_info.items() if
                                processed_stations.get(station, None) != updatetime}
        if not need_process_station:
            print("暂时没有站点数据更新，休息1分钟...")
            time.sleep(60)
            continue
        red_conn = public_function.Redis_Store()
        if len(need_process_station) > 0:
            # mark1 先不要删除站点存储的临时文件夹
            for station, _ in need_process_station.items():
                # 1. 处理概览
                try:
                    main_process_price_range_sku_perf(station)
                    print(f"{station} 完成概览...")
                except Exception as e:
                    print(e)
                    print(f"{station}处理概览失败。")
                # main_process_price_range_sku_perf(station)
                # 2.处理重复广告
                try:
                    red_keys = red_conn.keys()
                    files_key = 'FIVE_FILES_KEYS_SAVE'
                    station_camp_dir_key = \
                        [key for key in red_keys if (station in key) and (files_key in key) and ('CP' == key[-17:-15])][
                            0]
                    station_camp_dir = red_conn.get(station_camp_dir_key)
                    camp_ori_data = process_files.read_pickle_2_df(station_camp_dir)
                    get_n_db_upload_repeat_sku(camp_ori_data, station)
                    print(f"{station} 完成重复广告...")
                except Exception as e:
                    print(e)
                    print(f"{station} 重复广告处理失败。")
        processed_stations.update(need_process_station)
        red_conn.close()
        print("暂时没有站点更新，休息1分钟...")
        time.sleep(60)
        if datetime.now().hour == 23:
            processed_stations = dict()
            print('==================================')
            print("进入11点，开始休眠10个小时...")
            print('===================================')
            time.sleep(36000)


def active_total_columns(input_df):
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


def fulfillment_ondates_perf(ad_shop_data, sku_fulfillment_ondates,
                             on_date_range=[-1, 10, 30, 60, 120, 180, 360, 18888888]) -> list:
    """
    得到不同发货方式下的sku表现: 广告总花费、广告总销售、转化率、店铺总花费
    :param ad_shop_perf_data: 广告和店铺数据
    :param sku_fulfillment_ondates: sku的发货方式和上架时间
    :return: 不同发货方式和上架时间下的sku表现
    """
    # 得到每个广告的sku的发货方式
    ad_shop_data = pd.merge(ad_shop_data, sku_fulfillment_ondates, left_on='SKU', right_on='seller-sku', how='left')
    fulfillment_dist_pefr_data = ad_shop_data.groupby(['ship_type']).agg(
        {'Impressions': 'sum', 'Clicks': 'sum', 'Spend': 'sum', 'Orders': 'sum', 'Sales': 'sum', 'Units Ordered': 'sum',
         'Ordered Product Sales': 'sum'}).reset_index()
    fulfillment_dist_pefr_data['ACoS'] = [spend / sales if sales != 0 else 0 for spend, sales in
                                          zip(fulfillment_dist_pefr_data['Spend'], fulfillment_dist_pefr_data['Sales'])]
    fulfillment_dist_pefr_data['cpc'] = [spend / click if click != 0 else 0 for spend, click in
                                         zip(fulfillment_dist_pefr_data['Spend'], fulfillment_dist_pefr_data['Clicks'])]
    fulfillment_dist_pefr_data['cr'] = [order / click if click != 0 else 0 for order, click in
                                        zip(fulfillment_dist_pefr_data['Orders'], fulfillment_dist_pefr_data['Clicks'])]
    fulfillment_dist_pefr_data['prom_ratio'] = [spend / shop_sale if shop_sale != 0 else 0 for spend, shop_sale in
                                                zip(fulfillment_dist_pefr_data['Spend'],
                                                    fulfillment_dist_pefr_data['Ordered Product Sales'])]
    fulfillment_dist_pefr_data['sales_ratio'] = [ad_sale / shop_sale if shop_sale != 0 else 0 for ad_sale, shop_sale in
                                                 zip(fulfillment_dist_pefr_data['Sales'],
                                                     fulfillment_dist_pefr_data['Ordered Product Sales'])]

    ad_shop_data = ad_shop_data[ad_shop_data['on_dates'] >= 0]
    dates_dist_perf_data = ad_shop_data.groupby(pd.cut(ad_shop_data['on_dates'], on_date_range)).agg(
        {'Impressions': 'sum', 'Clicks': 'sum', 'Spend': 'sum', 'Orders': 'sum', 'Sales': 'sum', 'Units Ordered': 'sum',
         'Ordered Product Sales': 'sum'}).reset_index()
    # 对on_dates累计求和
    # for i in range(len(dates_dist_perf_data) - 1, 0, -1):
    #     temp_value = dates_dist_perf_data.iloc[:, 1:].iloc[:(i + 1), :].sum()
    #     dates_dist_perf_data.iloc[i, 1:] = temp_value
    dates_dist_perf_data['ACoS'] = [spend / sales if sales != 0 else 0 for spend, sales in
                                    zip(dates_dist_perf_data['Spend'], dates_dist_perf_data['Sales'])]
    dates_dist_perf_data['cpc'] = [spend / click if click != 0 else 0 for spend, click in
                                   zip(dates_dist_perf_data['Spend'], dates_dist_perf_data['Clicks'])]
    dates_dist_perf_data['cr'] = [order / click if click != 0 else 0 for order, click in
                                  zip(dates_dist_perf_data['Orders'], dates_dist_perf_data['Clicks'])]
    dates_dist_perf_data['prom_ratio'] = [spend / shop_sale if shop_sale != 0 else 0 for spend, shop_sale in
                                          zip(dates_dist_perf_data['Spend'],
                                              dates_dist_perf_data['Ordered Product Sales'])]
    dates_dist_perf_data['sales_ratio'] = [ad_sale / shop_sale if shop_sale != 0 else 0 for ad_sale, shop_sale in
                                           zip(dates_dist_perf_data['Sales'],
                                               dates_dist_perf_data['Ordered Product Sales'])]
    dates_dist_perf_data['on_dates'] = dates_dist_perf_data['on_dates'].apply(
        lambda x: str(x.left).replace(f'{on_date_range[0]}', '0') + '-' + str(x.right).replace(f'{on_date_range[-1]}',
                                                                                               ''))

    return [fulfillment_dist_pefr_data, dates_dist_perf_data]


def main_process_price_range_sku_perf(station):
    """
    主函数，统计sku随客单价变化以及sku的表现
    :param station:站点名
    :return:None
    """
    global site
    site = station[-2:].upper()
    request_camp = station[0:-3].upper()
    # 找到五个目标文件所需要的文件组

    # sku客单价的分布
    # 区间是左开右闭 ( ]
    price_range = [-0.0001, 10, 30, 50, 100, 200, 300, 400, 500, 1000, 100000000]
    # 连接redis
    red_conn = public_function.Redis_Store()
    red_keys = red_conn.keys()
    files_key = 'FIVE_FILES_KEYS_SAVE'
    station_key = [key for key in red_keys if (station in key) and (files_key in key)]
    # 生成全部表的路径字典
    file_dir_key = {'camp': 'CP', 'st': 'ST', 'ac': 'AC', 'ao': 'AO', 'br': 'BR'}
    file_dir = dict()
    for file_name, file_key in file_dir_key.items():
        file_dir[file_name] = red_conn.get([key for key in station_key if file_key == key[-17:-15]][0])
    #  t 读取广告报表数据
    # spend和sales已经转换成美元

    campaign_data = read_one_campaign(file_dir['camp'],
                                      need_columns=['SKU', 'Impressions', 'Clicks', 'Spend', 'Orders',
                                                    'Total Units', 'Sales'])
    [camp_sku_all_set, camp_sku_enable_set] = get_camp_sku_list(file_dir['camp'])

    # 得到广告报表sku的集合
    # 得到sku的发货方式/上架时间 以及区间分布

    sku_fulfillment_opendate_price_dist_data = sku_fulfillment_opendate_price_dist(
        file_dir['ac'],
        need_columns=['seller-sku', 'price', 'open-date', 'fulfillment-channel'],
        price_range=price_range)
    # t1.sku的价格区间分布数据
    sku_dist = sku_fulfillment_opendate_price_dist_data[1]
    # t2.sku的上架时间和发货方式
    sku_fulfillment_opendate = sku_fulfillment_opendate_price_dist_data[0]

    # t3 每个sku的客单价
    sku_price = sku_fulfillment_opendate_price_dist_data[2]

    # r1 sku的不同价格的出单情况

    sku_order_dist = sku_order_price_dist(file_dir['ao'], sku_dist, price_range,
                                          need_columns=['sku', 'quantity'])
    # 输出的第一个文件
    export_file1 = sku_order_dist.copy()

    # 插入三列
    export_file1['camp_sku总数'] = [len(set(active_sku_list) & camp_sku_all_set) for active_sku_list in
                                  export_file1['seller-sku']]
    export_file1.ix[-1, 'camp_sku总数'] = sum(export_file1['camp_sku总数'])
    export_file1['camp_sku未打广告总数'] = export_file1['sku总数'] - export_file1['camp_sku总数']
    export_file1['camp_sku未打广告总数'] = [int(i) for i in export_file1['camp_sku未打广告总数']]
    export_file1['camp_开启sku总数'] = [len(set(active_sku_list) & camp_sku_enable_set) for active_sku_list in
                                    export_file1['seller-sku']]
    export_file1.ix[-1, 'camp_开启sku总数'] = sum(export_file1['camp_开启sku总数'])
    export_file1['camp_关闭sku总数'] = export_file1['camp_sku总数'] - export_file1['camp_开启sku总数']
    export_file1['camp_sku(未打广告总数/开启/关闭)'] = [str(i) + "/" + str(j) + "/" + str(k) for i, j, k in
                                              zip(export_file1['camp_sku未打广告总数'], export_file1['camp_开启sku总数'],
                                                  export_file1['camp_关闭sku总数'])]
    del export_file1['seller-sku']
    del export_file1['have_order_sku']

    # 广告情况
    # A 不同客单价下 广告总花费,广告销售额,转化率,店铺花费,店铺销售额,转化率
    # 首先计算每个sku的广告总花费,广告销售额,有重复转化率,店铺花费,店铺销售额,转化率，
    # 然后再按照客单价进行分组
    # t 读取业务br数据
    # sales已经转换成美元
    br_data = get_business_info(file_dir['br'],
                                need_columns=['SKU', 'Units Ordered', 'Ordered Product Sales'])

    # 将br表和广告报表按照sku合并
    sku_ad_shop = pd.merge(campaign_data, br_data, how='left', on='SKU')
    sku_ad_shop.fillna(0, inplace=True)

    # 不同sku下的表现
    ordered_sku = []
    [ordered_sku.extend(list(sku)) for sku in sku_order_dist['have_order_sku']]
    ordered_sku = set(ordered_sku)
    all_sku = set(campaign_data['SKU'])
    orderedsku_or_not_perf = different_sku_perf(ordered_sku, all_sku, sku_ad_shop)
    format_num(orderedsku_or_not_perf, trans_columns=list(orderedsku_or_not_perf.columns)[1:8])

    # 3.1 计算客单价下的一些指标: 广告总花费、广告总销售、转化率、店铺总花费
    price_range_perf_data = price_range_perf(sku_ad_shop, sku_price, price_range=price_range)
    format_num(price_range_perf_data, trans_columns=list(price_range_perf_data.columns)[1:8])

    # 3.2 计算不同发货方式FBA/FBM的指标: 广告总花费、广告总销售、转化率、店铺总花费
    fulfillment_ondates_perf_data = fulfillment_ondates_perf(sku_ad_shop, sku_fulfillment_opendate,
                                                             on_date_range=[-1, 10, 30, 60, 120, 180, 360, 1888888])
    fulfillment_dist_perf_data = fulfillment_ondates_perf_data[0]
    dates_dist_perf_data = fulfillment_ondates_perf_data[1]
    format_num(fulfillment_dist_perf_data, trans_columns=list(price_range_perf_data.columns)[1:8])
    format_num(dates_dist_perf_data, trans_columns=list(price_range_perf_data.columns)[1:8])

    # 为输出的客单价/发货方式/以及发货方进行改名
    for df in [price_range_perf_data, fulfillment_dist_perf_data, dates_dist_perf_data]:
        df.rename(columns={'Units Ordered': '店铺订单', 'Ordered Product Sales': '店铺销售额', 'prom_ratio': '推广占比',
                           'sales_ratio': '广销比'}, inplace=True)

    # 1. 输出sku_order表现
    export_file1[['sku_order_rate', 'price_order_rate']] = export_file1[
        ['sku_order_rate', 'price_order_rate']].applymap(
        lambda x: str(round(x * 100, 1)) + '%')
    export_file1['price_interval'] = export_file1['price_interval'].apply(lambda x: x.replace('.0', ''))
    # export_file1.ix[0:1, 'have_order_sku'] = ''
    export_file1['have_order_sku_limit'] = ['' if order_sku == set() else order_sku for order_sku in
                                            export_file1['have_order_sku_limit']]

    # # sku_order_rate/price_order_rate 不保留小数
    # export_file1[['sku_order_rate', 'price_order_rate']] = export_file1[
    #     ['sku_order_rate', 'price_order_rate']].applymap(
    #     lambda x: re.sub('\..', '', x) if '%' in x else x)
    export_file1.rename(
        columns={'sku总数': 'sku总数(ac表)', 'price_interval': '价格区间', 'have_order_sku_limit': '出单sku',
                 'order_sku_quantity': '出单sku总单量',
                 'order_sku_num': '出单sku个数',
                 'sku_order_rate': 'sku_order_rate(该客单价区间出单sku总数/店铺总sku总数)',
                 'price_order_rate': 'order_rate(该客单价区间总出单量/店铺总出单量)'}, inplace=True)
    export_file1 = export_file1[
        ['价格区间', 'sku总数(ac表)', 'camp_sku(未打广告总数/开启/关闭)', '出单sku个数', '出单sku总单量', '出单SKU销售额',
         'sku_order_rate(该客单价区间出单sku总数/店铺总sku总数)',
         'order_rate(该客单价区间总出单量/店铺总出单量)', 'sales_rate', '出单sku']]
    export_file1[['sku总数(ac表)', '出单sku个数', '出单sku总单量']] = export_file1[['sku总数(ac表)', '出单sku个数', '出单sku总单量']].applymap(
        lambda x: int(x))

    # 2.客单价/发货方式/上架时间
    # 修改客单价和上架时间
    price_range_perf_data['price_range'] = price_range_perf_data['price_range'].apply(
        lambda x: x.replace('.0', '').replace('100000000', '').replace('-0001', '0').replace('1000-', 'sku客单价无/1000+'))
    # dates_dist_perf_data['on_dates'] = dates_dist_perf_data['on_dates'].apply(
    #     lambda x: x.replace('0-1888888', '所有汇总'))

    # 规范输出的数据: 价格区间汇总表、发货方式汇总表、发货方式汇总表、是否有订单汇总
    export_file2 = standard_export_file(price_range_perf_data, fulfillment_dist_perf_data,
                                        dates_dist_perf_data, orderedsku_or_not_perf)
    # 修改第一列格式
    first_row = ['汇总']
    camp_add = campaign_data[['Impressions', 'Clicks', 'Spend', 'Orders', 'Sales']].sum()
    camp_add = camp_add.astype('int')
    br_add = br_data[['Units Ordered', 'Ordered Product Sales']].sum()
    br_add = br_add.astype('int')
    first_row_acos = str(round(camp_add['Spend'] / camp_add['Sales'] * 100, 1)) + '%'
    first_row_cpc = round(camp_add['Spend'] / camp_add['Clicks'], 2)
    first_row_cr = str(round(camp_add['Orders'] / camp_add['Clicks'] * 100, 1)) + '%'
    prom_rate = str(round(camp_add['Spend'] / br_add['Ordered Product Sales'] * 100, 1)) + '%'
    sales_rate = str(round(camp_add['Sales'] / br_add['Ordered Product Sales'] * 100, 1)) + '%'
    first_row.extend(camp_add)
    first_row.extend(br_add)
    first_row.extend([first_row_acos, first_row_cpc, first_row_cr, prom_rate, sales_rate])
    export_file2.iloc[0, :] = first_row

    # 推广占比与店铺转换率增加两列
    # 花费占店铺总销售额的推广占比
    camp_whole_sales = export_file2.ix[0, '店铺销售额']
    spend_ratio2 = [str(round(spend * 100 / camp_whole_sales, 1)) + "%" if spend != "" else "" for spend in
                    export_file2['Spend']]
    export_file2['推广占比'] = [rate1 + "/" + rate2 if rate2 != "" else "" for rate1, rate2 in
                            zip(export_file2['推广占比'], spend_ratio2)]
    # 销售占店铺总销售额的推广占比
    sale_ratio2 = [str(round(sale * 100 / camp_whole_sales, 1)) + "%" if sale != "" else "" for sale in
                   export_file2['Sales']]
    export_file2['广销比'] = [rate1 + "/" + rate2 if rate2 != "" else "" for rate1, rate2 in
                           zip(export_file2['广销比'], sale_ratio2)]

    # 为了让输出的数字转换成html是科学计数法，将数字转换成字符串
    export_file2[['Impressions', 'Clicks', 'Spend', 'Orders', 'Sales', '店铺订单', '店铺销售额']] = export_file2[
        ['Impressions', 'Clicks', 'Spend', 'Orders', 'Sales', '店铺订单', '店铺销售额']].applymap(
        lambda x: str(x).replace(".0", '') if not isinstance(x, str) else x)

    # 调整输出的列顺序
    export_file2 = export_file2[['分类汇总项', 'Impressions', 'Clicks', 'Spend', 'Orders', 'Sales', '店铺订单',
                                 '店铺销售额', 'cpc', 'cr', 'ACoS', '推广占比', '广销比']]

    sheet_names = ['sku随客单价分布', 'sku汇总表现']
    export_data = [export_file1, export_file2]
    # # 将2个表分成2个表格
    # writer = pd.ExcelWriter(export_dir)
    # for sheet, file in zip(sheet_names, export_data):
    #     file.to_excel(writer, sheet_name=sheet, index=False)
    #     writer.save()
    # writer.close()
    # print('{}写入成功...'.format(os.path.basename(export_dir)))

    # 获取账号站点
    shop_station = os.path.basename(station)
    shop_station = shop_station.replace('-', '_').upper()
    for sheet, file in zip(sheet_names, export_data):
        file_out = ad_perf + '/' + shop_station + '_' + sheet + '.html'
        file.to_html(file_out, index=False)


if __name__ == '__main__':
    loop_station()
