import pandas as pd
import os
import numpy as np
import datetime
import pymysql
import langid
import time
import math
import shutil
import sys
import warnings
import zipfile
import json

sys.path.append('E:/ad_zyp/price_dist/my_toolkit')
from read_campaign import read_campaign
from init_campaign import init_campaign

'''
1.得到同erpsku下其他sellersku出单关键词
2.将所有出单关键词信息按照三类存储到数据库中：基本信息表，所有出单关键词表，同语言出单关键词表。
3.分别将：所有出单关键词表，同语言出单关键词表这两个写入到原始的st表中
4.将同语言出单关键词表整理成上传st表的格式，并按照对应的站点的对接人分类输出。
'''
warnings.filterwarnings("ignore")
st_folder_dir = "E:/ad_zyp/erpsku_sellersku_kws/static_files/st_info"
station_zipped_folder = 'E:/ad_zyp/erpsku_sellersku_kws/static_files/temp'
acos_click_adjust_dir = r'E:\ad_zyp\erpsku_sellersku_kws\static_files\按acos click两次浮动.xlsx'


# 解压文件包
def unzip_dir(zip_dir):
    z = zipfile.ZipFile(zip_dir, "r")
    # 打印zip文件中的文件列表
    file_name_list = z.namelist()
    writer_folder = os.path.join(station_zipped_folder, os.path.basename(zip_dir)[:-4])
    if os.path.exists(writer_folder):
        shutil.rmtree(writer_folder)
    os.mkdir(writer_folder)
    file_name_list = [file for file in file_name_list if (file.find('/') == -1) & ('bulksheet' in file)]
    for filename in file_name_list:
        content = z.read(filename)
        with open(writer_folder + '/' + filename, 'wb') as f:
            f.write(content)


# 连接数据库 import pymysql
def conn_gross_require_load_files():
    conn = pymysql.connect(
        host='47.106.127.183',
        user='mrp_read',
        password='mrpread',
        database='mrp_py',
        port=3306,
        charset='UTF8')

    # 创建游标
    cursor = conn.cursor()
    # 写sql
    sql = """SELECT 标识,sale_30,erp_sku FROM gross_require"""
    # 执行sql语句
    cursor.execute(sql)
    all_result = cursor.fetchall()
    all_result = pd.DataFrame([list(j) for j in all_result], columns=['标识', 'sale_30', 'erp_sku'])
    all_result['标识'] = all_result['标识'].astype('str')
    all_result['sale_30'] = all_result['sale_30'].astype('int')
    # 筛选出30天内有订单的erpsku
    all_result = all_result[all_result['sale_30'] > 0]
    all_result = all_result[all_result['标识'].notnull()]
    # 从标识中提取sellersku
    all_result['seller_sku'] = all_result['标识'].apply(lambda x: x.split('$')[1])
    all_result['asin'] = all_result['标识'].apply(lambda x: x.split('@')[-2][-10:])
    all_st_erp_sku_info = all_result[['seller_sku', 'asin', 'erp_sku', 'sale_30']]
    conn.commit()
    cursor.close()
    conn.close()
    return all_st_erp_sku_info


# 将erp_info,按照erp_sku分组
def groupd_erp_sku(erp_sku_data):
    grouped_erp_sku_dict = erp_sku_data.groupby(['erp_sku'])['seller_sku'].apply(lambda x: ','.join(x))
    return grouped_erp_sku_dict


# 将st报表按照account_site,sku分组
def grouped_st_sku(seller_st_data):
    grouped_st_info = seller_st_data.groupby(['account_site', 'SKU'])['Customer Search Term'].apply(
        lambda x: ','.join(x))
    return grouped_st_info


# 得到站点与文件之间的字典
def get_filename_account_dict(filename):
    account_site = [file.split(' ')[1] for file in filename]
    dict_filename_account = dict(zip(account_site, filename))
    return dict_filename_account


# 将erp上sku与kw表的sku按照sku合并，并按照sku与匹配方式分组，另外得到与按照站点得到其他信息。
def add_erpsku_restkws_cloumns(all_seller_kws_data, rest_kws_data):
    # 取部分其他出单关键词列，此account_site是要计算的站点
    rest_kws_data = rest_kws_data[['seller_sku', 'erp_sku', 'account_site', 'rest_kw']]
    rest_kws_data.drop_duplicates(inplace=True)
    # 此时用erp和关键词进行连接
    # 得到所有关键词的erp_sku
    all_seller_kws_data = pd.merge(all_seller_kws_data, rest_kws_data[['seller_sku', 'erp_sku']], left_on='SKU',
                                   right_on='seller_sku')
    all_seller_kws_data.drop_duplicates(inplace=True)
    all_info1 = pd.merge(rest_kws_data, all_seller_kws_data, left_on=['erp_sku', 'rest_kw'],
                         right_on=['erp_sku', 'Customer Search Term'])
    all_info1.drop_duplicates(inplace=True)
    all_grouped_sku_info = all_info1.groupby(
        ['seller_sku_x', 'rest_kw', 'account_site_x', 'Match Type', 'erp_sku']).agg(
        {'account_site_y': '/'.join, '展示次数': 'sum', \
         '点击量': 'sum', '花费': 'sum', '销售额': 'sum', '订单量': 'sum'})
    # 计算得到ctr=click/impression cpc=spend/click acos=spend/sale cr=order/click
    all_grouped_sku_info['CTR'] = all_grouped_sku_info['点击量'] / all_grouped_sku_info['展示次数']
    all_grouped_sku_info['CPC'] = all_grouped_sku_info['花费'] / all_grouped_sku_info['点击量']
    all_grouped_sku_info['ACoS'] = all_grouped_sku_info['花费'] / all_grouped_sku_info['销售额']
    all_grouped_sku_info['CR'] = all_grouped_sku_info['订单量'] / all_grouped_sku_info['点击量']
    all_grouped_sku_info = all_grouped_sku_info.replace([np.inf, np.nan], 0)
    all_grouped_sku_info[['CTR', 'ACoS', 'CR']] = all_grouped_sku_info[['CTR', 'ACoS', 'CR']].applymap(
        lambda x: str(round(x * 100, 1)) + "%")
    all_grouped_sku_info['CPC'] = all_grouped_sku_info['CPC'].apply(lambda x: round(x, 6))
    all_grouped_sku_info['seller_sku'] = [i[0] for i in all_grouped_sku_info.index]
    all_grouped_sku_info['rest_kw'] = [i[1] for i in all_grouped_sku_info.index]
    all_grouped_sku_info['account_site_x'] = [i[2] for i in all_grouped_sku_info.index]
    all_grouped_sku_info['Match Type'] = [i[3] for i in all_grouped_sku_info.index]
    all_grouped_sku_info['erp_sku'] = [i[4] for i in all_grouped_sku_info.index]
    all_grouped_sku_info.reset_index(drop=True, inplace=True)
    all_grouped_sku_info.rename(
        columns={'account_site_y': '关键词来源站点', 'account_site_x': 'account_site', 'Match Type': 'Match_Type'},
        inplace=True)
    all_grouped_sku_info.drop_duplicates(inplace=True)
    return all_grouped_sku_info


# 将erp_sku下其他seller_sku出单的关键词输入到st表中
def export_to_st_file(rest_kws_info, st_folder, sheetname):
    st_files_name = os.listdir(st_folder)
    file_accontsite_dict = get_filename_account_dict(st_files_name)
    # all_info2 = all_info2.explode('account_site_x')
    account_site_list = set(rest_kws_info['account_site'])
    for account_site in account_site_list:
        print('正在写入出单的关键词表： {}'.format(account_site))
        one_account_site_info = rest_kws_info[rest_kws_info['account_site'] == account_site]
        one_account_site_info['关键词来源国家'] = one_account_site_info['关键词来源站点'].apply(lambda x: x[-2:])
        one_account_site_info = one_account_site_info[
            ['seller_sku', 'rest_kw', '关键词来源国家', 'Match_Type', '展示次数', '点击量', '花费', '销售额', '订单量',
             'CTR', 'CPC', 'ACoS', 'CR']]
        count_seller_sku = one_account_site_info['seller_sku'].value_counts()
        one_account_site_info = pd.merge(one_account_site_info, count_seller_sku, left_on='seller_sku',
                                         right_index=True)
        one_account_site_info.sort_values(['seller_sku_y', 'seller_sku', '订单量'], ascending=[False, True, False],
                                          inplace=True)
        one_account_site_info.drop(['seller_sku_x', 'seller_sku_y'], axis=1, inplace=True)
        write_dir = os.path.join(st_folder, file_accontsite_dict[account_site])
        try:
            writer = pd.ExcelWriter(write_dir, engine="openpyxl", mode='a')
            one_account_site_info.to_excel(writer, sheetname, index=False)
            writer.save()
        except:
            print('{}有问题'.format(account_site))


def get_lang(words):
    langid.set_languages(['it', 'en', 'de', 'fr', 'es', 'ja'])
    array = langid.classify(words)
    lang = array[0]
    return lang


# 过滤点其他小语种国家的出单关键词
def filter_another_lang(all_erpskw_data):
    # 每个国家对应的语言，欧洲四国是本国语言+英语；加拿大、英国、美国是英语；墨西哥西班牙语和英语；日本是只有日语。
    country_lang = {'IT': ['it', 'en'], 'DE': ['de', 'en'], 'FR': ['fr', 'en'], 'ES': ['es', 'en'], 'CA': ['en'],
                    'UK': ['en'], 'US': ['en'], 'MX': ['es', 'en'], 'JP': ['ja'], 'AU': ['es']}
    all_erpskw_data['lang'] = all_erpskw_data['rest_kw'].apply(lambda x: get_lang(x))
    all_erpskw_data['source_site'] = all_erpskw_data['关键词来源站点'].apply(lambda x: x[-2:])
    all_erpskw_data['site'] = all_erpskw_data['account_site'].apply(lambda x: x[-2:].upper())
    # all_erpskw_data[['site', 'lang']].to_excel(r"C:\Users\Administrator\Desktop\site_lang.xlsx", index=False)
    another_site = set(all_erpskw_data['site']) - set(country_lang.keys())
    # 是否所有站点都在...
    if list(another_site):
        print('{}不存在...'.format(another_site))
    all_erpskw_trans_lang_data = pd.DataFrame(
        columns=['account_site', 'seller_sku', 'erp_sku', 'asin', 'rest_kw', 'Match_Type', '关键词来源站点', '展示次数', '点击量',
                 '花费',
                 '销售额', '订单量', \
                 'CTR', 'CPC', 'ACoS', 'CR', 'lang', 'site', 'source_site'])
    for key, values in country_lang.items():
        one_country_data = all_erpskw_data[
            (all_erpskw_data['site'] == key) & (all_erpskw_data['lang'].isin(values))]
        all_erpskw_trans_lang_data = pd.concat([all_erpskw_trans_lang_data, one_country_data], sort='False')
    all_erpskw_trans_lang_data = all_erpskw_trans_lang_data[
        ['account_site', 'seller_sku', 'erp_sku', 'asin', 'rest_kw', 'Match_Type', '关键词来源站点', '展示次数', '点击量', '花费',
         '销售额', '订单量', \
         'CTR', 'CPC', 'ACoS', 'CR', 'lang', 'source_site']]
    return all_erpskw_trans_lang_data


# 将添加其他信息后的其他erp_sku出单并过滤点其他小语种国家的出单的关键词导入到数据库中
def filtered_restkws_to_sql(addcolumns_erpsku_restkws_filtered_data):
    # df = df.astype(object).where((pd.notnull(df)), None)
    add_columns_erp_rest_kws_data = addcolumns_erpsku_restkws_filtered_data[
        ['account_site', 'seller_sku', 'erp_sku', 'asin', 'rest_kw', 'Match_Type', '关键词来源站点', '展示次数', '点击量', '花费',
         '销售额', '订单量', \
         'CTR', 'CPC', 'ACoS', 'CR', 'lang', 'source_site']]
    df = add_columns_erp_rest_kws_data.astype(object).replace(np.nan, 'None')
    df.drop_duplicates(inplace=True)
    df.sort_values(by=['account_site', 'erp_sku', 'seller_sku', '订单量', 'Match_Type'], inplace=True)
    df = np.array(df)
    # 创建连接
    conn = conn_mysql()
    all_list = []
    len_df = df.shape[0]
    for i in range(len_df):
        temp_tuple = df[i]
        a_emp_tuple = tuple(temp_tuple)
        all_list.append(a_emp_tuple)
    # df.to_sql()
    # 写入到数据库中
    # 创建游标
    cursor = conn.cursor()
    sql1 = """DELETE FROM erpsku_restkws_add_columns_filter_langs"""
    sql = """insert IGNORE into erpsku_restkws_add_columns_filter_langs (account_site, seller_sku, erp_sku, asin,rest_kw,Match_Type,关键词来源站点,展示次数,点击量,花费,销售额,订单量,\
                                                               CTR,CPC,ACoS,CR,lang,source_site) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    try:
        cursor.execute(sql1)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(e)
    try:
        cursor.executemany(sql, all_list)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(e)
    cursor.close()
    conn.close()


# 导入部分的st报表
def load_st_info(folder, sheetname='出单优质搜索词'):
    part_st_files_name = os.listdir(folder)
    all_st_info = pd.DataFrame()
    for file_name in part_st_files_name:
        account_site = file_name.split(" ")[1]
        one_file_dir = os.path.join(folder, file_name)
        try:
            one_file_info = pd.ExcelFile(one_file_dir)
        except:
            continue
        if sheetname not in one_file_info.sheet_names:
            print('{}没有{}这个表'.format(file_name, sheetname))
            continue
        else:
            one_file_info = one_file_info.parse(sheet_name=sheetname)
            if one_file_info.empty:
                continue
            else:
                one_file_info['account_site'] = account_site
                all_st_info = pd.concat([all_st_info, one_file_info])
    int_columns = ['展示次数', '点击量', '订单量']
    # float_precent_columns = ['CTR', 'ACoS', 'CR']
    float_columns = ['花费', '销售额', 'CPC']
    all_st_info[int_columns] = all_st_info[int_columns].astype('int')
    all_st_info[float_columns] = all_st_info[float_columns].astype('float')
    all_st_info.reset_index(inplace=True, drop=True)
    return all_st_info


# 得到同一erp_sku下其他seller_sku的关键词
def merge_erp_sku_seller_sku(erp_sku_info, seller_sku):
    erp_seller_info = pd.merge(erp_sku_info, seller_sku, on='seller_sku',
                               how='left')
    erp_seller_info = erp_seller_info[erp_seller_info['account_site'].notnull()]
    erp_seller_info['Customer Search Term'] = erp_seller_info['Customer Search Term'].astype('str')
    # erp_sku_kw = erp_seller_info.groupby(['erp_sku'])['Customer Search Term'].apply(lambda x: ','.join(x))
    erp_sku_kw = erp_seller_info.groupby(['erp_sku']).agg({'Customer Search Term': ','.join, 'account_site': 'unique'})
    erp_sku_kw['erp_sku'] = [i for i in erp_sku_kw.index]
    erp_sku_kw.reset_index(drop=True, inplace=True)
    # erp_sku_kw.rename(columns={'erp_sku2':"erp_sku"},inplace=True)
    all_info = pd.merge(erp_seller_info, erp_sku_kw, on='erp_sku', how='inner')
    all_info['Customer Search Term_y'] = all_info['Customer Search Term_y'].apply(lambda x: x.split(','))
    all_info['Customer Search Term_x'] = all_info['Customer Search Term_x'].apply(lambda x: x.split(','))
    all_info['rest_kw'] = [list(set(row['Customer Search Term_y']) - set(row['Customer Search Term_x'])) for _, row
                           in
                           all_info.iterrows()]
    all_erp_rest_kw_info = all_info.explode('rest_kw')
    all_erp_rest_kw_info = all_erp_rest_kw_info[all_erp_rest_kw_info['rest_kw'].notnull()]
    # all_erp_rest_kw_info = all_erp_rest_kw_info.drop_duplicates()
    all_erp_rest_kw_info.rename(columns={'account_site_x': 'account_site'}, inplace=True)
    all_erp_rest_kw_info = all_erp_rest_kw_info[['erp_sku', 'seller_sku', 'account_site', 'rest_kw']]
    return all_erp_rest_kw_info


# 连接我的数据库
def conn_mysql(process_database='server_camp_report'):
    conn = pymysql.connect(
        host='192.168.129.240',
        user='marmot',
        password='marmot123',
        database=process_database,
        port=3306,
        charset='UTF8')
    return conn


# 将其他erp_sku出单的关键词导入到数据库中
def load_erp_rest_kws_to_sql(erp_rest_kws_data):
    # df = df.astype(object).where((pd.notnull(df)), None)
    erp_rest_kws_data = erp_rest_kws_data[['account_site', 'seller_sku', 'erp_sku', 'rest_kw']]
    df = erp_rest_kws_data.astype(object).replace(np.nan, 'None')
    df.drop_duplicates(inplace=True)
    df.sort_values(by=['account_site', 'erp_sku', 'seller_sku'], inplace=True)
    df = np.array(df)
    # 创建连接
    conn = conn_mysql()
    all_list = []
    len_df = df.shape[0]
    for i in range(len_df):
        temp_tuple = df[i]
        a_emp_tuple = tuple(temp_tuple)
        all_list.append(a_emp_tuple)
    # df.to_sql()
    # 写入到数据库中
    # 创建游标
    cursor = conn.cursor()
    sql1 = """DELETE FROM erpsku_restkws_basecolumns"""
    try:
        cursor.execute(sql1)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(e)
    sql = """insert ignore into erpsku_restkws_basecolumns (account_site,seller_sku,erp_sku,rest_kw) values (%s,%s,%s,%s)"""
    try:
        cursor.executemany(sql, all_list)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(e)
    cursor.close()
    conn.close()


# 将添加其他信息后的其他erp_sku出单的关键词导入到数据库中
def load_add_columns_erp_rest_kws_to_sql(add_columns_erp_rest_kws_data):
    # df = df.astype(object).where((pd.notnull(df)), None)
    add_columns_erp_rest_kws_data = add_columns_erp_rest_kws_data[
        ['account_site', 'seller_sku', 'erp_sku', 'asin', 'rest_kw', 'Match_Type', '关键词来源站点', '展示次数', '点击量', '花费',
         '销售额', '订单量', \
         'CTR', 'CPC', 'ACoS', 'CR']]
    df = add_columns_erp_rest_kws_data.astype(object).replace(np.nan, 'None')
    df.drop_duplicates(inplace=True)
    df.sort_values(by=['account_site', 'erp_sku', 'seller_sku', '订单量', 'Match_Type'], inplace=True)
    df = np.array(df)
    # 创建连接
    conn = conn_mysql()
    all_list = []
    len_df = df.shape[0]
    for i in range(len_df):
        temp_tuple = df[i]
        a_emp_tuple = tuple(temp_tuple)
        all_list.append(a_emp_tuple)
    # df.to_sql()
    # 写入到数据库中
    # 创建游标
    cursor = conn.cursor()
    sql1 = """DELETE FROM erpsku_restkws_add_columns"""
    sql = """insert IGNORE into erpsku_restkws_add_columns (account_site, seller_sku, erp_sku, asin,rest_kw,Match_Type,关键词来源站点,展示次数,点击量,花费,销售额,订单量,\
                                                               CTR,CPC,ACoS,CR) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    try:
        cursor.execute(sql1)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(e)
    try:
        cursor.executemany(sql, all_list)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(e)
    cursor.close()
    conn.close()


# 得到sellersku的asin ，先用st表的ad group得到asin，然后再去erp信息匹配剩下的asin
def get_sellersku_asin(sellersku_info, erpsku_info):
    all_sellersku = set(sellersku_info['SKU'])
    sellersku_info['asin'] = sellersku_info['Ad Group Name'].apply(lambda x: x.split(' ')[-1][0:10])
    sellersku_asin = sellersku_info[['SKU', 'asin']]
    # 取ad group name中提取的asin (B0开头的十位数字)
    sellersku_asin = sellersku_asin[
        (sellersku_asin['asin'].str.find('B0', 0) == 0) & (sellersku_asin['asin'].str.len() == 10)]
    sellersku_asin.drop_duplicates(subset=['SKU'], keep='first', inplace=True)
    sellersku_asin.rename(columns={'SKU': 'seller_sku'}, inplace=True)
    # 剩余的asin部分从erpsku取
    rest_sellersku = list(all_sellersku - set(sellersku_asin['seller_sku']))
    if rest_sellersku:
        all_restsku_info = erpsku_info[['seller_sku', 'asin']][erpsku_info['seller_sku'].isin(rest_sellersku)]
        all_restsku_info.drop_duplicates(subset=['seller_sku'], keep='last', inplace=True)
        all_sellersku_asin = pd.concat([sellersku_asin, all_restsku_info], sort='False')
    else:
        all_sellersku_asin = sellersku_asin
    return all_sellersku_asin


# 计算每个店铺的单日出价（美元）
def camp_daily_bid(camp_info, one_sku_budget=5, camp_daily_min_budget=100, camp_daily_max_budget=200):
    sku_nums = len(set(camp_info['SKU']))
    camp_bid = one_sku_budget * sku_nums
    if camp_bid <= camp_daily_min_budget:
        camp_bid = camp_daily_min_budget
    elif camp_bid >= camp_daily_max_budget:
        camp_bid = camp_daily_max_budget
    return camp_bid


# 连接我的数据库
def conn_mysql(process_database='server_camp_report'):
    conn = pymysql.connect(
        host='192.168.129.240',
        user='marmot',
        password='marmot123',
        database=process_database,
        port=3306,
        charset='UTF8')
    return conn


# 将数据库中的数据读取
def download_erpsku_restkw_info(table='erpsku_restkws_add_columns_filter_langs', request_camp='all'):
    # 连接数据库
    conn = conn_mysql()
    # 创建游标
    cursor = conn.cursor()
    # 写sql
    if request_camp == 'all':
        sql = """SELECT * FROM {}""".format(table)
    else:
        sql = """SELECT * FROM {} where account_site = {}""".format(table, "'%s'" % request_camp)
    # 执行sql语句
    cursor.execute(sql)
    all_result = cursor.fetchall()
    # 请求erpsku下其他sellersku出单关键词表，没有过滤国家
    if table == 'erpsku_restkws_add_columns_filter_langs':
        all_result = pd.DataFrame([list(j) for j in all_result],
                                  columns=['account_site', 'seller_sku', 'erp_sku', 'asin', 'rest_kw', 'Match_Type',
                                           '关键词来源站点', '展示次数', '点击量', '花费', '销售额', '订单量', \
                                           'CTR', 'CPC', 'ACoS', 'CR', 'lang', 'source_site'])
    elif table == 'erpsku_restkws_add_columns':
        all_result = pd.DataFrame([list(j) for j in all_result],
                                  columns=['account_site', 'seller_sku', 'erp_sku', 'asin', 'rest_kw', 'Match_Type',
                                           '关键词来源站点', '展示次数', '点击量', '花费', '销售额', '订单量', \
                                           'CTR', 'CPC', 'ACoS', 'CR'])
    all_result.drop_duplicates(inplace=True)
    conn.commit()
    cursor.close()
    conn.close()
    return all_result


# 导入acos与click参考表来浮动cpc
def load_acos_click():
    path = acos_click_adjust_dir
    acos = pd.read_excel(path, sheet_name='acos')
    click = pd.read_excel(path, sheet_name='click')
    return [acos, click]


# 对acos表做处理，得到最小的acos和最大的acos,左闭右开
def process_acos(df):
    df['acos'] = df['acos'].apply(lambda x: (x.strip('[').strip(')').strip('(')))
    df['min_acos'] = df['acos'].apply(lambda x: int(x.split(',')[0].strip('%')) / 100)
    df['max_acos'] = df['acos'].apply(lambda x: int(x.split(',')[1].strip('%')) / 100)


# 对click表做处理，得到最小的click和最大的click,左闭右开
def process_click(df):
    df['click'] = df['click'].apply(lambda x: (x.strip('[').strip(')').strip('(')))
    df['min_click'] = df['click'].apply(lambda x: int(x.split(',')[0]))
    df['max_click'] = df['click'].apply(lambda x: int(x.split(',')[1]))


# 找到每个sku的手动出价进行调价，其中对cpc参照acos范围和click范围进行调价
def calc_manual_sku_bid(df):
    df['click'] = df['点击量'] / df['订单量']
    [acos, click] = load_acos_click()
    process_acos(acos)
    process_click(click)
    # 将百分号acos转换成字符串
    df['ACoS'] = df['ACoS'].str.replace('%', '')
    df['ACoS'] = df['ACoS'].apply(lambda x: float(x) / 100)
    # 按照acos进行一次调价
    for i in range(len(df)):
        df_acos = df.ix[i, 'ACoS']
        if df_acos >= 1000:
            df_acos = 1000
            print(df['account_site'].values[0])
        rate = acos['Broad_Acos'][(acos['min_acos'] <= df_acos) & (acos['max_acos'] > df_acos)].values[0]
        if df.ix[i, 'CPC'] * (rate - 1) >= 0:
            df.ix[i, 'CPC_change1'] = df.ix[i, 'CPC'] + min(abs(df.ix[i, 'CPC'] * (rate - 1)), abs(
                acos['Broad'][(acos['min_acos'] <= df_acos) & (acos['max_acos'] > df_acos)].values[0]))
        else:
            df.ix[i, 'CPC_change1'] = df.ix[i, 'CPC'] - min(abs(df.ix[i, 'CPC'] * (rate - 1)), abs(
                acos['Broad'][(acos['min_acos'] <= df_acos) & (acos['max_acos'] > df_acos)].values[0]))

    # 按照click进行二次调价
    for i in range(len(df)):
        df_click = df.ix[i, 'click']
        rate = click['Broad_Acos'][(click['min_click'] <= df_click) & (click['max_click'] > df_click)].values[0]
        if df.ix[i, 'CPC_change1'] * (rate - 1) >= 0:
            df.ix[i, 'CPC_change2'] = df.ix[i, 'CPC_change1'] + min(abs(df.ix[i, 'CPC_change1'] * (rate - 1)), abs(
                click['Broad'][(click['min_click'] <= df_click) & (click['max_click'] > df_click)].values[0]))
        else:
            df.ix[i, 'CPC_change2'] = df.ix[i, 'CPC_change1'] - min(abs(df.ix[i, 'CPC_change1'] * (rate - 1)), abs(
                click['Broad'][(click['min_click'] <= df_click) & (click['max_click'] > df_click)].values[0]))
    df['CPC_change2'] = df['CPC_change2'].apply(lambda x: math.floor(x * 100) / 100)
    return df


def db_download_station_id(station_name, db='team_station', table='only_station_name_index', ip='192.168.129.240',
                           port=3306,
                           user_name='marmot', password='marmot123') -> list:
    """
    加载广告组接手的站点名
    :return: 所有站点名
    """
    station_name = station_name.lower()
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
    sql = """SELECT id FROM {} where station = {} """.format(table, "'%s'" % station_name)
    # 执行sql语句
    cursor.execute(sql)
    station_id = cursor.fetchall()
    if station_id:
        station_id = int(station_id[0][0])
    else:
        station_id = 0
        print(f"{station_name}站点的索引不存在。")
    conn.commit()
    cursor.close()
    conn.close()
    return station_id


def db_download_station_uuid(station_name, db='team_station', table='station_uuid_index', ip='192.168.129.240',
                             port=3306,
                             user_name='marmot', password='marmot123'):
    conn = pymysql.connect(
        host=ip,
        user=user_name,
        password=password,
        database=db,
        port=port,
        charset='UTF8')
    # 创建游标
    cursor = conn.cursor()
    sql = """SELECT uuid from {} where station = {}""".format(table, "'%s'" % station_name)
    # 执行sql语句
    cursor.execute(sql)
    station_uuid = cursor.fetchall()
    if not station_uuid:
        return
    station_uuid = station_uuid[0][0]
    # 得到station_uuid:list
    station_uuid = json.loads(station_uuid)['uuid']
    conn.commit()
    cursor.close()
    conn.close()
    return station_uuid


# 站点每条sku信息
def db_download_choose_sku_info(account, site, db='team_station', table='sku_kws_info', ip='192.168.129.240', port=3306,
                                user_name='marmot', password='marmot123') -> list:
    """
    某个站点的手动组sku信息
    :return: sku信息:sku_kws_match_type
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
    account = account.rstrip("_")
    station_name = (account + '_' + site).lower()
    station_uuid = db_download_station_uuid(station_name)
    # station_id = db_download_station_id(station_name)
    if not station_uuid:
        print(f"{station_name}在station_uuid中不存在...")
        return
    station_uuid_str = ",".join(list(map(lambda x: "'%s'" % x, station_uuid)))
    # 写sql
    sql = """select concat(SKU,'_',Keyword_or_Product_Targeting,'_',Match_Type) as bided_sku \
                from {} where uuid in ({})""".format(table, station_uuid_str)
    # 执行sql语句
    cursor.execute(sql)
    station_manual_sku_info = cursor.fetchall()
    station_manual_sku_info = [j[0].upper() for j in station_manual_sku_info]
    conn.commit()
    cursor.close()
    conn.close()
    return station_manual_sku_info


def filter_bided_kws(station_info, bided_sku_list):
    """
    过滤掉那些调过价的关键词
    :param station_info: 站点数据
    :param bided_sku_list: 所有sku信息.: sku_kws_match_type
    :return: 过滤站点数据后的数据
    """
    share_sellersku_kws_data_temp = station_info.copy()
    share_sellersku_kws_data_temp.fillna('sign', inplace=True)
    share_sellersku_kws_data_temp['kws_index'] = [(sku + '_' + kw + '_' + match_type).upper() for
                                                  sku, kw, match_type in
                                                  zip(share_sellersku_kws_data_temp['SKU'],
                                                      share_sellersku_kws_data_temp['Keyword'],
                                                      share_sellersku_kws_data_temp['Match Type'])]
    station_info['kws_index'] = share_sellersku_kws_data_temp['kws_index']
    share_sellersku_kws_data = station_info[~station_info['kws_index'].isin(bided_sku_list)]
    del share_sellersku_kws_data['kws_index']
    share_sellersku_kws_data.reset_index(drop=True, inplace=True)
    return share_sellersku_kws_data


# 计算sku与kw
def trans_erpsku_restkw_to_upload(one_camp_sellersku_restkws, account, site, camp_name, incharge_name,
                                  one_kw_price=0.05, ad_group_budget=0.02,
                                  one_keyword_max_budget={'US': 0.8, "OTHER": 0.6},
                                  cpc_adjust_rate=1.3, exact_broad_rate=1.2):
    global change_current, my_columns
    # 表的列名
    my_columns = ['Campaign Name', 'Campaign Daily Budget', 'Campaign Start Date', 'Campaign End Date',
                  'Campaign Targeting Type', 'Ad Group Name', 'Max Bid', 'SKU', 'Keyword', 'Product Targeting ID',
                  'Match Type',
                  'Campaign Status', 'Ad Group Status', 'Status', 'Bidding strategy']
    # 汇率
    change_current = {'US': 1, 'CA': 1, 'UK': 1, 'DE': 1, 'IT': 1,
                      'ES': 1, 'JP': 0.018, 'JA': 0.018, 'FR': 1, 'MX': 0.1,
                      'IN': 0.03, 'AU': 1, 'EN': 1}

    # sku以及kws信息
    sku_info = one_camp_sellersku_restkws
    sku_info.rename(columns={'seller_sku': 'SKU'}, inplace=True)
    sku_info.reset_index(drop=True, inplace=True)

    # 将来源国家的cpc换成美金
    sku_info['CPC'] = [cpc * change_current[source_site.upper()] for cpc, source_site in
                       zip(sku_info['CPC'], sku_info['source_site'])]

    # 对cpc按照acos click进行两次调价
    sku_info = calc_manual_sku_bid(sku_info)

    # 所有的信息(第一部分:MANUAL广告,match type:BROAD)
    # 计算店铺的预算
    camp_budget = camp_daily_bid(sku_info)
    # 所有的信息
    empty_list = [np.nan] * len(my_columns)
    first_info = pd.DataFrame([empty_list], columns=my_columns)
    manual_campaign_name_info = "MANUAL-" + account + "_" + site + "-by-SP_Bulk"
    # 初始化第一行
    first_info.ix[0, 'Campaign Targeting Type'] = 'MANUAL'
    first_info.ix[0, 'Campaign Daily Budget'] = camp_budget
    first_info.ix[0, 'Campaign Status'] = 'enabled'
    # 将sku分组分开
    group_sku = sku_info.groupby(['SKU'])
    first_part_all_rest_info = pd.DataFrame(columns=my_columns)
    for one_sku, value in group_sku:
        if value.empty:
            continue
        asin = value['asin'].value_counts().index[0]
        # ad_group 第一行数据（第一部分）
        empty_list = [np.nan] * len(my_columns)
        first_row = pd.DataFrame([empty_list], columns=my_columns)
        first_row['Max Bid'] = round(ad_group_budget, 2)
        # 印度，日本，墨西哥分别为1,2,0.1本币
        if site == 'IN':
            first_row['Max Bid'] = 1 * change_current['IN']
        elif site in ('JP', 'JA'):
            first_row['Max Bid'] = 2 * change_current['JP']
        elif site == 'MX':
            first_row['Max Bid'] = 0.1 * change_current['MX']
        first_row['Ad Group Status'] = 'enabled'
        # ad_group 第二部分数据 sku
        second_part = pd.DataFrame([empty_list], columns=my_columns)
        second_part['SKU'] = one_sku
        second_part['Ad Group Status'] = 'enabled'
        second_part['Status'] = 'enabled'
        # ad_group 第三部分 出价和kw
        third_part = pd.DataFrame(columns=my_columns)
        third_part.reset_index(drop=True, inplace=True)
        # 计算每个词的出价 （每个单词的出价为0.05元，单词的个数与 调价后的cpc*1.3取最大值，但是最大值不能超过指定值（美国为0.8，其他国家为0.6））
        if site == 'US':
            max_bid_list = [
                round(min(max(len(one_keyword.split(' ')) * one_kw_price, cpc * cpc_adjust_rate),
                          one_keyword_max_budget['US']), 2) for one_keyword, cpc in
                zip(value['rest_kw'], value['CPC_change2'])]
        else:
            max_bid_list = [
                round(min(max(len(one_keyword.split(' ')) * one_kw_price, cpc * cpc_adjust_rate),
                          one_keyword_max_budget['OTHER']), 2) for one_keyword, cpc in
                zip(value['rest_kw'], value['CPC_change2'])]
        third_part['Max Bid'] = max_bid_list
        third_part['Keyword'] = value['rest_kw'].values
        third_part['Ad Group Status'] = 'enabled'
        third_part['Match Type'] = "Broad"
        third_part['Status'] = 'enabled'
        rest1_info = pd.concat([first_row, second_part, third_part], sort=False)
        rest1_info['Ad Group Name'] = one_sku + " " + asin
        rest1_info.drop_duplicates(inplace=True)
        first_part_all_rest_info = pd.concat([first_part_all_rest_info, rest1_info], sort=False)
    first_part_all_rest_info = pd.concat([first_info, first_part_all_rest_info])
    first_part_all_rest_info['Campaign Name'] = manual_campaign_name_info
    first_part_all_rest_info['Campaign Status'] = 'enabled'
    # 汇率换算
    first_part_all_rest_info['Campaign Daily Budget'] = first_part_all_rest_info['Campaign Daily Budget'].apply(
        lambda x: int(x / change_current[site]) if x > 0 else x)
    first_part_all_rest_info['Max Bid'] = first_part_all_rest_info['Max Bid'].apply(
        lambda x: round(x / change_current[site], 2) if x > 0 else x)
    # 所有的信息(第二部分:MANUAL广告,match type:EXACT)
    second_part_all_rest_info = first_part_all_rest_info.copy()
    second_part_all_rest_info['Max Bid'] = second_part_all_rest_info['Max Bid'].apply(
        lambda x: round(x * exact_broad_rate, 2) if x > 0 else x)
    second_part_all_rest_info['Campaign Daily Budget'] = second_part_all_rest_info['Campaign Daily Budget'].apply(
        lambda x: int(x * exact_broad_rate) if x > 0 else x)
    second_part_all_rest_info['Match Type'] = second_part_all_rest_info['Match Type'].str.replace('Broad', 'Exact')
    exact_campaign_name_info = 'MANUAL-ST-EXACT-by-SP_Bulk'
    second_part_all_rest_info['Campaign Name'] = exact_campaign_name_info
    # 合并BROAD与EXACT
    all_info = pd.concat([first_part_all_rest_info, second_part_all_rest_info], sort=False)

    # 规范成标准列
    all_info = all_info[my_columns]
    bided_sku = db_download_choose_sku_info(account, site)
    if bided_sku:
        all_info = filter_bided_kws(all_info, bided_sku)

    # 生成输出文件夹
    report_dir = os.path.join(export_dir, incharge_name, account + '_' + incharge_name)
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)
    out_path = os.path.join(report_dir, camp_name + '.xlsx')
    all_info.to_excel(out_path, na_rep='', index=False)


# 将(母表)添加负责人
def add_incharge_to_erpsku_restkws(erpsku_restkws_data, camp_incharge_list):
    new_erpsku_restkws_data = pd.merge(erpsku_restkws_data, camp_incharge_list, how='left', left_on='account_site',
                                       right_on='station')
    new_erpsku_restkws_data['ad_manger'][new_erpsku_restkws_data['ad_manger'].isna()] = 'Nobody'
    return new_erpsku_restkws_data


# 修改母表格式
def init_erpsku(erpsku_restkws_data):
    # 将站点全部大写
    erpsku_restkws_data['account_site'] = erpsku_restkws_data['account_site'].apply(lambda x: x.upper())


# 修改负责人表信息格式
def init_guys_incharge_camp(guys_incharge_camp_data):
    # 将站点全部大写
    guys_incharge_camp_data['station'] = guys_incharge_camp_data['station'].apply(lambda x: x.upper())


# 将母表按照负责人分组
def sepr_camp(erpsku_data):
    camp_list = set(erpsku_data['account_site'])
    for camp in camp_list:
        one_camp_data = erpsku_data[erpsku_data['account_site'] == camp]
        incharge_name = erpsku_data['ad_manger'][erpsku_data['account_site'] == camp].values[0]
        account = camp[:-3]
        site = camp[-2:]
        trans_erpsku_restkw_to_upload(one_camp_data, account, site, camp, incharge_name)


def erpsku_sellersku_kws(request_account, request_site):
    '''
    接口，请求站点，得到站点的st表
    :param request_account:
    :param request_site:
    :return: 得到一个字典:
            erpsku_another_sellersku_kws:同erpsku下其他sellersku的出单关键词
            erpsku_another_sellersku_kws_same_lang:同erpsku下其他sellersku的出单关键词（过滤掉其他语言）
    '''
    camp_name = (request_account + "_" + request_site).upper()
    erpsku_another_sellersku_kws = download_erpsku_restkw_info(table='erpsku_restkws_add_columns',
                                                               request_camp=camp_name)
    erpsku_another_sellersku_kws_same_lang = download_erpsku_restkw_info(request_camp=camp_name)
    camp_rest_sellersku_kws = {'erpsku_another_sellersku_kws': erpsku_another_sellersku_kws,
                               'erpsku_another_sellersku_kws_same_lang': erpsku_another_sellersku_kws_same_lang}
    return camp_rest_sellersku_kws


def db_download_station_names(db='team_station', table='only_station_info', ip='192.168.8.180', port=3306,
                              user_name='zhangyupeng', password='zhangyupeng') -> list:
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
    station_names = cursor.fetchall()
    station_names = pd.DataFrame([list(j) for j in station_names], columns=['station', 'ad_manger'])
    conn.commit()
    cursor.close()
    conn.close()
    print("STEP1: 完成下载站点名信息...")
    print("===================================================")
    return station_names


if __name__ == "__main__":
    global export_dir
    t0 = datetime.datetime.now()
    # 导入st信息
    st_files_data = load_st_info(st_folder_dir)
    all_erp_info = conn_gross_require_load_files()
    groupd_erp_sku_info = pd.DataFrame(groupd_erp_sku(all_erp_info))
    erp_seller_sku = [i for i in groupd_erp_sku_info.index]
    groupd_erp_sku_info['erp_sku'] = erp_seller_sku
    groupd_erp_sku_info.reset_index(drop=True, inplace=True)
    groupd_seller_sku_info = pd.DataFrame(grouped_st_sku(st_files_data))
    account_site = [i[0] for i in groupd_seller_sku_info.index]
    seller_sku = [i[1] for i in groupd_seller_sku_info.index]
    groupd_seller_sku_info['account_site'] = account_site
    groupd_seller_sku_info['seller_sku'] = seller_sku
    groupd_seller_sku_info.reset_index(drop=True, inplace=True)
    erp_info = pd.merge(all_erp_info, groupd_erp_sku_info['erp_sku'], on='erp_sku', how='inner')
    # 得到erpsku其他基本关键词
    all_erp_rest_kw_info = merge_erp_sku_seller_sku(erp_info, groupd_seller_sku_info)
    all_erp_rest_kw_info.drop_duplicates(inplace=True)
    # 将基本列存储到数据库中
    load_erp_rest_kws_to_sql(all_erp_rest_kw_info)
    # 得到erpsku其他关键词列
    all_erpsku_restkw_add_columns_info = add_erpsku_restkws_cloumns(st_files_data, all_erp_rest_kw_info)
    # 得到asin
    all_sellersku_asin = get_sellersku_asin(st_files_data, all_erp_info)
    all_erpsku_restkw_add_columns_info = pd.merge(all_erpsku_restkw_add_columns_info,
                                                  all_sellersku_asin, on='seller_sku')
    all_erpsku_restkw_add_columns_info.drop_duplicates(inplace=True)
    # 将erpsku其他关键词列存储到数据库中
    load_add_columns_erp_rest_kws_to_sql(all_erpsku_restkw_add_columns_info)
    # 得到erpsku其他关键词列输出到st表中
    # export_to_st_file(all_erpsku_restkw_add_columns_info, st_folder_dir, '同erp_sku下其他seller_sku出单关键词')
    # 得到erpsku其他关键词列并筛除掉其他国家的词语输出到st表中
    filter_another_countries_restkws = filter_another_lang(all_erpsku_restkw_add_columns_info)
    # 将过滤的关键词表输出到数据库中
    filtered_restkws_to_sql(filter_another_countries_restkws)
    # 将过滤的关键词表输出到st表中
    # export_to_st_file(filter_another_countries_restkws, st_folder_dir, '同erp_sku下其他seller_sku出单关键词(同国家)')
    # 从数据库中导入同erpsku下其他sellersku的出单关键词(母表)
    all_camp_restkws_data = download_erpsku_restkw_info()
    init_erpsku(all_camp_restkws_data)
    # 导入相应站点的负责人信息
    guys_incharge_camp = db_download_station_names(db='team_station', table='only_station_info',
                                                   ip='192.168.8.180',
                                                   port=3306, user_name='zhangyupeng', password='zhangyupeng')

    init_guys_incharge_camp(guys_incharge_camp)
    # 将母表添加负责人信息
    all_camp_restkws_data_test = add_incharge_to_erpsku_restkws(all_camp_restkws_data, guys_incharge_camp)
    export_dir = r"C:\Users\Administrator\Desktop\erpsku共享出单关键词_{}".format(t0.date())
    if os.path.exists(export_dir):
        shutil.rmtree(export_dir)
    # 将关键词表按照st上传表的格式，并按照相应的对接人输出。
    sepr_camp(all_camp_restkws_data_test)
    t1 = datetime.datetime.now()
    print('一共花费:{}s'.format((t1 - t0).seconds))
