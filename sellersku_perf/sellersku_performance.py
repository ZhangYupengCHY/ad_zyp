# -*- coding: utf-8 -*-
"""
Created on  11月11日 10:52 2019

@author: Zhang yp
"""

"""
  目的：分国家存储账号的广告数据，用于查询sellersku是否打广告，广告类型(自动/手动)，自动/手动广告的出价 。
  数据来源：取广告报表的的ad行(sku)，同时需要sku的广告类型以及广告出价。
  思路： 1. 首先通过接口请求每日的广告报表数据
        2. 将广告报表取出所需要的值
        3. 将数据存储到数据库中
"""

import datetime as dt
from datetime import datetime
import time,base64,rsa
import json
import requests
import os
import re
import pandas as pd
import shutil
import pymysql
import numpy as np


import public_function
'''
def request_bulk_use_new_api(camp,
                             request_source="http://120.78.243.154/services/api/advertise/getreport",
                             date=datetime.now().date()) -> list:
    """
    通过新接口的请求，读取某个站点最新一天的广告报表
    :param camp: 站点名
    :param request_source: 请求的接口名
    :param date:今天的日期
    :return:某个站点最近两天的广告报表的链接列表
    """

    # 获得请求的一些参数
    account = camp[:-3]
    site = camp[-2:]
    # 取今天之前的两天作为时间范围的最后日期
    report_end_date = (date - dt.timedelta(days=2)).strftime('%Y-%m-%d')
    # 考虑到广告报表的请求的时差性，取今天之前的三天作为时间范围的起始日期
    report_start_date = (date - dt.timedelta(days=3)).strftime('%Y-%m-%d')
    post_load = {
        'token': token,
        'data': json.dumps({
            0: {
                'date_range': 1,
                "child_type": 30,
                'account_id': station_name,
                "report_type": "Campaign"
            }
        })
    }
    response = requests.post(request_source, params=post_load).content
    # 解码：bytes类型转str类型
    data_url = response.decode()
    if 'http' not in data_url:
        return []
    # 返回的条件参数
    if json.loads(data_url)['code'] == 1000:
        json_data = json.loads(data_url)['data']
        file_list = [camp_file['file'] for camp_file in json_data]
    else:
        return []

    return file_list
'''

def write_all_bulk_last_day_files(camp_list, camp_dir='E:/ad_zyp/sellersku_perf/station_files'):
    """
    将新接口请求到的最新一天的所有的广告报表的数据写入到本地
    :param camp_list: 广告站点集合
    :return: empty
    """
    all_camp_bulk_files = []
    if os.path.exists(camp_dir):
        shutil.rmtree(camp_dir)
    os.mkdir(camp_dir)
    for station_name in camp_list:
        # camp_bulk_list = request_bulk_use_new_api(camp)

        def request_camp(station_name):
            key_path = r"E:\ad_zyp\api_request_files\public.key"
            download_url = "http://120.78.243.154/services/api/advertise/getreport"
            with open(key_path, 'r') as fp:
                public_key = fp.read()
            # pkcs8格式
            key = public_key
            password = "Kr51wGeDyBM39Q0REVkXn4lW7ZqCxdPLS8NO6iIfubTJcvsjt2YpmAgzHFUoah"
            pubkey = rsa.PublicKey.load_pkcs1_openssl_pem(key)
            password = password.encode('utf8')
            crypt_password = rsa.encrypt(password, pubkey)
            token = base64.b64encode(crypt_password).decode()
            station_name = station_name[0:-3].replace('_', '-') + station_name[-3:]
            post_load = {
                'token': token,
                'data': json.dumps({
                    0: {
                        'date_range': 1,
                        "child_type": 30,
                        'account_id': station_name,
                        "report_type": "Campaign"
                    }
                })
            }
            response = requests.post(download_url, data=post_load).content
            data = []
            try:
                data_basic = json.loads(response)['data']
                data.extend(data_basic)
            except:
                pass
            return data
        station_dir = request_camp(station_name)
        if station_dir:
            camp_file_info = {filename: re.findall('[0-9]{4}.[0-9]{2}.[0-9]{2}', filename)[-1] for filename in
                              station_dir}
            camp_bulk_last_day = max(camp_file_info.values())
            newest_bulk = [bulk_name for bulk_name, date in camp_file_info.items() if date == camp_bulk_last_day]
            all_camp_bulk_files.extend(newest_bulk)


    i = 1
    print("STEP2: 开始将最新的广告报表写入到本地...")
    for bulk_file in all_camp_bulk_files:
        request_file = requests.get(bulk_file)
        status_code = request_file.status_code
        file_name = bulk_file.split('/')[-1]
        if status_code == 200:
            out_content = request_file.content
        else:
            continue
        # 文件路径

        all_dir = os.path.join(camp_dir, file_name)
        if os.path.exists(all_dir):
            continue
        with open(all_dir, 'wb') as f:
            f.write(out_content)
        files_left = len(all_camp_bulk_files) - i
        print("完成写入第 {} 个文件,还剩 {} 个...".format(i, files_left))
        i += 1
    print('STEP2: FINISH!!!')
    print("===================================================")


def read_one_bulk(file_dirname, wanted_columns, camp_dir='E:/ad_zyp/sellersku_perf/station_files') -> pd.DataFrame:
    """
    读取某个站点的excel
    :param file_name: 站点的文件名
    :param camp_dir: 站点所在的文件夹
    :return: 站点的需要的ad信息
    """
    wanted_columns = [column.upper() for column in wanted_columns]
    init_camp = pd.DataFrame(columns=wanted_columns)
    dir_name = os.path.join(camp_dir, file_dirname)
    writer = pd.ExcelFile(dir_name)
    if 'Sponsored Products Campaigns' not in writer.sheet_names:
        print('bulk_error_1: 不存在Sponsored Products Campaigns {}.'.format(file_dirname))
        return init_camp
    camp_data = writer.parse(sheet_name='Sponsored Products Campaigns')
    if camp_data.empty:
        print("bulk_error_2: 'Sponsored Products Campaigns'表没数据 {}.".format(file_dirname))
        return init_camp
    camp_data.rename(columns={column: column.upper() for column in camp_data.columns}, inplace=True)
    if not set(camp_data.columns) >= set(wanted_columns):
        columns_short = set(wanted_columns) - set(camp_data.columns)
        print("bulk_error_3: 缺少{}列 {}.".format(columns_short, file_dirname))
        return init_camp
    one_camp_ad_info = get_sellersku_info(camp_data, wanted_columns)
    return one_camp_ad_info


def get_sellersku_info(bulk_data, wanted_columns):
    """
    通过某个站点广告报表数据，得到需要的sellersku的信息
    :param bulk_data: 广告报表数据
    :return: sellersku信息
    """
    # 用前一个非空值来充填缺失值 method = ffill(向后充填为bfill)
    bulk_data['CAMPAIGN TARGETING TYPE'].fillna(method='ffill', inplace=True)
    bulk_data['MAX BID'].fillna(method='ffill', inplace=True)
    record_type_ad = bulk_data[bulk_data['RECORD TYPE'] == 'Ad']
    sellersku_info = record_type_ad[wanted_columns]
    return sellersku_info


def read_all_bulk_files(camp_dir='E:/ad_zyp/sellersku_perf/station_files') -> pd.DataFrame:
    """
    读取文件夹下所有的最新的广告报表，并提取ad行(sellersku)的数据，同时获得广告类型(自动/手动),自动/手动的广告出价
    :param camp_dir:
    :return:
    """
    wanted_columns = ['Campaign', 'Campaign Targeting Type', 'Ad Group', 'Max Bid', 'SKU', 'Impressions', 'Clicks',
                      'Spend', 'Orders', 'Total Units', 'Sales', 'ACoS']
    init_all_bulk_info = pd.DataFrame(columns=wanted_columns)
    camp_files_dirnames = os.listdir(camp_dir)
    if not camp_files_dirnames:
        return init_all_bulk_info
    i = 1
    print("STEP3: 开始读取最新的广告报表数据...")
    for one_camp_file_dirname in camp_files_dirnames:
        camp_name = one_camp_file_dirname.split('_')[1]
        one_camp_ad_info = read_one_bulk(one_camp_file_dirname, wanted_columns, camp_dir=camp_dir)
        one_camp_ad_info.columns = wanted_columns
        one_camp_ad_info.insert(0, 'camp_name', camp_name)
        init_all_bulk_info = pd.concat([init_all_bulk_info, one_camp_ad_info], sort=False)
        file_left = len(camp_files_dirnames) - i
        print('完成读取 {} 个文件 ,还剩 {} 个...'.format(i, file_left))
        i += 1
    print("STEP3: FINISH!!!")
    print("===================================================")
    shutil.rmtree(camp_dir)
    new_columns = ['camp_name']
    new_columns.extend(wanted_columns)
    init_all_bulk_info = init_all_bulk_info[new_columns]
    init_all_bulk_info.reset_index(drop=True, inplace=True)
    return init_all_bulk_info


def db_download_station_names(db='team_station', table='only_station_info', ip='192.168.30.55', port=33061,
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
    sql = """SELECT station FROM {} """.format(table)
    # 执行sql语句
    cursor.execute(sql)
    station_names = cursor.fetchall()
    station_names = list(set([j[0] for j in station_names]))
    conn.commit()
    cursor.close()
    conn.close()
    print("STEP1: 完成下载站点名信息...")
    print("===================================================")
    return station_names


def db_download_erpsku_sellersku(db='mrp_py', table='gross_require', ip='47.106.127.183', port=33061,
                                 user_name='mrp_read', password='mrpread'):
    """
    连接gross_require/mrp_py数据库中的gross_require数据表，得到erpsku与sellersku信息
    :return:erpsku与sellersku的对应表
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
    sql = """SELECT 标识,erp_sku FROM {}""".format(table)
    # 执行sql语句
    cursor.execute(sql)
    all_result = cursor.fetchall()
    all_result = pd.DataFrame([list(j) for j in all_result], columns=['标识', 'erp_sku'])
    all_result['标识'] = all_result['标识'].astype('str')
    # 从标识中提取sellersku
    all_result['seller_sku'] = all_result['标识'].apply(lambda x: x.split('$')[1])
    all_st_erp_sku_info = all_result[['seller_sku', 'erp_sku']]
    conn.commit()
    cursor.close()
    conn.close()
    return all_st_erp_sku_info


def init_sellersku_info(sellersku_info):
    """
    初始化sellersku信息
    1. 将sellersku表中的逗号改为点号
    2. 将station名中提取账号名和国家,去掉一些特殊字符
    3. 将站点名由中文转换为英文
    4. 将浮点型数据转换成数值型
    :param sellersku_info:sellersku信息表
    :return: sellersku信息表
    """
    sellersku_info = sellersku_info.applymap(lambda x: x.replace(',', '.') if isinstance(x, str) else x)
    sellersku_info['account'] = sellersku_info['camp_name'].apply(lambda x: ''.join(re.findall('[a-zA-Z]', x)))
    sellersku_info['site'] = sellersku_info['camp_name'].apply(lambda x: re.sub("[a-zA-Z站 ]", "", x))
    dict_site = public_function.COUNTRY_CN_EN_DICT
    sellersku_info['site'] = [dict_site[site] for site in sellersku_info['site']]
    sellersku_info['camp_name'] = sellersku_info['account'] + '_' + sellersku_info['site']
    sellersku_info = sellersku_info[['camp_name', 'account', 'site', 'Campaign', 'Campaign Targeting Type', 'Ad Group',
                                     'Max Bid', 'SKU', 'Impressions', 'Clicks', 'Spend', 'Orders',
                                     'Total Units', 'Sales', 'ACoS']]
    return sellersku_info


def db_upload_site_sellersku_perf(sellersku_data, site, db='sellersku_performance', ip='127.0.0.1',
                                  user_name='marmot',
                                  password='marmot123', port=3306):
    """
    将每个站点的sellersku的表现分别存储在对应的服务器数据库表中
    :param sellersku_data:sellersku的表现信息
    :param site:站点国家
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
    # 将数据变成可进行读入数据库的dict格式
    all_list = []
    sellersku_data.reset_index(drop=True, inplace=True)
    df = sellersku_data.astype(object).replace(np.nan, 'None')
    df = np.array(df)
    len_df = df.shape[0]
    for i in range(len_df):
        temp_tuple = df[i]
        a_emp_tuple = tuple(temp_tuple)
        all_list.append(a_emp_tuple)
    # 写sql
    table_name = site.lower() + "_sellersku_perf"
    sql = """replace into {} (station, account, site, campaign, campaign_targeting_type, ad_group, max_bid,\
            sku, impressions, clicks, spend , orders ,total_units, sales, acos) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""".format(
        table_name)
    # 执行sql语句
    try:
        cursor.executemany(sql, all_list)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(e)
    cursor.close()
    conn.close()
    print('{}写入完成...'.format(site))


def db_upload_erpsku_type_perf(erpsku_data, ad_type, db='erpsku_perf', ip='127.0.0.1',
                               user_name='marmot',
                               password='marmot123', port=3306):
    """
    将erpsku的auto/manual表现分别存储在对应的服务器数据库表中
    :param erpsku_data:erpsku的表现信息
    :param site:站点国家
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
    # 将数据变成可进行读入数据库的dict格式
    all_list = []
    erpsku_data.reset_index(drop=True, inplace=True)
    df = erpsku_data.astype(object).replace(np.nan, 'None')
    df = np.array(df)
    len_df = df.shape[0]
    for i in range(len_df):
        temp_tuple = df[i]
        a_emp_tuple = tuple(temp_tuple)
        all_list.append(a_emp_tuple)
    # 写sql
    table_name = 'erpsku_' + ad_type + '_perf'
    sql = """replace into {} (erpsku, station, account, site, campaign, campaign_targeting_type, ad_group, max_bid,\
            sku, impressions, clicks, spend , orders ,total_units, sales, acos) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""".format(
        table_name)
    # 执行sql语句
    try:
        cursor.executemany(sql, all_list)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(e)
    cursor.close()
    conn.close()
    print("完成上传erpsku_{}数据...".format(ad_type))


if __name__ == "__main__":
    while 1:
        t0 = datetime.now()
        start_hour = [9,10]
        if t0.hour in start_hour:
            print('程序开始运行 :{}...'.format(t0))
            # 连接数据库，加载目前接受的所有站点
            all_station_names = db_download_station_names(db='team_station', table='only_station_info',
                                                          ip='127.0.0.1',
                                                          port=3306, user_name='marmot', password='marmot123')
            # 将所有站点的最新的广告报表文件写入到本地
            write_all_bulk_last_day_files(all_station_names, camp_dir='E:/ad_zyp/sellersku_perf/station_files')
            # 读所有站点广告报表的信息
            all_sellersku_info = read_all_bulk_files(camp_dir='E:/ad_zyp/sellersku_perf/station_files')
            # 初始化sellersku信息表
            all_sellersku_info = init_sellersku_info(all_sellersku_info)
            # 将sellersku信息表分国家存储在数据库中
            print("STEP4: 开始将每个国家的sellersku信息写入到数据库中...")
            '''
            for site, site_sellersku_info in all_sellersku_info.groupby('site'):
                db_upload_site_sellersku_perf(site_sellersku_info, site, db='sellersku_performance',
                                              ip='127.0.0.1',
                                              user_name='marmot',
                                              password='', port=3306)
            '''
            print("STEP4: FINISH!!!")
            print("===================================================")
            # 连接数据库，加载erpsku与sellersku信息
            erpsku_sellersku = db_download_erpsku_sellersku(db='mrp_py', table='gross_require', ip='47.106.127.183',
                                                            port=33061,
                                                            user_name='mrp_read', password='mrpread')
            erpsku_sellersku.drop_duplicates(inplace=True)
            # 将erpsku_sellersku信息对应表与每个sellersku的广告表现数据合并->erpsku表现
            erpsku_sellersku_perf = pd.merge(erpsku_sellersku, all_sellersku_info, left_on='seller_sku', right_on='SKU',
                                             how='inner')
            # erpsku表现 自动广告与手动广告
            print("STEP5: 开始将自动广告/手动广告erpsku信息写入到数据库中...")
            [erpsku_sellersku_auto_perf, erpsku_sellersku_manual_perf] = [
                erpsku_sellersku_perf[erpsku_sellersku_perf['Campaign Targeting Type'] == ad_type] for ad_type in
                ['Auto', 'Manual']]
            del erpsku_sellersku_auto_perf['seller_sku']
            del erpsku_sellersku_manual_perf['seller_sku']
            # 将erpsku的广告表现上传的数据库中
            db_upload_erpsku_type_perf(erpsku_sellersku_auto_perf, 'auto', db='erpsku_perf', ip='127.0.0.1',
                                       user_name='marmot',
                                       password='marmot123', port=3306)
            db_upload_erpsku_type_perf(erpsku_sellersku_manual_perf, 'manual', db='erpsku_perf', ip='127.0.0.1',
                                       user_name='marmot',
                                       password='marmot123', port=3306)
            print('STEP5: FINISH!!!')
            print("===================================================")
            print('ALL FINISH!!!')

            t1 = datetime.now()
            print(t1 - t0)
            if t1.hour in start_hour:
                continue
                print("继续运行...")
            else:
                sleep_hour = (start_hour[0] - t1.hour)
                sleep_sec = sleep_hour * 3600
                print("休眠{}小时...Zzzzz".format(sleep_hour))
                print("晚上10点开始运行...")
                time.sleep(sleep_sec)
        else:
            sleep_hour = (start_hour[0] - t0.hour)
            sleep_sec = sleep_hour * 3600
            print("休眠{}小时...Zzzzz".format(sleep_hour))
            print("晚上{}点开始运行...".format(start_hour[0]))
            time.sleep(sleep_sec)
