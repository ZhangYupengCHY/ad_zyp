# -*- coding: utf-8 -*-
"""
Proj: AD-Helper1
Created on:   2019/12/16 14:44
@Author: RAMSEY

Standard:
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""

"""
列出 花费占比(spend_rate)，店销额(shop_sales)，acos满足
    花费占比>6%,店销额>2w，acos>18%
    花费占比>7%,店销额>1w，acos>20%
    花费占比>8%，店销额>5k，acos>25%
的情况，输出成一个表
"""

import pymysql
import pandas as pd
import redis
import time, pickle
import os, codecs
import my_toolkit.change_rate as change_rate

# 预警条件
#     花费占比>6%,店销额>2w，acos>18%
#     花费占比>7%,店销额>1w，acos>20%
#     花费占比>8%，店销额>5k，acos>25%
warning_condition1 = {'spend_rate': 0.06, 'shop_sales': 50000, 'acos': 0.20}
warning_condition2 = {'spend_rate': 0.07, 'shop_sales': 20000, 'acos': 0.25}
warning_condition3 = {'spend_rate': 0.08, 'shop_sales': 10000, 'acos': 0.30}
all_warning_condition = [warning_condition1, warning_condition2, warning_condition3]
export_columns = ['station', 'ad_manger', 'spend_rate', 'shop_sales', 'acos']
# 预警分组名
warning_names = ['condition1', 'condition2', 'condition3']
warning_titles = ['花费占比>6%,店销额>5w，acos>20%', '花费占比>7%,店销额>2w，acos>25%', '花费占比>8%，店销额>1w，acos>30%']

# redis连接信息
redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=4, password='chy910624', decode_responses=True)
red4 = redis.StrictRedis(connection_pool=redis_pool)

# 汇率
exchange_rate = change_rate.change_current()


# 加载数据库中用于计算预警的数据，包括'station', 'ad_manger', 'acos', 'shop_sales', 'ad_sales'
def db_download_station_info(db='team_station', table='only_station_info', ip='192.168.8.180', port=3306,
                             user_name='zhangyupeng', password='zhangyupeng') -> list:
    """
    加载广告组接手的站点名
    :return: 需要的信息
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
    sql = """SELECT * FROM {} """.format(table)
    try:
        # 执行sql语句
        cursor.execute(sql)
        station_info = cursor.fetchall()
        # 获得列名
        cols_info = cursor.description  # 类似 desc table_name返回结果
        col_name = []  # 创建一个空列表以存放列名
        for col_info in cols_info:
            col_name.append(col_info[0])  # 循环提取列名，并添加到col空列表
        station_info = pd.DataFrame([list(one_info) for one_info in station_info],
                                    columns=col_name)
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()
    cursor.close()
    conn.close()
    print("完成从数据库加载基本信息...")
    return station_info


# 初始化数据库中的站点信息
def init_db_station_info(station_info):
    """
    初始化数据库中的站点信息（花费占比、店销额、acos），得到预警数据
    花费占比(spend_rate) = acos*ad_sales/shop_sales
    店销额 = shop_sales
    acos = acos
    :param station_info: 数据库站点信息
    :return:初始化处理后的station_info
    """
    station_info.dropna(inplace=True)
    # 改变汇率
    station_info['country'] = station_info['station'].apply(lambda x: x[-2:].upper())
    rates = [exchange_rate[country] for country in station_info['country']]
    station_info['ad_sales'] = station_info['ad_sales'] * rates
    station_info['shop_sales'] = station_info['shop_sales'] * rates
    station_info = station_info[~station_info['acos'].isin(['', ' '])]
    station_info['float_acos'] = station_info['acos'].copy()
    station_info['float_acos'] = station_info['float_acos'].apply(
        lambda x: float(x.replace('%', '')) / 100)
    station_info['spend_rate'] = [acos * ad_sales / shop_sales if shop_sales != 0 else 0 for acos, ad_sales, shop_sales
                                  in zip(station_info['float_acos'], station_info['ad_sales'],
                                         station_info['shop_sales'])]
    return station_info


# 根据预警条件:输出满足其中一条条件的站点信息
def export_warning_station_info(station_info):
    """
    根据预警条件，输出满足预警条件的站点
    :param station_info: 站点信息
    :return: 输出站点信息
    """
    warning_condition1_expression = (station_info['spend_rate'] > all_warning_condition[0]['spend_rate']) & (
            station_info['shop_sales'] > all_warning_condition[0]['shop_sales']) & (
                                            station_info['float_acos'] > all_warning_condition[0]['acos'])
    warning_condition2_expression = (station_info['spend_rate'] > all_warning_condition[1]['spend_rate']) & (
            station_info['shop_sales'] > all_warning_condition[1]['shop_sales']) & (
                                            station_info['float_acos'] > all_warning_condition[1]['acos'])
    warning_condition3_expression = (station_info['spend_rate'] > all_warning_condition[2]['spend_rate']) & (
            station_info['shop_sales'] > all_warning_condition[2]['shop_sales']) & (
                                            station_info['float_acos'] > all_warning_condition[2]['acos'])

    # 规范格式以及选择需要输出的列,以及输出的路径
    warning_info1 = station_info[warning_condition1_expression]
    warning_info2 = station_info[warning_condition2_expression]
    warning_info3 = station_info[warning_condition3_expression]

    # 去重
    old_columns = warning_info1.columns
    warning_info1['type'] = 'condition1'
    warning_info2['type'] = 'condition2'
    warning_info3['type'] = 'condition3'
    all_warning_info = pd.concat([warning_info1, warning_info2, warning_info3])
    all_warning_info.drop_duplicates(subset=old_columns, inplace=True)
    all_info = []
    for key, values in all_warning_info.groupby('type'):
        del values['type']
        values.reset_index(drop=True, inplace=True)
        all_info.append(values)

    for info, file_name,title in zip(all_info, warning_names,warning_titles):
        info = info[export_columns]
        info.sort_values(by='shop_sales', inplace=True, ascending=False)
        info.reset_index(drop=True, inplace=True)
        info['spend_rate'] = info['spend_rate'].apply(lambda x: str(round(x * 100, 2)) + '%')
        info['shop_sales'] = info['shop_sales'].apply(lambda x: int(x))


        # 输出为html
        export_file_name = os.path.join('C:/Users/Administrator/Desktop', f'{file_name}.html')
        with codecs.open(export_file_name, 'w', 'gbk') as html_file:
            html_file.write(f'{title}\n')
            html_file.write(info.to_html(header=True, index=True))

        # # 存储到redis中
        # info_2_list = info.to_dict(orient='list')
        # for key, value in info_2_list.items():
        #     key = file_name + '_' + key
        #     red4.ltrim(key, 1, 0)
        #     red4.rpush(key, *value)


if __name__ == "__main__":
    # 从数据库加载station_info信息
    db_station_info = db_download_station_info(db='team_station', table='only_station_info', ip='192.168.8.180',
                                               port=3306,
                                               user_name='zhangyupeng', password='zhangyupeng')
    # 初始化station_info信息
    db_station_info = init_db_station_info(db_station_info)
    # 输出满足条件的警告表
    export_warning_station_info(db_station_info)
