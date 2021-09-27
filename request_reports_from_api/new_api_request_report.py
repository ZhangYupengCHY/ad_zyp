# -*- coding: utf-8 -*-
"""
Created on Wed Sep 18 09:48:05 2019

@author: Administrator
"""

import requests
# import win32api
import os
import zipfile
from datetime import datetime
import datetime as dt
import numpy as np
import pandas as pd
import shutil
import re
import time
import json
import logging
import matplotlib.pyplot as plt
import matplotlib.dates as mdate
import glob
import pymysql
import threading
from multiprocessing import process, Pool
from request_reports_from_api.stg_calc_2_sql import write_to_mysql, load_account_info, load_dates
from request_reports_from_api.plot_account import save_accounts_png
from request_reports_from_api.calc_bulk import get_imp_cpc


def request_accounts():
    global camp, site, account, request_type, today_date, zip_dir, all_exist_dates
    all_exist_dates = load_dates()
    camps_list = db_download_station_names()
    camps_list = [station[:-2] + 'sp' if station[-2:] in ['es', 'ES'] else station for station in camps_list]
    print('站点一共为{}.请耐心等待...'.format(len(camps_list)))
    today_date = datetime.now().date().strftime("%Y-%m-%d")
    # file = r'C:\Users\Administrator\Desktop\camp_request_lost_dates.txt'
    # if os.path.exists(file):
    #     os.remove(file)
    logging.basicConfig(filename="new_api_request.log", filemode="a+",
                        format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
                        datefmt="%Y-%M-%d %H:%M:%S", level=logging.ERROR)
    request_types = ['campaign', 'business', 'search']
    times = 0
    zip_dir = "E:/ad_zyp/request_reports_from_api/reports/"
    if not os.path.exists(zip_dir):
        os.makedirs(zip_dir)
    for camp in camps_list:
        try:
            for request_type in request_types:
                site = camp[-2:]
                account = camp[:-3]
                report_end_date = datetime.now().date()
                report_start_date = (report_end_date - dt.timedelta(days=40))
                if request_type == 'campaign':
                    write_files(site=site, account=account, report_start_date=report_start_date,
                                report_end_date=report_end_date, type=request_type)
                    c = get_imp_cpc(zip_dir, camp)
                    write_to_mysql(c)
                # elif request_type == 'search':
                #     write_files(site=site, account=account, report_start_date=report_start_date,
                #                 report_end_date=report_end_date, type=request_type)
                # elif request_type == 'business':
                #     write_files(site=site, account=account, type=request_type)
                else:
                    pass
            shutil.rmtree(base_dir)
            times += 1
            print("oky!!!{}已经完成,一共完{}个,还剩{}.".format(camp, times, len(camps_list) - times))
        except:
            print('{}表有问题'.format(camp))


# 过滤那些已经存在的站点日期
def filter_account(account_site, file_list, exist_account_site_dates):
    file_dates = set([parse_date(file_name) for file_name in file_list])
    the_account = account_site[:-3]
    the_site = account_site[-2:]
    exist_account_site = [i + "_" + j for i, j in
                          zip(exist_account_site_dates['account'], exist_account_site_dates['site'])]
    if account_site not in exist_account_site:
        return file_list
    else:
        exist_dates = set(exist_account_site_dates['date'][(exist_account_site_dates['account'] == the_account) & (
                exist_account_site_dates['site'] == the_site)])
        new_dates = list(file_dates - exist_dates)
    file_list = [file_name for file_name in file_list if parse_date(file_name) in new_dates]
    return file_list


# 将请求的站点写入到临时文件中
def write_files(**kwargs):
    global station_name, data_url, base_dir
    station_name = camp
    download_url = "http://120.78.243.154//services/advertising/reportdownload/{}".format(kwargs['type'])
    base_dir = zip_dir + '{}'.format(camp).upper()
    if not os.path.exists(base_dir):
        os.mkdir(base_dir)
    if request_type in ['campaign', 'search']:
        post_load = {
            'site': kwargs['site'],
            'account': kwargs['account'],
            'report_start_date': kwargs['report_start_date'],
            'report_end_date': kwargs['report_end_date'],
        }
    else:
        post_load = {
            'site': kwargs['site'],
            'account': kwargs['account'],
        }
    response = requests.post(download_url, data=post_load).content
    data_url = response.decode()
    if json.loads(data_url)['code'] == 1000:
        json_data = json.loads(data_url)['data']
        file_list = [camp_file['file'] for camp_file in json_data]
        file_list = filter_account(station_name, file_list, all_exist_dates)
        if len(file_list) == 0:
            return
        for file in file_list:
            request_n_write_one_file(file)
    else:
        pass


def request_n_write_one_file(file):
    try:
        file_r = requests.get(file)
    except:
        file_r = []
        try_times = 1
        while 1:
            try_times += 1
            # print("第{}次请求{}...".format(try_times, file))
            try:
                file_r = requests.get(file)
                break
            except:
                time.sleep(0.5)
                pass
            if try_times > 3:
                # logging.exception("\tTYPE1.1: CANOT REQUEST: {}_{}  DAY: {}".format(account, site, file[-43:-33]))
                break
    if not file_r:
        return
    file_name = file.split('/')[-1]
    status_code = file_r.status_code
    if status_code == 200:
        out_content = file_r.content  # {'data': file_r.content, 'msg': 'complete'}
    else:
        return  # {'data': "", 'msg': status_code}
    with open(base_dir + '/' + file_name, 'wb') as f:
        f.write(out_content)


# 获得日期字符串
def parse_date(file_name):
    file_date = re.findall('2019.[0-9]{2}.[0-9]{2}', file_name)
    file_date = file_date[-1]
    if len(file_date) == 10:
        file_date = file_date[0:4] + '-' + file_date[5:7] + '-' + file_date[-2:]
    if len(file_date) == 8:
        file_date = file_date[0:4] + '-' + file_date[4:6] + '-' + file_date[-2:]
    return file_date


# 读取站点信息
def read_account(path):
    account_info = pd.read_excel(path, sheet_name='Sheet2')
    account_name = account_info['account_num']
    new_accounts = []
    for i, j in zip(account_name, account_info['site']):
        # if j == 'sp' or j == 'SP':
        #     j = 'es'
        new_account = i + '_' + j
        new_accounts.append(new_account)
    return new_accounts


def load_mysql():
    all_info = load_account_info()
    return all_info


"""重新定义带返回值的线程类"""


class MyThread(threading.Thread):
    def __init__(self, func, args=()):
        super(MyThread, self).__init__()
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            return self.result
        except Exception:
            return None


def db_download_station_names(db='team_station', table='only_station_info', ip='127.0.0.1',
                              port=3306, user_name='marmot', password='marmot123') -> list:
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


# 主循环，遍历
if __name__ == '__main__':
    while 1:
        t0 = datetime.now()
        start_hour = [20, 21, 22, 23, 0]
        if t0.hour in start_hour:
            print('程序开始运行 :{}...'.format(t0))
            request_accounts()
            t1 = datetime.now()
            print(t1 - t0)
            if t1.hour in start_hour:
                continue
                print("继续运行...")
            else:
                sleep_hour = (20 - t1.hour)
                sleep_sec = sleep_hour * 3600
                print("休眠{}小时...Zzzzz".format(sleep_hour))
                print("晚上10点开始运行...")
                time.sleep(sleep_sec)
        else:
            sleep_hour = (20 - t0.hour)
            sleep_sec = sleep_hour * 3600
            print("休眠{}小时...Zzzzz".format(sleep_hour))
            print("晚上10点开始运行...")
            time.sleep(sleep_sec)
