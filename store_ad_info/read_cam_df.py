# -*- coding: utf-8 -*-
"""
Proj: AD-Helper1
Created on:   2019/12/12 21:14
@Author: RAMSEY

Standard:
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""

import redis
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import sys, os, pandas, time, gc
from search_sku_or_asin import unzip_dir, get_camp_file_dir
from datetime import datetime

sys.path.append(r"E:\ad_zyp\price_dist\my_toolkit")
from read_campaign import read_campaign
from init_campaign import init_campaign

redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=1, decode_responses=True)
red = redis.StrictRedis(connection_pool=redis_pool)
THREAD_POOL = ThreadPoolExecutor(8)
PROCESS_POOL = ProcessPoolExecutor(2)
station_zipped_folder = r"F:\station_folder"
station_temp_folder = r"E:\ad_zyp\search_sku_asin\temp"


# 读取广告报表
def my_read_campaign(need_columns='all'):
    """
    读取广告报表
    :param file_dir:广告报表的全路径
    :param file_site:广告报表的国家(用于翻译)
    :param need_columns:需要的列(默认为全列)
    :return:需要的广告报表数据列
    """
    file_zip_dir = red.lpop('cam_zip_dir')
    if file_zip_dir:
        try:
            unzip_dir(file_zip_dir)
            station_name = os.path.basename(os.path.splitext(file_zip_dir)[0])
            site_name = file_zip_dir[-6:-4]
            ad_dir = os.path.join(station_temp_folder, station_name)
            file_dir = get_camp_file_dir(ad_dir)
            # print(file_dir)
            if file_dir:
                # 读取excel内容
                file_data = read_campaign(file_dir, site_name)
                file_data = init_campaign(file_data, site_name.upper(), 'empty')
                if file_data.empty:
                    print("error2: {}的广告报表的数据为空".format(os.path.dirname(file_dir)))
                # 选择全部列
                if need_columns.lower() == 'all':
                    file_name = file_dir.replace('xlsx', 'h5').replace('XLSX', 'h5')
                    # print(file_name)
                    file_data.to_hdf(file_name, key='df')
                    red.rpush('camp_h5', os.path.dirname(file_name))
                    print(f"{station_name}广告报表成功转换成h5...")
                    # return file_data
                # 选择部分列
                if set(need_columns).issubset(set(file_data.columns)):
                    file_data = file_data[need_columns]
                    file_name = file_dir.replace('xlsx', 'h5').replace('XLSX', 'h5')
                    # print(file_name)
                    file_data.to_hdf(file_name, key='df')
                    red.rpush('camp_h5', os.path.dirname(file_name))
                    print(f"{station_name}广告报表成功转换成h5...")
                    # return file_data
                # else:
                #     lost_columns = set(need_columns) - set(file_data.columns)
                #     print("error3: {}的广告报表的缺少{}列".format(os.path.dirname(file_dir), lost_columns))
        except Exception as e:
            print(e)
            # red.rpush('cam_zip_dir', file_zip_dir)
            # print(f"{station_name}广告报表转换成h5失败")
    else:
        # print("暂时没有站点需要写入到数据库中，休息1分钟...!!!")
        time.sleep(60)
        # print("暂时没有excel转换成h5，休息1分钟...")
        while datetime.now().hour == 1:
            print("进入到1点，早上9点再更新...")
            time.sleep(28800)



def thread_read_file():
    all_task = []
    for one_page in range(4):
        all_task.append(THREAD_POOL.submit(my_read_campaign))
    for future in as_completed(all_task):
        future.result()


def process_read_file():
    while 1:
        # all_station_sum = len(red.lrange('cam_zip_dir', 0, -1))
        # print(f'一共存储{all_station_sum}个站点转换成h5.')
        all_task = []
        for one_page in range(2):
            all_task.append(PROCESS_POOL.submit(thread_read_file))
        for future in as_completed(all_task):
            future.result()


if __name__ == "__main__":
    process_read_file()
    # while 1:
    #     my_read_campaign()
    #     while datetime.now().hour == 1:
    #         print("进入到1点，早上9点再更新...")
    #         time.sleep(28800)
