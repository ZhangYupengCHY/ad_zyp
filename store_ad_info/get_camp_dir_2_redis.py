# -*- coding: utf-8 -*-
"""
Proj: AD-Helper1
Created on:   2019/12/12 21:30
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
import os, time
from datetime import datetime, timedelta

redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=1, decode_responses=True)
red = redis.StrictRedis(connection_pool=redis_pool)
station_folder = r"F:\station_folder"


def refresh_cam_dir():
    processed_station_info = set()
    while 1:
        # 目前文件夹下的所有压缩文件
        station_zipped_list = os.listdir(station_folder)
        # 将目前文件夹的今日的所有站点和站点压缩文件组合成一个唯一的字符串
        now_date = datetime.now().date()
        # now_date = datetime.now().date()
        stations_dir = [os.path.join(station_folder, station) for station in station_zipped_list]
        # station_info = set(
        #     [station_dir + '_' + time.ctime(os.path.getmtime(station_dir)) for station_dir in
        #      stations_dir])
        station_info = set(
            [station_dir + '_' + time.ctime(os.path.getmtime(station_dir)) for station_dir in stations_dir if
             datetime.strptime(time.ctime(os.path.getmtime(station_dir)), '%a %b %d %H:%M:%S %Y').date() == now_date])
        # 需要处理的今天站点信息
        needed_process_station = station_info - processed_station_info
        # 需要处理站点的压缩包路径
        needed_process_station_zip_namelist = [station[:-25] for station in
                                               needed_process_station]
        if needed_process_station_zip_namelist:
            # red.ltrim('cam_zip_dir', 1, 0)
            red.rpush('cam_zip_dir', *needed_process_station_zip_namelist)
            processed_station_info.update(needed_process_station)
            print(f"此次更新{len(needed_process_station_zip_namelist)}个站点...")
        else:
            # print("暂时没有站点更新，休息1分钟...")
            time.sleep(60)
        while datetime.now().hour == 1:
            print("进入到1点，早上9点再更新...")
            time.sleep(28800)
        # print("finish step1:将站点名路径写入到redis中.")


if __name__ == "__main__":
    refresh_cam_dir()
