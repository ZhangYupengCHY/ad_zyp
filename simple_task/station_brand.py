# -*- coding: utf-8 -*-
"""
Proj: recommend
Created on:   2020/1/15 18:58
@Author: RAMSEY

Standard:  
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""

import pandas as pd
import os, zipfile, shutil, glob
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

THREAD_POOL = ThreadPoolExecutor(4)
PROCESS_POOL = ProcessPoolExecutor(2)


# 解压文件包
def unzip_dir(zip_dir: 'full_dir', station_zipped_folder):
    z = zipfile.ZipFile(zip_dir, "r")
    # 打印zip文件中的文件列表
    file_name_list = z.namelist()
    writer_folder = os.path.join(station_zipped_folder, os.path.basename(zip_dir)[:-4])
    if os.path.exists(writer_folder):
        shutil.rmtree(writer_folder)
    os.makedirs(writer_folder)
    file_name_list = [file for file in file_name_list if file.find('/') == -1]
    camp_file = [file for file in file_name_list if 'bulk' in file.lower()]
    if camp_file:
        camp_file = camp_file[0]
        content = z.read(camp_file)
        with open(writer_folder + '/' + camp_file, 'wb') as f:
            f.write(content)


# 读取站点的商标信息
def read_camp_brand_info(camp_dir: 'full_dir', station_name) -> pd.DataFrame:
    # 无广告报表
    if not camp_dir:
        return pd.DataFrame([])
    """
    camp_file_info = pd.ExcelFile(camp_dir)
    brand_sheet_name = 'Sponsored Brands Campaigns'
    if brand_sheet_name not in  camp_file_info.sheet_names:
        print(f"error1: {camp_dir}没有Sponsored Brands Campaigns表! ")
        return
    camp_data = camp_file_info.parse(brand_sheet_name)
    if camp_data.empty:
        print(f"error2: {camp_dir}Sponsored Brands Campaigns表没有数据! ")
        return
    need_columns = ['Campaign','Brand Name','Brand Logo Asset ID']
    if not set(need_columns).issubset(set(camp_data.columns)):
        lost_columns = need_columns - set(camp_data.columns)
        print(f"error3: {camp_dir}Sponsored Brands Campaigns表缺失{lost_columns}列 ")
        return
    """
    account = station_name[:-3]
    site = station_name[-2:]
    camp_brand_data = pd.read_excel(camp_dir, sheet_name='Sponsored Brands Campaigns')
    usecols = ['campaign', 'brand name', 'brand logo asset id']
    columns_name = [i.lower().replace('_',' ').strip() for i in camp_brand_data.columns]
    camp_brand_data.columns = columns_name
    try:
        camp_brand_data = camp_brand_data[usecols]
    except:
        return pd.DataFrame([])
    camp_brand_data = camp_brand_data[~pd.isna(camp_brand_data['brand name'])]
    if camp_brand_data.empty:
        return pd.DataFrame([])
    camp_brand_data['account'] = account
    camp_brand_data['site'] = site
    camp_brand_data = camp_brand_data[['account','site','campaign', 'brand name', 'brand logo asset id']]
    return camp_brand_data


def get_station_name() -> 'stations_zip_name':
    dirname = r"F:\station_folder"
    stations_name = os.listdir(dirname)
    return stations_name


def get_station_brand(stations_name):
    all_station_brand_data = pd.DataFrame(columns=['account','site','campaign', 'brand name', 'brand logo asset id'])
    while len(stations_name):
        station_zip_dir = stations_name.pop(0)
        dirname = r"F:\station_folder"
        station_dir = os.path.join(dirname, station_zip_dir)
        station_zipped_folder = r'F:\station_folder_temp'
        station_name = station_zip_dir.split('.')[0]
        print(f"正在处理:{station_name}")
        writer_folder = os.path.join(station_zipped_folder, station_name)
        # 解压站点的广告报表到临时文件下
        unzip_dir(station_dir, station_zipped_folder)
        camp_file = glob.glob(writer_folder + '/*bulk*')
        if camp_file:
            try:
                one_station_brand_info = read_camp_brand_info(camp_file[0],station_name)
            except Exception as e:
                print(e)
                continue
            shutil.rmtree(writer_folder)
            if not one_station_brand_info.empty:
                all_station_brand_data = all_station_brand_data.append(one_station_brand_info)
                print(f"处理完毕:{station_name}")
    return all_station_brand_data



def thread_read_file():
    all_task = []
    for one_page in range(2):
        all_task.append(THREAD_POOL.submit(get_station_brand))
    for future in as_completed(all_task):
        data = future.result()

    return "complete"


def process_read_file():
    while 1:
        all_task = []
        for one_page in range(2):
            all_task.append(PROCESS_POOL.submit(thread_read_file))
        for future in as_completed(all_task):
            data = future.result()


if __name__ == "__main__":
    global stations_name
    stations_name = get_station_name()
    redu = get_station_brand(stations_name)
    redu.to_excel(r"C:\Users\Administrator\Desktop\station_brand_info.xlsx",index=False)


