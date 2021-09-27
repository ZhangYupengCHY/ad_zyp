"""
同时执行多项任务:

"""
from datetime import datetime,timedelta
import time
import requests
import warnings
import gc
import shutil
import rsa, requests, os, redis, zipfile, time, re, xlsxwriter
# import Crypto.PublicKey.RSA
import base64, pymysql
import sqlalchemy.dialects.mysql as sqlTypes
import random
import json
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed,ProcessPoolExecutor
from retrying import retry
from random import randint
from hashlib import md5
from sqlalchemy import create_engine
from retry import retry
from requests.auth import HTTPBasicAuth
import hashlib
import paramiko as pm
import stat
from sqlalchemy.dialects.mysql import MEDIUMINT,VARCHAR,DATETIME
from sqlalchemy.dialects import mysql as sqlType


from my_toolkit import chinese_check,sql_write_read,public_function,commonly_params,change_rate,process_files,init_station_report,myZip,conn_db,process_station,my_api
from java_interface import account_id_index


RUNPYS= ['java_interface/query_station_shipment_monthly.py', 'ad_perf/ad_sku_have_ordered.py','java_interface/aliexpreee_account.py','java_interface/query_get_follow_up_list.py',
           'api_request_files/request_primary_listing.py', 'search_sku_asin/sku_map_upload_2_ad_server.py','simple_task/clear_station_older_files.py',
           'stations_not_take_over_sales.py', 'java_interface/query_nickname.py', 'java_interface/query_account_status.py','java_interface/query_company_organization.py',
           'java_interface/query_account_short_name.py','java_interface/account_id_index.py','java_interface/api_get_cj_amazon_account.py','api_request_files/api_walmart_get_account.py',
         'store_ad_info/store_camp_2_mysql.py','process_seller_upload_stations_five_files.py']

Thread_Pool = ThreadPoolExecutor(len(RUNPYS))
BASE_PATH = os.path.dirname(__file__)


# 实现队列
class Queue:
    def __init__(self):
        self.items = []

    def enqueue(self, item):
        self.items.append(item)

    def dequeue(self):
        return self.items.pop(0)

    def empty(self):
        return self.size() == 0

    def size(self):
        return len(self.items)


def run_py():
    if queue.size() == 0:
        return
    py_name = queue.dequeue()
    py_name = os.path.join(BASE_PATH, py_name)
    py_name = py_name.replace('\\', '/')
    print(py_name)
    with open(py_name, 'r', encoding='utf-8') as f:
        try:
            exec(f.read())
        except Exception as e:
            print(f"运行:{py_name}有问题.")
            print(e)


# 多线程执行test1.py和test2.py
def process_read_file():
    global queue
    # 日常运行脚本
    queue = Queue()
    [queue.enqueue(py) for py in RUNPYS]
    while 1:
        all_task = []
        for one_page in range(len(RUNPYS)):
            all_task.append(Thread_Pool.submit(run_py))
        for future in as_completed(all_task):
            future.result()
        if queue.size() == 0:
            break


if __name__ == "__main__":
    process_read_file()
