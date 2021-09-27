"""
远程接口请求5表
"""
import hashlib

"""
同时执行多项任务:

"""
from datetime import datetime, timedelta

import rsa, requests, os, redis, zipfile, time, re, xlsxwriter

import json
import numpy as np
import pandas as pd
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import paramiko as pm

from my_toolkit import chinese_check, sql_write_read, public_function, commonly_params, change_rate, process_files, \
    init_station_report

"""
    通过接口请求站点5表,若5表缺失,则需要提醒销售去下载补充
    1.通过接口获取cp,br,st表的接口获取的报表日志,通过日志链接去请求站点报表;通过其他接口获取ao,ac表
    2.将报表获取的报表类型上传到redis中(站点类型以及日期),同时将缺失的部分同步显示到销售端
    3.销售将缺失的报表上传,同时将上传的站点报表信息上传到redis中
    广告报表redis有效时长为两天
    其他报表redis有效时长为四天
    只将距离5~8天的站点开放为销售下载报表
"""

REMOTE_SAVE_FOLDER = r'F:\remote_get_five_files'


def load_account_id():
    # 连接账号数据表
    _connMysql = sql_write_read.QueryMySQL()
    accountTable = 'account_id_index'
    queryResult = _connMysql.read_table(accountTable)
    _connMysql.close()
    if len(queryResult.index) == 0:
        # 假定账号为10000个
        raise(f'站点索引表为空:{accountTable}')
    else:
        return queryResult


def request_jwt():
    """
    通过账号和密码的方式获取token
    Parameters
    ----------
    url :
    jss :
    secret :

    Returns
    -------

    """
    url = "http://python2.yibainetwork.com/yibai/python/services/jwt/token?iss=&secret="
    iss = 'sz_sales_ad_data_analysis'
    secret = 'hjaq24.cdta91ldDaqlcdqkb'
    params = {'iss': iss, 'secret': secret}
    response = requests.get(url, params=params)
    return json.loads(response.content)['jwt']


def get_account_monthly_data():
    # 保存文件夹路径
    saveFolder = r"F:\remote_get_station_monthly_data"
    if allAccountIdQueue.empty():
        return
    requestAccountId = allAccountIdQueue.dequeue()
    if not isinstance(requestAccountId,int):
        try:
            requestAccountId = int(requestAccountId)
        except Exception as e:
            raise e
    print(f'开始请求{requestAccountId}月数据。')
    params = {
        "account_id": requestAccountId,
    }
    response = requests.post(request_url, json=params)
    if response.status_code != 200:
        print(f'月数据请求错误,错误代码:{response.status_code},时间:{datetime.now()}')
    # 文件流
    html_str = response.content
    htmlData = json.loads(html_str.decode())['data']
    if not htmlData:
        # 暂时先打印
        print(f'{requestAccountId}月数据为空.')
    else:
        htmlDataDf = pd.DataFrame(htmlData)
        accountSaveFolder = os.path.join(saveFolder,allAccountIdDict.get(str(requestAccountId),f'{requestAccountId}没有对应站点名'))
        if not os.path.exists(accountSaveFolder):
            os.mkdir(accountSaveFolder)
        accountSavePath = os.path.join(accountSaveFolder,'br.csv')
        htmlDataDf.to_csv(accountSavePath,index=False)
    print(f'完成请求{requestAccountId}月数据。')


def thread_get_account_monthly_data():
    """
    多线程通过接口请求站点月数据
    :return:
    """
    while 1:
        # 多线程
        all_task = []
        for _ in range(THREAD_NUM):
            all_task.append(THREAD_POOL.submit(request_monthly_br))
        for future in as_completed(all_task):
            future.result()
        if allAccountIdQueue.empty():
            # 关闭远程连接
            print(f'完成今天站点月数据报表请求.')
            break


def request_reports_log():
    """
    获取月数据
    """
    global allAccountIdQueue,request_url,allAccountIdDict
    global REMOTE_SAVE_FOLDER,THREAD_NUM,THREAD_POOL
    REMOTE_SAVE_FOLDER = r'F:\remote_get_five_files'
    THREAD_NUM = 4
    THREAD_POOL = ThreadPoolExecutor(THREAD_NUM)
    jwt = request_jwt()
    request_url = f"http://bi.yibainetwork.com:8000/bi/bigdata/ad/skudata?jwt={jwt}"
    # 加载全部站点
    allAccountIdInfo = load_account_id()
    allAccountId = list(allAccountIdInfo['id'])
    #站点id与站点名 字典
    allAccountIdDict = {id:accountName for id,accountName in zip(allAccountIdInfo['id'],allAccountIdInfo['account_name'])}
    allAccountIdQueue = public_function.Queue()
    allAccountIdQueue.enqueue_items(allAccountId)
    # 请求全部站点的月数据
    thread_get_account_monthly_data()


def request_monthly_br():
    """
    通过接口获取br表
    Returns
    -------

    """

    # todo MD5加密,固定的写法
    def encrypt_MD5(params):
        m = hashlib.md5()
        m.update(params.encode("utf-8"))
        return m.hexdigest()

    def natural_key(s):
        return [int(s) if s.isdigit() else s for s in re.split(r'(\d+)', s)]


    # 保存文件夹路径
    saveFolder = r"F:\remote_get_station_monthly_data"
    requestAccountId = allAccountIdQueue.dequeue()
    if not isinstance(requestAccountId,int):
        try:
            requestAccountId = int(requestAccountId)
        except Exception as e:
            raise e
    # print(f'开始请求{requestAccountId}:br月数据。')

    params = {
        "account_id": requestAccountId,
        "year": 2021,
        "month": 1,
        "key": "amazon_ad",
        "secret": "3n#F29bx"
    }

    l = sorted(params.items(), key=lambda x: natural_key(x[0]))
    # l = sorted(params.items(), key=lambda x: x[0])
    query = map(lambda x: "{}={}".format(x[0], x[1]), l)
    query_string = '&'.join(list(query))
    sign = encrypt_MD5(query_string).upper()
    params.update({"sign": sign})
    del (params['secret'])
    del (params['key'])

    request_url = r"http://szf.yibai-it.com:33668/br/bydate/getByAccountId"

    response = requests.get(request_url, params=params)
    if response.status_code != 200:
        print(f'br月数据请求错误,错误代码:{response.status_code},时间:{datetime.now()}')
        return
    # 文件流
    html_str = response.content
    htmlData = json.loads(html_str.decode())['data']
    if not htmlData:
        # 暂时先打印
        print(f'{requestAccountId}月数据为空.')
    else:
        htmlDataDf = pd.DataFrame(htmlData)
        accountSaveFolder = os.path.join(saveFolder,allAccountIdDict.get(str(requestAccountId),f'{requestAccountId}没有对应站点名'))
        if not os.path.exists(accountSaveFolder):
            os.mkdir(accountSaveFolder)
        accountSavePath = os.path.join(accountSaveFolder,'br.csv')
        htmlDataDf.to_csv(accountSavePath,index=False)
        print(f'完成请求{requestAccountId}:br月数据。')


if __name__ == '__main__':
    # 请求月数据
    # request_reports_log()
    request_reports_log()
