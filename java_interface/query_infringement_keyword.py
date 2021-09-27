"""
通过侵权接口查询关键词是否侵权
"""
import re
import requests
import json
from requests.auth import HTTPBasicAuth
import time
import hashlib
import pandas as pd
import sqlalchemy
from sqlalchemy.dialects import mysql as sqlType
from datetime import datetime
import gc
from retry import retry
from concurrent.futures import ThreadPoolExecutor, as_completed

from my_toolkit import my_api,public_function

THREAD_NUM = 10
THREAD_POOL = ThreadPoolExecutor(THREAD_NUM)

token = my_api.CompanyApiBase.get_java_token()

requestTokenTime = 0


def api_query_infringement_keyword(queryWords,url=r'http://rest.java.yibainetwork.com/risk/yibaiInfringementLibrary/findLibrary'):
    """
    通过侵权接口查询关键词是否侵权
    """
    global token,requestTokenTime
    if not isinstance(queryWords,(str,list)):
        raise TypeError(f'qeuryWords type must str or list,but yours is {type(queryWords)}.')
    params = {
        'access_token': token
    }
    if isinstance(queryWords,list):
        """用逗号拼接查询的关键词"""
        queryWordsStr = ','.join(queryWords)
        queryWordsStr = queryWordsStr.strip(', ')
    else:
        queryWordsStr = queryWords.strip(', ')
    # 查询词
    jsonParmas = {
        'keyword': queryWordsStr,
        'size':5000,
    }
    headers = {'Content-Type': 'application/json;charset=UTF-8'}
    infringementResponse = requests.post(url, params=params, json=jsonParmas,headers=headers)

    if infringementResponse.status_code != 200:
        if infringementResponse.status_code == 401:
            """token过期,重新请求token"""
            requestTokenTime += 1
            if requestTokenTime >= 3:
                raise ConnectionError(
                    f'url:{url}连接失败,错误代码:{infringementResponse.status_code},详情:token无效')
            token = my_api.CompanyApiBase.get_java_token()
            api_query_infringement_keyword(queryWords)
        else:
            print(keywordsQueue.size())
            print(json.loads(infringementResponse.content))
            print(json.loads(infringementResponse.content)['message'])
            raise ConnectionError(f'url:{url}连接失败,错误代码:{infringementResponse.status_code},详情:{infringementResponse}')
    requestTokenTime = 0
    # 返回的结果中,关键词出现,旧说明此词就是侵权词(todo 后期可以添加平台)
    infringementKws = set([record['keyword'].lower() for record in json.loads(infringementResponse.content)['data']['records']])
    if isinstance(queryWords,list):
        return {kw:True if kw.lower() in infringementKws else False for kw in queryWords}
        # return {kw:True for kw in queryWords if kw.lower() in infringementKws}
    if isinstance(queryWords,str):
        if queryWords.lower() in infringementKws:
            return True
        else:
            return False


def thread_request_keywords_status(size=100,interval=0.05):
    """
    多线程请求站点每月到货数据
    :return:
    """
    while 1:
        all_task = []
        for _ in range(THREAD_NUM):
            if keywordsQueue.size() == 0:
                break
            elif keywordsQueue.size() >= size:
                queryWords = keywordsQueue.dequeue_items(size)
            else:
                queryWords = keywordsQueue.dequeue_items(keywordsQueue.size())
            all_task.append(THREAD_POOL.submit(api_query_infringement_keyword,(queryWords)))
        for future in as_completed(all_task):
            result = future.result()
            if isinstance(result,dict):
                keywordsQueueInfringementStatus.update(result)
        time.sleep(interval)
        if keywordsQueue.empty():
            break


# @public_function.run_time
def query_kws_infringement_status(keywords:list,size=5000):
    """批量查询关键词的是否侵权"""
    if not isinstance(keywords,list):
        raise TypeError(f'批量查询侵权关键词类型应该为list,而输入的类型为:{type(keywords)}.')
    #采用多线程去请求
    # 请求关键词队列
    global keywordsQueue, keywordsQueueInfringementStatus

    keywordsQueue = public_function.Queue()
    keywordsQueue.enqueue_items(keywords)
    # 请求关键词队列返回结果
    keywordsQueueInfringementStatus = {}
    thread_request_keywords_status(size=size)
    return keywordsQueueInfringementStatus


if __name__ == '__main__':
    # path = r"C:\Users\Administrator\Desktop\21.09.22 ASTYER_FR ST报表.xlsx"
    # data = pd.read_excel(path,sheet_name='同erp_sku下其他seller_sku出单词')
    # kws = list(set(data['customer_search_term']))
    # print(len(kws))
    kws = list(set(["mercedes","vespa","jeep wrangler","rav4","royal enfield","tundra","tundra","amg","skunk2","head up display","akai","jcb","suzuki","turbo","turbo","turbo","turbo","turbo","turbo","ford focus","powertec","powertec","ktm","cafe racer","cafe racer","head up display","head up display","head up display","head up display","head up display","head up display","head up display","head up display","head up display","head up display","head up display","head up display","head up display","head up display","head up display","head up display","head up display","head up display","head up display","head up display","head up display","head up display","harley","audi","audi","sportster","ram mount","ram mount","ram mount","ram mounts","ram mount","ram mount","briggs stratton","tecumseh","tecumseh","renault","moto guzzi","creeper","creeper","iveco","honda","cafe racer","cafe racer","cafe racer","cafe racer","hyosung","akrapovic","akrapovic","cafe racer","cafe racer","mini cooper","sparco","passat cc","ingersoll rand","alfa romeo","gti","dsg","dsg","cafe racer","cafe racer","lambda","bmw","cafe racer","amarok","amarok","cafe racer","cafe racer","cafe racer","cafe racer","passat","gmc","gmc","gmc","subaru","subaru","subaru","subaru","elm327","elm327","seat","jeep wrangler","mercedes","bmw","bmw","massey ferguson","massey ferguson","massey ferguson","massey ferguson","akrapovic","akrapovic","ktm","ktm","alfa romeo","jeep wrangler","jeep wrangler","lambda","ducati scrambler","shock absorber","power inverter","tornador","ford focus","mini cooper","turbine","royal enfield","iseki","land rover","compustar","palette","dsg","touareg","mini cooper","mini cooper","mini cooper","hb3","shock absorber","subaru","ebike","cummins","honda","honda","audi","audi","audi","audi","pcx","pcx","jaguar","jaguar","seadoo","royal enfield","royal enfield","seat ibiza","nissan","nissan","cafe racer","cafe racer"]))
    result = query_kws_infringement_status(kws)
    print(result)