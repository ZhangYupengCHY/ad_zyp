# -*- coding: utf-8 -*-
"""
Proj: AD-Helper1
Created on:   2019/12/20 9:24
@Author: RAMSEY

Standard:
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""
import gc

"""
亚马逊重点listing数据拉取接口
1.请求地址：
    http://amazon.yibainetwork.com/services/amazon/amazonkeylisting/run
2.请求方法
    POST传参
3.请求参数
    page:请求页码
    sign:签名（md5 secret_key+time）
    time:请求时间linux时间戳
4签名规则
  md5(secret_key+time)  
"""

import time, os, requests, json, re, pymysql
import numpy as np
import pandas as pd
from hashlib import md5
from sqlalchemy import create_engine
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

from my_toolkit import sql_write_read,public_function


# 通过参数请求重点listing数据
@public_function.loop_func(update_time=18)
def request_key_listing_update():
    column_name = ['id', 'account_id', 'account_name', 'account_name_en', 'asin1', 'seller_sku',
                   'fulfillment_channel', 'listing_status', 'sku', 'product_status',
                   'review_rate', 'early_warning', 'listing_type', 'open_date', 'sku_sale', 'linelist_cn_name_degree_1',
                   'linelist_cn_name_degree_2', 'linelist_cn_name_degree_3', 'create_date', 'update_date', '利润', '利润率',
                   '利润更新时间', '3天销量', '7天销量',
                   '15天销量', '30天销量', '60天销量', '当前库存', '在途库存', '不可售库存']

    domain_name = "http://amazon.yibainetwork.com/"
    api = "services/amazon/amazonkeylisting/run"
    secret_key = '!@#$%~Ybai7895&$^^GoPingRun?'
    all_pages_data = pd.DataFrame(columns=column_name)


    page = 1
    while 1:
        # 1.得到请求参数sign
        now_timestamp = int(time.time())
        # sign为secret_key+time加密
        sign_str = (secret_key + str(now_timestamp)).encode()
        sign = md5(sign_str).hexdigest()
        full_request_api = os.path.join(domain_name, api)
        # 2.通过参数请求
        post_dict = {'page': page, 'sign': sign, 'time': now_timestamp}
        response = requests.post(full_request_api, params=post_dict).content.decode()
        # 如果stats为0,则跳出,有message字段
        if 'message' in response:
            print("请求异常")
            print("已经到最后一页，跳出。")
            break
        try:
            json_data = json.loads(response)['data']
        except Exception as e:
            print(f"第{page}页出错.")
            print(e)
            continue
        df_data = pd.DataFrame(json_data)
        # 将多级目录拆分
        df_data['linelist_cn_name_degree_1'] = [degree.split('/')[0] if len(degree.split('/')) >= 1 else '' for degree
                                                in df_data['linelist_cn_name']]
        df_data['linelist_cn_name_degree_2'] = [degree.split('/')[1] if len(degree.split('/')) >= 2 else '' for degree
                                                in df_data['linelist_cn_name']]
        df_data['linelist_cn_name_degree_3'] = [degree.split('/')[-1] if len(degree.split('/')) >= 3 else '' for degree
                                                in df_data['linelist_cn_name']]

        # 将字典列拆分
        df_profit_info = df_data['profit_info'].apply(pd.Series)
        df_sales = df_data['sales'].apply(pd.Series)
        df_stock_info = df_data['stock_info'].apply(pd.Series)

        df_final = pd.concat([df_data, df_profit_info, df_sales, df_stock_info], axis=1).drop(
            ['profit_info', 'sales', 'stock_info'], axis=1)

        # 插入一列,站点_国家(英文)
        df_final['account_name'] = df_final['account_name'].apply(lambda x: x.replace('站', '').replace('-', '_'))
        df_final['account_name_en'] = df_final['account_name'].apply(
            lambda x: ((re.sub("[^A-Za-z _]", '', x)) + '_' + public_function.COUNTRY_CN_EN_DICT[
                (re.sub("[A-Za-z _]", '', x))]).upper() if (re.sub("[A-Za-z _]", '',
                                                                   x)) in public_function.COUNTRY_CN_EN_DICT.keys() else x)

        # 修改列名
        df_final.rename(
            columns={'profit': '利润', 'profit_rate': '利润率', 'profit_date': '利润更新时间', 'd3': '3天销量', 'd7': '7天销量',
                     'd15': '15天销量', 'd30': '30天销量', 'd60': '60天销量', 'zk_stock': '当前库存', 'zt_stock': '在途库存',
                     'bks_stock': '不可售库存'}, inplace=True)

        # 删除sku为空的数据行
        df_final = df_final[~pd.isna(df_final['sku'])]
        df_final = df_final[df_final['sku'] != '']

        # 将product_status行中的字典json化
        df_final['product_status'] = df_final['product_status'].apply(
            lambda x: json.dumps(x) if isinstance(x, dict) else x)

        df_final = df_final[column_name]

        all_pages_data = pd.concat([all_pages_data, df_final])
        print(f"成功读取了{page}页。")
        page += 1

    now_datetime = str(datetime.now())
    all_pages_data['upload_time'] = now_datetime

    try:
        primary_listing_table_name = 'station_primary_listing'
        sql_write_read.to_table_replace(all_pages_data, primary_listing_table_name)
        print(f"{datetime.now()}:成功上传到数据库中。。。")
    except Exception as err:
        print(err)
        print(f"{datetime.now()}: 上传到数据库中失败。。。")

    del all_pages_data
    del df_final
    gc.collect()


if __name__ == "__main__":
    request_key_listing_update()
