#!/usr/bin/env python
# coding=utf-8
# author:marmot

import requests
import json
from datetime import datetime

from my_toolkit import public_function


# 转美金汇率

def change_current():
    """
    先从redeis中加载,如果redis中没有,则通过接口请求,然后再将结果存储在redis中
    Returns:

    """
    # 首先先检查redis中是否存在key
    red_conn = public_function.Redis_Store(db=0)
    red_conn = red_conn.red
    redis_keys = red_conn.keys()
    exchange_rate_key_sign = 'exchange_rate'
    exchange_rate_key = [key for key in redis_keys if exchange_rate_key_sign in key]
    if exchange_rate_key:
        exchange_rate_json = red_conn.get(exchange_rate_key[0])
        exchange_rate = json.loads(exchange_rate_json)
        red_conn.close()
        return exchange_rate
    # 从接口请求
    url = 'https://www.mycurrency.net/US.json'
    response_connect = requests.get(url)
    status_code = response_connect.status_code
    if status_code != 200:
        print('====================================================================')
        print('ERROR CURRENT API.CANT CONNECT "https://www.mycurrency.net/US.json" ')
        print('USE BACK CHANGE_CURRENT ')
        exchange_rate = {'US': 1.0, 'CA': 0.738, 'UK': 1.2483, 'DE': 1.1245, 'IT': 1.1245, 'ES': 1.1245, 'JP': 0.0093,
                         'FR': 1.1245, 'MX': 0.0447, 'IN': 0.0134, 'AU': 0.6945, 'AE': 0.2723,'CN':'0.1538'}
        print(f'使用的汇率为:{exchange_rate}')
        print('====================================================================')
        return exchange_rate
    response = json.loads(response_connect.text)
    response_rate = response['rates']
    response_rate_dict = {country_info['code']: country_info['rate'] for country_info in response_rate}
    country_current_dict = {'US': 'US', 'CA': 'CA', 'UK': 'GB', 'DE': 'EU', 'IT': 'EU', 'ES': 'EU', 'JP': 'JP',
                            'FR': 'EU', 'MX': 'MX', 'IN': 'IN', 'AU': 'AU', 'AE': 'AE','CN':'CN'}
    exchange_rate = {country_name: round(1 / response_rate_dict[current_code], 4) for country_name, current_code in
                     country_current_dict.items()}
    exchange_rate_json = json.dumps(exchange_rate)
    dateNow = datetime.now().strftime('%Y-%m-%d')
    exchange_rate_redis_key_now = exchange_rate_key_sign+'_'+dateNow
    red_conn.set(exchange_rate_redis_key_now, exchange_rate_json)
    # 删除redis中老key
    now_date = datetime.now().strftime('%Y-%m-%d')
    [red_conn.delete(key) for key in redis_keys if (exchange_rate_key_sign in key) and (now_date not in key)]
    red_conn.close()
    return exchange_rate


# if __name__ == '__main__':
#     change_current()