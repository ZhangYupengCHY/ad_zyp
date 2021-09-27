#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/5/3 0003 15:54
# @Author  : Zhang YP
# @Email   : 1579922399@qq.com
# @github  :  Aaron Ramsey
# @File    : api_walmart_getitemperformance.py


# -*- coding: utf-8 -*-
"""
Proj: ad_helper
Created on:   2020/7/2 14:36
@Author: RAMSEY

"""

import json
import requests
from datetime import datetime


import pandas as pd
from my_toolkit import conn_db,public_function


"""
查询速卖通中信息
"""


@public_function.loop_func(update_time=14)
def get_walmart_account():
    """
    获取walmart账号
    Returns
    -------

    """

    def get_walmart_token():
        """获取walmart平台的token"""
        request_url = "http://python2.yibainetwork.com/yibai/python/services/jwt/token?iss=sz_sales_ad_data_analysis&secret=hjaq24.cdta91ldDaqlcdqkb"
        return json.loads(requests.get(request_url).content)['jwt']

    walmartAccountUrl = "http://bi.yibainetwork.com:8000/apiPython/get_walm_id_name"
    walmartJwt = get_walmart_token()
    params = {'jwt':walmartJwt}
    response = requests.get(walmartAccountUrl,params=params)
    if response.status_code!=200:
        raise ConnectionError(f'cant connect walmart account url,code :{response.status_code}')
    responseContent = json.loads(response.content)
    if responseContent['status'] !=200:
        raise ConnectionError(f"request walmart account error,detail is {responseContent['msg']}")
    # 将walmart账号信息上传到数据库中
    walmartAccountInfo = pd.DataFrame(responseContent['msg'])
    if walmartAccountInfo.empty:
        return
    # 添加时间列
    walmartAccountInfo['updatetime'] = datetime.now().replace(microsecond=0)
    walmartAccountInfo.rename(columns={'s_name':'account'},inplace=True)
    walmartAccountTableName = 'walmart_account'
    conn_db.to_sql_replace(walmartAccountInfo,walmartAccountTableName)


if __name__ == '__main__':
    get_walmart_account()
