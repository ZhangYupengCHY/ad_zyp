import gc
import json
import requests
from datetime import datetime
import pandas as pd

from my_toolkit import chinese_check,sql_write_read,public_function


"""
获取公司的账号名以及账号的简称
"""


@public_function.loop_func(update_time=15)
# @public_function.loop_func(update_time=16)
def request_erp_account():
    """
    接口一天一次请求erp上账号以及账号的简称,将请求结果保存到Mysql数据库中
    Returns
    -------

    """
    # 站点名中英文转换
    requestPageNum = 1
    allPageResult = pd.DataFrame()
    insertDataBaseColumns = ['account_name', 'account_name_cn', 'station', 'site', 'short_name']
    while 1:
        requestUrl = r"http://amazon.yibainetwork.com/services/api/advertise/Getshortname?token=877F52DE27FA8C737A7EB34E064981D3&page=%s&type=1"%(requestPageNum)
        response = requests.post(url=requestUrl,headers={'Content-type': 'text/plain; charset=utf-8'})
        if response.status_code != 200:
            raise ConnectionError(f"错误连接代码:{response.status_code}.\n无法通过api{requestUrl}\n获取账号信息")
        onePageResult = json.loads(response.content.decode('utf-8'))
        onePageDataList = json.loads(onePageResult['data'])
        if len(onePageDataList) == 0:
            break
        else:
            onePageDataDf = pd.DataFrame.from_records(onePageDataList)
            onePageDataDf['account_name_cn'] = onePageDataDf['account_name'].copy()
            onePageDataDf['site'] = [public_function.COUNTRY_CN_EN_DICT.get(chinese_check.extract_chinese(account).replace('站',''),None) for account in onePageDataDf['account_name']]
            onePageDataDf['station'] = [chinese_check.filter_chinese(account) if ~pd.isna(account) else "" for account in onePageDataDf['account_name']]
            onePageDataDf['station'] = [station.replace('-','_').replace(' ','_') for station in onePageDataDf['station']]
            onePageDataDf['account_name'] = onePageDataDf['station'].str.lower() + '_' + onePageDataDf['site'].str.lower()
            onePageDataDf = onePageDataDf[insertDataBaseColumns]
            allPageResult = pd.concat([allPageResult,onePageDataDf],ignore_index=True)
            # 循环
            requestPageNum += 1

    # 添加更新时间列
    allPageResult['update_datetime'] = datetime.now().replace(microsecond=0)
    # 将请求结果上传到Mysql数据库中
    accountShortNameMysqlTable = 'account_short_name'
    sql_write_read.to_table_replace(allPageResult,accountShortNameMysqlTable)

    # 将账号对照名更新到redis数据中
    allPageResult.drop_duplicates(subset=['account_name'],inplace=True)
    allPageResult.drop_duplicates(subset=['short_name'],inplace=True)
    allPageResult = allPageResult[(~pd.isna(allPageResult['short_name'])) & (~pd.isna(allPageResult['account_name']))]
    _connRedis = public_function.Redis_Store(db=0)

    accountStoreRedisSign = '账号简称正向'
    now_datetime = datetime.now().replace(microsecond=0).strftime('%Y-%m-%d_%H-%M-%S')
    accountShortDict = {str(accountName).lower():shortName.lower() for accountName,shortName in zip(allPageResult['account_name'],allPageResult['short_name'])}
    # 删除历史的redis key
    [_connRedis.delete(key) for key in _connRedis.keys() if accountStoreRedisSign in key]
    accountRedisKey = accountStoreRedisSign+'_'+now_datetime
    _connRedis.hmset(accountRedisKey, accountShortDict)

    accountStoreRedisReverseSign = '账号简称逆向'
    accountShortDictReverse = {shortName.lower():str(accountName).lower() for accountName,shortName in zip(allPageResult['account_name'],allPageResult['short_name'])}
    # 删除历史的redis key
    [_connRedis.delete(key) for key in _connRedis.keys() if accountStoreRedisReverseSign in key]
    accountReverseRedisKey = accountStoreRedisReverseSign+'_'+now_datetime
    _connRedis.hmset(accountReverseRedisKey, accountShortDictReverse)

    print("通过api完成账号简称的获取:存储到mysql与redis中.")

    del allPageResult
    del accountShortDict
    gc.collect()


if __name__ == '__main__':
    request_erp_account()