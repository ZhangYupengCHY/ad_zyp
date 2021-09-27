import gc
import json
import requests
from datetime import datetime

import pandas as pd
from sqlalchemy.dialects import mysql as sqlType

from my_toolkit import chinese_check,sql_write_read,public_function

"""
通过接口获取公司的花名,同时将花名存储到redis与mysql数据库中
"""


def request_nickname():
    """
    接口一天一次请求花名,将请求结果保存到Mysql数据库中
    Returns
    -------

    """
    requestPageNum = 1
    allPageResult = pd.DataFrame()
    while 1:
        requestUrl = r"http://amazon.yibainetwork.com/services/api/advertise/Getshortname?token=877F52DE27FA8C737A7EB34E064981D3&page=%s&type=2"%(requestPageNum)
        response = requests.post(url=requestUrl,headers={'Content-type': 'text/plain; charset=utf-8'})
        if response.status_code != 200:
            raise ConnectionError(f"错误连接代码:{response.status_code}.\n无法通过api{requestUrl}\n获取账号信息")
        onePageResult = json.loads(response.content.decode('utf-8'))
        onePageDataList = json.loads(onePageResult['data'])
        if len(onePageDataList) == 0:
            break
        else:
            onePageDataDf = pd.DataFrame.from_records(onePageDataList)
            allPageResult = pd.concat([allPageResult,onePageDataDf],ignore_index=True)
            # 循环
            requestPageNum += 1

    # 添加实际姓名列和花名列
    allPageResult['real_name'] = [chinese_check.extract_chinese(name) for name in allPageResult['user_full_name']]
    allPageResult['nickname'] = [nickNameWorkNumber[:len(nickNameWorkNumber)-len(workNumber)] for
                                 workNumber,nickNameWorkNumber in zip(allPageResult['work_number'],allPageResult['nickname_work_number'])]
    allPageResult['work_number'] = [public_function.standardize_user_number(workNumber) for workNumber in allPageResult['work_number']]
    # 添加更新时间列
    allPageResult['update_datetime'] = datetime.now().replace(microsecond=0)
    # 将请求结果上传到Mysql数据库中
    nicknameMysqlTableName = 'nickname'
    try:
        # 将信息存储到数据库中
        columnsDtypes = {
            'user_name': sqlType.VARCHAR(128),
            'user_full_name': sqlType.VARCHAR(20),
            'work_number': sqlType.VARCHAR(20),
            'nickname_work_number': sqlType.VARCHAR(128),
            'real_name': sqlType.VARCHAR(20),
            'nickname': sqlType.VARCHAR(128),
            'update_time': sqlType.DATETIME,
        }
        sql_write_read.to_table_replace(allPageResult,nicknameMysqlTableName,dtype=columnsDtypes)

        print("通过api完成花名获取:存储到mysql与redis中.")
    except:
        pass
    # 将账号对照名更新到redis数据中
    # allPageResult.drop_duplicates(subset=['nickname'],inplace=True)
    # allPageResult = allPageResult[~pd.isna(allPageResult['nickname'])]
    # _connRedis = public_function.Redis_Store(db=0)
    # nicknameStoreRedisSign = '花名对照表'
    # now_datetime = datetime.now().replace(microsecond=0).strftime('%Y-%m-%d_%H-%M-%S')
    # nicknameDict = {str(nickname).lower():realName.lower() for nickname,realName in zip(allPageResult['nickname'],allPageResult['real_name'])}
    # # 删除历史的redis key
    # [_connRedis.delete(key) for key in _connRedis.keys() if nicknameStoreRedisSign in key]
    # nicknameRedisKey = nicknameStoreRedisSign + '_'+ now_datetime
    # _connRedis.hmset(nicknameRedisKey, nicknameDict)

    del allPageResult
    # del nicknameDict
    gc.collect()



if __name__ == '__main__':
    request_nickname()