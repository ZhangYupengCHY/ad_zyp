from datetime import datetime
import requests
import json
import sqlalchemy.dialects.mysql as sqlTypes
import pandas as pd
from requests.auth import HTTPBasicAuth

from my_toolkit import sql_write_read,public_function
from my_toolkit import chinese_check
"""
"""
@public_function.loop_func(update_time=14)
def refresh_account_status():
    """
    更新账号的状态,将信息存储到数据库中
    Returns
    -------

    """
    def get_token(username='prod_ads', password='1XQP767x7x9bnRvnQ',
                  auth_url='http://oauth.java.yibainetwork.com/oauth/token?grant_type=client_credentials'):
        """
        通过用户名与密码以及认证的连接获取token
        Args:
            username: str
                用户名
            password: str
                密码
            auth_url:path
                认证链接

        Returns:int or string
                若能正常返回则返回token,否则返回错误代码
        """
        response = requests.post(url=auth_url, auth=HTTPBasicAuth(username, password))
        resp_content = json.loads(response.content)
        if 'access_token' not in resp_content.keys():
            raise ImportError('获取token失败.')
        return resp_content['access_token']

    def query_station_status():
        """
        通过接口获取账号状态
        """
        # request_url = r"http://rest.java.yibainetwork.com/publish/yibaiAmazonAccount/selectAccountList?access_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzY29wZSI6WyJyZWFkIl0sImV4cCI6MTYxNTI2OTUyMSwiYXV0aG9yaXRpZXMiOlsiMCJdLCJqdGkiOiJhZjIyY2ZmNi1iZGMxLTQ2OTMtODE2MC05ZGYxNmE3NDNhZDciLCJjbGllbnRfaWQiOiJwcm9kX2FtYXpvbiJ9.q2ti4-l9ZeBoiRTLQBhqEq4uvamZIZ9fH9PkYY5c0uo"
        token = get_token()
        request_url = r"http://rest.java.yibainetwork.com/publish/yibaiAmazonAccount/selectAccountList?access_token={}".format(token)
        response = requests.post(request_url)
        if response.status_code != 200:
            raise ConnectionError(f'{request_url} status code is {response.status_code}.')
        response = json.loads(response.content)
        stationStatus = pd.DataFrame(response['data'])
        return stationStatus

    accountStatusDf = query_station_status()
    if accountStatusDf is None or len(accountStatusDf) == 0:
        return
    # 将api请求到的站点状态信息存储到mysql中
    # 添加站点状态更新时间

    # accountStatusDf['account_name_cn'] = accountStatusDf['accountName'].copy()
    accountStatusDf['site'] = [public_function.COUNTRY_CN_EN_DICT.get(chinese_check.extract_chinese(account).replace('站', ''), None) for
                             account in accountStatusDf['accountName']]
    accountStatusDf['station'] = [chinese_check.filter_chinese(account) if ~pd.isna(account) else "" for account in
                                accountStatusDf['accountName']]
    accountStatusDf['station'] = [station.replace('-', '_').replace(' ', '_') for station in accountStatusDf['station']]
    accountStatusDf['account_name'] = accountStatusDf['station'].str.lower() + '_' + accountStatusDf['site'].str.lower()
    
    accountStatusDf['updatetime'] = datetime.now().replace(microsecond=0)
    accountStatusTableName = 'account_status'
    columnsDtypes={
        'id':sqlTypes.MEDIUMINT(7),
        'accountName':sqlTypes.VARCHAR(255),
        'shortName':sqlTypes.VARCHAR(255),
        'status':sqlTypes.TINYINT(1),
        'site':sqlTypes.CHAR(2),
        'account_name':sqlTypes.VARCHAR(255),
        'updatetime':sqlTypes.DATETIME
    }
    sql_write_read.to_table_replace(accountStatusDf,accountStatusTableName,dtype=columnsDtypes)


if __name__ == '__main__':
    refresh_account_status()