import requests
from datetime import datetime
from sqlalchemy.dialects.mysql import MEDIUMINT,VARCHAR,DATETIME

import pandas as pd


from my_toolkit import sql_write_read,public_function


def station_id_index_from_api():
    """
     通过api获取站点的id与站点名对应关系

     Returns
     -------
         pd.DataFrame()
         columns:id,account_name
     """

    # 站点名中英文转换


    url = r'http://amazon.yibainetwork.com/services/amazon/amazonayncdata/accountlist'
    data = requests.get(url)
    station_info_str = data.content.decode()
    station_info_str = station_info_str.replace('站', '')
    for cn_name, en_name in public_function.COUNTRY_CN_EN_DICT.items():
        station_info_str = station_info_str.replace(cn_name, f'_{en_name}')
    station_info = [row.split(',') for row in station_info_str.split('\n') if len(row.split(',')) == 2]
    if len(station_info) > 2:
        station_info = pd.DataFrame(station_info[1:], columns=station_info[0])
        station_info['update_time'] = datetime.now().replace(microsecond=0)
        station_info['account_name'] = [station.strip().replace("-","_").replace(" ","_").lower() for station in station_info['account_name']]
        # ## 全部的redis key
        # _conn_redis = public_function.Redis_Store(db=0, decode_responses=False)
        # all_redis_key = _conn_redis.keys()
        # ## 将站点销售信息上传到redis中,替换到历史的
        # account_index_redis_sign_key = 'account_id_index'
        # nowDataStr = datetime.now().strftime('%Y-%m-%d')
        # account_index_redis_key = account_index_redis_sign_key + '_'+nowDataStr
        # [_conn_redis.delete(key) for key in all_redis_key if account_index_redis_sign_key.encode() in key]
        # _conn_redis.redis_upload_df(account_index_redis_key,station_info[['id','account_name']])
        station_info.drop_duplicates(subset=['account_name'],inplace=True,keep='last')
        # 将站点id对应关系表上传到数据库中
        account_id_index_mysql_table_name = 'account_id_index'
        # 先执行删除,再执行插入
        try:
            dtype={'id':MEDIUMINT,'account_name':VARCHAR(255),'update_time':DATETIME}
            sql_write_read.to_table_replace(station_info,account_id_index_mysql_table_name,dtype=dtype)
            with sql_write_read.engine.connect() as con:
                con.execute(f'ALTER TABLE `{account_id_index_mysql_table_name}` ADD PRIMARY KEY (`id`),ADD unique idx_account_name(`account_name`);')
            # 刷新redis
            public_function.Redis_Store(db=0).refresh_df(station_info,account_id_index_mysql_table_name)
        except Exception as e:
            print(e)
        return station_info
    else:
        return pd.DataFrame()


def station_id_index_from_mysql():
    """
    通过mysql数据库获取站点索引表

    :return:
    """
    account_id_index_mysql_table_name = 'account_id_index'
    _conn_mysql = sql_write_read.QueryMySQL()
    account_id_index = _conn_mysql.read_table(account_id_index_mysql_table_name)
    _conn_mysql.close()
    return account_id_index


def station_id_index():
    """
    获取站点的id与站点名对应关系

    Returns
    -------
        pd.DataFrame()
        columns:id,account_name
    """

    # 站点名中英文转换
    # 1.首先通过api获取account id对应关系表
    account_id_index = station_id_index_from_api()
    if account_id_index is not None and len(account_id_index.index) > 0:
        return account_id_index
    else:
        # 通过数据库直接获取
        return station_id_index_from_mysql()


if __name__ == '__main__':
    station_id_index_from_api()