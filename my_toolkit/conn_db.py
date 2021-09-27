# -*- coding: utf-8 -*-
"""
Proj: my_plotly
Created on:   2020/6/10 10:51
@Author: RAMSEY

"""
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pymysql

engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(
    'marmot', 'marmot123', '127.0.0.1', 3306, 'team_station', 'utf8'))


# 数据库类型:mysql
# 数据库驱动选择:pymysql
# 数据库用户名:user
# 用户密码:password
# 服务器地址:wuhan.yibai-it.com
# 端口: 33061
# 数据库: team_station


def detect_db_conn(func):
    def wrapper(*args, **kwargs):
        try:
            conn = engine.connect()
            conn.close()
        except:
            raise ConnectionError('CAN NOT CONNECT MYSQL:ip:wuhan.yibai-it.com db:team_station')
        return func(*args, **kwargs)

    return wrapper



def read_table(sql,db = 'team_station'):
    # 执行sql
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(
        'marmot', 'marmot123', '127.0.0.1', 3306, db, 'utf8'))
    conn = engine.connect()
    df = pd.read_sql(sql, conn)
    conn.close()
    engine.dispose()
    return df


# If table exists, do nothing.
@detect_db_conn
def to_sql(df, table):
    # 执行sql
    conn = engine.connect()
    try:
        df.to_sql(table, conn, if_exists='fail', index=False)
    except:
        conn.rollback()
    conn.close()
    engine.dispose()


#  If table exists, drop it, recreate it, and insert data.
@detect_db_conn
def to_sql_replace(df, table,db='team_station'):
    # 执行sql
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(
        'marmot', 'marmot123', '127.0.0.1', 3306, db, 'utf8'))
    conn = engine.connect()
    try:
        df.to_sql(table, conn, if_exists='replace', index=False)
    except:
        conn.rollback()
    conn.close()
    engine.dispose()


# If table exists, insert data. Create if does not exist.
def to_sql_append(df, table,db='team_station'):
    # 执行sql
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(
        'marmot', 'marmot123', '127.0.0.1', 3306,db , 'utf8'))
    conn = engine.connect()
    try:
        df.to_sql(table, conn, if_exists='append', index=False)
    except:
        pass
    conn.close()
    engine.dispose()


# 删除数据库中的某一些
def to_sql_delete(sql,db='team_station',port = 3306, ip='127.0.0.1',
                              user_name='marmot', password='marmot123'):
    conn = pymysql.connect(host=ip,port=port, user=user_name, password=password, db = db, charset='utf8')
    # 创建游标
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()
    cursor.close()
    conn.close()


