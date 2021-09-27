import pandas as pd
import numpy as np
import pymysql


def conn_mysql(process_database='camp_report'):
    conn = pymysql.connect(
        host='127.0.0.1',
        user='marmot',
        password='marmot123',
        database=process_database,
        port=3306,
        charset='UTF8')
    return conn


def write_to_mysql(df):
    # df = df.astype(object).where((pd.notnull(df)), None)
    df = df.astype(object).replace(np.nan, 'None')
    df.drop_duplicates(inplace=True)
    df = np.array(df)
    # 创建连接
    conn = conn_mysql()
    all_list = []
    len_df = df.shape[0]
    for i in range(len_df):
        temp_tuple = df[i]
        a_emp_tuple = tuple(temp_tuple)
        all_list.append(a_emp_tuple)
    # df.to_sql()
    # 写入到数据库中
    # 创建游标
    cursor = conn.cursor()
    sql = """insert into calc_camp (account, site, date, acos, cpc, cr, spend, sales,
     impression, clicks) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    try:
        cursor.executemany(sql, all_list)
    except Exception as e:
        print(e)
    conn.commit()
    cursor.close()
    conn.close()


# 将数据库中的数据读取
def load_account_info(table='calc_camp'):
    # 连接数据库
    conn = conn_mysql()
    # 创建游标
    cursor = conn.cursor()
    # 写sql
    sql = """SELECT * FROM {} order by account,site,date""".format(table)
    # 执行sql语句
    cursor.execute(sql)
    all_result = cursor.fetchall()
    all_result = pd.DataFrame([list(j) for j in all_result], columns=['account', 'site', 'date', 'acos', 'cpc', 'cr', 'spend', 'sales',\
                                                                      'impression', 'clicks'])
    all_result.drop_duplicates(inplace=True)
    conn.commit()
    cursor.close()
    conn.close()
    return all_result


def load_dates(table='calc_camp'):
    # 连接数据库
    conn = conn_mysql()
    # 创建游标
    cursor = conn.cursor()
    # 写sql
    sql= """SELECT account,site,date FROM {} group by account,site,date order by account,site,date""".format(table)
    # 执行sql语句
    cursor.execute(sql)
    dates = cursor.fetchall()
    dates_result = pd.DataFrame([list(i) for i in dates], columns=['account', 'site', 'date'])
    conn.commit()
    cursor.close()
    conn.close()
    return dates_result

