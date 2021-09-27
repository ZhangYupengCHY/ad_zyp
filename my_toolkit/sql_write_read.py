import traceback

import pymysql
from sqlalchemy import create_engine
import pandas as pd
from retry import retry
# import MySQLdb._mysql as _mysql
# import MySQLdb
import numpy as np
from my_toolkit.public_function import *

engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(
    'marmot', 'marmot123', '172.16.128.240', 3306, 'team_station', 'utf8'))

yibai_product_engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(
    'amazonaduser', 'Whamazond2020+++', '121.37.29.133', 3306, 'yibai_product', 'utf8'))


def commit_sql(sql):
    db = pymysql.connect(host="127.0.0.1", port=3306, user="marmot", password="marmot123", db="team_station",
                         charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    try:
        # 执行sql语句
        cursor.execute(sql)
        # 提交到数据库执行
        db.commit()
    except:
        # Rollback in case there is any error
        db.rollback()
        print("写入失败")
    # 关闭数据库连接
    finally:
        cursor.close()
        db.close()


@retry(tries=3, delay=3)
def conn_db(db='team_station', port=3306, ip='172.16.128.240',
            user_name='marmot', password='marmot123', engine=engine):
    """
    连接数据库
    :param db:
    :param port:
    :param ip:
    :param user_name:
    :param password:
    :return:
    """
    if engine == engine:
        return engine.connect()
    if engine == yibai_product_engine:
        return engine.connect()
    return create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(
        user_name, password, ip, port, db, 'utf8')).connect()


@retry(tries=3, delay=3)
def conn_db_by_pymysql(db='team_station', port=3306, host='127.0.0.1',
                       user_name='marmot', password='marmot123', charset='utf8'):
    """
    使用pymysql连接数据库
    :return:
    """
    return pymysql.connect(host=host, port=port, user=user_name, password=password, db=db, charset=charset)


@retry(tries=3, delay=5)
def to_table_replace(df, db_name,dtype=None,**kwargs):
    con = conn_db()  # 创建连接
    try:
        df.to_sql(name=db_name, con=con, if_exists='replace', index=False,dtype=dtype,**kwargs)
    except Exception as e:
        print(e)
    finally:
        con.close()


def to_table_append(df, db_name):
    con = conn_db()  # 创建连接
    try:
        df.to_sql(name=db_name, con=con, if_exists='append', index=False)
    except Exception as e:
        print(e)
    finally:
        con.close()


# 查询数据库中某些表的数据
def delete_table(sql, db='team_station', port=3306, host='127.0.0.1',
                 user_name='marmot', password='marmot123', charset='utf8'):
    conn = conn_db_by_pymysql(host=host, port=port, user_name=user_name, password=password, db=db, charset='utf8')
    # 创建游标
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
    except:
        try:
            conn.ping(reconnect=True)
            db_cursor = conn.cursor()
            db_cursor.execute(sql)
        except Exception as e:
            print(e)
            conn.rollback()
    finally:
        cursor.close()
        conn.close()


# 查询数据库中内容
def query_table(sql, db='team_station', port=3306, host='127.0.0.1',
                user_name='marmot', password='marmot123'):
    conn = conn_db_by_pymysql(host=host, port=port, user_name=user_name, password=password, db=db, charset='utf8')
    # 创建游标
    cursor = conn.cursor()

    try:
        cursor.execute(sql)
        # 使用fetall()获取全部数据
        data = cursor.fetchall()
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()
    cursor.close()
    conn.close()
    # 获取表头
    columns = []
    for field in cursor.description:
        columns.append(field[0])
    # 返回成df
    data = pd.DataFrame([list(row) for row in data], columns=columns)
    return data


# 执行sql语言,无返回结果
def query_sql(sql, db='team_station', port=3306, host='127.0.0.1',
              user_name='marmot', password='marmot123'):
    conn = conn_db_by_pymysql(host=host, port=port, user_name=user_name, password=password, db=db, charset='utf8')
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


def read_table(sql, engine=engine):
    con = conn_db(engine=engine)  # 创建连接
    df_data = pd.read_sql(sql, con)
    con.close()
    return df_data


def to_db_table_fail(df, db_name, sheet_name):
    engine_db = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(
        'marmot', 'marmot123', '192.168.129.240', 3306, db_name, 'utf8'))
    con = engine_db.connect()  # 创建连接
    df.to_sql(name=sheet_name, con=con, if_exists='fail', index=False)
    con.close()


def read_table_kw(sql):
    engine_kw = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(
        'marmot', 'marmot123', '192.168.129.240', 3306, 'sku_ad_history', 'utf8'))
    con = engine_kw.connect()  # 创建连接
    # find_list_new=[str(m_data) for m_data in find_list if m_data!='']
    # sql="select 单据编号,往来单位 from %s where 单据编号 in (%s)"%(table_name,','.join(find_list))
    df_data = pd.read_sql(sql, con)
    con.close()

    return df_data


def read_local_db(sql):
    local_con = conn_db()  # 创建连接
    df_data = pd.read_sql(sql, local_con)
    local_con.close()

    return df_data


def to_local_table_replace(df, db_name,dtype=None):
    con = conn_db()  # 创建连接
    df.to_sql(name=db_name, con=con, if_exists='replace', index=False,dtype=dtype)
    con.close()


def query_list_to_str(query_list: list) -> str:
    """
    将查询的list格式转换为数据库能识别的字符串格式
    :param query_list: list
        查询的list
    :return: str
        转换成的string
    """
    if not isinstance(query_list, (set,list,tuple)):
        raise TypeError(
            f'query_list param type must be list.But query_list param you input {query_list} type is {type(query_list)}.')
    query_str_list = [str(element).replace('"', '').replace("'", '') for element in query_list if element != ""]
    return '"' + '","'.join(query_str_list) + '"'


def query_sku_tied(sku: str or list, is_seller_sku=True) -> pd.DataFrame:
    """
        查询sku捆绑表 亚马逊平台:根据seller_sku查询erp_sku 或是更新erp_sku查询seller_sku
    :param sku:string or list
        需要查询的sku
    :param is_seller_sku:bool default True
        is false means query sku is erp_sku
    :return:pd.DataFrame or None
        查询的返回结果 seller_sku和erp_sku列
    """
    if not isinstance(sku, (str, list)):
        raise TypeError(f'sku type must be string or list.But sku param you input {sku} type is {type(sku)}.')
    if not isinstance(is_seller_sku, bool):
        raise TypeError(f'sku type must be bool.But is_seller_sku param you input {is_seller_sku} type is {type(sku)}.')
    # 初始化数据库连接
    _conn_mysql = QueryMySQL()
    sku_map_db_table_name = 'yibai_amazon_sku_map'
    if isinstance(sku, str):
        if is_seller_sku:
            query_sku = "SELECT seller_sku,erp_sku from %s where seller_sku = '%s'" % (sku_map_db_table_name, sku)
        else:
            query_sku = "SELECT seller_sku,erp_sku from %s where erp_sku = '%s'" % (sku_map_db_table_name, sku)
    if isinstance(sku, list):
        sku_query = query_list_to_str(sku)
        if is_seller_sku:
            query_sku = "SELECT seller_sku,erp_sku from %s where seller_sku in (%s)" % (
                sku_map_db_table_name, sku_query)
        else:
            query_sku = "SELECT seller_sku,erp_sku from %s where erp_sku in (%s)" % (sku_map_db_table_name, sku_query)
    sku_map_data_response = _conn_mysql.read_table(sku_map_db_table_name, query_sku, columns=['seller_sku', 'erp_sku'])
    # 关闭数据库连接
    _conn_mysql.close()
    sku_map_data_response.drop_duplicates(inplace=True)
    sku_map_data_response.reset_index(inplace=True, drop=True)
    return sku_map_data_response


def query_asin(sku):
    """
    查询asin
    :param query: str,list
        查询的sellersku
    :return: pd.DataFrame
         返回的结果:
            包括seller-sku,asin两列
    """
    if not isinstance(sku, (str, list)):
        raise TypeError(f'sku type must be string or list.But sku param you input {sku} type is {type(sku)}.')
    # 初始化数据库连接
    _conn_mysql = QueryMySQL()
    sku_asin_db_table_name = 'station_ac_major_data'
    if isinstance(sku, str):
        query_sku = "SELECT `seller-sku`,asin from %s where `seller-sku` = '%s'" % (sku_asin_db_table_name, sku)
    if isinstance(sku, list):
        sku_query = query_list_to_str(sku)
        query_sku = "SELECT `seller-sku`,asin from %s where `seller-sku` in (%s)" % (
            sku_asin_db_table_name, sku_query)
    sku_asin_data_response = _conn_mysql.read_table(sku_asin_db_table_name, query_sku, columns=['seller-sku', 'asin'])
    # 关闭数据库连接
    _conn_mysql.close()
    sku_asin_data_response.drop_duplicates(inplace=True)
    sku_asin_data_response.reset_index(inplace=True, drop=True)
    return sku_asin_data_response


@retry(tries=3, delay=1)
def query_category(seller_sku):
    """
    查询seller_sku对应的类目
    首先通过seller_sku查询得到erp_sku,然后通过erp_sku查询得到产品线id,然后通过产品线id查询得到多级类目

    Parameters
    ----------
    seller_sku : str,list
        查询的sku
    Returns
    -------
        pd.DataFrame or None
        columns = ['seller_sku','linelist_cn_name_degree_3','linelist_cn_name_degree_2','linelist_cn_name_degree_1']
    """
    # 输入类型检验
    is_variables_types_valid(seller_sku, (str, list))
    # step1.seller_sku查找erp_sku
    sku_tied_info = query_sku_tied(seller_sku)
    if (sku_tied_info is None) or (sku_tied_info.empty):
        return
    sku_tied_info['erp_sku'].astype(str, errors='ignore')
    query_erp_sku = list(set(sku_tied_info['erp_sku']))
    query_erp_sku_str = query_list_to_str(query_erp_sku)
    # step2.查询产品线id
    yibai_product_table_name = "yibai_product"
    query_product_linelist_id_sql = "SELECT sku,product_linelist_id FROM %s where sku in (%s)" % (
        yibai_product_table_name, query_erp_sku_str)
    product_linelist_id_data = read_table(query_product_linelist_id_sql, engine=yibai_product_engine)
    if (product_linelist_id_data is None) or (product_linelist_id_data.empty):
        return
    # 为查询信息添加产品线id列
    query_seller_sku_category_info = pd.merge(sku_tied_info, product_linelist_id_data, how='left', left_on='erp_sku',
                                              right_on='sku')
    product_linelist_id = list(set(query_seller_sku_category_info['product_linelist_id']))
    product_linelist_id_str = query_list_to_str(product_linelist_id)
    # step3.查询类目
    yibai_product_linelist_table_name = "yibai_product_linelist"
    query_product_linelist_sql = "SELECT id,path_name FROM %s where id in (%s)" % (
        yibai_product_linelist_table_name, product_linelist_id_str)
    product_linelist_data = read_table(query_product_linelist_sql, engine=yibai_product_engine)
    if (product_linelist_data is None) or (product_linelist_data.empty):
        return
    # 为查询信息添加类目列
    query_seller_sku_category_info = pd.merge(query_seller_sku_category_info, product_linelist_data, how='left',
                                              left_on='product_linelist_id', right_on='id')
    # 将类目分成三级,只获取最后三级的类目
    query_seller_sku_category_info['path_name'] = query_seller_sku_category_info['path_name'].astype(str,
                                                                                                     errors='ignore')
    category_value = query_seller_sku_category_info['path_name']
    splited_category_value = list(map(lambda x: x.split('>>'), category_value))
    query_seller_sku_category_info['linelist_cn_name_degree_3'] = list(
        map(lambda x: x[-1] if len(x) > 0 else '', splited_category_value))
    query_seller_sku_category_info['linelist_cn_name_degree_2'] = list(
        map(lambda x: x[-2] if len(x) > 1 else '', splited_category_value))
    query_seller_sku_category_info['linelist_cn_name_degree_1'] = list(
        map(lambda x: x[-3] if len(x) > 2 else '', splited_category_value))

    export_columns = ['seller_sku', 'linelist_cn_name_degree_3', 'linelist_cn_name_degree_2',
                      'linelist_cn_name_degree_1']

    return query_seller_sku_category_info[export_columns]


class QueryMySQL(object):
    """
    使用mysqlclient模块来查询数据库

    """

    def __init__(self, host='172.16.128.240', port=3306, username='marmot', password='marmot123',
                 database='team_station'):
        """
        初始化连接MySQL数据
            host
              string, host to connect

            username
              string, user to connect as

            password
              string, password to use

            database
              string, database to use

            port
              integer, TCP/IP port to connect to
        """
        try:
            self.connect = pymysql.connect(host=host,
                                           port=port,
                                           user=username,
                                           passwd=password,
                                           db=database,
                                           charset='utf8')
            # 游标，输出格式为字典
            self.cursor = self.connect.cursor(cursor=pymysql.cursors.DictCursor)
        except Exception as e:
            print(f'连接{host}:{database}失败.')
            print(e)

    def query_columns(self, table_name):
        """
        查询数据库表的字段名

        Parameters
        ----------
        table_name :string
            table name

        Returns
        -------
            tuple of column names
        """

        columns_query_sql = f"DESC {table_name}"
        tableInfo = self.select(columns_query_sql)
        return [col['Field'] for col in tableInfo]

    def select(self, sql, one=False):
        """数据库查询"""
        try:
            self.cursor.execute(sql)
            if one:
                return self.cursor.fetchone()
            else:
                return self.cursor.fetchall()
        except:
            print('Error: unable to fecth data')
            print(traceback.format_exc())

    def commit(self, sql):
        """数据库执行"""
        try:
            # 执行sql
            self.cursor.execute(sql)
            self.connect.commit()
        except:
            # 发生错误时回滚
            self.connect.rollback()
            print('Error: unable to fecth data')
            print(sql)
            print(traceback.format_exc())

    # @retry(tries=3, delay=2)
    def read_table(self, table_name, sql=None, columns=None):

        if sql is None:
            if columns is None:
                sql = f"SELECT * from {table_name}"
            else:
                if not isinstance(columns, (set, list)):
                    raise TypeError(f'read_table columns type is list or set not {type(columns)}')
                else:
                    sql = "select `" + "`,`".join(columns) + "` from `%s`" % table_name
        try:
            # 执行查询
            tableInfo = self.select(sql, one=False)
            # 存储数据
            if len(tableInfo) > 0:
                return pd.DataFrame(tableInfo)
            else:
                if columns is None:
                    columns = self.query_columns(table_name)
                return pd.DataFrame(columns=columns)

        except Exception as e:
            print(f"执行 {sql} 语句失败.")
            print(e)

    @staticmethod
    def fast_read_table(table_name, sql=None, columns=None, host='172.16.128.240', port=3306, username='marmot',
                        password='marmot123',
                        database='team_station'):
        connMysql_ = QueryMySQL(host=host, port=port, username=username, password=password, database=database)
        queryInfo = connMysql_.read_table(table_name, sql=sql, columns=columns)
        connMysql_.close()
        return queryInfo

    def close(self):
        """
        关闭mysql连接
        Returns
        -------

        """
        try:
            self.cursor.close()
        except Exception as e:
            pass
        try:
            self.connect.close()
        except Exception as e:
            pass


class QueryDatabaseFrequentlyInfo(QueryMySQL, Redis_Store):
    """
    查询数据库中信息,首先从redis中获取,如果redis中数据不是很新,则从sql数据库中获取
    min_expire_time:datetime.datetime
        信息存在redis中的最小有效时间
    redis_interval_updatetime:存储在redis中的有效期
    """

    def __init__(self, mysql_table, columns=None, sql=None, redisSignKey=None, strFormat='%Y-%m-%d_%H-%M-%S',
                 redis_host='127.0.0.1', redis_port=6379, redis_db=0, redis_interval_updatetime=None,
                 redis_password='', decode_responses=True, expire_time=None, min_expire_time=None,
                 mysql_host='127.0.0.1', mysql_port=3306, mysql_username='marmot', mysql_password='marmot123',
                 mysql_db='team_station', redis_time_split_sign='$$'):

        Redis_Store.__init__(self, host=redis_host, port=redis_port, db=redis_db,
                                            password=redis_password,
                                            decode_responses=decode_responses, expire_time=expire_time)
        QueryMySQL.__init__(self, host=mysql_host, port=mysql_port, username=mysql_username,
                                           password=mysql_password, database=mysql_db)
        self._redis_db = redis_db
        self._mysql_table = mysql_table
        if (min_expire_time is not None) and (not isinstance(min_expire_time, datetime)):
            raise TypeError('min_expire_time type must datetime')
        self._min_expire_time = min_expire_time
        if sql is None:
            if columns is None:
                self._sql = f"SELECT * from `{self._mysql_table}`"
            else:
                if not isinstance(columns, (set, list)):
                    raise TypeError(f'read_table columns type is list or set not {type(columns)}')
                else:
                    self.sql = "select `" + "`,`".join(columns) + "` from `%s`" % self._mysql_table
        self._columns = columns
        self._redis_time_split_sign = redis_time_split_sign
        if redisSignKey is None:
            self._redisSignKey = self._mysql_table
        else:
            self._redisSignKey = redisSignKey
        self._strFormat = strFormat
        if (redis_interval_updatetime is not None) and (not isinstance(redis_interval_updatetime, int)):
            raise TypeError(f'redis_interval_updatetime type is int not {type(columns)}')
        self._redis_interval_updatetime = redis_interval_updatetime

    def _query_from_sql(self):
        """
        从mysql获取工号对应站点
        Returns
        -------

        """
        #
        sellerAccountInfo = self.read_table(table_name=self._mysql_table, columns=self._columns)
        self.close()
        # 将信息上传到redis中
        self._update_redis_info(sellerAccountInfo)
        return sellerAccountInfo

    def _update_redis_info(self, saveDf, saveTime=None):
        """
        更新redis某个信息
        Returns
        -------

        """
        if not isinstance(self._redisSignKey, str):
            raise TypeError('signWord type is str.')
        if not isinstance(saveDf, pd.DataFrame):
            raise TypeError('saveDf type is not pd.DataFrame.')
        if len(saveDf.index) == 0:
            return
        if saveTime is None:
            saveTime = datetime.strftime(datetime.now().replace(microsecond=0), self._strFormat)
        if isinstance(saveTime, datetime):
            saveTime = datetime.strftime(saveTime.replace(microsecond=0), self._strFormat)
        if not isinstance(saveTime, str):
            raise TypeError("saveDf type is datetime.datetime or string.")
        # 将信息上传到redis中
        redisKey = f"{self._redisSignKey}{self._redis_time_split_sign}{saveTime}"
        # 先删除旧的,然后再添加新的
        [self.delete(key) for key in self.keys() if self._redisSignKey in key]
        self.redis_upload_df(redisKey, saveDf)

    def _query_from_redis(self):
        """
        从redis中获取工号账号信息
        Parameters
        ----------
        redisSign :
        db :

        Returns
        -------

        """
        redisKeys = [key for key in self.keys() if self._redisSignKey in key]
        if not redisKeys:
            return None, None
        redisKey = redisKeys[0]
        redisTime = redisKey.split(self._redis_time_split_sign)
        if redisTime:
            redisTime = redisTime[-1]
        else:
            redisTime = None
        _connRedis = Redis_Store(db=self._redis_db, decode_responses=False)
        return [redisTime, _connRedis.redis_download_df(redisKey)]

    def read_table_info(self):
        redisTime, redisSaveInfo = self._query_from_redis()
        if redisTime is None:
            saveInfo = self._query_from_sql()
        else:
            redisTime = datetime.strptime(redisTime, self._strFormat)
            if self._redis_interval_updatetime is None:
                if self._min_expire_time is None:
                    saveInfo =  redisSaveInfo
                else:
                    if redisTime > self._min_expire_time:
                        saveInfo =  redisSaveInfo
                    else:
                        saveInfo =  self._query_from_sql()
            else:
                if int((datetime.now() - redisTime).total_seconds()) > self._redis_interval_updatetime:
                    saveInfo =  self._query_from_sql()
                else:
                    saveInfo =  redisSaveInfo

        # 如果请求的列不够,则需要重新从数据库中请求
        if (self._columns is not None) and (not set(self._columns).issubset(set(saveInfo.columns))):
            saveInfo = self._query_from_sql()

        #关闭数据库连接
        try:
            super(QueryDatabaseFrequentlyInfo,self).close()
        except:
            pass

        if self._columns is not None:
            return saveInfo[self._columns]
        else:
            return saveInfo


# 处理redis
class Redis_Store(redis.StrictRedis):
    """
    继承redis.StrictRedis类,使用redis来进行操作:
        1.存入五表的数据
        2.存入list数据
        3.存入dataframe
        4.存入string
    """

    def __init__(self, host='127.0.0.1', port=6379, db=2,
                 password='', decode_responses=True, expire_time=None):
        redis.StrictRedis.__init__(self, host='127.0.0.1', port='6379', password='', db=db,
                                   decode_responses=decode_responses)
        self.redis_pool = redis.ConnectionPool(host=host, port=port, password=password,
                                               db=db, decode_responses=decode_responses)
        self.red = redis.StrictRedis(connection_pool=self.redis_pool)
        self.expire_time = expire_time

    def redis_upload_list(self, list_data, list_key):
        """
        将list数据上传到redis
        :param list_data:list型的数据
        :param list_key:存储的键名称
        :param save_datetime:保存时间20200218
        :return:
        """
        if list_data is None:
            return
        if not isinstance(list_data, list):
            return
        # 判断键名称是否存在
        if self.red.exists(list_key):
            self.red.delete(list_key)
        self.red.lpush(list_key, *list_data)
        if self.expire_time is None:
            self.red.expire(list_key)
        else:
            self.red.expire(list_key, self.expire_time)

    # 将取出存储在redis中list
    def redis_download_list(self, list_key):
        """
        :return:key对应的list
        """
        if not self.red.exists(list_key):
            return
        list_from_redis = self.red.lrange(list_key, 0, -1)
        return list_from_redis

    # 将dataframe存储到redis中
    def redis_upload_df(self, key, data):
        """
        将DataFrame存储到redis中
        Args:
            key:str
                保存DataFrame的键
            data:DataFrame
                 DataFrame数据

        Returns:None

        """
        df_bytes = data.to_msgpack(encoding='utf-8')
        self.red.set(key, df_bytes)

    # 将redis中的bytes型数据以DataFrame取出来
    def redis_download_df(self, key):
        """

        Args:
            key: str
                存储binary型数据的redis键

        Returns:DataFrame

        """
        df_from_redis = pd.read_msgpack(self.red.get(key))
        return df_from_redis

    def refresh_df(self,df,keyName,addDateTime=True):
        """更新redis中的df"""
        if addDateTime is True:
            dateNowStr = datetime.strftime(datetime.now(),'%Y-%m-%d_%H-%M-%S')
        # 先删除已经有的
        existKey = [key for key in self.keys() if keyName in key]
        if existKey:
            self.red.delete(*existKey)
        self.redis_upload_df(f'{keyName}$${dateNowStr}',df)


    def download_df(self, key):
        """

        Args:
            key: str
                存储binary型数据的redis键

        Returns:DataFrame

        """
        redTemp = Redis_Store(db=self.db,decode_responses=False)
        return pd.read_msgpack(redTemp.get(key))
