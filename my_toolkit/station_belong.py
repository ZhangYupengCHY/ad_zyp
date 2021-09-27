import pandas as pd

from my_toolkit import sql_write_read,public_function



def station_to_account(stations):
    """
    站点信息转化为账号
    Parameters
    ----------
    stations :

    Returns
    -------

    """
    return set([station[:-3].lower() if len(station) > 3 else 'None' for station in stations])


def cx_account():
    """
    获取楚讯站点
        楚讯站点是account_belong中公司为楚讯的站点
    Returns
    -------

    """
    # 连接数据库
    _conn_mysql = sql_write_read.QueryMySQL()
    # 加载only_station_info中的station列
    station_sql_columns = ['账号']
    station_sql = "select 账号 from account_belong where `公司` = '楚讯' "
    stations = _conn_mysql.read_table('account_belong', station_sql, columns=station_sql_columns)
    # 关闭数据库连接
    _conn_mysql.close()
    return set([station.lower() for station in stations['账号']])


def dy_account():
    """
    获取东益站点
        东益站点是account_belong中公司为东益的站点
    Returns
    -------

    """
    # 连接数据库
    _conn_mysql = sql_write_read.QueryMySQL()
    # 加载only_station_info中的station列
    station_sql_columns = ['账号']
    station_sql = "select 账号 from account_belong where `公司` = '东益' "
    stations = _conn_mysql.read_table('account_belong', station_sql, columns=station_sql_columns)
    # 关闭数据库连接
    _conn_mysql.close()
    return set([station.lower() for station in stations['账号']])


def yy_station():
    """
    获取云逸站点
        运营的广告专员为 ['时丹丹', '孙文康', '何宇辰','左菲','王维']
    Returns
    -------

    """
    # 云逸的运营人员
    yy_ad_manager = ['时丹丹', '孙文康', '何宇辰', '左菲', '王维']
    # 连接数据库
    _conn_mysql = sql_write_read.QueryMySQL()
    # 加载only_station_info中的station列
    station_sql_columns = ['station']
    # 云逸广告人员string
    yy_ad_manager_str = sql_write_read.query_list_to_str(yy_ad_manager)
    station_sql = "select station from only_station_info where ad_manger in (%s)" % yy_ad_manager_str
    stations = _conn_mysql.read_table('only_station_info', station_sql, columns=station_sql_columns)
    # 关闭数据库连接
    _conn_mysql.close()
    return [station.lower() for station in stations['station']]


def yy_account():
    """
    获取云逸账号
    Returns
    -------

    """
    yyStations = yy_station()
    return station_to_account(yyStations)


def sh_station():
    """
    获取松华站点
        松华的广告专员为 是以SH开头或是为亚马逊
    Returns
    -------
    """
    # 连接数据库
    _conn_mysql = sql_write_read.QueryMySQL()
    # 加载only_station_info中的station列
    station_sql_columns = ['station']
    station_sql = "select station from only_station_info where ad_manger like '%SH%' or ad_manger = '亚马逊' "
    stations = _conn_mysql.read_table('only_station_info', station_sql, columns=station_sql_columns)
    # 关闭数据库连接
    _conn_mysql.close()
    return [station.lower() for station in stations['station']]


def sh_account():
    """
    获取松华账号
        松华的广告专员为 是以SH开头
    Returns
    -------

    """
    # 获取松华站点
    shStations = sh_station()
    return station_to_account(shStations)


def all_station():
    """
    获取全部站点
        only_station_info中加载站点信息
    Returns
    -------

    """
    # 连接数据库
    _conn_mysql = sql_write_read.QueryMySQL()
    # 加载only_station_info中的station列
    station_sql_columns = ['station']
    station_sql = "select station from only_station_info"
    stations = _conn_mysql.read_table('only_station_info', station_sql, columns=station_sql_columns)
    # 关闭数据库连接
    _conn_mysql.close()
    return set([station.lower() for station in stations['station']])


def all_account():
    """
    全部的账号
    Returns
    -------

    """
    allStations = all_station()
    return station_to_account(allStations)


def other_company_account():
    """
    其他公司账号:
        楚讯,东益,松华,云逸
    Returns
    -------

    """
    cxAccount = cx_account()
    shAccount = sh_account()
    yyAccount = yy_account()
    dyAccount = dy_account()
    unKnownAccount = unknown_account()

    return set(cxAccount) | set(shAccount) | set(yyAccount) | set(dyAccount) | set(unKnownAccount)


def other_company_station():
    """
    其他公司站点
    Returns
    -------

    """
    # 全部账号
    allStations = all_station()
    otherCompanyAccount = other_company_account()
    return set([station.lower() for station in allStations if
                (len(station) > 3) and (station[:-3].lower() in otherCompanyAccount)])


def unknown_station():
    """
    不知道公司的站点
    Returns
    -------

    """
    unknownAdManager = ['曾琦','王艳']
    # 连接数据库
    _conn_mysql = sql_write_read.QueryMySQL()
    # 加载only_station_info中的station列
    station_sql_columns = ['station']
    # 查询管理员字符串
    unknownAdManagerStr = sql_write_read.query_list_to_str(unknownAdManager)
    station_sql = "select station from only_station_info where ad_manger in (%s)" % unknownAdManagerStr
    stations = _conn_mysql.read_table('only_station_info', station_sql, columns=station_sql_columns)
    # 关闭数据库连接
    _conn_mysql.close()
    return [station.lower() for station in stations['station']]


def unknown_account():
    """
    不知道账号归属公司的站点
    Returns
    -------

    """
    unKnownStation = unknown_station()
    return station_to_account(unKnownStation)


def yibai_account_from_erp():
    """
    从公司接口获取易佰站点
    :return:
    """
    _connMysql = sql_write_read.QueryMySQL()
    table = 'account_id_index'
    stationInfo = _connMysql.read_table(table)
    _connMysql.close()
    return list([station.replace('-','_').replace(' ','_').strip().lower() for station in stationInfo['account_name'].values])



def yibai_account():
    """
    获取易佰账号:
        从全部站点中剔除其他公司账号:楚讯,东益,松华,云逸
    Returns
    -------
    """
    # 全部站点
    allAccount = all_account()
    # 其他公司
    otherCompanyAccount = other_company_account()
    # 易佰账号
    return set(allAccount) - set(otherCompanyAccount)


def yibai_station():
    """
    易佰站点
    Returns
    -------

    """
    return set(all_station()) - set(other_company_station())


def erp_account():
    """
    erp账号
    Returns
    -------

    """
    erpInfo = erp_stations()
    return set([station[:-3].lower() for station in erpInfo['account_name'] if len(station)>3])


def erp_stations():
    """
    获取公司erp站点与id
    Returns
    -------

    """
    _connMysql = sql_write_read.QueryMySQL()
    table = 'account_id_index'
    erpStationInfo = _connMysql.read_table(table)
    _connMysql.close()
    return erpStationInfo


def station_name_2_erp_id():
    """
    将易佰账号名称修改为erpid
    Returns
    -------

    """
    # 连接数据库
    _connMysql = sql_write_read.QueryMySQL()
    stationSql = "select station from only_station_info"
    adStations = _connMysql.read_table('only_station_info',sql=stationSql,columns=['station'])
    erpIndex = _connMysql.read_table('account_id_index')
    # 关闭数据库连接
    _connMysql.close()
    adStations['station'] = [public_function.standardize_station(station) for station in adStations['station']]
    adStations = set(adStations['station'])
    erpIndex.drop_duplicates(subset=['account_name'],inplace=True)
    erpStationsDict ={public_function.standardize_station(stationName):id for id,stationName in zip(erpIndex['id'],erpIndex['account_name'])}
    return {station.lower():erpStationsDict.get(station,None) for station in adStations}


def station_name_2_erp_station_name():
    """
    将易佰账号名称修改为erp_station_name
    Returns
    -------

    """
    # 连接数据库
    _connMysql = sql_write_read.QueryMySQL()
    stationSql = "select station from only_station_info"
    adStations = _connMysql.read_table('only_station_info',sql=stationSql,columns=['station'])
    erpIndex = _connMysql.read_table('account_id_index')
    # 关闭数据库连接
    _connMysql.close()
    adStations['station'] = [public_function.standardize_station(station) for station in adStations['station']]
    adStations = set(adStations['station'])
    erpIndex.drop_duplicates(subset=['account_name'],inplace=True)
    erpStationsDict ={public_function.standardize_station(stationName):stationName for stationName in erpIndex['account_name']}
    return {station.lower():erpStationsDict.get(station,None) for station in adStations}


def station_company(station):
    """
    查询站点的公司归属
    :param station:
    :return:
    """
    # todo 找到站点对应公司
    _connMysql = sql_write_read.QueryMySQL()
    querySql = "select company from only_station_info where station = '%s' "%station
    queryResult = _connMysql.read_table('only_station_info',querySql,columns=['company'])
    if len(queryResult) == 0:
        return "未知"
    else:
        return queryResult['company'].values[0]


def get_account(status=None):
    """
    获取全部的账号信息
    z
    Parameters
    ----------
    status :None,1,2
        None:全部的账号
        1:有效账号
        2:冻结了的账号

    Returns
    -------

    """
    accountStatusTable = 'account_status'
    if status is None:
        sql = 'select * from %s' % accountStatusTable
    elif status == 1:
        sql = "select * from %s where status = '1'" % accountStatusTable
    elif status == 2:
        sql = "select * from %s where status = '2'" % accountStatusTable
    else:
        return
    _connMySQL = sql_write_read.QueryMySQL()
    accountStatusInfo = _connMySQL.read_table(accountStatusTable, sql)
    _connMySQL.close()
    # 添加账号列
    return set([station.strip().replace('-', '_').replace(' ', '_').lower() for station in
                accountStatusInfo['account_name'] if isinstance(station,str)])


def account_id_index():
    """
    获取账号索引
    Returns
    -------

    """
    _mysqlTable = 'account_id_index'
    _connMysql = sql_write_read.QueryMySQL()
    accountId = _connMysql.read_table(_mysqlTable)
    _connMysql.close()
    return accountId


if __name__ == '__main__':
    a= station_name_2_erp_id()
    print(1)