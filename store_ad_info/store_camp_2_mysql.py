from datetime import datetime, timedelta
import time
import numpy as np
import os

import public_function, process_files, conn_db
from calc_station_st_and_sku import commonly_params

"""
将站点广告报表中的camp层级存储到mysql中
"""

@public_function.run_time
def main_loop():

    def store_camp():
        """
        计算站点的st和sku表现
        :return:
        """
        # 得出需要计算的站点
        global redisConn2, stationRedisTypeDict, stationsQueue
        redisConn0 = public_function.Redis_Store(db=0)
        redisConn2 = public_function.Redis_Store(db=2)
        stationCpRedisKey = redisConn0.blpop(commonly_params.station_cp_camp_degree_key,timeout=5)
        if stationCpRedisKey is None:
            sleepTime = 600
            # print(f'无站点更新camp层级,休息{sleepTime}秒.')
            time.sleep(sleepTime)
            return
        stationCpRedisKey = stationCpRedisKey[1]
        stationName = stationCpRedisKey[len(commonly_params.five_files_redis_sign) + 1:-18]
        stationName = public_function.standardize_station(stationName)
        process_station_camp(stationName,stationCpRedisKey)

    def get_standard_time(country, initial_date):
        if country in ["US", "CA"]:
            date_str = "%m/%d/%Y"
        elif country == "JP":
            date_str = "%Y/%m/%d"
        else:
            date_str = "%d/%m/%Y"
        standard_date = datetime.strptime(initial_date, date_str)
        standard_date_str = datetime.strftime(standard_date, "%Y-%m-%d")
        return standard_date_str

    def save_campaigns_data(account_station, camp_data):
        """
        保存campaigns的数据

        :param account_station: string 站点名
        :param camp_data: 广告活动数据
        :return:
        """
        account_station = account_station.upper()
        country = account_station[-2:]
        campaign_row = camp_data.loc[(camp_data['Record Type'] == 'Campaign')].copy()
        for col in ['Spend', 'Sales', 'Campaign Daily Budget']:
            campaign_row[col] = campaign_row[col].astype('str')  # 三行转换类型字符串
            campaign_row[col] = campaign_row[col].str.replace('%', '')
            campaign_row[col] = campaign_row[col].str.replace(',', '.')
            campaign_row[col] = public_function.series_to_numeric(campaign_row[col])  # 转换为浮点数

        need_col = ['Record ID', 'Campaign Name', 'Campaign Daily Budget', 'Campaign Start Date',
                    'Campaign Targeting Type',
                    'Campaign Status', 'Impressions', 'Clicks', 'Spend', 'Orders', 'Sales']
        if ('Campaign Name' not in campaign_row.columns) and ('Campaign' in campaign_row.columns):
            campaign_row.rename(columns={'Campaign': 'Campaign Name'}, inplace=True)
        if not set(need_col).issubset(set(campaign_row.columns)):
            loseColumns = set(need_col) - set(campaign_row.columns)
            print(f'{account_station}广告报表camp层级无法正常上传到数据库中,缺失{",".join(list(loseColumns))}列.')
            return
        campaign_row = campaign_row[need_col]
        campaign_row.rename(columns={'Record ID': 'campaign_id',
                                     'Campaign Name': 'campaign_name',
                                     'Campaign Daily Budget': 'campaign_budget',
                                     'Campaign Start Date': 'campaign_start_date',
                                     'Campaign Targeting Type': 'campaign_targeting_type',
                                     'Campaign Status': 'campaign_status',
                                     'Impressions': 'impressions',
                                     'Clicks': 'clicks',
                                     'Spend': 'spend',
                                     'Orders': 'orders',
                                     'Sales': 'sales'}, inplace=True)
        campaign_row["acos"] = campaign_row["spend"] / campaign_row["sales"]
        campaign_row["acos"] = campaign_row["acos"].replace([np.inf, -np.inf, np.nan], 0)
        campaign_row["acos"] = campaign_row["acos"].apply(lambda z: round(z, 4))
        try:
            campaign_row["campaign_start_date"] = campaign_row["campaign_start_date"].apply(
                lambda z: get_standard_time(country, z))
        except Exception as e:
            print(f'{account_station}:{e}')
            return
        campaign_row["account_station"] = account_station
        campaign_row["country"] = country
        campaign_row['create_time'] = datetime.now().replace(microsecond=0)
        campaign_row['update_time'] = campaign_row['create_time']
        return campaign_row

    def update_mysql_camp_row(station, camp_data):
        """
        更新数据库中站点camp层级的数据
        :param station:
        :return:
        """
        db = 'amazon_data'
        table = 'campaigns'
        deleteSql = "delete from `%s` where account_station = '%s'" % (table, station)
        # 先删除后添加
        try:
            conn_db.to_sql_delete(deleteSql, db=db)
            conn_db.to_sql_append(camp_data, table, db=db)
            # print(f'{station}:成功更新camp层级.')
        except Exception as e:
            print(e)
            print(f'{station}:失败更新camp层级.')

    def process_station_camp(station, cpPklRedisKey):
        """
        站点计算出camp层级数据,并将该层级数据保存到mysql中
        stationFilesTypeDict:{'ST':st_redis_key,'BR':br_redis_key}
        :return:
        """
        if not isinstance(station, str):
            return
        if cpPklRedisKey is None:
            return
        cpPklPath = redisConn2.get(cpPklRedisKey)
        if cpPklPath is None:
            return
        if not os.path.exists(cpPklPath):
            return
        cpData = process_files.read_pickle_2_df(cpPklPath)
        if len(cpData.index) == 0:
            return
        station = public_function.standardize_station(station, case='upper')
        try:
            stationCampRow = save_campaigns_data(station, cpData)
        except Exception as e:
            print(f'{station}生成camp层级失败.')
            print(e)
        update_mysql_camp_row(station, stationCampRow)

    while 1:
        store_camp()


if __name__ == '__main__':
    main_loop()
