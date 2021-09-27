"""
  active listing中信息上传到数据库中
"""
from public_function import *
from init_station_report import init_report
import sql_write_read


def get_redis_db_all_key(sign: str, db=2):
    """
    获取redis中数据库中的全部包含某个字符串的键值
    :param sign: str or list
    :param db:
    :return: dict
    """
    local_redis = Redis_Store(db=db)
    sign_key_values = local_redis.include(sign)
    local_redis.close()
    return sign_key_values


def all_ac_info_upload():
    """
    将全部的ac包信息上传到数据库中
        主要包括:seller-sku,asin1,price,open-date,fulfillment-channel
    :return:
    """
    ac_sign = ['FIVE_FILES_KEYS_SAVE', 'AC_']
    ac_path_all_station = get_redis_db_all_key(ac_sign).values()
    ac_path_all_station = set(ac_path_all_station)
    station_len = 0

    for path in ac_path_all_station:
        # 加载数据
        try:
            if not os.path.exists(path):
                continue
            ac_data_one_station = pd.read_pickle(path)
            if (ac_data_one_station is None) or (ac_data_one_station.empty):
                continue
            # 站点名:站点名在
            station = os.path.basename(path)[:-7]
            # 初始化站点数据
            ac_data_one_station = init_report(ac_data_one_station, 'ac')
            # 判断站点列名是否正确
            major_columns = ['seller-sku', 'asin1', 'price', 'open-date', 'fulfillment-channel']
            ac_columns = ac_data_one_station.columns
            if not set(major_columns).issubset(ac_columns):
                print('*' * 15)
                print(station)
                print(ac_columns)
                print('*' * 15)
                continue
            # 将数据写入到数据库中
            # 添加一列写入日期
            major_columns.insert(0,'id')
            major_columns.insert(1,'station')
            major_columns.append('update_time')
            # 添加站点列
            ac_data_one_station['station'] = station
            # 添加时间列
            ac_data_one_station['update_time'] = datetime.today().replace(microsecond=0)
            ac_data_one_station['id'] = range(station_len,station_len+ac_data_one_station.shape[0])
            # 写入需要的列
            ac_data_one_station = ac_data_one_station[major_columns]
            # 修改列名
            ac_data_one_station.rename(columns={'asin1':'asin'},inplace=True)
            table_name = 'station_ac_major_data'
            # 若站点数据数据存在,则先删除
            sql_write_read.query_table("""DELETE FROM {} where station = {}""".format(table_name,"'%s'" % station))
            # 将站点ac数据写入大数据库中
            sql_write_read.to_table_append(ac_data_one_station, table_name)
            print(f'{station}:完成ac表数据上传')
            station_len += ac_data_one_station.shape[0]
        except Exception as e:
            print(e)
            print(path)


if __name__ == '__main__':
    all_ac_info_upload()
