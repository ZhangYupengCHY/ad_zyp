"""
统计近30天广告报表,点击,点击率
"""
import os
from datetime import datetime,timedelta

import pandas as pd

from my_toolkit import public_function,process_files
from concurrent.futures import ThreadPoolExecutor, as_completed



THREAD_NUM = 4
ThreadPool = ThreadPoolExecutor(THREAD_NUM)


def series_to_numeric(series,fill_na=0):
    """
    将series转换为数值型
    Parameters
    ----------
    series :
    fillna :

    Returns
    -------

    """
    if not isinstance(series,pd.Series):
        return series
    else:
        return pd.to_numeric(series,errors='coerce').fillna(value=fill_na,downcast='infer')


def get_camp_param(station_name,camp_data):
    """
    计算广告报表得到订单,点击等
    :param camp_path:
    :return:
    """
    if camp_data is None or camp_data.empty:
        return [0,0]
    # 判断计算的列是否存在
    impression_column = 'impressions'
    click_column = 'clicks'
    # 初始化广告报表中的列
    camp_data.columns = [col.strip().lower() for col in camp_data.columns]
    if not set([impression_column,click_column]).issubset(set(camp_data.columns)):
        print(f'{station_name}:缺少必要的计算列.impressions/clicks')
        return [0,0]
    camp_data[impression_column] = series_to_numeric(camp_data[impression_column])
    camp_data[click_column] = series_to_numeric(camp_data[click_column])
    impressions_sum = int(camp_data[impression_column].sum()/5)
    clicks_sum = int(camp_data[click_column].sum()/5)
    return [impressions_sum,clicks_sum]

def get_all_path():
    """
    dict {station:path}
    :return:
    """
    conn_redis = public_function.Redis_Store()
    # 得到redis
    keys = conn_redis.keys()
    # 筛选出cp表
    station_cps = [key for key in keys if key[-17:-15] == 'CP']
    # 站点信息
    station_cp_report_info = {redis_key[21:-18]: {'time': redis_key[-14:], 'path': conn_redis.get(redis_key)} for redis_key in
                              station_cps}
    # 40天以前
    day_before = datetime.strftime(datetime.now() - timedelta(days=40), '%Y%m%d%H%M%S')
    for key in list(station_cp_report_info.keys()):
        if station_cp_report_info[key]['time'] < day_before:
            del station_cp_report_info[key]
    # 关闭redis
    conn_redis.close()
    return {key:value['path'] for key,value in station_cp_report_info.items()}


def get_all_param():
    """
    获取全部站点的广告和点击
    :return:
    """
    # 获取站点全部满足条件的路径
    station_path_info = get_all_path()

    # 多线程去请求
    query_station_queue = public_function.Queue()
    query_station_queue.enqueue_items(list(station_path_info.keys()))

    # 四线程请求
    all_result = []
    while 1:
        all_task = []
        if query_station_queue.empty():
            break
        for _ in range(THREAD_NUM):
            if not query_station_queue.empty():
                # 请求站点列表
                station_name = query_station_queue.dequeue()
                station_cp_path = station_path_info[station_name]
                if os.path.exists(station_cp_path):
                    station_cp_data = process_files.read_pickle_2_df(station_cp_path)
                else:
                    continue
                all_task.append(ThreadPool.submit(get_camp_param, station_name, station_cp_data))
                print(f'完成{station_name}.')
        for future in as_completed(all_task):
            result = future.result()
            if result is not None and result:
                all_result.append(result)
    return pd.DataFrame(all_result,columns=['impressions','clicks'])


if __name__ == '__main__':
    # 得到redis
    info = get_all_param()
    info.to_csv(r"C:\Users\Administrator\Desktop\新建文本文档.txt",index=False)
    print('完成')