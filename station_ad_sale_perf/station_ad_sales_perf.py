# -*- coding: utf-8 -*-
"""
Proj: ad_helper
Created on:   2020/4/21 16:34
@Author: RAMSEY

"""

"""
=============================================
目的与意义:
    对每天每个站点的广告销售整体表现进行存储，为了展现和预警。
步骤:
    1.redis中获得又刷新的数据
    2.提取含有30天广告报表和br表的数据，对其中的广告花费、广告销售额、站点销售额进行汇总
    3.将汇总数据上传到数据库中
"""

import pandas as pd
import numpy as np
import re, os, pymysql, time, redis
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

import my_toolkit.public_function as public_function

PROCESS_POOL = ProcessPoolExecutor(4)
sale_exchange_rate = {'CA': 0.7519, 'DE': 1.0981, 'FR': 1.0981, 'IT': 1.0981, 'SP': 1.0981, 'JP': 0.009302,
                      'UK': 1.2445, 'MX': 0.05147, 'IN': 0.01412, 'US': 1, 'ES': 1.0981, 'AU': 0.6766, 'AE': 0.2723}
redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, password='', db=2, decode_responses=True)
red = redis.StrictRedis(connection_pool=redis_pool)


# 货币转换
def currency_trans(currency) -> 'digit':
    """
    将货币装换成数字
    逻辑:
        通过判断倒数第三位是否是,(逗号)或是.(点号)来判断是小数还是整数
    :param currency:需要转换的货币
    :return: 整型或浮点型货币
    """
    if pd.isnull(currency):
        return
    if not isinstance(currency, str):
        return
    else:
        currency = currency.strip(' ')
        currency_temp = re.findall('\d.*', currency)
        if len(currency_temp) == 1:
            currency_temp = currency_temp[-1]
            try:
                if currency_temp[-3] in [',', '.']:
                    # 该数字为包含两位小数的数字
                    return float(re.sub('[,.]', '', currency_temp)) / 100
                else:
                    # 该数字不包含两位小数的数字
                    return int(re.sub('[,.]', '', currency_temp))
            except:
                return int(re.sub('[,.]', '', currency_temp))
        if not currency_temp:
            return
        if len(currency_temp) > 1:
            return


# 计算单个站点的总体数据情况(站点花费,销售额,店铺销售额)
def calc_station_ad_sales_perf() -> dict:
    """
    计算单个完整站点的广告花费,广告销售额和店铺销售额
    :return: 将信息写入到redis中  {'spend':station_spend,'sales':station_sales,'acos':station_acos,'shop_sales':br_total_sales}
    """

    # 计算30天广告整体表现
    def get_ad_total_perf(station_name, camp_ori_data):
        """
        初始化30天广告报表数据:判断和修改列名，修改数据类型等
        :param station_name: 站点名
        :param camp_ori_data: 广告报表数据
        :return: 站点广告整体表现
        """
        # 1.判断camp是否有效
        if (camp_ori_data is None) or (not isinstance(camp_ori_data, pd.DataFrame)) or (camp_ori_data.empty):
            print(f'camp data error: {station_name}')
            return
        # 2.判断列是否存在
        camp_ori_data.columns = [col.strip(' ') for col in camp_ori_data.columns]
        columns = camp_ori_data.columns
        need_columns = {'Spend', 'Sales'}
        if not need_columns.issubset(columns):
            print(f'lost columns:{station_name} 的camp表缺少 {need_columns - columns}')
            return
        site = station_name[-2:].upper()
        for column in ['Spend', 'Sales']:
            if camp_ori_data[column].dtype not in [np.float64, np.int64]:
                camp_ori_data[column] = camp_ori_data[column].apply(lambda x: currency_trans(x))
        # camp表中包含五个层级的数据，于是需要除以5
        station_spend = round(sum(camp_ori_data['Spend']) * sale_exchange_rate[site] / 5, 2)
        station_sales = round(sum(camp_ori_data['Sales']) * sale_exchange_rate[site] / 5, 2)
        if station_sales > 0:
            station_acos = round(station_spend / station_sales, 4)
        else:
            station_acos = 0
        return [station_spend, station_sales, station_acos]

    # 计算店铺的销售额
    def get_shop_sales(station_name, br_ori_data):
        """
        初始化店铺的数据:判断和修改列名，修改数据类型等
        :param station_name: 站点名
        :param br_ori_data: BR原始表
        :return: 店铺的销售额
        """
        # 1.判断BR是否有效
        if (br_ori_data is None) or (not isinstance(br_ori_data, pd.DataFrame)) or (br_ori_data.empty):
            print(f'br data error: {station_name}')
            return
            # 2.判断列是否存在
        br_ori_data.columns = [col.strip(' ') for col in br_ori_data.columns]
        columns = br_ori_data.columns
        need_column = 'Ordered Product Sales'
        if need_column not in columns:
            print(f'lost columns:{station_name} 的br表缺少 {need_column},无法对店铺的销售额汇总!')
            return
        site = station_name[-2:].upper()
        if br_ori_data[need_column].dtype not in [np.float64, np.int64]:
            br_ori_data[need_column] = br_ori_data[need_column].apply(lambda x: currency_trans(x))
        br_total_sales = round(sum(br_ori_data['Ordered Product Sales']) * sale_exchange_rate[site], 2)
        return br_total_sales

    today_str = datetime.now().strftime('%Y-%m-%d')
    station_name = red.lpop(f'station_sales_overall:{today_str}')
    if not station_name:
        return
    print(f'开始上传站点表现:{station_name}')
    # completed_reports_station_save_folder = os.path.join(station_files_save_folder, today_str, station_name)
    # report_feature_kw = {'广告30天报表': '30天', 'BR表': 'business'}
    # camp_data = get_camp30_report(completed_reports_station_save_folder)
    # station_name = os.path.basename(completed_reports_station_save_folder)
    # br_data = get_br_report(completed_reports_station_save_folder)
    # ad_total_perf = get_ad_total_perf(station_name, camp_data)
    try:
        camp_data = public_function.get_station_data(station_name, 'CP')
        br_data = public_function.get_station_data(station_name, 'BR')
    except:
        return
    ad_total_perf = get_ad_total_perf(station_name, camp_data)
    shop_sales = get_shop_sales(station_name, br_data)
    redis_files_sign = 'FIVE_FILES_KEYS_SAVE'
    one_key_of_station = [key for key in red.keys() if (station_name in key) & (redis_files_sign in key)][0]
    file_datetime = one_key_of_station.split('_')[-1]
    update_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if (ad_total_perf is None) or (shop_sales is None):
        return
    else:
        ad_total_perf.append(shop_sales)
        ad_total_perf.append(file_datetime)
        ad_total_perf.append(update_datetime)
        red.rpush(f"station_sales_overall:{today_str}:{station_name}", *ad_total_perf)
        print(f'完成上传站点表现:{station_name}')


# 将广告总体表现上传到数据库中
def db_upload_ad_sales_perf():
    """
    描述:
        将当日全部站点的广告的整体表现上传到数据库中
        原始的数据类型:
            {station1:[spend1,sales1,acos1,shop_sales1],station2:[spend2,sales2,acos2,shop_sales2]}
        另外计算的数据:
            销售占比、花费占比、店铺、账号、站点、日期、更新时间
    :return:上传站点的广告信息到数据库中
    """

    # 将站点的整体表现数据上传到数据库中
    def db_update_ad_perf(ad_perf, db='team_station', table_name='station_sales_overall', ip='127.0.0.1',
                          port=3306,
                          user_name='marmot', password='marmot123'):
        try:
            conn = pymysql.connect(
                host=ip,
                user=user_name,
                password=password,
                database=db,
                port=port,
                charset='UTF8')
        except:
            return
        # 创建游标
        cursor = conn.cursor()
        all_list = []
        if (ad_perf is None) or (not isinstance(ad_perf, pd.DataFrame)) or (ad_perf.empty):
            return
        ad_perf.reset_index(drop=True, inplace=True)
        df = ad_perf.astype(object).replace(np.nan, 'None')
        df = np.array(df)
        len_df = df.shape[0]
        for i in range(len_df):
            temp_tuple = df[i]
            a_emp_tuple = tuple(temp_tuple)
            all_list.append(a_emp_tuple)

        # 执行sql语句
        try:
            insert_sql = """replace INTO {} (station,account,site,file_date,ad_spend,ad_sales,acos,shop_sales,spend_rate,sale_rate,file_datetime,update_datetime) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""".format(
                table_name)
            cursor.executemany(insert_sql, all_list)
            conn.commit()
            # print(f'ok! 今日站点总体表现上传成功,一共上传{len(all_list)}个站点!')
        except Exception as e:
            conn.rollback()
            # print('error! 今日站点总体表现上传失败')
            print(e)
        cursor.close()
        conn.close()

    # 整理上传的格式
    def init_station_perf(station_perf):
        if not station_perf:
            return
        # 将字典类型转换成dataframe
        all_station_sales_perf = pd.DataFrame.from_dict(station_perf, orient='index',
                                                        columns=['spend', 'sales', 'acos', 'shop_sales',
                                                                 'file_datetime', 'update_datetime'])
        all_station_sales_perf[['spend', 'sales', 'acos', 'shop_sales']] = all_station_sales_perf[[
            'spend', 'sales', 'acos', 'shop_sales']].applymap(lambda x: float(x) if isinstance(x, str) else x)
        # 添加其他列
        all_station_sales_perf['spend_rate'] = [spend / sale if sale > 0 else 0 for spend, sale in
                                                zip(all_station_sales_perf['spend'],
                                                    all_station_sales_perf['shop_sales'])]
        all_station_sales_perf['sale_rate'] = [ad_sale / shop_sales if shop_sales > 0 else 0 for ad_sale, shop_sales in
                                               zip(all_station_sales_perf['sales'],
                                                   all_station_sales_perf['shop_sales'])]
        all_station_sales_perf['station'] = all_station_sales_perf.index
        all_station_sales_perf['account'] = [station[:-3] for station in all_station_sales_perf['station']]
        all_station_sales_perf['site'] = [station[-2:] for station in all_station_sales_perf['station']]
        all_station_sales_perf['file_datetime'] = pd.to_datetime(list(all_station_sales_perf['file_datetime']),
                                                                 format='%Y%m%d%H%M%S')
        all_station_sales_perf['file_date'] = all_station_sales_perf['file_datetime'].apply(lambda x: x.date())
        all_station_sales_perf['file_datetime'] = all_station_sales_perf['file_datetime'].apply(
            lambda x: datetime.strftime(x, '%Y-%m-%d %H:%M:%S'))
        # 将全部的站点信息按照列名排列2020-06-05 18:16:38
        export_columns = ['station', 'account', 'site', 'file_date', 'spend', 'sales', 'acos', 'shop_sales',
                          'spend_rate',
                          'sale_rate', 'file_datetime', 'update_datetime']
        all_station_sales_perf = all_station_sales_perf[export_columns]
        return all_station_sales_perf

    # 从redis中获取数据
    def red_download_station_perf():
        today_str = datetime.now().strftime('%Y-%m-%d')
        today_key_kw = f'station_sales_overall:{today_str}'
        today_key_dict = {key.split(':')[-1]: red.lrange(key, 0, -1) for key in red.keys() if today_key_kw in key}
        [red.delete(key) for key in red.keys() if today_key_kw in key]
        return today_key_dict

    # 从reids中加载今日的站点总体数据
    station_perf = red_download_station_perf()

    # 规范上传的格式
    all_station_sales_overall = init_station_perf(station_perf)

    # 将今日站点数据上传到数据库中
    db_update_ad_perf(all_station_sales_overall)


# 多进程执行读写计算任务
def process_read_file():
    while 1:
        today_str = datetime.now().strftime('%Y-%m-%d')
        all_task = []
        for one_page in range(2):
            all_task.append(PROCESS_POOL.submit(calc_station_ad_sales_perf))
        for future in as_completed(all_task):
            future.result()
        if red.llen(f'station_sales_overall:{today_str}') == 0:
            break


# 站点整体销售概况主函数(汇总数据)
def station_ad_sales():
    """
    站点销售表现主函数,通过站点广告报表和BR报表,得到站点的一些汇总数据,用于web端显示.

    :return:
    """
    # step 1. 扫描文件,得到今日拥有cp表和br站点列表
    print(f'开始:{datetime.now()}')
    processed_stations = dict()
    while 1:
        completed_report_stations_updatetime_info = public_function.get_station_updatetime(need_file_types=['ST', 'BR'])
        # 需要处理的站点时间集合
        need_process_station = {station:updatetime for station, updatetime in
                                completed_report_stations_updatetime_info.items() if
                                processed_stations.get(station, None) != updatetime}
        if not need_process_station:
            print("暂时没有站点更新，休息10分钟...")
            time.sleep(600)
            continue
        today_str = datetime.now().strftime('%Y-%m-%d')
        # 删除redis库中的键
        keys = red.keys()
        [red.delete(key) for key in keys if f'station_sales_overall:{today_str}' in key]

        # 剔除掉已经处理的站点
        completed_report_stations = need_process_station.keys()
        red.rpush(f'station_sales_overall:{today_str}', *completed_report_stations)
        print(f'开始上传站点表现到数据库:{len(completed_report_stations)}个.')
        process_read_file()

        # 将redis中的数据取出，转换成dict格式,然后上传到数据库中
        db_upload_ad_sales_perf()
        # 将完成过的站点写到字典中
        processed_stations.update(completed_report_stations_updatetime_info)
        print(f'完成上传站点表现到数据库:{len(completed_report_stations)}个.')
        print("暂时没有站点更新，休息10分钟...")
        print('==========================================')
        time.sleep(600)
        # 重置字典
        if datetime.now().hour in set(range(0, 7)):
            # 休息到早上九点再开始更新
            restart_hour = 9
            reset_time = (restart_hour - datetime.now().hour) * 3600
            time.sleep(reset_time)
            print(f'早上{restart_hour}再开始.')
            processed_stations = set()


if __name__ == '__main__':
    station_ad_sales()
