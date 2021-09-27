import copy
import gc
import hashlib
import json
import os
import re
from datetime import datetime, timedelta
from concurrent.futures import as_completed, ProcessPoolExecutor,ThreadPoolExecutor
import time
import shutil
import warnings
from functools import reduce
import numpy as np

import pandas as pd
import requests

import my_toolkit.process_files as process_files
import my_toolkit.public_function as public_function
import my_toolkit.init_station_report as init_station_report
import my_toolkit.sql_write_read as sql_write_read
import my_toolkit.change_rate as change_rate
import my_toolkit.conn_db as conn_db
from my_toolkit import station_belong, commonly_params, query_frequently_table_info,process_station

warnings.filterwarnings(action="ignore")
change_current = change_rate.change_current()
PROCESS_NUM = 1
PROCESSPOOL = ProcessPoolExecutor(PROCESS_NUM)

"""
计算站点的st和sku表现
"""


def get_all_ad_data(camp_data, ac_data):
    """
    所有活跃广告数据
    """
    ad_row = camp_data[(camp_data["Campaign Status"] != "paused") & (camp_data["Ad Group Status"] != "paused") &
                       (camp_data["Status"] != "paused") & (camp_data["Record Type"] == "Ad")].copy()
    ad_data = ad_row[['Campaign', 'Ad Group', 'SKU', 'Impressions', 'Clicks', 'Spend', 'Orders', 'Sales']].copy()
    sku_asin = ac_data[["SKU", "ASIN"]].copy()
    ad_data = pd.merge(ad_data, sku_asin, on="SKU", how="right")
    return ad_data[['Campaign', 'Ad Group', 'SKU', "ASIN", 'Impressions', 'Clicks', 'Spend', 'Orders', 'Sales']]


def get_all_sku_data(camp_data, ac_data, br_data):
    ad_row = camp_data[camp_data["Record Type"] == "Ad"].copy()
    # 统计广告数据
    ad_data_by_sku = ad_row[['SKU', 'Impressions', 'Clicks', 'Spend', 'Orders', 'Sales']].copy()
    ad_data_by_sku = ad_data_by_sku.groupby("SKU").sum().reset_index()
    # ad_data_by_sku["ACoS"] = ad_data_by_sku.apply(lambda z: z["Spend"] / z['Sales'] if z['Sales'] else 0, axis=1)
    ad_data_by_sku["ACoS"] = ad_data_by_sku["Spend"] / ad_data_by_sku["Sales"]
    ad_data_by_sku["ACoS"] = ad_data_by_sku["ACoS"].replace([np.inf, -np.inf, np.nan], 0)
    # ad_data_by_sku["广告转化率"] = ad_data_by_sku.apply(lambda z: z["Orders"] / z['Clicks'] if z['Clicks'] else 0, axis=1)
    ad_data_by_sku["广告转化率"] = ad_data_by_sku["Orders"] / ad_data_by_sku["Clicks"]
    ad_data_by_sku["广告转化率"] = ad_data_by_sku["广告转化率"].replace([np.inf, -np.inf, np.nan], 0)

    # 统计广告数量
    # 统计活跃广告数量
    active_ad_row = ad_row[(ad_row["Campaign Status"] != "paused") & (ad_row["Ad Group Status"] != "paused") &
                           (ad_row["Status"] != "paused")].copy()
    ad_quantity_by_sku = active_ad_row[["SKU", "Campaign Targeting Type"]].copy()
    # for x, group in ad_quantity_by_sku.groupby("SKU"):
    #     ad_quantity_by_sku.loc[group.index, '活跃自动广告数'] = len(group[group["Campaign Targeting Type"] == "Auto"])
    #     ad_quantity_by_sku.loc[group.index, '活跃手动广告数'] = len(group[group["Campaign Targeting Type"] == "Manual"])
    ad_quantity_by_sku = ad_quantity_by_sku.groupby(by=['SKU', 'Campaign Targeting Type']).agg({'SKU': "count"})
    ad_quantity_by_sku.rename(columns={'SKU': 'num'}, inplace=True)
    ad_quantity_by_sku.reset_index(inplace=True)
    ad_quantity_by_sku_auto = ad_quantity_by_sku[['SKU', 'Campaign Targeting Type', 'num']][
        ad_quantity_by_sku['Campaign Targeting Type'] == 'Auto']
    ad_quantity_by_sku_auto.rename(columns={'num': '活跃自动广告数'}, inplace=True)
    ad_quantity_by_sku_manual = ad_quantity_by_sku[['SKU', 'Campaign Targeting Type', 'num']][
        ad_quantity_by_sku['Campaign Targeting Type'] == 'Manual']
    ad_quantity_by_sku_manual.rename(columns={'num': '活跃手动广告数'}, inplace=True)
    ad_quantity_by_sku = pd.merge(ad_quantity_by_sku_auto, ad_quantity_by_sku_manual, on=['SKU'], how='outer')
    ad_quantity_by_sku['活跃自动广告数'].fillna(0, inplace=True)
    ad_quantity_by_sku['活跃手动广告数'].fillna(0, inplace=True)
    ad_quantity_by_sku = ad_quantity_by_sku[["SKU", "活跃自动广告数", "活跃手动广告数"]].drop_duplicates()

    # ---------------------------------------获取Active Listing Report数据----------------------------------------------
    ac_data.rename(columns={'seller-sku': 'SKU', 'asin1': 'ASIN'}, inplace=True)
    ac_need_columns = ['SKU', 'ASIN', 'price', 'quantity', 'fulfillment-channel', 'open-date']
    if not set(ac_need_columns).issubset(set(ac_data.columns)):
        print(f"ac 缺失列:{set(ac_need_columns) - set(ac_data.columns)}")
        return
    listing_by_sku = ac_data[ac_need_columns]
    listing_by_sku["quantity"].fillna(0, inplace=True)

    # ------------------------------------------获取Business Report数据-------------------------------------------------
    # br_data.rename(columns={'(Child) ASIN': 'ASIN'}, inplace=True)
    br_need_columns = ['SKU', 'ASIN', 'Sessions', 'Units Ordered', 'Ordered Product Sales']
    if not set(br_need_columns).issubset(set(br_data.columns)):
        print(f"br缺失列:{set(br_need_columns) - set(br_data.columns)}")
        return
    br_by_sku = br_data[br_need_columns]
    br_by_sku = br_by_sku.groupby('SKU', as_index=False).agg(
        {'Sessions': 'sum', 'Units Ordered': 'sum', 'Ordered Product Sales': 'sum', 'ASIN': 'min'})
    for col in ['Units Ordered', 'Sessions']:
        if isinstance(br_by_sku[col], object):
            br_by_sku[col] = [int(sessions.replace(',', '')) if type(sessions) == str else sessions for sessions in
                              br_by_sku[col]]
    br_by_sku["店铺转化率"] = br_by_sku["Units Ordered"] / br_by_sku['Sessions']
    br_by_sku["店铺转化率"] = br_by_sku["店铺转化率"].replace([np.inf, -np.inf, np.nan], 0)

    # ------------------------------------------------------------------------------------------------------------------
    # 统计所有数据
    all_data_by_sku = pd.merge(listing_by_sku, ad_data_by_sku, on="SKU", how="right")
    all_data_by_sku = pd.merge(all_data_by_sku, ad_quantity_by_sku, on="SKU", how="left")
    all_data_by_sku = pd.merge(all_data_by_sku, br_by_sku, on=["SKU", "ASIN"], how="left")
    all_data_by_sku.fillna(0, inplace=True)
    all_data_by_sku.rename(columns={'Impressions': '广告总曝光', 'Clicks': '广告总点击', 'Spend': '广告总花费',
                                    'Orders': '广告总订单', 'Sales': '广告总销售额', 'Sessions': '店铺总流量',
                                    'Units Ordered': '店铺总订单', 'Ordered Product Sales': '店铺总销售额'}, inplace=True)
    # 店铺总销售额转换为数值型
    # all_data_by_sku["店铺总销售额"] = public_function.series_to_numeric(all_data_by_sku["店铺总销售额"])

    all_data_by_sku["广告流量占比"] = all_data_by_sku["广告总点击"] / all_data_by_sku["店铺总流量"]
    all_data_by_sku["广告流量占比"] = all_data_by_sku["广告流量占比"].replace([np.inf, -np.inf, np.nan], 0)

    all_data_by_sku["广告销售额占比"] = all_data_by_sku["广告总销售额"] / all_data_by_sku["店铺总销售额"]
    all_data_by_sku["广告销售额占比"] = all_data_by_sku["广告销售额占比"].replace([np.inf, -np.inf, np.nan], 0)
    # 转百分比
    # for col in ['ACoS', '广告转化率', '广告流量占比', '广告销售额占比', '店铺转化率']:
    #     all_data_by_sku[col] = all_data_by_sku[col].apply(lambda z: "{}%".format(round(z * 100, 2)))

    columns_list = ['SKU', 'ASIN', 'price', 'quantity', 'fulfillment-channel', 'open-date',
                    '广告总曝光', '广告总点击', '广告总花费', '广告总订单', '广告总销售额', 'ACoS', '广告转化率',
                    '广告流量占比', '广告销售额占比', '活跃自动广告数', '活跃手动广告数', '店铺总流量', '店铺总订单',
                    '店铺总销售额', '店铺转化率']
    all_data_by_sku = all_data_by_sku[columns_list]
    all_data_by_sku.sort_values(["店铺总订单"], ascending=False, inplace=True)
    # 筛选出店铺总订单大于0
    # all_data_by_sku = all_data_by_sku[all_data_by_sku['店铺总订单'] > 0]
    # 填充nan
    all_data_by_sku['广告流量占比'].fillna(0, inplace=True)
    all_data_by_sku['广告销售额占比'].fillna(0, inplace=True)
    # 转化为百分比
    percent_columns = {'ACoS': 2, '广告转化率': 2, '广告流量占比': 2, '广告销售额占比': 2, '店铺转化率': 2}
    for column, point in percent_columns.items():
        all_data_by_sku[column] = public_function.series_numeric_to_percent(all_data_by_sku[column], points_keep=point)
    return all_data_by_sku


def api_push_sku_perf(erpid, erpStationName, sku_perf):
    """
    将站点sku表现推送
    Parameters
    ----------
    sku_perf_df :

    Returns
    -------

    """

    # todo MD5加密,固定的写法
    def encrypt_MD5(params):
        m = hashlib.md5()
        m.update(params.encode("utf-8"))
        return m.hexdigest()

    def natural_key(s):
        return [int(s) if s.isdigit() else s for s in re.split(r'(\d+)', s)]

    # 获取erpid以及account_name

    sku_perf_df = sku_perf.copy()
    sku_perf_df.rename(columns={"fulfillment-channel": "fulfillment_channel", "open-date": 'open_date',
                                '广告总曝光': 'ad_impressions', '广告总点击': 'ad_clicks', '广告总花费': 'ad_spend',
                                '广告总订单': 'ad_orders', '广告总销售额': 'ad_sales', '广告转化率': 'ad_cr',
                                '广告流量占比': 'ad_traffic_rate', '广告销售额占比': 'ad_sales_rate', '活跃自动广告数': 'active_auto_ad',
                                '活跃手动广告数': 'active_manual_ad', '店铺总流量': 'shop_traffic',
                                '店铺总订单': 'shop_orders', '店铺总销售额': 'shop_sales', '店铺转化率': 'shop_cr'}, inplace=True)

    data_columns = sku_perf_df.columns
    sku_perf_df['account_name'] = erpStationName
    sku_perf_df['account_id'] = erpid

    now_datetime = datetime.now().strftime('%Y%m%d%H%M%S')
    sku_perf_df['update_time'] = now_datetime

    all_export_columns = ['account_id', 'account_name', 'update_time']
    all_export_columns.extend(data_columns)
    data = sku_perf_df[all_export_columns]

    json_data = data.to_json(orient='records')

    params = {
        "body": json_data,
        "t": int(time.time()),
        # "t": fake_time,
        "key": "amazon_ad",
        "secret": "b57c#N1!",
    }

    l = sorted(params.items(), key=lambda x: natural_key(x[0]))
    query = map(lambda x: "{}={}".format(x[0], x[1]), l)
    query_string = '&'.join(list(query))
    sign = encrypt_MD5(query_string).upper()
    params.update({"sign": sign})

    del (params['secret'])
    del (params['body'])

    headers = {
        "Content-Length": "<calculated when request is sent>",
        "Content-Type": "application/json",
        "Host": 'salesworkbench.yibainetwork.com',
    }
    # request_url = 'http://szf.yibai-it.com:33668/api/v2/sales/amazon/skuPerformance'
    request_url = 'http://salesworkbench.yibainetwork.com:8001/api/v2/sales/amazon/skuPerformance/'
    try:
        requests_data = requests.post(url=request_url, headers=headers, params=params, data=json_data, timeout=(10, 60))
    except Exception as e:
        print(e)
        print(f'{request_url}无法连接.{datetime.now().replace(microsecond=0)}')
        return False
    try:
        response_dict = json.loads(requests_data.text)
        if 'data' in response_dict.keys():
            return True
        else:
            return False
    except:
        return False


def stations_sku_perf(station_name, camp_data, ac_data, br_data, report_time,
                      station_sku_perf_folder=f"F:\station_sku_perf_for_seller"):
    """
    计算广告sku，ad_group表现给销售生成表给销售使用
    :param camp_data:
    :param ac_data:
    :param br_data:
    :return:
    """
    #
    site = station_name[-2:].upper()
    if site not in public_function.COUNTRY_CN_EN_DICT.values():
        raise ValueError(f'{site}站点不能识别')

    exchangeRate = change_current[site]

    # 初始化站点列表数据
    if len(camp_data.index) != 0:
        camp_data = init_station_report.init_report(camp_data, 'cp')
    if len(ac_data.index) != 0:
        ac_data = init_station_report.init_report(ac_data, 'ac')
    if len(br_data.index) != 0:
        br_data = init_station_report.init_report(br_data, 'br')
    all_data_by_sku = get_all_sku_data(camp_data, ac_data, br_data)
    all_ad_data = get_all_ad_data(camp_data, ac_data)

    # 将结果存储为pkl文件
    # now_date = datetime.now().date().strftime('%Y-%m-%d')
    write_df_dict = {'data': [], 'sheet_name': []}
    # save_name = f'{station_name}_{now_date}_sku_表现.xlsx'
    save_name = f'{report_time} {station_name} sku表现.xlsx'
    save_path = os.path.join(station_sku_perf_folder, save_name)
    if isinstance(all_data_by_sku, pd.DataFrame) and len(all_data_by_sku.index) > 0:
        # 汇率转换
        exchangeSKUColumns = ['广告总花费', '广告总销售额', '店铺总销售额', 'price']
        allSKUcolumns = all_data_by_sku.columns
        for col in exchangeSKUColumns:
            if col in allSKUcolumns:
                if isinstance(all_data_by_sku[col], object):
                    all_data_by_sku[col] = sql_write_read.series_to_numeric(all_data_by_sku[col])
                all_data_by_sku[col] = [round(money * exchangeRate, 2) for money in all_data_by_sku[col]]

        write_df_dict['data'].append(all_data_by_sku)
        write_df_dict['sheet_name'].append('sku表现')
        adStationErpIdDict = station_belong.station_name_2_erp_id()
        erpId = adStationErpIdDict.get(station_name.lower(), None)
        stationCompany = station_belong.station_company(station_name)
        # 推送给公司服务器
        if erpId is not None:
            adStationErpStationDict = station_belong.station_name_2_erp_station_name()
            erpStation = adStationErpStationDict.get(station_name.lower(), None)
            # 将站点的sku表现推送给公司服务器
            push_result = api_push_sku_perf(erpId, erpStation, all_data_by_sku)
            if push_result:
                print(f'{station_name}:成功推送sku表现信息给服务器.')
            else:
                print(f'{station_name}:失败推送sku表现信息给服务器.')
        else:
            print(f'{station_name}属于其他公司找不到erpId,无法推送给公司服务器.')

    if isinstance(all_ad_data, pd.DataFrame) and len(all_ad_data.index) > 0:
        # 转换货币列
        exchangeAdColumns = ['Spend', 'Sales']
        allAdColumns = all_ad_data.columns
        for col in exchangeAdColumns:
            if col in allAdColumns:
                if isinstance(all_ad_data[col], object):
                    all_ad_data[col] = sql_write_read.series_to_numeric(all_ad_data[col])
                all_ad_data[col] = [round(money * exchangeRate, 2) for money in all_ad_data[col]]
        write_df_dict['data'].append(all_ad_data)
        write_df_dict['sheet_name'].append('广告组表现')

    # 删除历史该站点文件
    [os.remove(os.path.join(station_sku_perf_folder, file)) for file in os.listdir(station_sku_perf_folder) if
     (station_name in file)]

    # 将结果写入到excel中
    writer = process_files.WriteDf2Excel()
    writer.to_excel(write_df_dict['data'], sheets_name=write_df_dict['sheet_name'])
    writer.save(save_path)
    print(f'{datetime.now().replace(microsecond=0)} {station_name}完成sku表现.')


def station_st_perf(station_name, st, cam_df, ac_data, save_basename):
    """
        将st表中的不同的搜索词的表现分成5个等级:
            1.出单优质搜索词
            2.未出单高点击搜索词
            3.近期低于平均点击率的SKU
            4.后台Search Term参考
            5.不出单关键词
        同时将表现不好的词引入同erpsku的出单词：
            6.同erp_sku下其他seller_sku出单关键词

    Parameters
    ----------
    station_name : str
        站点名
    st : pd.DataFrame
        搜索词数据
    cam_df: pd.DataFrame
        广告报表数据
    ac_data: pd.DataFrame
        active报表数据
    save_basename: str
        文件保存的路径

    Returns
    -------

    """

    def excavate_auto_low_ctr(cam_df):
        """整理点击率低于平均值的自动广告"""
        now_df = cam_df.copy()
        now_df = now_df.loc[(now_df['Record Type'] == 'Ad') &
                            (now_df['Type'] == 'Auto') &
                            (now_df['Campaign Status'] == 'enabled') &
                            (now_df['Ad Group Status'] == 'enabled') &
                            (now_df['Status'] == 'enabled') &
                            (now_df['Impressions'] > 1000),
                            ['SKU', 'Impressions', 'Clicks', 'Spend', 'Orders', 'Sales']]
        if now_df.empty:
            # print("曝光大于1000的没有，无法生成点击率表")
            return pd.DataFrame()
        now_df.loc[:, 'Clicks'] = now_df.loc[:, 'Clicks'].astype(float)
        now_df = now_df.groupby('SKU').sum()
        now_df.reset_index(drop=False, inplace=True)
        now_df.loc[:, 'CTR'] = (now_df.loc[:, 'Clicks'] / now_df.loc[:, 'Impressions']).round(4)
        mean_ctr = round(now_df['Clicks'].sum() / now_df['Impressions'].sum(), 4)
        now_df = now_df.loc[now_df['CTR'] < mean_ctr]
        # 如果没有低于平均点击率的则返回空白的now_df
        if now_df.empty:
            return now_df
        now_df.loc[:, 'ACoS'] = now_df.apply(
            lambda m_data: m_data['Spend'] / m_data['Sales'] if m_data['Sales'] != 0 else 0, axis=1)
        now_df = now_df.sort_values(by='CTR', axis=0, ascending=True)

        now_df.rename(columns={'CTR': 'CTR' + '-平均' + str(mean_ctr)}, inplace=True)
        # 将CTR和ACoS转化为百分数，暂时不转
        return now_df

    # 整理广告报表中关st键词出现数量最多的组成一句话
    def excavate_st_many_kw(cam_df, st_df):
        now_df = cam_df.copy()

        # 增加广告组类型
        for one_cam, cam_group in now_df.groupby('Campaign'):
            now_df.loc[cam_group.index, 'type'] = cam_group.loc[cam_group.index[0], 'Campaign Targeting Type']

        # 取出所有的关键词的SKU
        all_manual_sku = now_df.loc[(now_df['Record Type'] == 'Ad') &
                                    (now_df['Campaign Status'] == 'enabled') &
                                    (now_df['Ad Group Status'] == 'enabled') &
                                    (now_df['Status'] == 'enabled'),
                                    ['Campaign', 'Ad Group', 'SKU']].copy()

        # 去除一对多广告
        all_manual_sku["Campaign&Ad Group"] = all_manual_sku["Campaign"] + all_manual_sku["Ad Group"]
        df_tmp = all_manual_sku["Campaign&Ad Group"].value_counts().to_frame().reset_index()
        df_tmp.columns = ['Campaign&Ad Group', 'count']
        all_manual_sku = pd.merge(all_manual_sku, df_tmp, on=['Campaign&Ad Group'], how='left')
        all_manual_sku = all_manual_sku[all_manual_sku['count'] == 1]  # 规范广告
        all_manual_sku.drop(['Campaign&Ad Group', 'count'], axis=1, inplace=True)

        all_manual_sku.rename(columns={'Campaign': 'Campaign Name', 'Ad Group': 'Ad Group Name'}, inplace=True)

        # st_df.columns = [one_col.encode('utf-8') for one_col in st_df.columns]
        st_df = st_df.loc[(st_df['Clicks'] > 0) &
                          (st_df['7 Day Total Orders'] > 0) &
                          (~st_df['Customer Search Term'].str.contains('b0')),
                          ['Campaign Name', 'Ad Group Name', 'Customer Search Term', '7 Day Total Orders']]
        now_df = pd.merge(st_df, all_manual_sku, on=['Campaign Name', 'Ad Group Name'], how='left')

        if now_df.empty:
            # print("关键词表为空，无法生成多行高频一句话")
            return pd.DataFrame()
        sku_order = now_df.loc[:, ['SKU', '7 Day Total Orders']].copy()
        sku_order_sum = sku_order.groupby('SKU').sum()
        sku_order_sum.reset_index(drop=False, inplace=True)

        # 汇总同一SKU下的词数，获取前200位的数据
        # now_df.loc[:, 'word'] = now_df.apply(
        #     lambda m_data: (m_data['Customer Search Term'] + ' ') * m_data['7 Day Total Orders (#)'], axis=1)
        now_df = now_df[['SKU', 'Customer Search Term']]
        now_df.set_index('SKU', inplace=True)
        split_df = now_df['Customer Search Term'].str.split(' ', expand=True)
        split_df.reset_index(drop=False, inplace=True)
        # print split_df.columns

        total_sku_str = {}
        for one_sku, sku_group in split_df.groupby('SKU'):
            count_0_list = []
            for one_col in sku_group.columns:
                if one_col != "SKU":
                    series_count = sku_group[one_col].value_counts()
                    count_0_list.append(series_count)
            count_df = pd.DataFrame(count_0_list)  # ,index=[one_sku]*len(count_0_list))

            for one_col in set(count_df.columns):
                if one_col in ['&', '', ' ', 'for', 'on', 'if', 'of', 's']:
                    del count_df[one_col]
                try:
                    num = float(one_col)
                    del count_df[one_col]
                except:
                    pass

            count_df.fillna(0, inplace=True)
            count_series = count_df.sum()
            count_df = pd.DataFrame(count_series.values, index=count_series.index, columns=['SKU'])

            count_df = count_df.sort_values(by='SKU', axis=0, ascending=False).head(200)

            if len(list(count_df.index)) == 1:
                sku_str = list(count_df.index)[0][0:250].split(' ')
            elif len(list(count_df.index)) > 1:
                sku_str = reduce(lambda m_data, n_data: m_data + ' ' + n_data, list(count_df.index))[0:250].split(
                    ' ')
            else:
                sku_str = ""

            if sku_str:
                now_real_str = sku_str[0:(len(sku_str))]
                if len(now_real_str) > 1:
                    sku_str_abbr = reduce(lambda m_data, n_data: m_data + ' ' + n_data, sku_str[0:(len(sku_str))])
                elif len(now_real_str) == 1:
                    sku_str_abbr = now_real_str[0]
                else:
                    sku_str_abbr = ""
                total_sku_str[one_sku] = sku_str_abbr

        total_df = pd.DataFrame(list(total_sku_str.values()), columns=['Search Term'])
        total_df.insert(0, 'SKU', list(total_sku_str.keys()))

        if not sku_order_sum.empty and not total_df.empty:
            total_df = pd.merge(total_df, sku_order_sum, on='SKU', how='left')
            # del total_df['SKU']
            total_df = total_df.sort_values(by='7 Day Total Orders', axis=0, ascending=False)
            total_df.rename(columns={'7 Day Total Orders': '近期广告出单量'}, inplace=True)

        return total_df

    # 整理广告报表中不出单的st键词出现数量最多的组成一句话，输入为广告报表df和搜索词df,输出为一个df。
    # 另外需要将这个df保存为ST 报表添加一个表，表名叫'listing屏蔽不转化词'.
    def excavate_no_order_st_many_kw(cam_df, st_df):
        now_df = cam_df.copy()

        # 增加广告组类型
        for one_cam, cam_group in now_df.groupby('Campaign'):
            now_df.loc[cam_group.index, 'type'] = cam_group.loc[cam_group.index[0], 'Campaign Targeting Type']

        # now_df = now_df[now_df["Clicks"] > 0]
        # 取出所有的关键词的SKU
        all_manual_sku = now_df.loc[(now_df['Record Type'] == 'Ad') &
                                    (now_df['Campaign Status'] == 'enabled') &
                                    (now_df['Ad Group Status'] == 'enabled') &
                                    (now_df['Status'] == 'enabled'),
                                    ['Campaign', 'Ad Group', 'SKU']].copy()
        # 去除一对多广告
        all_manual_sku["Campaign&Ad Group"] = all_manual_sku["Campaign"] + all_manual_sku["Ad Group"]
        df_tmp = all_manual_sku["Campaign&Ad Group"].value_counts().to_frame().reset_index()
        df_tmp.columns = ['Campaign&Ad Group', 'count']
        all_manual_sku = pd.merge(all_manual_sku, df_tmp, on=['Campaign&Ad Group'], how='left')
        all_manual_sku = all_manual_sku[all_manual_sku['count'] == 1]  # 规范广告
        all_manual_sku.drop(['Campaign&Ad Group', 'count'], axis=1, inplace=True)

        all_manual_sku.rename(columns={'Campaign': 'Campaign Name', 'Ad Group': 'Ad Group Name'}, inplace=True)

        st_df_now = st_df.loc[(st_df['Clicks'] > 0) &
                              (st_df['7 Day Total Units'] == 0) &
                              (~st_df['Customer Search Term'].str.contains('b0')),
                              ['Campaign Name', 'Ad Group Name', 'Customer Search Term', 'Clicks']]
        now_df = pd.merge(st_df_now, all_manual_sku, on=['Campaign Name', 'Ad Group Name'], how='left')

        if now_df.empty:
            # print("关键词表为空，无法生成多行高频一句话")
            return pd.DataFrame()
        sku_click = now_df.loc[:, ['SKU', 'Clicks']].copy()
        sku_click_sum = sku_click.groupby('SKU').sum()
        sku_click_sum.reset_index(drop=False, inplace=True)

        # 汇总同一SKU下的词数，获取前200位的数据
        now_df.loc[:, 'word'] = now_df.apply(
            lambda m_data: (m_data['Customer Search Term'] + ' ') * m_data['Clicks'], axis=1)
        now_df = now_df[['SKU', 'Customer Search Term']]
        now_df.set_index('SKU', inplace=True)
        # print(len(now_df))
        split_df = now_df['Customer Search Term'].str.split(' ', expand=True)
        split_df.reset_index(drop=False, inplace=True)
        # print(split_df.columns)

        total_sku_str = {}
        for one_sku, sku_group in split_df.groupby('SKU'):
            count_0_list = []
            for one_col in sku_group.columns:
                if one_col != "SKU":
                    series_count = sku_group[one_col].value_counts()
                    count_0_list.append(series_count)
            count_df = pd.DataFrame(count_0_list)  # ,index=[one_sku]*len(count_0_list))

            for one_col in set(count_df.columns):
                if one_col in ['&', '', ' ', 'for', 'on', 'if', 'of', 's']:
                    del count_df[one_col]
                try:
                    num = float(one_col);
                    del count_df[one_col]
                except:
                    pass

            count_df.fillna(0, inplace=True)
            count_series = count_df.sum()
            count_df = pd.DataFrame(count_series.values, index=count_series.index, columns=['SKU'])

            count_df = count_df.sort_values(by='SKU', axis=0, ascending=False).head(200)

            if len(list(count_df.index)) == 1:
                sku_str = list(count_df.index)[0][0:250].split(' ')
            elif len(list(count_df.index)) > 1:
                sku_str = reduce(lambda m_data, n_data: m_data + ' ' + n_data, list(count_df.index))[0:250].split(
                    ' ')
            else:
                sku_str = ""

            if sku_str:
                now_real_str = sku_str[0:(len(sku_str))]
                if len(now_real_str) > 1:
                    sku_str_abbr = reduce(lambda m_data, n_data: m_data + ' ' + n_data, sku_str[0:(len(sku_str))])
                elif len(now_real_str) == 1:
                    sku_str_abbr = now_real_str[0]
                else:
                    sku_str_abbr = ""
                total_sku_str[one_sku] = sku_str_abbr

        total_df = pd.DataFrame(list(total_sku_str.values()), columns=['Search Term'])
        if total_df.empty:
            return total_df
        total_df.insert(0, 'SKU', list(total_sku_str.keys()))
        total_df = pd.merge(total_df, sku_click_sum, on='SKU', how='left')
        # del total_df['SKU']
        total_df = total_df.sort_values(by='Clicks', axis=0, ascending=False)
        total_df.rename(columns={'Clicks': '近期广告点击量'}, inplace=True)

        have_ordered_search_term_info = excavate_st_many_kw(cam_df, st_df)
        have_ordered_search_term_info.rename(columns={'Search Term': 'have_order_search_term'}, inplace=True)
        if have_ordered_search_term_info.empty:
            total_df['have_order_search_term'] = ''
        else:
            total_df = pd.merge(total_df, have_ordered_search_term_info, how='left', on='SKU')
        total_df[['Search Term', 'have_order_search_term']] = total_df[
            ['Search Term', 'have_order_search_term']].applymap(
            lambda x: set(str(x).split(' ')))
        total_df['Search Term'] = total_df['Search Term'] - total_df['have_order_search_term']
        total_df['Search Term'] = total_df['Search Term'].apply(lambda x: ' '.join(x))

        total_df.rename(columns={'Search Term': '未出单关键词后台一句',
                                 'have_order_search_term': '关键词后台一句'}, inplace=True)

        return total_df

    # 生成广告系列名字
    station_name = station_name.upper()
    account = station_name[:-3]
    country = station_name[-2:]

    # 初始化数据
    st = init_station_report.init_report(st, 'st')

    cam_df = init_station_report.init_report(cam_df, 'cp')  # 加广告类型辅助列
    # for x, group in cam_df.groupby('Campaign'):
    #     cam_df.loc[group.index, 'Type'] = cam_df.loc[group.index[0], 'Campaign Targeting Type']
    cam_df['Campaign Targeting Type'].fillna(method='ffill', inplace=True)
    cam_df['Type'] = cam_df['Campaign Targeting Type']
    ac_data = init_station_report.init_report(ac_data, 'ac')

    if 'item-name' not in ac_data.columns:
        ac_data['item-name'] = ''

    ac_data = ac_data[['seller-sku', 'item-name']]

    # 获取点击率低于平均值的自动广告"
    ctr_auto_df = excavate_auto_low_ctr(cam_df)

    # 获取词数最多的组成一句后台语句
    st_kw_str = excavate_st_many_kw(cam_df, st.copy())

    # 获取未出单的生成单独的表单
    no_order_df = excavate_no_order_st_many_kw(cam_df, st.copy())

    # 取出需要的行
    campaigndata1 = cam_df.loc[(cam_df['Record Type'] == 'Ad')]  # &

    # 取出满足一个adgroup一个Ad的行
    indexdata = []
    for (k1, k2), group in campaigndata1.groupby(['Campaign', 'Ad Group']):
        if len(group.index) == 1:
            indexdata.append(list(group.index)[0])
    campaigndata2 = campaigndata1.loc[indexdata]

    # 获取ST的相关数据数据
    for one_col in st.columns:
        if re.search('Targeting', one_col):
            st.rename(columns={'Targeting': 'Keyword'}, inplace=True)
            break
    st['Targeting'] = st['Keyword']
    st_report0 = st.loc[:, ['Campaign Name', 'Ad Group Name', 'Keyword', 'Match Type', 'Targeting',
                            'Customer Search Term', 'Impressions', 'Clicks', 'Spend',
                            '7 Day Total Orders', "7 Day Total Sales"]]

    # 根据一对一广告组获取SKU
    if campaigndata2.empty:
        st_report1 = st_report0
        st_report1.loc[:, 'SKU'] = ' '
    # 未提取到一一对应的广告组弹窗
    else:
        # 提取需要的列，并更换索引
        campaigndata3 = campaigndata2.loc[:, ['Campaign', 'Ad Group', 'SKU']]
        campaigndata3.loc[:, 'index'] = campaigndata3.apply(
            lambda m_data: m_data['Campaign'] + str(m_data['Ad Group']), axis=1)
        del campaigndata3['Campaign']
        del campaigndata3['Ad Group']
        campaigndata3.set_index('index', drop=True, inplace=True)

        # 更换索引
        st_report0['Ad Group Name'] = st_report0.loc[:, 'Ad Group Name'].apply(lambda m_data: str(m_data))
        st_report0.loc[:, 'index'] = st_report0.loc[:, 'Campaign Name'] + st_report0.loc[:, 'Ad Group Name']
        st_report0.set_index('index', drop=True, inplace=True)

        # 按campaigndata3索引合并st_report0
        st_report1 = st_report0.join(campaigndata3)
        # print st_report1[['SKU','Campaign Name']]
        # Nan填充为空
        st_report1.fillna(' ', inplace=True)
        st_report1.reset_index(drop=True, inplace=True)

        #  解析出SKU
        if st_report1.empty:
            return
        st_report1 = st_report1.loc[st_report1['SKU'] != ' ']
        if st_report1.empty: return
        # 解析targettype
        st_report1.loc[(st_report1['Keyword'] != '*'), 'Targeting type'] = 'MANUAL'
        st_report1.loc[(st_report1['Keyword'] == '*'), 'Targeting type'] = 'AUTO'

        # 判断是否加入搜索词
        st_report1.loc[((st_report1['Clicks'] >= 1) & (st_report1['7 Day Total Orders'] >= 1)), '是否出单'] = '是'
        st_report1.loc[~((st_report1['Clicks'] >= 1) & (st_report1['7 Day Total Orders'] >= 1)), '是否出单'] = '否'

        # print st_report1
        # 修改某一列的名字
        report2 = st_report1.rename(columns={'Impressions': '展示次数',
                                             'Clicks': '点击量',
                                             'Spend': '花费',
                                             '7 Day Total Sales': '销售额',
                                             '7 Day Total Orders': '订单量'
                                             })
        # 计算相应数据
        report2.loc[:, 'CTR'] = report2.apply(
            lambda m_data: str(round(float(m_data['点击量']) / m_data['展示次数'], 3) * 100) + '%' if m_data[
                                                                                                   '展示次数'] != 0 else 0,
            axis=1)
        report2.loc[:, 'CPC'] = report2.apply(
            lambda m_data: round(float(m_data['花费']) / m_data['点击量'], 2) if m_data['点击量'] != 0 else 0, axis=1)
        report2.loc[:, 'ACoS'] = report2.apply(
            lambda m_data: str(round(float(m_data['花费']) / m_data['销售额'], 3) * 100) + '%' if m_data[
                                                                                                 '销售额'] != 0 else 0,
            axis=1)
        report2.loc[:, 'CR'] = report2.apply(
            lambda m_data: str(round(float(m_data['订单量']) / m_data['点击量'], 3) * 100) + '%' if m_data[
                                                                                                  '点击量'] != 0 else 0,
            axis=1)

        file_folder = r"D:/st_info"
        file_path = os.path.join(file_folder, save_basename)
        # 删除站点原先存在的st表信息表
        [os.remove(os.path.join(file_folder, file)) for file in os.listdir(file_folder) if
         station_name.upper() in file.upper()]
        writer = pd.ExcelWriter(file_path)  # , enginge='openpyxl')

        # 计算出单了的优质广告，并输出
        report3 = report2[(report2['是否出单'] == '是') &
                          ~(report2['Customer Search Term'].str.contains('b0'))]
        report4 = report3[['SKU', 'Campaign Name', 'Ad Group Name', 'Match Type', 'Targeting', 'Customer Search Term',
                           '展示次数', '点击量', '花费', '销售额', '订单量', 'CTR', 'CPC', 'ACoS', 'CR']]
        if report4.empty:
            # print("优质广告为空")
            pass
        else:
            # 计算每个sku的销售额
            for one_sku, group in report4.groupby('SKU'):
                report4.loc[group.index, 'SKU总销售额'] = sum(list(group.loc[:, '销售额']))
            report4 = report4.sort_values(by=['SKU总销售额', 'SKU', '销售额'], axis=0, ascending=False)
            report4.reset_index(drop=True, inplace=True)
            report4['SKU'] = report4['SKU'].astype(str)
            ac_data['seller-sku'] = ac_data['seller-sku'].astype(str)
            report4 = pd.merge(report4, ac_data, left_on='SKU', right_on='seller-sku', how='left')
            # report4.loc[:, 'item-name'] = report4.loc[:, 'item-name'].apply(lambda m_data: json.dumps(m_data))
            del report4['seller-sku']

            report4.to_excel(writer, sheet_name='出单优质搜索词', na_rep='', index=False)

        # 计算未出单高点击的词，并输出
        report5 = report2[(report2['是否出单'] == '否') &
                          ~(report2['Customer Search Term'].str.contains('b0'))]
        report5 = report5[['SKU', 'Campaign Name', 'Ad Group Name', 'Match Type', 'Customer Search Term',
                           '展示次数', '点击量', '花费', '销售额', '订单量', 'CTR', 'CPC', 'ACoS', 'CR']]
        if report5.empty:
            print("未出单高点击的词为空")
        else:
            # 计算每个sku的点击量
            for one_sku, group in report5.groupby('SKU'):
                report5.loc[group.index, 'SKU总点击量'] = sum(list(group.loc[:, '点击量']))
            report5 = report5.sort_values(by=['SKU总点击量', 'SKU', '点击量'], axis=0, ascending=False)
            report5.reset_index(drop=True, inplace=True)
            report5.to_excel(writer, sheet_name='未出单高点击搜索词', na_rep='', index=False)

        # 输出点击率低于平均的SKU
        if not ctr_auto_df.empty:
            ctr_auto_df.to_excel(writer, sheet_name='近期低于平均点击率的SKU', na_rep='', index=False)

        # 输出出单关键词组成的一句话
        if not st_kw_str.empty:
            st_kw_str.to_excel(writer, sheet_name='后台Search Term参考', na_rep='', index=False)

        if not no_order_df.empty:
            no_order_df.to_excel(writer, sheet_name='不出单关键词', na_rep='', index=False)

        # 获得站点sellersku:数据库中的active表
        station_sku_map_query_sql = "select `seller-sku` from station_ac_major_data where station = '%s'" % station_name
        station_sku_map = list(sql_write_read.read_table(station_sku_map_query_sql)['seller-sku'])
        if station_sku_map:
            # 获得站点erpsku:数据库中的sellersku-erpsku捆绑表
            station_erpsku_map_info = sql_write_read.query_sku_tied(station_sku_map)
            if (station_erpsku_map_info is not None) and (not station_erpsku_map_info.empty):
                station_erpsku_map = list(station_erpsku_map_info['erp_sku'])
                if station_erpsku_map:
                    # 获取erpsku对应的关键词库信息
                    station_erpsku_map_str = sql_write_read.query_list_to_str(station_erpsku_map)
                    high_quality_keywords_by_erpsku = "select station,erpsku,asin,sku,campaign_name,ad_group_name,match_type,customer_search_term,kws_lang,impression,click,spend, " \
                                                      "sale,`order`,ctr,cpc,acos,cr,sku_sale," \
                                                      "updatetime from high_quality_keywords where erpsku in (%s)" % station_erpsku_map_str
                    high_quality_keywords_info_by_erpsku = sql_write_read.query_table(
                        high_quality_keywords_by_erpsku,
                        db='server_camp_report')
                    if not high_quality_keywords_info_by_erpsku.empty:
                        high_quality_keywords_info_by_erpsku.drop_duplicates(inplace=True)
                        # 获取到站点erp与sellersku对应关系表,同时添加本站点的sellersku
                        stationErpskuNSellerSku = process_station.Station.erpsku_sellersku_dict(station_name)
                        stationSkuColumnName = f'{station_name}站点sku'
                        high_quality_keywords_info_by_erpsku.insert(3,stationSkuColumnName,[list(stationErpskuNSellerSku.get(erp_sku,set())) for erp_sku in high_quality_keywords_info_by_erpsku['erpsku']])
                        high_quality_keywords_info_by_erpsku = high_quality_keywords_info_by_erpsku.explode(stationSkuColumnName)
                        high_quality_keywords_info_by_erpsku.drop_duplicates(inplace=True)
                        high_quality_keywords_info_by_erpsku.to_excel(writer, sheet_name="同erp_sku下其他seller_sku出单词",
                                                                      index=False)

        # 保存并关闭
        writer.save()
    print(f'{datetime.now().replace(microsecond=0)} {station_name}完成st表现.')


class FiveFileRedisKey(object):
    fiveFileSignKey = commonly_params.five_files_redis_sign
    timeFormat = '%Y%m%d%H%M%S'

    def __init__(self, fiveFilePklKey):
        if self.fiveFileSignKey not in fiveFilePklKey:
            raise ValueError(f'{fiveFilePklKey} 不包含五表的关键词{self.fiveFileSignKey}')
        self.fiveFilePklKey = fiveFilePklKey

    @classmethod
    def time(cls, fiveFilePklKey):
        # 提取日期
        try:
            keyDatetime = datetime.strptime(fiveFilePklKey[-14:], cls.timeFormat)
            return keyDatetime
        except ValueError as e:
            print(f'五表在redis中保存的键中日期无法转换成时间,{cls.timeFormat}')
            print(e)
            return

    def station(self):
        return self.fiveFilePklKey[len(self.fiveFileSignKey) + 1:-18]

    def file_type(self):
        return self.fiveFilePklKey[-17:-15]


def twice_cooked_sku_st_perf(station,reportTime):
    """
        重新处理精品一组的sku和st表现:
            1.将SKU报表整合在ST报表里面，只保留精品SKU表现
            2.ST报表中“未出单高点击搜索词”sheet表中点击量只显示5次及以上
            3.ST报表中“近期低于平均点击率的SKU"sheet表中”CTR-平均0.0045“一列与"sku表现"表整合，低与平均点击率的sku标注出来
            4.实现第三条，可以删除“近期低于平均点击率的SKU"sheet表
            5.ST报表中“出单优质搜索词”sheet表中"Customer Search Term"一列前面加入广告出单词"Targeting"一列
            6.sku表现表加一列“优化建议
            Parameters:
        station:str
                站点全称
    Returns:None
        export sku perf and st to Excel file
    """
    if (station is None) or (not isinstance(station, str)):
        raise TypeError('station type must string.')

    station = public_function.standardize_station(station, case='upper')

    allSpecialInfo = query_frequently_table_info.query_depart_unusual_special_sku(
        columns=['account_name', 'sellersku', 'erpsku'])
    if station.lower() not in set(allSpecialInfo['account_name']):
        return

    stPerfSaveFolder = r'D:/st_info'
    skuPerfSaveFolder = r'F:\station_sku_perf_for_seller'

    # 判断是否存在st/sku表现文件
    stationStPerfPath = [stationPath for stationPath in os.listdir(stPerfSaveFolder) if station in stationPath.upper()]
    if stationStPerfPath:
        stationStPerfPath = os.path.join(stPerfSaveFolder, stationStPerfPath[0])
    else:
        stationStPerfPath = None
    stationSkuPerfPath = [stationPath for stationPath in os.listdir(skuPerfSaveFolder) if
                          station in stationPath.upper()]
    if stationSkuPerfPath:
        stationSkuPerfPath = os.path.join(skuPerfSaveFolder, stationSkuPerfPath[0])
    else:
        stationSkuPerfPath = None

    # 处理sku表现表
    if stationSkuPerfPath is not None:
        # 加载精品sku信息
        stationSpecialSku = set(allSpecialInfo['sellersku'][allSpecialInfo['account_name'] == station.lower()])
        stationSpecialErpSku = set(allSpecialInfo['erpsku'][allSpecialInfo['account_name'] == station.lower()])
        skuSheets = pd.ExcelFile(stationSkuPerfPath)
        skuSheetsName = skuSheets.sheet_names
        skuSheetInfoDict = {'sku表现': None, '广告组表现': None}
        for signWord in skuSheetInfoDict.keys():
            if signWord not in skuSheetsName:
                return
            else:
                skuSheetInfoDict[signWord] = skuSheets.parse(signWord)
        # 处理1.将SKU报表整合在ST报表里面，只保留精品SKU表现
        if skuSheetInfoDict['sku表现'] is not None:
            skuSheetSkuPerfInfo = skuSheetInfoDict['sku表现']

            # 只截取精品sku部分
            skuSheetSkuPerfInfo = skuSheetSkuPerfInfo[skuSheetSkuPerfInfo['SKU'].isin(stationSpecialSku)]

            # 计算店铺平均acos,平均转换率,广告销售额占比
            skuSheetSkuPerfInfo = public_function.init_df(skuSheetSkuPerfInfo,
                                                          change_columns_type={'float': ['广告总花费', '广告总销售额', '店铺总销售额'],
                                                                               'int': ['广告总订单', '广告总曝光', '广告总点击']})
            if sum(skuSheetSkuPerfInfo['广告总销售额']) != 0:
                stationAcosSum = sum(skuSheetSkuPerfInfo['广告总花费']) / sum(skuSheetSkuPerfInfo['广告总销售额'])
            else:
                stationAcosSum = 0
            if sum(skuSheetSkuPerfInfo['广告总点击']) != 0:
                stationAdCrSum = sum(skuSheetSkuPerfInfo['广告总订单']) / sum(skuSheetSkuPerfInfo['广告总点击'])
            else:
                stationAdCrSum = 0
            if sum(skuSheetSkuPerfInfo['店铺总销售额']) != 0:
                stationSalesRateSum = sum(skuSheetSkuPerfInfo['广告总销售额']) / sum(skuSheetSkuPerfInfo['店铺总销售额'])
            else:
                stationSalesRateSum = 0

            # 添加计算列acos,adCtr,SalesRate
            skuSheetSkuPerfInfo['acos_numerical'] = skuSheetSkuPerfInfo['广告总花费'] / skuSheetSkuPerfInfo['广告总销售额']
            skuSheetSkuPerfInfo['ad_cr_numerical'] = skuSheetSkuPerfInfo['广告总订单'] / skuSheetSkuPerfInfo['广告总点击']
            skuSheetSkuPerfInfo['sales_rate_numerical'] = skuSheetSkuPerfInfo['广告总销售额'] / skuSheetSkuPerfInfo['店铺总销售额']
            warningWordDict = {'acos_numerical': {'warningNumerical': stationAcosSum, 'warningWord': 'acos高'},
                               'ad_cr_numerical': {'warningNumerical': stationAdCrSum, 'warningWord': '广告点击率低'},
                               'sales_rate_numerical': {'warningNumerical': stationSalesRateSum,
                                                        'warningWord': '广告销售额占比低'},
                               }
            skuSheetSkuPerfInfo['优化建议'] = ''
            for col, warningWordInfo in warningWordDict.items():
                if col == 'acos_numerical':
                    skuSheetSkuPerfInfo['优化建议'] = [f"{advise},{warningWordInfo['warningWord']}" if (
                                value > warningWordInfo['warningNumerical']) else advise
                                                   for value, advise in
                                                   zip(skuSheetSkuPerfInfo[col], skuSheetSkuPerfInfo['优化建议'])]
                else:
                    skuSheetSkuPerfInfo['优化建议'] = [f"{advise},{warningWordInfo['warningWord']}" if (
                                value < warningWordInfo['warningNumerical']) else advise
                                                   for value, advise in
                                                   zip(skuSheetSkuPerfInfo[col], skuSheetSkuPerfInfo['优化建议'])]
            skuSheetSkuPerfInfo['优化建议'] = [advise.lstrip(",") for advise in skuSheetSkuPerfInfo['优化建议']]

            # 添加ctr列
            skuSheetSkuPerfInfo['ctr'] = skuSheetSkuPerfInfo['广告总点击'] / skuSheetSkuPerfInfo['广告总曝光']
            skuSheetSkuPerfInfo['ctr'] = skuSheetSkuPerfInfo['ctr'].replace([np.nan, np.inf, -np.inf], 0)
            skuSheetSkuPerfInfo['ctr'] = [round(ctr, 4) for ctr in skuSheetSkuPerfInfo['ctr']]

            # 重置索引
            skuSheetSkuPerfInfo.reset_index(drop=True, inplace=True)
            # 添加汇总列
            skuSheetSkuPerfInfo.ix[0, '广告平均acos'] = str(round(stationAcosSum * 100, 2)) + '%'
            skuSheetSkuPerfInfo.ix[0, '广告平均转换率'] = str(round(stationAdCrSum * 100, 1)) + '%'
            skuSheetSkuPerfInfo.ix[0, '平均销售额占比'] = str(round(stationSalesRateSum * 100, 1)) + '%'
            skuSheetSkuPerfInfo[['广告平均acos', '广告平均转换率', '平均销售额占比']] = skuSheetSkuPerfInfo[
                ['广告平均acos', '广告平均转换率', '平均销售额占比']].fillna('')

            exportColumns = ["SKU", "ASIN", "price", "quantity", "fulfillment-channel", "open-date",
                             "广告总曝光", "广告总点击", "ctr", "广告总花费", "广告总订单", "广告总销售额",
                             "ACoS", "广告转化率", "广告流量占比", "广告销售额占比", "活跃自动广告数",
                             "活跃手动广告数", "店铺总流量", "店铺总订单", "店铺总销售额", "店铺转化率", '优化建议', '广告平均acos', '广告平均转换率', '平均销售额占比']
            skuSheetSkuPerfInfo = skuSheetSkuPerfInfo[exportColumns]
            skuSheetInfoDict['sku表现'] = skuSheetSkuPerfInfo

        if skuSheetInfoDict['广告组表现'] is not None:
            skuSheetGroupInfo = skuSheetInfoDict['广告组表现']
            skuSheetGroupInfo = skuSheetGroupInfo[skuSheetGroupInfo['SKU'].isin(stationSpecialSku)]
            skuSheetInfoDict['广告组表现'] = skuSheetGroupInfo
    # 然后处理st表现
    if stationStPerfPath is not None:
        stSheets = pd.ExcelFile(stationStPerfPath)
        stSheetsName = stSheets.sheet_names
        stSheetInfoDict = {'出单优质搜索词': None, '未出单高点击搜索词': None, '近期低于平均点击率的SKU': None,
                           '后台Search Term参考': None, '不出单关键词': None, '同erp_sku下其他seller_sku出单词': None}

        for signWord in stSheetInfoDict.keys():
            if signWord not in stSheetsName:
                return
            else:
                stSheetInfoDict[signWord] = stSheets.parse(signWord)
                if signWord == '同erp_sku下其他seller_sku出单词':
                    stSheetInfoDict[signWord] = stSheetInfoDict[signWord][
                        stSheetInfoDict[signWord]['erpsku'].isin(stationSpecialErpSku)]
                else:
                    stSheetInfoDict[signWord] = stSheetInfoDict[signWord][
                        stSheetInfoDict[signWord]['SKU'].isin(stationSpecialSku)]

        # 处理2.ST报表中“未出单高点击搜索词”sheet表中点击量只显示5次及以上
        if stSheetInfoDict['未出单高点击搜索词'] is not None:
            stSheetHighClickInfo = stSheetInfoDict['未出单高点击搜索词']
            stSheetHighClickInfo['点击量'] = public_function.series_to_numeric(stSheetHighClickInfo['点击量'])
            stSheetHighClickInfo = stSheetHighClickInfo[stSheetHighClickInfo['点击量'] >= 5]
            stSheetInfoDict['未出单高点击搜索词'] = stSheetHighClickInfo

    write_df_dict = skuSheetInfoDict
    write_df_dict.update(stSheetInfoDict)
    del write_df_dict['近期低于平均点击率的SKU']
    write_df_dict = {sheetName: public_function.init_df(sheetValue) for sheetName, sheetValue in write_df_dict.items()
                     if sheetValue is not None}

    # 删除站点原先存在的st表信息表
    [os.remove(os.path.join(skuPerfSaveFolder, file)) for file in os.listdir(skuPerfSaveFolder) if
     station.upper() in file.upper()]

    # 将结果写入到excel中
    stationCombinePerfSaveBasePath = f'{reportTime} {station} sku报表.xlsx'
    stationCombinePerfSavePath = os.path.join(skuPerfSaveFolder, stationCombinePerfSaveBasePath)
    writer = process_files.WriteDf2Excel()
    [writer.to_excel(sheetValue, sheets_name=sheetName) for sheetName, sheetValue in write_df_dict.items() if
     sheetValue is not None]
    writer.save(stationCombinePerfSavePath)
    print(f'完成精品站点:{station}sku和st表现的合并。')


def timedelta_hour(timeDelta):
    """
    timeDelta 计算hour
    :param timeDelta:
    :return:
    """
    if not isinstance(timeDelta, timedelta):
        return None
    else:
        return timeDelta.days * 24 + int(timeDelta.seconds / 3600)


def multiProcess_station():
    """
    多进程请求计算
    :return:
    """
    while 1:
        all_task = []
        for _ in range(PROCESS_NUM):
            try:
                all_task.append(PROCESSPOOL.submit(calc_one_station_st_and_sku))
            except Exception as e:
                print(e)
                continue
        for future in as_completed(all_task):
            future.result()


def calc_one_station_st_and_sku():
    """计算某个站点的st和sku表现"""
    redisConn  = public_function.Redis_Store(db=0)
    try:
        if redisConn.llen(commonly_params.STATION_SKU_ST_REDIS_KEY) == 0:
            print(f'暂时没有st,sku站点更新.')
            time.sleep(10)
            return
    except Exception as e:
        print(e)
        return
    stationRedisInfo = redisConn.blpop(commonly_params.STATION_SKU_ST_REDIS_KEY,timeout=5)
    if stationRedisInfo is None:
        return
    else:
        stationRedisInfo = stationRedisInfo[1]
        # stationRedisInfo redis保存格式
        # {station_name:{'cp':cpFilePklPath,'ac':acFilePklPath,'st':stFilePklPath,'br':brFilePklPath,'report_time':report_time}}}
        stationRedisInfo = json.loads(stationRedisInfo)
        stationRedisInfo = list(stationRedisInfo.items())[0]
    station = stationRedisInfo[0]
    station = public_function.standardize_station(station,case='upper')
    stationFilePlkPathDict = stationRedisInfo[1]
    acFilePath = stationFilePlkPathDict.get('ac')
    cpFilePath = stationFilePlkPathDict.get('cp')
    stFilePath = stationFilePlkPathDict.get('st')
    brFilePath = stationFilePlkPathDict.get('br')
    # global reportTime
    reportTime = stationFilePlkPathDict.get('report_time')
    # # todo 处理某一个站点
    # if station.upper() != "JUNLUCK_CA":
    #     return
    print(f'处理{station}')
    try:
        if (not os.path.exists(acFilePath)) or (not os.path.exists(cpFilePath)):
            return
    except Exception as e:
        return
    stationAcData = process_files.read_pickle_2_df(acFilePath)
    stationCpData = process_files.read_pickle_2_df(cpFilePath)
    # 计算st
    if not os.path.exists(stFilePath):
        return
    else:
        stationStData = process_files.read_pickle_2_df(stFilePath)
    station = public_function.standardize_station(station, case='upper')
    # 这里使用cp报表的时间
    stSavePath = reportTime + ' ' + f'{station}' + ' ' + 'ST报表' + '.xlsx'
    try:
        station_st_perf(station, stationStData, stationCpData, stationAcData, stSavePath)
    except Exception as e:
        print(e)
        return
    # 计算sku表现
    if not os.path.exists(brFilePath):
        return
    else:
        stationBrData = process_files.read_pickle_2_df(brFilePath)
    # 这里使用cp报表的时间
    try:
        stations_sku_perf(station, stationCpData, stationAcData, stationBrData, reportTime)
    except Exception as e:
        print(e)
        return

    # 将精品站点的st和sku报表合并
    twice_cooked_sku_st_perf(station,reportTime)

    # print(f'{station} st,sku表现处理完.')



# def need_process_station(refresh_start_time, refresh_end_time=None)->dict:
#     """需要处理的站点"""
#     if not isinstance(refresh_start_time, datetime):
#         raise TypeError(f'refresh_start_time must datetime')
#     if refresh_end_time is None:
#         refresh_end_time = datetime.now()
#     if not isinstance(refresh_end_time, datetime):
#         raise TypeError(f'refresh_end_time must datetime')
#     redisConn = public_function.Redis_Store(db=2)
#     fiveFiveRedisKey = [key for key in redisConn.keys() if commonly_params.five_files_redis_sign]
#     validFiveRedisKey = [key for key in fiveFiveRedisKey if
#                          (FiveFileRedisKey.time(key) is not None) and (
#                                  FiveFileRedisKey.time(key) < refresh_end_time) and (
#                                  FiveFileRedisKey.time(key) >= refresh_start_time)]
#     if len(validFiveRedisKey) == 0:
#         return
#     # stationRedisTypeDict结构
#     # {'kimiss_de':{'st':st_redis_key,'br':br_redis_key}}
#     stationRedisTypeDict = {}
#     for key in validFiveRedisKey:
#         redisKey = FiveFileRedisKey(key)
#         redisKeyStation = redisKey.station()
#         redisKeyStationType = redisKey.file_type()
#         stationRedisExistType = stationRedisTypeDict.get(redisKeyStation, None)
#         if stationRedisExistType == None:
#             stationRedisExistType = {redisKeyStationType: key}
#         else:
#             stationRedisExistType.update({redisKeyStationType: key})
#         stationRedisTypeDict[redisKeyStation] = stationRedisExistType
#     return stationRedisTypeDict


if __name__ == '__main__':
    multiProcess_station()
