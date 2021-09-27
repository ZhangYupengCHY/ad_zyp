#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/7/23 0023 9:34
# @Author  : Zhang YP
# @Email   : 1579922399@qq.com
# @github  :  Aaron Ramsey
# @File    : init_station_report.py


"""
初始化站点数据:
        去除列名中的空格
        填充空白值
        重命名
        修改列名数据类型
"""

import pandas as pd
import numpy as np

from my_toolkit import public_function,process_files,commonly_params,change_rate


# 站点列名
station_columns_dict = {
    'cp': ['Record ID', 'Record Type', 'Campaign ID', 'Campaign', 'Campaign Daily Budget', 'Portfolio ID',
           'Campaign Start Date', 'Campaign End Date', 'Campaign Targeting Type', 'Ad Group', 'Max Bid',
           'Keyword or Product Targeting', 'Product Targeting ID', 'Match Type', 'SKU', 'Campaign Status',
           'Ad Group Status', 'Status', 'Impressions', 'Clicks', 'Spend', 'Orders', 'Total Units', 'Sales', 'ACoS',
           'Bidding strategy', 'Placement Type', 'Increase bids by placement'],
    'bd': ['Record ID', 'Record Type', 'Campaign ID', 'Campaign', 'Campaign Daily Budget', 'Portfolio ID',
           'Campaign Start Date', 'Campaign End Date', 'Budget Type', 'Landing Page Url', 'Landing Page ASINs',
           'Brand Name', 'Brand Entity ID', 'Brand Logo Asset ID', 'Headline', 'Creative ASINs', 'Automated Bidding',
           'Bid Multiplier', 'Ad Group', 'Max Bid',
           'Keyword', 'Match Type', 'Campaign Status', 'Serving Status',
           'Ad Group Status', 'Status', 'Impressions', 'Clicks', 'Spend', 'Orders', 'Total Units', 'Sales', 'ACoS',
           'Placement Type']}


def strip_space(data):
    """
    站点五表都需要进行初始化
        列名去掉空格
    Args:
        data:pd.DataFrame
            站点数据
    Returns:pd.DataFrame
            初始化后的站点数据

    """
    # 1.删除列名中的空格
    data.columns = [column.strip() for column in data.columns]
    # 2.删除全部数据的空格
    data = data.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    return data


def fill_value(data, report_type, fill_type='ffill'):
    """
        站点填充空白值
    Args:
        data:pd.DataFrame
            站点列表数据
        report_type:str
            报表类型
        fill_type:str default ffill
            站点列表类型

    Returns:pd.DataFrame

    """
    # 处理广告报表
    if report_type in ['cp', 'bd']:
        process_columns = ['Max Bid','Campaign Targeting Type']
        for col in process_columns:
            if col in data.columns:
                data[col].fillna(method=fill_type, inplace=True)
        numerical_process_columns = ['Impressions', 'Clicks', 'Spend', 'Orders', 'Total Units', 'Sales']
        for col in numerical_process_columns:
            if col in data.columns:
                data[col].fillna(value=0, inplace=True)


def rename_columns(data, report_type):
    """
    修改站点列名
    Args:
        report_type:str
            站点类型
        data: pd.DataFrame
            站点数据

    Returns:pd.DataFrame

    """
    # 修改广告报表列名
    # 首先检测报表列名是否有效
    if report_type == 'cp':
        data.rename(columns={'Total units': 'Total Units'}, inplace=True)
        columns = data.columns
        if not set(columns).issubset(set(station_columns_dict['cp'])):
            new_col = set(columns) - set(station_columns_dict['cp'])
            raise ValueError(f'cp report have new col:{new_col}')
    if report_type == 'ac':
        data.rename(columns={'出品者SKU': 'seller-sku', 'ASIN 1': 'asin1', '卖家 SKU': 'seller-sku', 'ASIN1': 'asin1','商品名':'item-name',
                             '出品日': 'open-date', '価格': 'price', '价格': 'price', '开售日期': 'open-date','数量':'quantity',
                             '配送渠道': 'fulfillment-channel','フルフィルメント・チャンネル':'fulfillment-channel','fulfilment-channel':'fulfillment-channel'},
                    inplace=True)
    if report_type == 'br':
        data.rename(columns={'已订购商品销售额': 'Ordered Product Sales','(Child) ASIN': 'ASIN'},inplace=True)
        multiStringRename = {col:col.title() for col in data.columns if ' ' in col}
        data.rename(columns=multiStringRename,inplace=True)
    if report_type == 'bd':
        data.rename(columns={'Total units': 'Total Units'}, inplace=True)
        # columns = data.columns
        # if not set(columns).issubset(set(station_columns_dict['bd'])):
        #     new_col = set(columns) - set(station_columns_dict['bd'])
        #     raise ValueError(f'bd report have new col:{new_col}')
    if report_type == 'st':
        replace_str = ['(#)', '(£)', '($)', '(₹)']
        for re_str in replace_str:
            data.columns = [col.replace(re_str, '') for col in data.columns]
        data.columns = [col.strip() for col in data.columns]
        data.rename(columns={'Advertising Cost of Sales (ACoS)': 'Total Advertising Cost of Sales (ACoS)',
                             'Return on Advertising Spend (RoAS)': 'Total Return on Advertising Spend (RoAS)'},
                    inplace=True)


def trans_columns_type(data, report_type):
    """
    修改站点中列的数据类型
    Args:
        data: pd.DataFrame
            站点数据
        report_type: str
            报表类型

    Returns:pd.DataFrame

    """
    # 广告报表中广告产品报表和品牌报表需要处理的列为:
    # int: Impressions,Clicks,Orders,Total units
    # float:Spend,Sales,Max Bid
    if report_type in ['cp', 'bd']:
        int_columns = ['Impressions', 'Clicks', 'Orders', 'Total Units']
        for col in int_columns:
            if not isinstance(data[col].values[0], (np.int64, np.int32)):
                data[col] = data[col].apply(lambda x: public_function.trans_into_numerical(x))
        float_columns = ['Spend', 'Sales', 'Max Bid']
        for col in float_columns:
            if not isinstance(data[col].values[0], (np.float64, np.float32)):
                data[col] = data[col].apply(lambda x: public_function.trans_into_numerical(x, type='float', point=2))
    if report_type in ['ac']:
        float_columns = ['price']
        for col in float_columns:
            if not isinstance(data[col].values[0], (np.float64, np.float32)):
                data[col] = data[col].apply(lambda x: public_function.trans_into_numerical(x, type='float', point=2))
    if report_type in ['br']:
        float_columns = ['Ordered Product Sales']
        for col in float_columns:
            if not isinstance(data[col].values[0], (np.float64, np.float32)):
                data[col] = data[col].apply(lambda x: public_function.trans_into_numerical(x, type='float', point=2))


def change_currency(data, report_type, site, currency_columns=None, currency='local', point_keep=2):
    # 修改报表货币列表
    # 获取汇率
    rate_dict = change_rate.change_current()
    exchange_rate = rate_dict[site]
    if currency == 'local':
        pass
    elif currency == 'dollar':
        if report_type == 'cp':
            if currency_columns is None:
                currency_columns = ['Spend', 'Sales']
            for col in currency_columns:
                data[col] = data[col].apply(lambda x: round(exchange_rate*x, point_keep) if pd.notna(x) else 0)


def init_report(data, report_type):
    """
    初始化站点数据
        去除列名中的空格
        填充空白值
        重命名
        修改列名数据类型
        填充丢失列
    Args:
        data: pd.DataFrame
            站点报表数据
        report_type: str
            站点报表类型:

    Returns:

    """
    if not public_function.is_valid_df(data):
        return
    if report_type not in commonly_params.station_report_type:
        raise ValueError(f'ERROR FILE TYPE.Input type must be one of {commonly_params.station_report_type}')
    # 1.去除列中的空格和数据中的空格
    data = strip_space(data)
    # 2.重命名
    rename_columns(data, report_type)
    # 3.填充空白值
    fill_value(data, report_type)
    # 4.修改列的数据类型
    trans_columns_type(data, report_type)
    return data
