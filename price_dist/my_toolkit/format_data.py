# -*- coding: utf-8 -*-
"""
Proj: AD-Helper1
Created on:   2019/11/20 18:21
@Author: RAMSEY

Standard:  
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""

import pandas as pd
import numpy as np


def format_num(ori_df: pd.DataFrame, trans_columns='all', float_dicimal_point=2, percent_decimal_point=0):
    """
    整理整型数据以及百分号数据:
    int: impression,impressions,clicks,orders,Total Units,Total Unit,quantity,Sessions
            Page Views,Units Ordered,Units Ordered - B2B,Total Order Items，Total Order Items - B2B，
            7 Day Advertised SKU Units (#)，7 Day Total Orders (#)，7 Day Total Units (#)，7 Day Other SKU Units (#)
    float: spend,sales,price,item-price，shipping-price，ship-promotion-discount，Campaign Daily Budget，Max Bid,Ordered Product Sales,
            cpc
    precent: acos,Session Percentage,Page Views Percentage,Buy Box Percentage,Unit Session Percentage
            Unit Session Percentage - B2B,Ordered Product Sales,Ordered Product Sales - B2B
            cr
    :param ori_df:数组名
    :param column:列名
    :param float_dicimal_point:浮点型数据保留的小数点位数
    :param percent_decimal_point:百分号保留的位数
    :return:返回规整后的数值型
    """
    # 所有列名
    all_columns = ['IMPRESSION', 'IMPRESSIONS', 'CLICKS', 'ORDERS', 'TOTAL UNITS', 'TOTAL UNIT', 'QUANTITY', 'SESSIONS',
                   'PAGE VIEWS', 'UNITS ORDERED', 'UNITS ORDERED - B2B', 'TOTAL ORDER ITEMS', 'TOTAL ORDER ITEMS - B2B',
                   '7 DAY ADVERTISED SKU UNITS (#)', '7 DAY TOTAL ORDERS (#)', '7 DAY TOTAL UNITS (#)',
                   '7 DAY OTHER SKU UNITS (#)', 'SPEND', 'SALES', 'PRICE', 'ITEM-PRICE', 'SHIPPING - PRICE',
                   'SHIP - PROMOTION - DISCOUNT', 'CAMPAIGN DAILY BUDGET', 'MAX BID', 'ACOS', 'SESSION PERCENTAGE',
                   'PAGE VIEWS PERCENTAGE', 'BUY BOX PERCENTAGE', 'UNIT SESSION PERCENTAGE',
                   'UNIT SESSION PERCENTAGE - B2B', 'CPC','ORDERED PRODUCT SALES', 'ORDERED PRODUCT SALES- B2B', 'ORDER',
                   'CR', 'PROM_RATIO', 'SALES_RATIO']
    # 所有整型列
    int_columns = ['IMPRESSION', 'IMPRESSIONS', 'CLICKS', 'ORDERS', 'ORDER', 'TOTAL UNITS', 'TOTAL UNIT', 'QUANTITY',
                   'SESSIONS',
                   'PAGE VIEWS', 'UNITS ORDERED', 'UNITS ORDERED - B2B', 'TOTAL ORDER ITEMS', 'TOTAL ORDER ITEMS - B2B',
                   '7 DAY ADVERTISED SKU UNITS (#)', '7 DAY TOTAL ORDERS (#)', '7 DAY TOTAL UNITS (#)',
                   '7 DAY OTHER SKU UNITS (#)']

    # 所有浮点型列
    float_columns = ['SPEND', 'SALES', 'PRICE', 'ITEM-PRICE', 'SHIPPING - PRICE',
                     'SHIP - PROMOTION - DISCOUNT', 'CAMPAIGN DAILY BUDGET', 'MAX BID', 'ORDERED PRODUCT SALES',
                     'ORDERED PRODUCT SALES- B2B', 'CPC']

    # 所有百分比列
    percent_columns = ['ACOS', 'SESSION PERCENTAGE',
                       'PAGE VIEWS PERCENTAGE', 'BUY BOX PERCENTAGE', 'UNIT SESSION PERCENTAGE',
                       'UNIT SESSION PERCENTAGE - B2B', 'CR', 'PROM_RATIO', 'SALES_RATIO']

    df = ori_df.copy()
    len_trans_columns = len(trans_columns)
    # trans_columns == 'all'
    if trans_columns == 'all':
        # 如果输入的是df
        if df.ndim == 2:
            calc_columns = list(map(lambda x: str(x).upper(), df.columns))
            trans_columns = ori_df.columns
        # 如果输入的是series
        elif df.ndim == 1:
            calc_columns = df.name.upper()
            trans_columns = ori_df.name

    # trans_columns的长度大于等于2,df取特定列

    elif isinstance(trans_columns, list) & (len_trans_columns >= 2):
        # 计算的数组对象为df
        df = df[trans_columns]
        calc_columns = list(map(lambda x: str(x).upper(), df.columns))
    # trans_columns的长度大于等于1
    elif isinstance(trans_columns, list) & (len_trans_columns == 1):
        # 计算的数组对象为series
        df = df[trans_columns]
        calc_columns = df.name.upper()

    elif isinstance(trans_columns, str):
        df = df[trans_columns]
        calc_columns = trans_columns.upper()

    else:
        raise ValueError("输入的列格式不对，请重新输入...(列表或是字符串或是'all')")

    # 计算的列
    if isinstance(calc_columns, list):
        calc_columns_set = list(set(calc_columns) & set(all_columns))
        calc_columns = sorted(calc_columns_set, key=lambda x: calc_columns.index(x))
    else:
        calc_columns = list(set(calc_columns.split('#@#@#@#$%$%')) & set(all_columns))

    # 2.判断输入列是否在总列中
    if len(calc_columns) == 0:
        raise ValueError('输入的列名不在总表中,请将列添加到总表中...')

    if len(calc_columns) >= 2:
        columns_dict = {ori_column: column for ori_column, column in zip(trans_columns, calc_columns)}
    else:
        try:
            columns_dict = {trans_columns: calc_columns[0]}
        except:
            raise ValueError('输出的列名不正确，或是不在总表中.')

    df.columns = calc_columns

    # 3. 将列按照不同的数据类型输出
    if df.ndim == 2:
        new_df = pd.DataFrame(
            [df[input_column_name].astype('int') if input_column_name in int_columns else df[input_column_name].apply(
                lambda x: round(float(x), float_dicimal_point)) if input_column_name in float_columns else df[
                input_column_name].apply(
                lambda x: str(round(float(x) * 100, percent_decimal_point)) + '%') for
             input_column_name in calc_columns]).T

        for column in trans_columns:
            ori_df[column] = new_df[columns_dict[column]]
    else:
        new_df = [df.astype('int') if input_column_name in int_columns else df.apply(
            lambda x: round(float(x), float_dicimal_point)) if input_column_name in float_columns else df.apply(
            lambda x: str(round(float(x) * 100, percent_decimal_point)) + '%') for
                  input_column_name in calc_columns][0]
        if ori_df.ndim == 2:
            ori_df[trans_columns] = new_df
        else:
            ori_df[:] = list(new_df)

# if __name__ == '__main__':
#     test_a = pd.DataFrame(
#         [['1', '1234.23', '0.56'], ['0', '0.78', '0.25'], ['1', '123', '0.89'], ['0', '123', '1233.56'],
#          ['-1', '0', '0.98']],
#         columns=['impressions', 'price', 'acos'])
#     format_num(test_a)
