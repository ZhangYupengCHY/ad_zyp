#!/usr/bin/env python
# coding=utf-8
# author:marmot
import glob

import pandas as pd
import sys




# 初始化active_listing
def clean_active_listing(active_df):
    all_columns = active_df.columns
    if 'ASIN 1' in all_columns:
        # report = active_df[['出品者SKU', '価格', 'ASIN 1']]
        active_df.rename(columns={'出品者SKU': 'seller-sku',
                               '価格': 'price',
                               'ASIN 1': 'asin1'
                               }, inplace=True)
    elif 'ASIN1' in all_columns:
        # report = active_df[['卖家 SKU', '价格', 'ASIN1']]
        active_df.rename(columns={'卖家 SKU': 'seller-sku',
                               '价格': 'price',
                               'ASIN1': 'asin1'
                               }, inplace=True)

    return active_df


# 获取active_listing标题
def get_active_title(active_df):
    all_columns = active_df.columns
    if 'ASIN 1' in all_columns:
        report = active_df[['出品者SKU', '価格', 'ASIN 1', '商品名']]
        report.rename(columns={'出品者SKU': 'seller-sku',
                               '価格': 'price',
                               'ASIN 1': 'asin1',
                               '商品名': 'item-name'
                               }, inplace=True)
    elif 'ASIN1' in all_columns:
        report = active_df[['卖家 SKU', '价格', 'ASIN1', '商品名称']]
        report.rename(columns={'卖家 SKU': 'seller-sku',
                               '价格': 'price',
                               'ASIN1': 'asin1',
                               '商品名称': 'item-name'
                               }, inplace=True)
    elif 'asin1' in all_columns:
        report = active_df[['seller-sku', 'price', 'asin1', 'item-name']]
    else:
        report = 0

    return report


def active_total_columns(input_df):
    active_df = input_df.copy()
    all_columns = active_df.columns
    if 'ASIN 1' in all_columns:
        report = active_df[['出品者SKU', '価格', 'ASIN 1', '商品名', '出品日', 'フルフィルメント・チャンネル']]
        report.rename(columns={'出品者SKU': 'seller-sku',
                               '価格': 'price',
                               'ASIN 1': 'asin1',
                               '商品名': 'item-name',
                               '出品日': 'open-date',
                               'フルフィルメント・チャンネル': 'fulfillment-channel',
                               }, inplace=True)
    elif 'ASIN1' in all_columns:
        report = active_df[['卖家 SKU', '价格', 'ASIN1', '商品名称', "开售日期", "配送渠道"]]
        report.rename(columns={'卖家 SKU': 'seller-sku',
                               '价格': 'price',
                               'ASIN1': 'asin1',
                               '商品名称': 'item-name',
                               "开售日期": "open-date",
                               "配送渠道": "fulfillment-channel"
                               }, inplace=True)
    elif 'asin1' in all_columns:
        report = active_df[['seller-sku', 'asin1','price',  'item-name', 'open-date', 'fulfillment-channel']]
    else:
        report = 0

    return report


# if __name__ == "__main__":
#     test_dir = glob.glob(unicode(r"D:\待处理\ZJCHAO\ZJCHAO_JP\Active*"))[0]
#     test_listing = pd.read_table(test_dir)
#     test_listing.columns = [one_col.encode('utf-8') for one_col in test_listing.columns]
#     print active_total_columns(test_listing)
