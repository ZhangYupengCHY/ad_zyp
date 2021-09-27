#!/usr/bin/env python
# coding=utf-8
# author:marmot
import sys

# mport request_reports_from_api.change_campaign_sheet_head, request_reports_from_api.change_digital, request_reports_from_api.translation
# from listing_to_auto_ad import translation
import re
import glob
import pandas as pd
import request_reports_from_api.translation
from request_reports_from_api import change_campaign_sheet_head, translation,change_digital


def init_campaign(initdata, station_abbr, advertised_dir):
    initdata.fillna(' ', inplace=True)  # 此处fillna要是' '
    # 统一campaign列名
    if station_abbr == 'SP':
        station_abbr = 'ES'
    if initdata.columns[0] == '记录ID':
        initdata.rename(columns={'记录ID': 'Record ID', '记录类型': 'Record Type', '活动编号': 'Campaign ID', '广告商品': 'Campaign',
                                 '广告商品每日预算': 'Campaign Daily Budget', '产品组合ID': 'Portfolio ID',
                                 '开始日期': 'Campaign Start Date', '结束日期': 'Campaign End Date', '目标受众类型': 'Campaign Targeting Type', '广告组': 'Ad Group',
                                 '最大每点击成本': 'Max Bid', '关键字或商品投放': 'Keyword or Product Targeting',
                                 '产品投放ID': 'Product Targeting ID', '匹配类型': 'Match Type', '广告商品状态': 'Campaign Status',
                                 '广告组状态': 'Ad Group Status', '状态': 'Status', '展现量': 'Impressions', '点击量': 'Clicks',
                                 '花费': 'Spend', '订单': 'Orders', '单位总数': 'Total Units', '销售': 'Sales',
                                 '竞价策略': 'Bidding strategy', '投放类型': 'Placement Type',
                                 '按展示位置提高竞价': 'Increase bids by placement'}, inplace=True)
    if initdata.columns[0] != 'Record ID':
        initdata = change_campaign_sheet_head.campaign_sheet_head_stand(initdata, station_abbr)
        # print initdata.columns
        # 翻译成英文
        if station_abbr not in ['US', 'UK', 'CA', 'IN']:
            # 获取站点翻译词典
            language = translation.all_language[station_abbr]
            initdata.loc[:, 'Record Type'] = initdata['Record Type'].apply(
                lambda s: language[s.strip()] if s.strip() in language.keys() else s.strip())
            initdata.loc[:, 'Campaign Targeting Type'] = initdata['Campaign Targeting Type'].apply(
                lambda m: language[m.strip()] if m.strip() in language.keys() else m)
            initdata.loc[:, 'Campaign Status'] = initdata['Campaign Status'].apply(
                lambda m: language[m.strip()] if m.strip() in language.keys() else m)
            initdata.loc[:, 'Ad Group Status'] = initdata['Ad Group Status'].apply(
                lambda n: language[n.strip()] if n.strip() in language.keys() else n)
            initdata.loc[:, 'Status'] = initdata['Status'].apply(
                lambda o: language[o.strip()] if o.strip() in language.keys() else o)
            initdata.loc[:, 'Match Type'] = initdata['Match Type'].apply(
                lambda o: language[o.strip()] if o.strip() in language.keys() else o)

    # 去掉acos的%号
    # initdata['ACoS'] = initdata.loc[:, 'ACoS'].apply(lambda m: m.strip('%'))
    initdata['ACoS'] = initdata['ACoS'].apply(lambda m: m.strip('%') if re.search('%', str(m)) else float(m) * 100)

    # 将spend acos sale 中的逗号去掉或变成小数点
    initdata['Sales'] = initdata.loc[:, 'Sales'].apply(change_digital.num_del)
    initdata['Spend'] = initdata.loc[:, 'Spend'].apply(change_digital.num_del)
    initdata['ACoS'] = initdata.loc[:, 'ACoS'].apply(change_digital.num_del)
    initdata['Orders'] = initdata.loc[:, 'Orders'].apply(change_digital.num_del)
    initdata['Orders'] = initdata.loc[:, 'Orders'].apply(change_digital.num_del)
    initdata['Clicks'] = initdata.loc[:, 'Clicks'].apply(change_digital.num_del)

    # 修复因为美站省略广告部分状态后导致程序无法正常工作的问题
    if station_abbr in []:
        for cam_now, cam_group in initdata.groupby('Campaign'):
            initdata.loc[cam_group.index, 'Campaign Status'] = cam_group.loc[cam_group.index[0], 'Campaign Status']
        for (cam_now, adgroup), cam_ad_group in initdata.groupby(['Campaign', 'Ad Group']):
            initdata.loc[cam_ad_group.index, 'Ad Group Status'] = cam_ad_group.loc[
                cam_ad_group.index[0], 'Ad Group Status']

    # 补充缺少的ad行，以补充SKU信息
    # if station_abbr not in ['US']:
    if station_abbr in []:
        advertised_dir = glob.glob(advertised_dir + '/Sponsored Products Advertised product report.xlsx')
        if advertised_dir:
            advertised_df = pd.read_excel(advertised_dir[0])
            advertised_df.columns = [one_col.strip() for one_col in advertised_df.columns]
            advertised_df = advertised_df[['Campaign Name', 'Ad Group Name', 'Advertised SKU',
                                           'Impressions', 'Clicks', 'Spend', '7 Day Total Sales',
                                           'Total Advertising Cost of Sales (ACoS)', '7 Day Total Orders (#)']]
            advertised_df.rename(columns={'Campaign Name': 'Campaign', 'Ad Group Name': 'Ad Group',
                                          'Advertised SKU': 'SKU', '7 Day Total Sales': 'Sales',
                                          'Total Advertising Cost of Sales (ACoS)': 'ACoS',
                                          '7 Day Total Orders (#)': 'Orders'}, inplace=True)
            advertised_df['Record Type'] = 'Ad'
            advertised_df['Campaign Status'] = 'enabled'
            advertised_df['Ad Group Status'] = 'enabled'
            advertised_df['Status'] = 'enabled'
            # advertised_df_rows, advertised_df_cols = advertised_df.shape
            # 补充缺少的ad行，以补充SKU信息
            initdata = initdata.append(advertised_df, ignore_index=True)
            initdata.drop_duplicates(subset=['Record Type', 'Campaign', 'Ad Group',
                                             'SKU', 'Keyword'], keep='last', inplace=True)
            # 排序
            # initdata = initdata.sort_values(by=['Campaign', 'Ad Group', 'Max Bid', 'SKU'], axis=0, ascending=True)
            """
            all_cam_list = []
            for (cam_now, adgroup), cam_ad_group in initdata.groupby(['Campaign', 'Ad Group']):
                if 'Ad' not in list(cam_ad_group['Record Type']):
                    ad_campaign = cam_ad_group.loc[cam_ad_group.index[-1],'Campaign']
                    ad_ad_group = cam_ad_group.loc[cam_ad_group.index[-1],'Ad Group']
                    ad_row_df = advertised_df.loc[(advertised_df['Campaign']==ad_campaign)&
                                                  (advertised_df['Ad Group']==ad_ad_group)]
                    ad_row_df['Record Type'] = 'Ad'
                    ad_row_df['Campaign Status'] = ''
                    ad_row_df['Ad Group Status'] = ''
                    ad_row_df['Status'] = 'enabled'
                    all_cam_list.append(cam_ad_group.append(ad_row_df, ignore_index=True))
                else:
                    all_cam_list.append(cam_ad_group)
            new_df = pd.DataFrame()
            cam_df = new_df.append(all_cam_list, ignore_index=True)
            initdata = cam_df
            """

    # 将sku变成字符转
    # initdata['SKU'] = initdata['SKU'].astype(str)
    # initdata['Campaign'] = initdata['Campaign'].astype(str)

    # 返回数据
    return initdata
