# -*- coding: utf-8 -*-
"""
Proj: ad_helper
Created on:   2020/6/3 14:15
@Author: RAMSEY

"""
import copy
import gc
import os
import re
from datetime import datetime, timedelta
import time
import shutil
import warnings
from functools import reduce

import pandas as pd

import my_toolkit.process_files as process_files
import my_toolkit.public_function as public_function
import my_toolkit.init_station_report as init_station_report
import my_toolkit.sql_write_read as sql_write_read
import my_toolkit.change_rate as change_rate
import my_toolkit.conn_db as conn_db

warnings.filterwarnings(action="ignore")
change_current = change_rate.change_current()

"""
概述:
    将不同数据来源的五表pickle化后,方便读取
实现逻辑:
        将广告专员下载的压缩包解压后,将文件解压成pickle,
        文件名为:账号_站点_表类型.pkl,
        将账号_站点_表类型+事件key存储在redis中

        # 将ac表信息更新存储到mysql中
"""


def ac_major_info_upload_mysql(path):
    """
    将ac表关键信息上传到数据库中
    :param path: str
        pkl path
    :return:
    """
    if not os.path.exists(path):
        return
    ac_data_one_station = pd.read_pickle(path)
    if (ac_data_one_station is None) or (ac_data_one_station.empty):
        return
    # 站点名:站点名在
    station = os.path.basename(path)[:-7]
    # 初始化站点数据
    ac_data_one_station = init_station_report.init_report(ac_data_one_station, 'ac')
    # 判断站点列名是否正确
    major_columns = ['seller-sku', 'asin1', 'price', 'open-date', 'fulfillment-channel']
    ac_columns = ac_data_one_station.columns
    if 'fulfillment-channel' not in ac_columns:
        ac_data_one_station['fulfillment-channel'] = 'DEFAULT'
        ac_columns = ac_data_one_station.columns
    if not set(major_columns).issubset(ac_columns):
        print('*' * 15)
        print(f'{station}的ac表缺失{set(major_columns)-set(ac_columns)}')
        print('*' * 15)
        return
    # 将数据写入到数据库中
    # 添加一列写入日期
    major_columns.insert(0, 'id')
    major_columns.insert(1, 'station')
    major_columns.append('update_time')
    # 添加站点列
    ac_data_one_station['station'] = station
    # 添加时间列
    ac_data_one_station['update_time'] = datetime.today().replace(microsecond=0)
    ac_data_one_station['id'] = range(0, 0 + ac_data_one_station.shape[0])
    # 写入需要的列
    ac_data_one_station = ac_data_one_station[major_columns]
    # 修改列名
    ac_data_one_station.rename(columns={'asin1': 'asin'}, inplace=True)
    table_name = 'station_ac_major_data'
    # 若站点数据数据存在,则先删除
    sql_write_read.delete_table("""DELETE FROM {} where station = {}""".format(table_name, "'%s'" % station))
    # 将站点ac数据写入大数据库中
    sql_write_read.to_table_append(ac_data_one_station, table_name)
    print(f'{station}:完成ac表数据上传到数据库(station_ac_major_data)中.')


def zipped_folders_2_pickle(zipped_files: 'path', unzipped_file_save_folder=None, folder_save_pkl=False,
                            delete_file=True) -> None:
    """
    将存储压缩文件夹的压缩文件中的文件序列化存储为pickle,同时将路径存在在redis中
    Parameters:
        zipped_files:path object
                     广告专员压缩文件夹的压缩文件
        folder_save_pkl:path object,or False,default False
                        存储pickle文件的文件夹,默认解压到当前文件夹
        delete_file:bool,default True
                    是否删除压缩后的文件,默认为删除文件
    Returns: None
    """
    if not os.path.exists(zipped_files):
        raise FileNotFoundError('{} cant found.'.format(zipped_files))
    # 若没有指定解压文件存储的文件夹,则将压缩文件压缩到当前文件夹
    if unzipped_file_save_folder is None:
        unzipped_file_save_folder = os.path.dirname(zipped_files)
    if not os.path.isdir(unzipped_file_save_folder):
        raise ValueError('{} is not a folder.'.format(unzipped_file_save_folder))
    if not os.path.exists(unzipped_file_save_folder):
        os.mkdir(unzipped_file_save_folder)
    # 若没有指定文件夹,则解压pkl文件指定为文件夹
    if not folder_save_pkl:
        folder_save_pkl = unzipped_file_save_folder
    if not os.path.isdir(folder_save_pkl):
        raise ValueError('{} is not a folder.'.format(folder_save_pkl))
    if not os.path.exists(folder_save_pkl):
        os.mkdir(folder_save_pkl)
    # 1. 解压文件
    process_files.unzip_file(zipped_files, save_folder=unzipped_file_save_folder)
    # 2. 将文件pkl化
    # 2.1 获得全部文件完整路径
    all_files = []
    station_name = os.path.splitext(os.path.basename(zipped_files))[0]
    station_name = station_name.upper()
    station_name = station_name[:-3] + "_" + station_name[-2:]
    station_folder = os.path.join(unzipped_file_save_folder, station_name)
    if not os.path.exists(station_folder):
        wrong_station_name = station_name[:-3] + "-" + station_name[-2:]
        station_folder = os.path.join(unzipped_file_save_folder, wrong_station_name)
    for root, _, files in os.walk(station_folder):
        for file in files:
            file_path = os.path.join(root, file)
            all_files.append(file_path)

    # 2.2 pickle化(这里文件的sheet名应该规范化,防止其他语言)
    # 保存pickle到当前文件夹下面
    # 规范输出后的pkl文件名(账号_站点_数据类型)
    def standardize_file_pickle_name(file_path, cp_sheet_name='cp'):
        """
        规范文件输出后pickle文件命名(账号_站点_数据类型_时间)
        :param file_path:
        :return:
        """
        if not os.path.isfile(file_path):
            raise FileExistsError('{} not a file.')
        if not os.path.exists(file_path):
            raise FileExistsError('{} not exists.')
        campaign_sheet_name_sign_list = ['cp', 'bd']
        if cp_sheet_name not in campaign_sheet_name_sign_list:
            raise ValueError(f'cp_sheet_name must be one of {campaign_sheet_name_sign_list}.')
        station_name = os.path.basename(os.path.dirname(file_path))
        station_name = station_name.upper()
        account = station_name[:-3]
        site = station_name[-2:]
        # 关键词判断无法判断all order表
        file_type = [type for type, keyword in public_function.FILE_RECOGNIZE.items() if keyword in file_path.lower()]
        if file_type == campaign_sheet_name_sign_list:
            return account + '_' + site + '_' + cp_sheet_name + '.pkl'
        if len(file_type) == 1:
            file_type = file_type[0]
        else:
            if os.path.splitext(os.path.basename(file_path))[0].isdigit():
                file_type = 'ao'
            else:
                file_type = 'None'
        return account + '_' + site + '_' + file_type + '.pkl'

    # 当有新的站点文件时,
    # 便更新与st有关的信息表
    # 出单优质搜索词、未出单高点击搜索词、近期低于平均点击率的SKU、后台Search Term参考、不出单关键词

    def st_search_words_rating(station_name, st, cam_df, ac_data, save_basename):
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
                print("曝光大于1000的没有，无法生成点击率表")
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
                print("关键词表为空，无法生成多行高频一句话")
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
                print("关键词表为空，无法生成多行高频一句话")
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
        st_report0 = st.loc[:, ['Campaign Name', 'Ad Group Name', 'Keyword', 'Match Type',
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
            report4 = report3[['SKU', 'Campaign Name', 'Ad Group Name', 'Match Type', 'Customer Search Term',
                               '展示次数', '点击量', '花费', '销售额', '订单量', 'CTR', 'CPC', 'ACoS', 'CR']]
            if report4.empty:
                print("优质广告为空")
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
                            high_quality_keywords_info_by_erpsku.to_excel(writer, sheet_name="同erp_sku下其他seller_sku出单词",
                                                                          index=False)

            # 保存并关闭
            writer.save()

    # 将保存为pickle同时将文件路径保存在redis中
    redis_store = public_function.Redis_Store(db=2)
    keys = redis_store.keys()
    # 压缩文件的最后修改的时间
    last_time_timestamp = os.path.getmtime(zipped_files)
    last_time = datetime.fromtimestamp(last_time_timestamp).strftime('%Y%m%d%H%M%S')
    sign_key = 'FIVE_FILES_KEYS_SAVE'

    # 广告报表和站点品牌报表的名
    camp_sheet_name = 'Sponsored Products Campaigns'
    bd_sheet_name = 'Sponsored Brands Campaigns'
    for file_path in all_files:
        # 由于广告报表中需要输出产品报表和品牌，于是需要输出两次
        if 'bulk' in file_path.lower():
            file_pickle_path_cp = os.path.join(folder_save_pkl, standardize_file_pickle_name(file_path))
            file_pickle_path_bd = os.path.join(folder_save_pkl,
                                               standardize_file_pickle_name(file_path, cp_sheet_name='bd'))
            # 单独计算广告报表和品牌信息
            try:
                read_bulk_file = pd.read_excel(file_path, None)
            except Exception as e:
                print(f'{station_name}: 读取广告报表工作簿失败.')
                print(e)
                # 写广告报表

            if camp_sheet_name in read_bulk_file.keys():
                station_cp_data = read_bulk_file[camp_sheet_name]
                if not station_cp_data.empty:
                    process_files.write_df_2_pickle(station_cp_data, file_pickle_path_cp)

                    # redis保存的键为FIVE_FILES_KEYS_SAVE+站点+日期 FIVE_FILES_KEYS_SAVE为项目标志
                    file_redis_key = sign_key + ':' + standardize_file_pickle_name(file_path).replace('.pkl',
                                                                                                      '') + '_' + last_time
                    file_redis_key = file_redis_key.upper()
                    redis_store.set(file_redis_key, file_pickle_path_cp)
                    # 删除该站点之前存储的键
                    [redis_store.delete(key) for key in keys if
                     (sign_key in key) and (station_name == key[21:-18]) and (last_time not in key)]
                    # 打印完成
                    pickle_name = standardize_file_pickle_name(file_path).replace('.pkl', '')
                    print(f"pickle化完成:{pickle_name}")
                else:
                    print(f'{station_name}:广告报表数据为空.')

            else:
                print(f'{station_name}:缺失广告报表.')

            if bd_sheet_name in read_bulk_file.keys():
                station_brand_data = read_bulk_file[bd_sheet_name]
                if not station_brand_data.empty:
                    # 首先将信息存入到pickle中
                    process_files.write_df_2_pickle(station_brand_data, file_pickle_path_bd)

                    # redis保存的键为FIVE_FILES_KEYS_SAVE+站点+日期 FIVE_FILES_KEYS_SAVE为项目标志
                    file_redis_key = sign_key + ':' + standardize_file_pickle_name(file_path,
                                                                                   cp_sheet_name='bd').replace('.pkl',
                                                                                                               '') + '_' + last_time
                    file_redis_key = file_redis_key.upper()
                    redis_store.set(file_redis_key, file_pickle_path_bd)
                    # 删除该站点之前存储的键
                    [redis_store.delete(key) for key in keys if
                     (sign_key in key) and (station_name == key[21:-18]) and (last_time not in key)]
                    # 打印完成
                    pickle_name = standardize_file_pickle_name(file_path, cp_sheet_name='bd').replace('.pkl', '')
                    print(f"pickle化完成:{pickle_name}")

                    """
                        将品牌信息写入到mysql中
                        1.将品牌信息规范化
                            初始化
                            得到需要输出的列:曝光,点击,花费,广告销售额,acos,
                             ----   需要添加的列:站点名,上传时间,cpc
                        2.删除MySQL库中原有的信息
                        3.将更新的信息上传到数据库中
                    """
                    station_brand_data_inited = init_station_report.init_report(station_brand_data, 'bd')  # 初始化
                    # 将与货币相关的转换为美元
                    site = station_name[-2:]
                    if site not in public_function.SITES:
                        raise ValueError(f'Unexpected site:{site}')
                    # 去Record Type中值为Campaign的数据来计算
                    station_brand_data_inited = station_brand_data_inited[
                        station_brand_data_inited['Record Type'] == 'Campaign']
                    # 转换汇率
                    station_brand_data_spend = sum(station_brand_data_inited['Spend']) * change_current[site]
                    station_brand_data_sales = sum(station_brand_data_inited['Sales']) * change_current[site]
                    station_brand_data_impressions = sum(station_brand_data_inited['Impressions'])
                    station_brand_data_clicks = sum(station_brand_data_inited['Clicks'])
                    station_brand_data_orders = sum(station_brand_data_inited['Orders'])
                    updatetime = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                    if station_brand_data_clicks > 0:
                        station_brand_data_cpc = round(station_brand_data_spend / station_brand_data_clicks, 2)
                    else:
                        station_brand_data_cpc = 0.00
                    if station_brand_data_sales > 0:
                        station_brand_data_acos = round(station_brand_data_spend / station_brand_data_sales, 4)
                    else:
                        station_brand_data_acos = 0.00

                    # 添加广告专员的姓名和店铺销售额:通过站点名匹配
                    query_ad_manager_and_shop_sales_sql = "SELECT ad_manger,shop_sales FROM only_station_info where station = '%s' "% station_name
                    ad_manager_and_shop_sales_data = sql_write_read.read_table(query_ad_manager_and_shop_sales_sql)
                    if not ad_manager_and_shop_sales_data.empty:
                        ad_manger = ad_manager_and_shop_sales_data['ad_manger'].values[0]
                        shop_sales = ad_manager_and_shop_sales_data['shop_sales'].values[0] * change_current[site]
                    else:
                        ad_manger = ''
                        shop_sales = ''

                    # 添加销售占比列:广告销售额/店铺销售额
                    if shop_sales != '':
                        sale_rate = station_brand_data_sales/shop_sales
                    else:
                        sale_rate = ''


                    # 输出原始品牌广告中的列
                    station_brand_data_export_sql_columns = ['station', 'ad_manger','impressions', 'clicks', 'ad_spend',
                                                             'ad_sales', 'orders','percentage', 'acos', 'shop_sales','cpc',
                                                             'update_time']

                    station_brand_data_export = pd.DataFrame([[station_name, ad_manger,station_brand_data_impressions,
                                                               station_brand_data_clicks, station_brand_data_spend,
                                                               station_brand_data_sales, station_brand_data_orders,sale_rate,
                                                               station_brand_data_acos,shop_sales,
                                                               station_brand_data_cpc, updatetime]],
                                                             columns=station_brand_data_export_sql_columns)

                    # 删除数据库中站点原有数据
                    delete_mysql_station_brand_data = "DELETE FROM station_brand_advertising_info where station = '%s'" % station_name

                    conn_db.to_sql_delete(delete_mysql_station_brand_data)
                    # 添加数据
                    conn_db.to_sql_append(station_brand_data_export, 'station_brand_advertising_info')
            del read_bulk_file
            gc.collect()
        else:
            file_pickle_path = os.path.join(folder_save_pkl, standardize_file_pickle_name(file_path))
            if 'None' in file_pickle_path:
                continue
            # 将站点的品牌信息写入到redis中,以及mysql数据库
            if '_ac' in file_pickle_path:
                station_ac_data = process_files.read_file(file_path)
                process_files.write_df_2_pickle(station_ac_data, file_pickle_path)
            # 读取站点广告数据
            elif '_st' in file_pickle_path:
                station_st_data = process_files.read_file(file_path)
                process_files.write_df_2_pickle(station_st_data, file_pickle_path)

            # 其他的报表暂时全部直接pickle化,不保存在内存中
            else:
                process_files.write_file_2_pickle(file_path, file_pickle_path)

            # sub-mission1 将ac表关键信息存储到数据库中
            if os.path.basename(file_pickle_path)[-6:-4] == 'ac':
                ac_major_info_upload_mysql(file_pickle_path)

            # 保存为redis
            file_redis_key = sign_key + ':' + standardize_file_pickle_name(file_path).replace('.pkl',
                                                                                              '') + '_' + last_time
            file_redis_key = file_redis_key.upper()
            redis_store.set(file_redis_key, file_pickle_path)
            # 打印完成
            pickle_name = standardize_file_pickle_name(file_path).replace('.pkl', '')
            print(f"pickle化完成:{pickle_name}")
    # 删除该站点之前存储的键
    [redis_store.delete(key) for key in keys if
     (sign_key in key) and (station_name == key[21:-18]) and (last_time not in key)]
    # # sub-mission2:将st表信息存储到xlsx中 站点同时拥有广告报表、st报表、ac报表
    have_three_report = []
    for file_path in all_files:
        if public_function.FILE_RECOGNIZE['cp'] in file_path.lower():
            have_three_report.append('cp')
        elif public_function.FILE_RECOGNIZE['ac'] in file_path.lower():
            have_three_report.append('ac')
        elif public_function.FILE_RECOGNIZE['st'] in file_path.lower():
            have_three_report.append('st')
    if set(['cp', 'ac', 'st']) == set(have_three_report):
        report_time = datetime.fromtimestamp(last_time_timestamp).strftime('%y.%m.%d')
        save_basename = report_time + ' ' + f'{station_name.upper()}' + ' ' + 'ST报表' + '.xlsx'
        st_search_words_rating(station_name, station_st_data, station_cp_data, station_ac_data, save_basename)
        print(f"完成st报表五张关键表生成:{station_name}")

    # 将站点的广告报表,st报表以及ac报表的内存释放
    del station_st_data
    del station_cp_data
    del station_ac_data
    gc.collect()

    if delete_file:
        [os.remove(file_path) for file_path in all_files]
    redis_store.close()
    # 删除临时文件夹
    try:
        shutil.rmtree(station_folder)
    except:
        station_name = station_name[:-3] + "-" + station_name[-2:]
        shutil.rmtree(os.path.join(unzipped_file_save_folder, station_name))


def refresh_folder(zipped_folder, unzipped_file_save_folder=None, folder_save_pkl=False,refresh_before_hour=None):
    """
    判断文件夹中更新压缩文件,
    然后对压缩文件进行解压pickle化处理
    Parameters:
        zipped_folder:path object
                      销售压缩文件存放的文件夹
        unzipped_file_save_folder:path object
                    用于存储解压文件后的临时文件夹
        folder_save_pkl:path object
                    用于存储pickle文件的文件夹
        refresh_before_hour :None,int,default None
            时间段内站点都刷新
    Returns:None
    """
    if not os.path.isdir(zipped_folder):
        raise ValueError(f'{zipped_folder} is not a path.Please input saler zipped folder path')
    if not os.path.exists(zipped_folder):
        return
    # 获得更新的zip报表
    stations_dir = [os.path.join(zipped_folder, zipped_file) for zipped_file in os.listdir(zipped_folder) if
                    '.zip' in zipped_file.lower()]
    old_zipped_files_modify_time = {file: os.path.getmtime(file) for file in stations_dir}
    while 1:
        try:
            new_files_list = [os.path.join(zipped_folder, file) for file in os.listdir(zipped_folder) if
                              ('.zip' in file) or ('.ZIP' in file)]
            new_zipped_files_modify_time = {file: os.path.getmtime(file) for file in new_files_list}
            if refresh_before_hour is None:
                needed_process_station_zip_namelist = [file for file, file_time in new_zipped_files_modify_time.items() if
                                file_time != old_zipped_files_modify_time.get(file, None)]
            else:
                now_timestamp = time.time()
                before_second = 3600*refresh_before_hour
                needed_process_station_zip_namelist = [file for file,file_time in new_zipped_files_modify_time.items() if (now_timestamp - file_time)<before_second]
                refresh_before_hour = None
        except:
            continue

        if len(needed_process_station_zip_namelist) > 0:
            print(f'{datetime.now().replace(microsecond=0)} 处理站点总数为：{len(needed_process_station_zip_namelist)}')
            for one_station_zipped in needed_process_station_zip_namelist:
                try:
                    zipped_folders_2_pickle(one_station_zipped, unzipped_file_save_folder=unzipped_file_save_folder,
                                            folder_save_pkl=folder_save_pkl)

                except Exception as e:
                    station_name = os.path.splitext(os.path.basename(one_station_zipped))[0]
                    public_function.print_color(f'{station_name}:{e}')
                    print(e)
                # zipped_folders_2_pickle(one_station_zipped, unzipped_file_save_folder=unzipped_file_save_folder,
                #                         folder_save_pkl=folder_save_pkl)
        else:
            print("暂时没有站点更新，休息1分钟...")
            print('======================================================')
            if datetime.now().hour in set(range(1, 8)):
                # 休息到早上九点再开始更新
                print("暂时没有站点更新，休息10分钟...")
                print('======================================================')
                time.sleep(540)
        old_zipped_files_modify_time = copy.deepcopy(new_zipped_files_modify_time)
        time.sleep(60)


if __name__ == "__main__":
    # 存储销售上传的压缩文件路径
    zipped_folder = r"F:\station_folder"
    # 临时文件夹,用于存储解压后文件
    temp_folder = r'F:\unzipped_file_temp'
    # 将压缩文件存储为pickle文件的文件夹
    pkl_save_folder = r"G:\pickle_files"
    refresh_folder(zipped_folder, unzipped_file_save_folder=temp_folder, folder_save_pkl=pkl_save_folder,refresh_before_hour=2)
