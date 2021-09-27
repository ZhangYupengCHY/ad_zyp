# -*- coding: utf-8 -*-
"""
Proj: recommend
Created on:   2020/1/9 16:08
@Author: RAMSEY

Standard:  
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""

import os, re, pymysql, json
import tkinter as tk
from tkinter import ttk
import pandas as pd
import numpy as np
import tkinter.messagebox
from datetime import datetime
import warnings

"""
将固定格式的表转换成上传表格式
要求:
    表名：日期 站点 广告需求（0108 Theatly-de 广告需求）有空格
    表头固定：SKU-ASIN-词(关键词/精否词/asin)-匹配方式-竞价-备注（黑体可选填）
    新品自动（SKU+ASIN+竞价）
    手动关键词（SKU+ASIN+词+匹配方式+竞价）
    精否（SKU+ASIN+词+匹配方式）
    asin定向/否定（SKU+ASIN+词+匹配方式+竞价）

    原:
        SKU	    ASIN	词	                        匹配方式	                    竞价	备注
        SKU1	ASIN1			                                                    0.2	    新品自动
        SKU2	ASIN2			                                                    0.25	新品自动
        SKU3	ASIN3	cat water fountain	        broad	                        0.3	    手动关键词
                        cat fountain	            broad	                        0.35    手动关键词
        SKU4	ASIN4	butterfly pin	            phrase	                        0.4	    手动关键词
                        butterfly broches for women	phrase	                        0.45	手动关键词
        SKU5	ASIN5	nail steamer	            exact	                        0.5	    手动关键词
                        nail polish remover machine	exact	                        0.55	手动关键词
        SKU6	ASIN6	water bowl	negative        exact		                            精否
                        automatic water bowl	    negative phrase		                    精否
        SKU7	ASIN7	b0776n45r2	                Negative Targeting Expression		    asin否定
                        b076qb2p7n	                Targeting Expression		    0.6     asin定向
        SKU8	ASIN8	b07blymg6c		                                            0.65	asin定向

    改:

        Campaign Name	                    Campaign Daily Budget	Campaign Start Date	Campaign End Date	Campaign Targeting Type	Ad Group Name	    Max Bid	    SKU 	Keyword 	Product Targeting ID	Match Type	                    Campaign Status	Ad Group Status	Status	Bidding strategy		
        AUTO-CEAVO_US-by-SP_Bulk-New					                                                                            SKU1 ASIN1_200108	0.2					                                                                    enabled					                        新品自动
        AUTO-CEAVO_US-by-SP_Bulk-New					                                                                            SKU1 ASIN1_200108		        SKU1					                                                                     enabled				        新品自动
        AUTO-CEAVO_US-by-SP_Bulk-New					                                                                            SKU2 ASIN2_200108	0.25					                                                                enabled					                        新品自动
        AUTO-CEAVO_US-by-SP_Bulk-New					                                                                            SKU2 ASIN2_200108		        SKU2					                                                                     enabled				        新品自动
        MANUAL-CEAVO_US-by-SP_Bulk					                                                                                SKU3 ASIN3_200108	0.02					                                                                enabled 	     enabled				        手动关键词
        MANUAL-CEAVO_US-by-SP_Bulk					                                                                                SKU3 ASIN3_200108		        SKU3				                                                        enabled 	     enabled	    enabled			手动关键词
        MANUAL-CEAVO_US-by-SP_Bulk					                                                                                SKU3 ASIN3_200108	0.3		            cat water fountain		            broad	                        enabled 	     enabled	    enabled			手动关键词
        MANUAL-CEAVO_US-by-SP_Bulk					                                                                                SKU3 ASIN3_200108	0.35		        cat fountain		                broad	                        enabled 	     enabled	    enabled			手动关键词
        MANUAL-CEAVO_US-by-SP_Bulk-Order					                                                                        SKU4 ASIN4_200108	0.02					                                                                                 enabled	    enabled		    手动关键词
        MANUAL-CEAVO_US-by-SP_Bulk-Order					                                                                        SKU4 ASIN4_200108		        SKU4				                                                        enabled	         enabled	    enabled			手动关键词
        MANUAL-CEAVO_US-by-SP_Bulk-Order					                                                                        SKU4 ASIN4_200108	0.4		            butterfly pin		                phrase	                        enabled	         enabled	    enabled			手动关键词
        MANUAL-CEAVO_US-by-SP_Bulk-Order					                                                                        SKU4 ASIN4_200108	0.45		        butterfly broches for women		    phrase	                        enabled	         enabled	    enabled			手动关键词
        MANUAL-ST-EXACT-by-SP_Bulk					                                                                                SKU5 ASIN5_200108	0.02					                                                                enabled 	     enabled				        手动关键词
        MANUAL-ST-EXACT-by-SP_Bulk					                                                                                SKU5 ASIN5_200108		        SKU5				                                                        enabled 	     enabled	    enabled			手动关键词
        MANUAL-ST-EXACT-by-SP_Bulk					                                                                                SKU5 ASIN5_200108	0.5		            nail steamer		                exact	                        enabled 	     enabled	    enabled			手动关键词
        MANUAL-ST-EXACT-by-SP_Bulk					                                                                                SKU5 ASIN5_200108	0.55		        nail polish remover machine		    exact	                        enabled 	     enabled	    enabled			手动关键词
        AUTO-CEAVO_US-by-SP_Bulk					                                                                                SKU6 ASIN6_200108			            water bowl		                    negative exact	                enabled	         enabled	    enabled			精否
        AUTO-CEAVO_US-by-SP_Bulk					                                                                                SKU6 ASIN6_200108			            automatic water bowl		        negative phrase	                enabled	         enabled	    enabled			精否
        Negative Targeting Expression-SP_Bulk					                                                                    SKU7 ASIN7_200108	0.02					                                                                enabled 	     enabled				        asin定向
        Negative Targeting Expression-SP_Bulk					                                                                    SKU7 ASIN7_200108		        SKU7				                                                        enabled 	     enabled	    enabled			asin定向
        Negative Targeting Expression-SP_Bulk					                                                                    SKU7 ASIN7_200108			    asin="B0776n45r2"	asin="B0776n45r2"	    Negative Targeting Expression	enabled 	     enabled	    enabled			asin否定
        Negative Targeting Expression-SP_Bulk					                                                                    SKU7 ASIN7_200108	0.6		    asin="Bb076qb2p7n"	asin="Bb076qb2p7n"	    Targeting Expression	        enabled 	     enabled	    enabled			asin定向
        Negative Targeting Expression-SP_Bulk					                                                                    SKU8 ASIN8_200108	0.02					                                                                enabled 	     enabled				        asin定向
        Negative Targeting Expression-SP_Bulk					                                                                    SKU8 ASIN8_200108		        SKU8				                                                        enabled 	     enabled	    enabled			asin定向
        Negative Targeting Expression-SP_Bulk					                                                                    SKU8 ASIN8_200108	0.65		asin="B07blymg6c"	asin="B07blymg6c"	    Targeting Expression	        enabled 	     enabled	    enabled			asin定向

    注意事项：
        1.新品自动：若SKU无竞价，则Max Bid = price*20%*3%
        2.手动关键词：广告组竞价=0.02，单个词组个数*0.08;如果匹配方式空白，则按照broad和exact两种方式
        3.精否：如果匹配方式空白，则按照negative exact、命名中ad_group 的 日期应该为SKU6 ASIN6_200108
        4.asin定向：广告组竞价=0.02，单个asin = 0.2
        5.asin否定：做asin否定，必须要自带一个asin定向，竞价为空
"""

warnings.filterwarnings(action='ignore')

# 从路径中解析出站点
def parse_station_name_and_date(file_dir):
    if not os.path.exists(file_dir):
        print("'%s'不存在,请检查后重新输入." % file_dir)
        return
    base_name = os.path.basename(file_dir)
    station_whole_name = re.sub('[^A-Za-z]', '', os.path.splitext(base_name)[0])
    station_name = station_whole_name[0:-2] + '_' + station_whole_name[-2:]
    file_date = re.sub('[^0-9]', '', os.path.splitext(base_name)[0])
    return [station_name.upper(), file_date]


# 1.读取原始上传表
def read_ori_upload_file(file_path):
    if not os.path.exists(file_path):
        print("'%s'不存在,请重新检查路径." % file_path)
        return
    # 获取文件类型
    file_type = os.path.splitext(file_path)[-1]
    # 读取xlsx,xls文件
    if file_type in ('.xlsx', '.xls'):
        file_data = pd.read_excel(file_path, sheet_name=0)  # 默认读取第一个文件
    elif file_type == '.csv':
        try:
            file_data = pd.read_csv(file_path, sep=',')
        except:
            try:
                file_data = pd.read_csv(file_path, sep='\t')
            except:
                print("'%s'文件有问题，无法读取,请再次检查." % file_path)
                return
    else:
        print("'%s'文件为其他数据类型,请存储为xlsx,csv格式." % file_path)
        return
    if file_data.empty:
        print("'%s'没数据,请再次检查." % file_path)
        return
    return file_data


# 2.1.处理新品自动
def process_auto_new(auto_new_data, station_name, file_date):
    export_columns = ['Campaign Name', 'Campaign Daily Budget', 'Campaign Start Date', 'Campaign End Date',
                      'Campaign Targeting Type', 'Ad Group Name', 'Max Bid', 'SKU', 'Keyword', 'Product Targeting ID',
                      'Match Type', 'Campaign Status', 'Ad Group Status', 'Status', 'Bidding strategy']
    # 竞价为空，bid = price * 20% * 3%
    auto_new_data.reset_index(drop=True, inplace=True)
    sku = auto_new_data['SKU'].values[0]
    asin = auto_new_data['ASIN'].values[0]
    bid = auto_new_data['竞价'].values[0]
    empty_list = [np.nan] * len(export_columns)
    processed_auto_new_data = pd.DataFrame([empty_list, empty_list], columns=export_columns)
    processed_auto_new_data['Campaign Name'] = f"AUTO-{station_name}-by-SP_Bulk-New"
    processed_auto_new_data['Ad Group Name'] = "%s %s_%s" % (sku, asin, file_date)
    processed_auto_new_data.ix[0, 'Max Bid'] = bid
    processed_auto_new_data.ix[1, 'SKU'] = sku
    processed_auto_new_data.ix[0, 'Campaign Status'] = 'enabled'
    processed_auto_new_data.ix[1, 'Ad Group Status'] = 'enabled'
    processed_auto_new_data.drop_duplicates(inplace=True)
    return processed_auto_new_data


# 2.2.处理手动广告
def process_manual_kws(manual_kws_data, station_name, manual_ad_group_bid=0.02, one_word_price=0.08):
    """
    处理全部的手动广告。 关键词的出价：关键词的个数*0.08
    :param manual_kws_data:全部的手动广告
    :return: 处理后的手动广告
    """
    # 初始化手动广告
    # manual_kws_data.dropna(inplace=True)
    export_columns = ['Campaign Name', 'Campaign Daily Budget', 'Campaign Start Date', 'Campaign End Date',
                      'Campaign Targeting Type', 'Ad Group Name', 'Max Bid', 'SKU', 'Keyword', 'Product Targeting ID',
                      'Match Type', 'Campaign Status', 'Ad Group Status', 'Status', 'Bidding strategy']
    manual_kws_data.drop_duplicates(inplace=True)
    manual_kws_data['SKU'].fillna(method='ffill', inplace=True)
    manual_kws_data['ASIN'].fillna(method='ffill', inplace=True)
    manual_kws_data['匹配方式'].fillna(value='broad/exact', inplace=True)
    manual_kws_data['匹配方式'].replace(' ', 'broad/exact', inplace=True)
    manual_kws_data = manual_kws_data['匹配方式'].str.split('/', expand=True).stack().reset_index(level=0).set_index(
        'level_0').rename(
        columns={0: '匹配方式'}).join(manual_kws_data.drop('匹配方式', axis=1))
    manual_kws_data.reset_index(drop=True, inplace=True)
    manual_kws_data_groupby_type = manual_kws_data.groupby('匹配方式')

    # 处理三种匹配方式数据
    def process_manual_kws_three_types(broad_data, match_type, station_name):
        broad_data.drop_duplicates(inplace=True)
        if match_type == 'broad':
            campaign_name = "MANUAL-%s-by-SP_Bulk" % station_name
        elif match_type == 'phrase':
            campaign_name = "MANUAL-%s-by-SP_Bulk-Order" % station_name
        elif match_type == 'exact':
            campaign_name = "MANUAL-ST-EXACT-by-SP_Bulk"
        else:
            print("%s中存在匹配类型为%s,请将匹配类型改为broad,phrase,exact." % (station_name, match_type))
            return
        broad_data_groupby_sku = broad_data.groupby('SKU')
        processed_broad_data = pd.DataFrame(columns=export_columns)
        for sku, sku_data in broad_data_groupby_sku:
            sku = sku_data['SKU'].values[0]
            asin = sku_data['ASIN'].values[0]
            ad_group_name = "%s %s" % (sku, asin)
            ad_group_degree = pd.DataFrame(
                [[campaign_name, '', '', '', '', ad_group_name, manual_ad_group_bid, '', '', '', '',
                  'enabled', 'enabled', '', '']], columns=export_columns)
            sku_degree = pd.DataFrame([[campaign_name, '', '', '', '', ad_group_name, '', sku, '', '', '',
                                        'enabled', 'enabled', 'enabled', '']], columns=export_columns)
            # 计算kws层级
            match_type = sku_data['匹配方式'].values
            kws = sku_data['词'].values
            kws_prices_default = [one_word_price * len(kw.split(" ")) for kw in kws]
            kws_bid = sku_data['竞价'].values
            kws_bid = [prices_default if (pd.isna(kw_bid)) or (kw_bid in ('', ' ')) else kw_bid for
                       prices_default, kw_bid in zip(kws_prices_default, kws_bid)]
            sku_kws_num = len(kws)
            empty_list = [np.nan] * len(export_columns)
            kws_degree = pd.DataFrame([empty_list] * sku_kws_num, columns=export_columns)
            kws_degree['Campaign Name'] = campaign_name
            kws_degree['Ad Group Name'] = ad_group_name
            kws_degree['Max Bid'] = kws_bid
            kws_degree['Keyword'] = kws
            kws_degree['Match Type'] = match_type
            kws_degree[['Campaign Status', 'Ad Group Status', 'Status']] = 'enabled'
            kws_degree.drop_duplicates(inplace=True)
            manual_one_sku = pd.concat([ad_group_degree, sku_degree, kws_degree])
            processed_broad_data = processed_broad_data.append(manual_one_sku)
        return processed_broad_data

    manual_kws_data_processed = pd.DataFrame(columns=export_columns)
    for type, manual_kws_data_groupby_data in manual_kws_data_groupby_type:
        proecssed_broad = process_manual_kws_three_types(manual_kws_data_groupby_data, type, station_name)
        manual_kws_data_processed = manual_kws_data_processed.append(proecssed_broad)
    return manual_kws_data_processed


# 2.3 处理精否
def process_exact_nega(exact_nega_data, station_name):
    export_columns = ['Campaign Name', 'Campaign Daily Budget', 'Campaign Start Date', 'Campaign End Date',
                      'Campaign Targeting Type', 'Ad Group Name', 'Max Bid', 'SKU', 'Keyword', 'Product Targeting ID',
                      'Match Type', 'Campaign Status', 'Ad Group Status', 'Status', 'Bidding strategy']
    exact_nega_data['SKU'].fillna(method='ffill', inplace=True)
    exact_nega_data['ASIN'].fillna(method='ffill', inplace=True)
    exact_nega_data['匹配方式'].fillna(value='negative exact', inplace=True)
    exact_nega_data['匹配方式'].replace(' ', 'negative exact', inplace=True)
    # exact_nega_data.reset_index(drop=True, inplace=True)
    all_sku = set(exact_nega_data['SKU'])
    processed_all_exact_nega = pd.DataFrame(columns=export_columns)
    for sku in all_sku:
        one_sku_exact_nega_data = exact_nega_data[exact_nega_data['SKU'] == sku]
        sku = one_sku_exact_nega_data['SKU'].values[0]
        asin = one_sku_exact_nega_data['ASIN'].values[0]

        # 通过station,sku,asin去sku_kws_info请求得到对应的ad_group
        def get_ad_group_name(station_name, sku, asin):
            station_name = station_name.lower()

            # 从服务器的team_station.station_uuid_index 上获得站点uuid
            def db_download_station_old_uuid(station_name, db='team_station', ip='192.168.129.240',
                                             user_name='marmot',
                                             password='marmot123', port=3306, table='station_uuid_index'):
                conn = pymysql.connect(
                    host=ip,
                    user=user_name,
                    password=password,
                    database=db,
                    port=port,
                    charset='UTF8')

                # 创建游标
                cursor = conn.cursor()
                # table_name
                table_name = table

                # 规范站点名
                station_name = station_name.lower()
                station_name = station_name.replace("-", '_').replace(" ", "_")

                try:
                    # 查询旧的uuid
                    select_sql = """SELECT * FROM {} where station = {}""".format(table_name, "'%s'" % station_name)
                    # 执行sql
                    cursor.execute(select_sql)
                    all_result = cursor.fetchall()
                    station_uuid_index = list(all_result)
                    if station_uuid_index:
                        station_old_uuid = json.loads(station_uuid_index[0][1])['uuid']
                    else:
                        station_old_uuid = []
                except Exception as e:
                    print(e)
                    print("获取uuid出错:%s" % station_name)
                    conn.rollback()
                    conn.close()
                    return []
                return station_old_uuid

            # 通过uuid,sku,asin得到ad_group
            def db_download_ad_group_info(station_uuid, sku, asin, db='team_station', ip='192.168.129.240',
                                          user_name='marmot',
                                          password='marmot123', port=3306, table_name='sku_kws_info'):
                conn = pymysql.connect(
                    host=ip,
                    user=user_name,
                    password=password,
                    database=db,
                    port=port,
                    charset='UTF8')

                # 创建游标
                cursor = conn.cursor()
                if not station_uuid:
                    return []
                station_uuid_str = ",".join(list(map(lambda x: "'%s'" % x, station_uuid)))
                select_sql = """SELECT Campaign,Ad_Group,SKU,asin FROM {} where (uuid in ({})) and (SKU = {}) and (asin = {})""".format(
                    table_name, station_uuid_str, "'%s'" % sku, "'%s'" % asin)
                # 执行sql语
                cursor.execute(select_sql)
                all_result = cursor.fetchall()
                if len(all_result) == 0:
                    all_result = pd.DataFrame(columns=['Campaign', 'Ad_Group', 'sku', 'asin'])
                else:
                    all_result = pd.DataFrame([list(j) for j in all_result],
                                              columns=['Campaign', 'Ad_Group', 'sku', 'asin'])
                conn.commit()
                cursor.close()
                conn.close()
                return all_result

            # 筛选出符合条件的ad_group
            # Auto组中，优先Campaign中不含new
            def filter_ad_group(ad_group_info):
                """
                :param ad_group_info:Campaign, Ad_Group两列
                :return:符合条件的ad_group
                """
                if ad_group_info.empty:
                    pass
                campaign_set = set(ad_group_info['Campaign'])
                auto_campaign_set = set([camp for camp in campaign_set if 'AUTO' in camp])
                filtered_ad_group = set(ad_group_info[ad_group_info['Campaign'].isin(auto_campaign_set)]['Ad_Group'])
                return filtered_ad_group

            station_uuid = db_download_station_old_uuid(station_name)
            if not station_uuid:
                print('sku_kws_info数据库中没有{}站点,请检查...'.format(station_name))
                return []
            ad_group_info = db_download_ad_group_info(station_uuid, sku, asin)
            ad_group_set = filter_ad_group(ad_group_info)
            return ad_group_set

        ad_group_name_set = get_ad_group_name(station_name, sku, asin)
        if not ad_group_name_set:
            continue
        all_ad_group = pd.DataFrame(columns=export_columns)
        # 计算ad_group下的精否:processed_exact_nega_data
        for one_ad_group in ad_group_name_set:
            len_sku = len(one_sku_exact_nega_data)
            empty_list = [np.nan] * len(export_columns)
            processed_exact_nega_data = pd.DataFrame([empty_list] * len_sku, columns=export_columns)
            processed_exact_nega_data['Campaign Name'] = "AUTO-{}-by-SP_Bluk".format(station_name)
            processed_exact_nega_data['Ad Group Name'] = one_ad_group
            processed_exact_nega_data['Keyword'] = list(one_sku_exact_nega_data['词'])
            processed_exact_nega_data['Match Type'] = list(one_sku_exact_nega_data['匹配方式'])
            processed_exact_nega_data['Campaign Status'] = 'enabled'
            processed_exact_nega_data['Ad Group Status'] = 'enabled'
            processed_exact_nega_data['Status'] = 'enabled'
            processed_exact_nega_data.drop_duplicates(inplace=True)
            all_ad_group = all_ad_group.append(processed_exact_nega_data)
        processed_all_exact_nega = processed_all_exact_nega.append(all_ad_group)
    return processed_all_exact_nega


# 2.4 处理asin定向与否定
def process_asin_director(asin_data, ad_group_default_bid=0.02, asin_default_bid=0.2):
    """
    处理asin定向和否定asin 广告组定价:0.02,asin的缺失值为:0.2
    :param asin_data: asin数据
    :return: 处理后的asin数据
    """
    export_columns = ['Campaign Name', 'Campaign Daily Budget', 'Campaign Start Date', 'Campaign End Date',
                      'Campaign Targeting Type', 'Ad Group Name', 'Max Bid', 'SKU', 'Keyword', 'Product Targeting ID',
                      'Match Type', 'Campaign Status', 'Ad Group Status', 'Status', 'Bidding strategy']
    asin_data.drop_duplicates(inplace=True)
    asin_data['SKU'].fillna(method='ffill', inplace=True)
    asin_data['ASIN'].fillna(method='ffill', inplace=True)
    asin_data['竞价'].fillna(value=asin_default_bid, inplace=True)
    asin_data['竞价'].replace(' ', asin_default_bid, inplace=True)
    campaign_name = 'Negative Targeting Expression-SP_Bulk'
    asin_data_grouped = asin_data.groupby('SKU')
    processed_asin_director = pd.DataFrame(columns=export_columns)
    for sku, asin_one_sku_data in asin_data_grouped:
        # 第一层级：ad_group
        asin = asin_one_sku_data['ASIN'].values[0]
        asin_ad_group_name = "{} {}".format(sku, asin)
        asin_ad_group_degree = pd.DataFrame(
            [[campaign_name, '', '', '', '', asin_ad_group_name, ad_group_default_bid, '', '', '', '',
              'enabled', 'enabled', '', '']], columns=export_columns)
        # 第二层级:sku
        sku_degree = pd.DataFrame([[campaign_name, '', '', '', '', asin_ad_group_name, '', sku, '', '', '',
                                    'enabled', 'enabled', 'enabled', '']], columns=export_columns)
        # 第三层级:kws
        match_type = asin_one_sku_data['匹配方式'].values
        kws = asin_one_sku_data['词'].values
        kws = ['asin=' + '"' + kw.capitalize() + '"' for kw in kws if ~((pd.isna(kw)) or kw in ['', ' '])]
        kws_bid = asin_one_sku_data['竞价'].values
        sku_kws_num = len(kws)
        empty_list = [np.nan] * len(export_columns)
        kws_degree = pd.DataFrame([empty_list] * sku_kws_num, columns=export_columns)
        kws_degree['Campaign Name'] = campaign_name
        kws_degree['Ad Group Name'] = asin_ad_group_name
        kws_degree['Max Bid'] = kws_bid
        kws_degree['Keyword'] = kws
        kws_degree['Product Targeting ID'] = kws
        kws_degree['Match Type'] = match_type
        # 处理Matct Type为空的情况
        kws_degree['Match Type'].fillna(value='Targeting Expression', inplace=True)
        kws_degree['Match Type'].replace(' ', 'Targeting Expression', inplace=True)
        kws_degree[['Campaign Status', 'Ad Group Status', 'Status']] = 'enabled'
        kws_degree.drop_duplicates(inplace=True)
        asin_director_one_sku = pd.concat([asin_ad_group_degree, sku_degree, kws_degree])
        processed_asin_director = processed_asin_director.append(asin_director_one_sku)
    processed_asin_director['Max Bid'][processed_asin_director['Match Type'] == 'Negative Targeting Expression'] = ''
    return processed_asin_director


# 2. 按照SKU的类型（'新品自动','手动关键词','精否','asin否定','asin定向'进行分类处理
def process_upload_data(ori_upload_data, file_dir, station_name, file_date):
    """
        SKU的类型:'新品自动','手动关键词','精否','asin否定','asin定向'
        1.新品自动(auto_new)：若SKU无竞价，则Max Bid = price*20%*3%
        2.手动关键词(manual_kws)：广告组竞价= `0.02，单个词组个数*0.08;如果匹配方式空白，则按照broad和exact两种方式
        3.精否(exact_nega)：如果匹配方式空白，则按照negative exact
        4.asin定向(asin_director)：广告组竞价= 0.02，单个asin = 0.2
        5.asin否定(asin_director_nega)：做asin否定，必须要自带一个asin定向
    """
    # 1. 初始化upload_data
    #     1.去重、2.删除列名中的前后空白并将列大写、
    #     3.判断必须列是否缺失、4.判断是否有新的广告类型出现
    export_columns = ['Campaign Name', 'Campaign Daily Budget', 'Campaign Start Date', 'Campaign End Date',
                      'Campaign Targeting Type', 'Ad Group Name', 'Max Bid', 'SKU', 'Keyword', 'Product Targeting ID',
                      'Match Type', 'Campaign Status', 'Ad Group Status', 'Status', 'Bidding strategy']
    ori_upload_data.drop_duplicates(inplace=True)
    # ori_upload_data = ori_upload_data[~pd.isna(ori_upload_data['匹配方式'])]
    columns_name = [col.strip(' ').upper() for col in ori_upload_data.columns]
    ori_upload_data.columns = columns_name
    ori_upload_data = ori_upload_data[pd.notnull(ori_upload_data['SKU']) & pd.notnull(ori_upload_data['备注'])]
    need_columns = {'SKU', 'ASIN', '词', '匹配方式', '竞价', '备注'}
    if not need_columns.issubset(set(columns_name)):
        lost_col = need_columns - set(columns_name)
        print('{}缺失{}列'.format(file_dir, lost_col))
        return
    ad_upload_type_set = set(ori_upload_data['备注'])
    ad_upload_type_set = set([col.strip().lower() for col in ad_upload_type_set if pd.notnull(col)])
    need_upload_type_set = {'新品自动', '手动关键词', '精否', 'asin否定', 'asin定向'}
    if not ad_upload_type_set.issubset(need_upload_type_set):
        new_type = ad_upload_type_set - need_upload_type_set
        print("路径:{}".format(file_dir))
        print("< '备注'列 > 有新的广告类型:{},请修改{}为五种固定广告类型.".format(new_type, new_type))
        return
    all_types_data = pd.DataFrame(columns=export_columns)
    # 2. 处理五种上传的广告类型
    #    SKU的类型:'新品自动','手动关键词','精否','asin否定','asin定向'
    #    2.1 处理新品自动:auto_new_processed
    auto_new = ori_upload_data[ori_upload_data['备注'] == '新品自动']
    if not auto_new.empty:
        auto_new_data_processed = pd.DataFrame(columns=export_columns)
        auto_new.dropna(subset=['SKU'], inplace=True)
        auto_new_sku = set(auto_new['SKU'])
        for sku in auto_new_sku:
            one_auto_new_data = auto_new[auto_new['SKU'] == sku]
            processed_one_auto_new_data = process_auto_new(one_auto_new_data, station_name, file_date)
            auto_new_data_processed = auto_new_data_processed.append(processed_one_auto_new_data)
        all_types_data = all_types_data.append(auto_new_data_processed)
    #   2.2 处理手动关键词:manual_kws_processed
    manual_kws = ori_upload_data[ori_upload_data['备注'] == '手动关键词']
    if not manual_kws.empty:
        manual_kws_processed = process_manual_kws(manual_kws, station_name)
        all_types_data = all_types_data.append(manual_kws_processed)
    #   2.3 处理精否
    exact_nega = ori_upload_data[ori_upload_data['备注'] == '精否']
    if not exact_nega.empty:
        exact_nega_processed = process_exact_nega(exact_nega, station_name)
        all_types_data = all_types_data.append(exact_nega_processed)
    #   2.4 处理asin定向与否定
    asin_director = ori_upload_data[(ori_upload_data['备注'] == 'asin否定') | (ori_upload_data['备注'] == 'asin定向')]
    if not asin_director.empty:
        asin_director_processed = process_asin_director(asin_director)
        all_types_data = all_types_data.append(asin_director_processed)
    all_types_data.reset_index(drop=True, inplace=True)
    return all_types_data


# 主运行程序
def main():
    # 得到需要处理的文件
    file_dir = address.get()
    showtxt = f"{file_dir} Is Processing...\n如果运行超过10秒，请仔细检查{file_dir}后重启启动。"
    lab1.insert('insert', showtxt + '\n')
    lab1.insert('insert', '\n' + '======================================================================')
    lab1.insert('insert', '\n' + '======================================================================' + '\n')
    lab1.update()
    file_dir = file_dir.strip("'").strip('"').strip('’').strip('“').strip('‘').strip('”')
    if not os.path.exists(file_dir):
        tkinter.messagebox.showinfo(message="文件路径不存在!请仔细检查！重新输入")
    station_name = parse_station_name_and_date(file_dir)[0]
    # 当前日期
    file_date = datetime.now().strftime("%Y%m%d %H:%M:%S")[2:8]
    # file_date = parse_station_name_and_date(file_dir)[1]
    try:
        file_data = read_ori_upload_file(file_dir)
    except:
        tkinter.messagebox.showinfo(message=f"{file_dir}文件中内容为非规范需求格式,文件中内容只保留需要内容,其他的内容请删除掉,并只保留第一个sheet。")
    export_upload_data = process_upload_data(file_data, file_dir, station_name, file_date)
    export_upload_dir = os.path.dirname(file_dir) + '\\' + os.path.splitext(os.path.basename(file_dir))[0] + '_out' + \
                        os.path.splitext(os.path.basename(file_dir))[1]
    export_upload_data.to_excel(export_upload_dir, index=False)
    showtxt = f"{file_dir}处理完成.\n输出路径:{export_upload_dir} 的文件! \n请关闭窗口查看处理后的上传文件"
    lab1.insert('insert', '\n' + showtxt + '\n')
    lab1.update()


if __name__ == "__main__":
    win = tk.Tk()
    win.title('批量上传')  # 添加标题
    ttk.Label(win, text="filer Address(文件完整路径):").grid(column=0, row=0)  # 添加一个标签，并将其列设置为1，行设置为0
    # Address 文本框
    address = tk.StringVar()  # StringVar是Tk库内部定义的字符串变量类型，在这里用于管理部件上面的字符；不过一般用在按钮button上。改变StringVar，按钮上的文字也随之改变。
    addressEntered = ttk.Entry(win, width=60,
                               textvariable=address)  # 创建一个文本框，定义长度为12个字符长度，并且将文本框中的内容绑定到上一句定义的name变量上，方便clickMe调用
    addressEntered.grid(column=1, row=0)  # 设置其在界面中出现的位置  column代表列   row 代表行
    addressEntered.focus()  # 当程序运行时,光标默认会出现在该文本框中
    # 按钮
    action = ttk.Button(win, text="Enter",
                        command=main)  # 创建一个按钮, text：显示按钮上面显示的文字, command：当这个按钮被点击之后会调用command函数
    action.grid(column=2, row=0)  # 设置其在界面中出现的位置  column代表列   row 代表行

    tip = tk.Label(win, background='seashell', foreground='red',
                   text="tips:复制完整路径可以按住shift，然后右键点击文件，选择<复制为路径(A)>即可复制完整路径。" + '\n')
    tip.grid(column=1, row=1)

    showtxt = tk.StringVar()
    lab1 = tk.Text(win, fg='blue')
    lab1.grid(row=2, column=0, columnspan=2)

    win.mainloop()  # 当调用mainloop()时,窗口才会显示出来
