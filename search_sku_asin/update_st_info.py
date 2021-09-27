"""
更新st_info数据
将路径E:\AD_WEB\file_dir\st_info下全部站点的st报表文件中的全部工作表数据存储在
mysql的server_camp_report数据库中
    工作表：                                        数据库表
    出单优质搜索词
    未出单高点击搜索词
    近期低于平均点击率的SKU
    后台Search Term参考
    不出单关键词
    同erp_sku下其他seller_sku出单关键词-全部        erpsku_restkws_add_columns
    同erp_sku下其他seller_sku出单关键词-同国家      erpsku_restkws_add_columns_filter_langs
"""
import os
import time
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import warnings
import copy
import string
import gc
from sqlalchemy.types import VARCHAR
from retrying import retry

import my_toolkit.process_files as process_files
import my_toolkit.public_function as public_function
import my_toolkit.conn_db as conn_db
import sql_write_read
from my_toolkit import process_station

warnings.filterwarnings('ignore')




def add_columns(df, kw_columns, add_column_num=10):
    rest_kw_langs = public_function.detect_list_lang(list(df[kw_columns]))
    df[kw_columns].fillna(value='', inplace=True)
    df['rest_kws_list'] = list(map(public_function.split_sentence, list(df[kw_columns]), rest_kw_langs))
    for i in range(add_column_num):
        df[f'keyword_{i + 1}'] = [word_list[i] if len(word_list) > i else '' for word_list in
                                  df['rest_kws_list'].values]
    del df['rest_kws_list']


def upload_file(st_file_path):
    """
    将st表中工作表的数据添加station,erpsku,asin和datetime后上传到数据库
    :param file_path:str
            st表路径
    :return: None
    """
    # 获得station_name

    station_name = os.path.basename(st_file_path)[9:-10]
    if not station_name:
        return
    station_name = process_station.standardStation(station_name,case='upper')
    # 将每个工作簿添加生成时间和erpsku
    # 将sheet中的表头改名为sql中的字段名以及顺序
    sheet_dict = {
        '出单优质搜索词':
            {
                'sql_table_name': 'high_quality_keywords',
                'rename_columns': ['sku', 'campaign_name', 'ad_group_name', 'match_type', 'customer_search_term','impression', 'click', 'spend', 'sale', 'order', 'ctr', 'cpc', 'acos',
                                   'cr','sku_sale', 'item_name']
            },
        '未出单高点击搜索词':
            {
                'sql_table_name': 'high_click_no_order_keywords',
                'rename_columns': ['sku', 'campaign_name', 'ad_group_name', 'match_type', 'customer_search_term',
                                   'impression', 'click', 'spend', 'sale', 'order', 'ctr', 'cpc', 'acos', 'cr',
                                   'sku_click']
            },
        '近期低于平均点击率的SKU':
            {
                'sql_table_name': 'lower_ctr_sellersku',
                'rename_columns': ['sku', 'impression', 'click', 'spend', 'order', 'sale', 'ctr', 'acos']
            },
        '后台Search Term参考':
            {
                'sql_table_name': 'search_term_refer',
                'rename_columns': ['sku', 'search_term', 'recent_orders']
            },
        '不出单关键词':
            {
                'sql_table_name': 'no_order_keywords',
                'rename_columns': ['sku', 'no_order_kws_sentence', 'recent_clicks', 'kws_sentence', 'recent_orders']
            }
    }

    for sheet in sheet_dict.keys():
        st_sheet_data = process_files.read_file(st_file_path, sheet_name=sheet)
        if (st_sheet_data is None) or (st_sheet_data.empty):
            continue
        # 重命名
        columns = sheet_dict[sheet]['rename_columns']
        # 扩展最后的输出的数据库的列名
        extend_columns = ['station', 'erpsku', 'asin']
        extend_columns.extend(columns)
        if sheet == '未出单高点击搜索词':
            extend_end_columns = ['updatetime', 'keyword_1', 'keyword_2', 'keyword_3', 'keyword_4', 'keyword_5']
            extend_columns.extend(extend_end_columns)
        elif sheet in ['后台Search Term参考']:
            extend_end_columns = ['updatetime', 'keyword_1', 'keyword_2', 'keyword_3', 'keyword_4', 'keyword_5',
                                  'keyword_6', 'keyword_7', 'keyword_8', 'keyword_9', 'keyword_10']
            extend_columns.extend(extend_end_columns)
        elif sheet in ['出单优质搜索词']:
            # 删除targeting
            if 'Targeting' in st_sheet_data.columns:
                del st_sheet_data['Targeting']
            extend_end_columns = ['kws_lang','updatetime', 'keyword_1', 'keyword_2', 'keyword_3', 'keyword_4', 'keyword_5',
                                  'keyword_6', 'keyword_7', 'keyword_8', 'keyword_9', 'keyword_10']
            extend_columns.extend(extend_end_columns)
        else:
            extend_end_columns = 'updatetime'
            extend_columns.append(extend_end_columns)

        # '不出单关键词'有两种不同的列名情况
        if sheet == '不出单关键词':
            if len(st_sheet_data.columns) == 4:
                st_sheet_data['最近出单'] = 0
        st_sheet_data.columns = columns
        st_sheet_data_modify_datetime = datetime.strptime(time.ctime(os.path.getmtime(st_file_path)),
                                                          '%a %b %d %H:%M:%S %Y')

        # 出单优质搜索词添加搜索词语言列
        if sheet == '出单优质搜索词':
            st_sheet_data['kws_lang'] = public_function.detect_list_lang(list(st_sheet_data['customer_search_term']))

        st_sheet_data['station'] = station_name
        st_sheet_data['updatetime'] = st_sheet_data_modify_datetime
        # 将customer_search_term分裂
        if sheet == '未出单高点击搜索词':
            split_columns_num = 5
        else:
            split_columns_num = 10
        if sheet == '后台Search Term参考':
            add_columns(st_sheet_data, 'search_term', split_columns_num)
        if sheet in ['出单优质搜索词', '未出单高点击搜索词']:
            add_columns(st_sheet_data, 'customer_search_term', split_columns_num)

        # 获取erpsku
        sellsersku_list = list(st_sheet_data['sku'])
        sellersku_erpsku_tied_info = sql_write_read.query_sku_tied(sellsersku_list)
        # 添加erpsku
        if sellersku_erpsku_tied_info.empty:
            st_sheet_data['erp_sku'] = ''
        else:
            st_sheet_data = pd.merge(st_sheet_data, sellersku_erpsku_tied_info[['seller_sku', 'erp_sku']], how='left',
                                     left_on='sku',
                                     right_on='seller_sku')
        st_sheet_data.rename(columns={'erp_sku': 'erpsku'}, inplace=True)

        # 获取asin捆绑表信息
        sellersku_asin_tied_info = sql_write_read.query_asin(sellsersku_list)
        # 添加asin列
        if sellersku_asin_tied_info.empty:
            st_sheet_data['asin'] = ''
        else:
            st_sheet_data = pd.merge(st_sheet_data, sellersku_asin_tied_info[['seller-sku', 'asin']], how='left',
                                     left_on='sku', right_on='seller-sku')

        # 按照一定的格式输出
        st_sheet_data = st_sheet_data[extend_columns]

        if len(st_sheet_data.index) != 0:
            # 更新到数据库中 先删除掉原本站点存在的数据,后添加站点数据
            sku_search_db = 'server_camp_report'
            delete_sql = "delete from %s where station = '%s'" % (sheet_dict[sheet]['sql_table_name'], station_name)
            conn_db.to_sql_delete(delete_sql, db=sku_search_db)
            conn_db.to_sql_append(st_sheet_data, sheet_dict[sheet]['sql_table_name'], db=sku_search_db)


def db_upload_st_file(st_info_folder=r'D:/st_info'):
    """
    主函数,将st_info文件夹中的更新文件存储到数据库中
    :param st_info_folder: str
            st_info文件夹路径
    :return: None
    """
    # 获得更新的st报表
    # 先初始化st
    old_files_list = [os.path.join(st_info_folder, file) for file in os.listdir(st_info_folder) if 'ST' in file]
    old_files_modify_time = {file: os.path.getmtime(file) for file in old_files_list}
    while 1:
        try:
            new_files_list = [os.path.join(st_info_folder, file) for file in os.listdir(st_info_folder) if 'ST' in file]
            new_files_modify_time = {file: os.path.getmtime(file) for file in new_files_list}
            process_st_files = [file for file, file_time in new_files_modify_time.items() if
                                file_time != old_files_modify_time.get(file, None)]
        except:
            continue

        if process_st_files:
            st_files = [file for file in process_st_files]
            for file in st_files:
                station_name = os.path.basename(file).split(' ')
                if not station_name:
                    continue
                station_name = station_name[1][:-3] + '_' + station_name[1][-2:]
                try:
                    upload_file(file)
                except Exception as e:
                    print(f'{station_name}:st表上传失败.{e}')
                print(f'{station_name}: 完成.')
        else:
            time.sleep(30)
            print('暂无st表更新,休息60秒.')
        old_files_modify_time = copy.deepcopy(new_files_modify_time)
        time.sleep(30)

        if datetime.now().hour in set(range(0, 7)):
            restart_hour = 8
            reset_time = (restart_hour - datetime.now().hour) * 3600
            time.sleep(reset_time)
            print(f'早上{restart_hour}再开始.')


if __name__ == '__main__':
    db_upload_st_file()
