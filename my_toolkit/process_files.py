# -*- coding: utf-8 -*-
"""
Proj: ad_helper
Created on:   2020/5/22 14:45
@Author: RAMSEY

"""
import gc
import time
import os

"""
处理五表的报表文件数据:
    read_file:读取五表文件，主要包括识别文件类别、文件分隔符以及文件的编码
"""

import pandas as pd
import os
from langdetect import detect_langs
from my_toolkit import public_function
import zipfile
import shutil
import h5py
from pyexcelerate import Workbook
from datetime import datetime
import datetime as dt
import pickle


# 读取单个文件数据(若为excel,则读取单个sheet)
def read_file(file_path: 'full_path', sheet_name='Sheet1', is_binary=False) -> 'pd.DataFrame':
    """
    读取常见的文本文件(csv,txt,xls,xlsx),解决不同的编码和不同的分隔符有较好的适应能力。
    :param file_path:文件的全路径
    :param sheet_name:工作簿的工作表名
    :return:df
    """
    # 常见的编码方式
    REPORT_ENCODINGS = ('utf-8', 'utf-16', "latin_1", "ISO-8859-1", 'cp932', 'unicode_escape')
    # 可以读取的常见文件类型
    CAN_READ_FILE_TYPE = ('.xlsx', '.xls', '.txt', '.csv')
    # 判断文件是否存在
    if not os.path.exists(file_path):
        print(f'PATH DONT EXISTS:{file_path}')
        return
    file_path = file_path.lower()
    #  判断文件类型
    file_type = os.path.splitext(file_path)[1]
    if file_type not in CAN_READ_FILE_TYPE:
        print(F'ERROR FILE TYPE:{file_type} 文件类型不能读。输入文件类型是{CAN_READ_FILE_TYPE}中的一种.')
        return
    # 文件内容的格式
    if not is_binary:
        read_mode = 'r'
    else:
        read_mode = 'rb'
    # 常见的编码方式

    # 读取'.txt','.csv文件'
    if ('.txt' in file_path) or ('.csv' in file_path):
        for e in REPORT_ENCODINGS:
            try:
                with open(file_path, read_mode, encoding=e) as f:
                    first_line = f.readline()
                    # 检测文件的分隔符：用逗号、\t去分隔文件第一行，取分隔后最长的分隔符号
                    init_delimiter = (',', '\t')
                    detect_delimiter = ','
                    first_line_len = 1
                    for delimiter in init_delimiter:
                        first_line_list = first_line.split(delimiter)
                        first_line_len_temp = len(first_line_list)
                        if first_line_len_temp > first_line_len:
                            first_line_len = first_line_len_temp
                            detect_delimiter = delimiter
                            first_line_element = first_line.split(detect_delimiter)
                    if first_line_len == 1:
                        continue
                # 判断是否有乱码
                for element in first_line_element:
                    try:
                        detect_langs(element)
                    except Exception as ep:
                        continue
                # 读csv,txt
                try:
                    file_data = pd.read_csv(file_path, sep=detect_delimiter, encoding=e, error_bad_lines=False,
                                            warn_bad_lines=False)
                    return file_data
                except:
                    raise TypeError(f"{file_path}实在是无法正确读取...")
            except:
                continue
    if ('.xlsx' in file_path) or ('.xls' in file_path):
        # 设置st表和广告报表的sheet_name
        # (这里文件的sheet名应该规范化,防止其他语言)
        if ("search" in file_path) and (sheet_name == 'Sheet1'):
            sheet_name = 'Sponsored Product Search Term R'
        if ("bulk" in file_path) and (sheet_name == 'Sheet1'):
            sheet_name = 'Sponsored Products Campaigns'
            try:
                return pd.read_excel(file_path, sheet_name=sheet_name, encoding='utf-8')
            except:
                pass
        if ("bulk" in file_path) and (sheet_name == 'Sponsored Brands Campaigns'):
            try:
                return pd.read_excel(file_path, sheet_name=sheet_name, encoding='utf-8')
            except:
                pass
        for e in REPORT_ENCODINGS:
            try:
                # 判断文件的分隔符
                for delimiter in (',', '\t'):
                    first_line = pd.read_excel(file_path, sheet_name=sheet_name, nrows=1, encoding=e, sep=delimiter)
                    first_line = first_line.columns
                    if len(first_line) > 1:
                        detect_delimiter = delimiter
                        first_line_element = first_line
                        break
                # 判断是否有乱码
                for element in first_line_element:
                    try:
                        detect_langs(element)
                    except Exception as e:
                        continue
                # 读xls,xlsx文件
                try:
                    read_excel = pd.ExcelFile(file_path)
                    sheet_names = read_excel.sheet_names
                    if sheet_name not in sheet_names:
                        raise FileNotFoundError(f'{file_path}中没有{sheet_name}.')
                    else:
                        file_data = read_excel.parse(sheet_name, sep=detect_delimiter, encoding=e,
                                                     error_bad_lines=False, warn_bad_lines=False)
                        return file_data
                except:
                    raise TypeError(f"{file_path}是在是无法读取。请检查文件正确编码方式是否为{REPORT_ENCODINGS}中的一种.")
            except:
                continue


# 处理5表文件的列名
def init_file_data_columns(file_data: 'pd.DataFrame'):
    """
    由于列名中存在前后有空格的现象以及非英语的情况，于是需要初始化文件的列名
        1.去除列名中前后空格
        2.重命名
    :param file_data:文件的数据
    :return:初始化文件列名后的文件
    """
    # 去除列名中的空格
    file_columns = file_data.columns
    if not file_columns:
        return None
    file_data.columns = [column.strip(' ') for column in file_columns]
    # 将5表中的列名重命名
    # 1.广告报表重命名列表
    # 中文标题
    cp_cn_columns_translation = {'记录ID': 'Record ID', '记录类型': 'Record Type', '活动编号': 'Campaign ID', '广告商品': 'Campaign',
                                 '广告商品每日预算': 'Campaign Daily Budget', '产品组合ID': 'Portfolio ID',
                                 '开始日期': 'Campaign Start Date', '结束日期': 'Campaign End Date',
                                 '目标受众类型': 'Campaign Targeting Type', '广告组': 'Ad Group',
                                 '最大每点击成本': 'Max Bid', '关键字或商品投放': 'Keyword or Product Targeting',
                                 '产品投放ID': 'Product Targeting ID', '匹配类型': 'Match Type', '广告商品状态': 'Campaign Status',
                                 '广告组状态': 'Ad Group Status', '状态': 'Status', '展现量': 'Impressions', '点击量': 'Clicks',
                                 '花费': 'Spend', '订单': 'Orders', '单位总数': 'Total Units', '销售': 'Sales',
                                 '竞价策略': 'Bidding strategy', '投放类型': 'Placement Type',
                                 '按展示位置提高竞价': 'Increase bids by placement'}
    # 德国站
    cp_de_columns_translation = {'Datensatz-ID': 'Record ID',
                                 'Datensatztyp': 'Record Type',
                                 'Kampagnen': 'Campaign',
                                 'Tagesbudget Kampagne': 'Campaign Daily Budget',
                                 'Portfolio ID': 'Portfolio ID',
                                 'Portfolio-ID': 'Portfolio ID',
                                 'Startdatum der Kampagne': 'Campaign Start Date',
                                 'Enddatum der Kampagne': 'Campaign End Date',
                                 'Ausrichtungstyp der Kampagne': 'Campaign Targeting Type',
                                 'Anzeigengruppe': 'Ad Group',
                                 'Maximales Gebot': 'Max Bid', 'Schlagwort oder Product Targeting': 'Keyword',
                                 'Produkt-Targeting-ID': 'Product Targeting ID',
                                 u'Übereinstimmungstyp': 'Match Type', 'SKU': 'SKU',
                                 'Kampagnenstatus': 'Campaign Status', 'Anzeigengruppe Status': 'Ad Group Status',
                                 'Status': 'Status',
                                 'Sichtkontakte': 'Impressions', 'Klicks': 'Clicks', 'Ausgaben': 'Spend',
                                 'Bestellungen': 'Orders',
                                 'Einheiten insgesamt': 'Total units', 'Vertrieb': 'Sales', 'ACoS': 'ACoS',
                                 'Gebot+': 'Bid+'}
    # FR站
    cp_fr_columns_translation = {u'ID d’enregistrement': 'Record ID', u'Type d’enregistrement': 'Record Type',
                                 u'Campagne ': 'Campaign',
                                 u'Budget de campagne quotidien ': 'Campaign Daily Budget',
                                 'Portfolio ID': 'Portfolio ID',
                                 u'ID du portefeuille': 'Portfolio ID',
                                 u'Date de début': 'Campaign Start Date', u'Date de fin ': 'Campaign End Date',
                                 u'Type de ciblage ': 'Campaign Targeting Type', u'CPC max. ': 'Max Bid',
                                 u'Groupe d’annonces': 'Ad Group', u'Enchère maximale': 'Max Bid',
                                 u'Mot-clef ou ciblage de produit ': 'Keyword',
                                 'Identifiant de ciblage des produits': 'Product Targeting ID',
                                 'Type de correspondance': 'Match Type',
                                 'SKU': 'SKU', 'Statut de la campagne': 'Campaign Status',
                                 u'Statut du groupe de publicités': 'Ad Group Status', 'Statut': 'Status',
                                 'Impressions': 'Impressions', 'Clics': 'Clicks', u'Dépense': 'Spend',
                                 'Commandes': 'Orders',
                                 u'Total des unités': 'Total units',
                                 'Ventes': 'Sales', u'Coût publicitaire de vente (ACoS)': 'ACoS'}
    # ES站
    cp_es_columns_translation = {'ID de registro': 'Record ID',
                                 'Tipo de registro': 'Record Type',
                                 u'Campaña': 'Campaign',
                                 u'Presupuesto diario de campaña': 'Campaign Daily Budget',
                                 u'ID del portafolio': 'Portfolio ID',
                                 u'Fecha de inicio de la campaña': 'Campaign Start Date',
                                 u'Fecha de finalización de la campaña': 'Campaign End Date',
                                 u'Tipo de segmentación': 'Campaign Targeting Type',
                                 'Nombre del grupo de anuncios': 'Ad Group',
                                 u'Puja máxima': 'Max Bid',
                                 'Palabra clave o direccionamiento del producto': 'Keyword',
                                 'ID de direccionamiento por producto': 'Product Targeting ID',
                                 'Tipo de coincidencia': 'Match Type',
                                 'SKU': 'SKU',
                                 u'Estado de campaña': 'Campaign Status',
                                 'Estado de grupo de anuncios': 'Ad Group Status',
                                 'Estado': 'Status',
                                 'Impresiones': 'Impressions',
                                 'Clics': 'Clicks',
                                 'Gasto': 'Spend',
                                 'Pedidos': 'Orders',
                                 'Unidades totales': 'Total units',
                                 'Ventas': 'Sales',
                                 'ACoS': 'ACoS'}
    cp_it_columns_translation = {'ID record': 'Record ID',
                                 'Tipo di record': 'Record Type',
                                 'campagna': 'Campaign',
                                 'Budget giornaliero campagna': 'Campaign Daily Budget',
                                 'Portfolio ID': 'Portfolio ID',
                                 'ID portfolio': 'Portfolio ID',
                                 'data di inizio della campagna': 'Campaign Start Date',
                                 'data di fine della campagna': 'Campaign End Date',
                                 'Tipo di targeting': 'Campaign Targeting Type',
                                 'Gruppo di annunci': 'Ad Group',
                                 'Offerta CPC': 'Max Bid',
                                 'Parola chiave o targeting del prodotto': 'Keyword',
                                 'ID targeting prodotto': 'Product Targeting ID',
                                 'Tipo di corrispondenza': 'Match Type',
                                 'Codice SKU': 'SKU',
                                 'Stato campagna': 'Campaign Status',
                                 'stato gruppo di annunci': 'Ad Group Status',
                                 'Stato': 'Status',
                                 'Impressioni': 'Impressions',
                                 'Clic': 'Clicks',
                                 'Spesa': 'Spend',
                                 'Ordini': 'Orders',
                                 u'Totale unità': 'Total units',
                                 'Vendite': 'Sales',
                                 'ACoS': 'ACoS'}
    # 表头发生变化，需要变更，将'キーワードまたは製品ターゲティング'变为
    # 'キーワードまたは商品ターゲティング'-2019.07.29
    cp_jp_columns_translation = {u'レコードID': 'Record ID',
                                 u'レコードタイプ': 'Record Type',
                                 u'キャンペーン': 'Campaign',
                                 u'キャンペーンの1日当たりの予算': 'Campaign Daily Budget',
                                 'Portfolio ID': 'Portfolio ID',
                                 u'キャンペーンの開始日': 'Campaign Start Date',
                                 u'キャンペーンの終了日': 'Campaign End Date',
                                 u'キャンペーンのターゲティングタイプ': 'Campaign Targeting Type',
                                 u'広告グループ': 'Ad Group',
                                 u'入札額': 'Max Bid',
                                 u'キーワードまたは商品ターゲティング': 'Keyword', u'キーワードまたは製品ターゲティング': 'Keyword',
                                 u'商品ターゲティングID': 'Product Targeting ID',
                                 u'マッチタイプ': 'Match Type',
                                 u'広告(SKU)': 'SKU',
                                 u'キャンペーンステータス': 'Campaign Status',
                                 u'広告グループステータス': 'Ad Group Status', u'ステータス': 'Status', u'インプレッション': 'Impressions',
                                 u'クリック': 'Clicks', u'広告費': 'Spend',
                                 u'注文数': 'Orders',
                                 u'合計販売数': 'Total units', u'総売上': 'Sales',
                                 u'売上高に占める広告費の割合 （ACoS)': 'ACoS',
                                 # u'Bidding strategy':'Bidding strategy',
                                 # u'広告枠の種類':'Placement Type',
                                 # u'Increase bids by placement':'Increase bids by placement'
                                 }
    cp_mx_columns_translation = {'Identificador de registro': 'Record ID',
                                 'Tipo de registro': 'Record Type',
                                 u'Campaña': 'Campaign',
                                 u'Presupuesto diario de campaña': 'Campaign Daily Budget',
                                 u'Portfolio ID': 'Portfolio ID',
                                 u'ID del pedido': 'Portfolio ID',
                                 'Fecha de inicio': 'Campaign Start Date',
                                 u'Fecha de finalización': 'Campaign End Date',
                                 u'Tipo de segmentación': 'Campaign Targeting Type',
                                 'Grupo de anuncios': 'Ad Group',
                                 u'Puja máxima': 'Max Bid',
                                 'Keyword or Product Targeting': 'Keyword',
                                 u'Segmentación por producto o palabra clave': 'Keyword',
                                 'Product Targeting ID': 'Product Targeting ID',
                                 u'Identificador de segmentación por producto': 'Product Targeting ID',
                                 'Tipo de coincidencia': 'Match Type',
                                 'SKU': 'SKU',
                                 u'Estado de la Campaña': 'Campaign Status',
                                 'Estado del grupo de anuncios': 'Ad Group Status',
                                 'Estado': 'Status',
                                 'Impresiones': 'Impressions',
                                 'Clics': 'Clicks',
                                 u'Inversión': 'Spend',
                                 'Pedidos': 'Orders',
                                 'Total de unidades': 'Total units',
                                 'Ventas': 'Sales',
                                 'Costo publicitario de las ventas (ACoS)': 'ACoS'}

    # 2.listing表重命名列表
    listing_rename_columns = {'出品者SKU': 'seller-sku',
                              '価格': 'price',
                              'ASIN 1': 'asin1',
                              'ASIN1': 'asin1',
                              '商品名': 'item-name',
                              '出品日': 'open-date',
                              'フルフィルメント・チャンネル': 'fulfillment-channel',
                              '卖家 SKU': 'seller-sku',
                              '价格': 'price',
                              '商品名称': 'item-name',
                              "开售日期": "open-date",
                              "配送渠道": "fulfillment-channel"}


# 解压zip文件(将多个文件压缩形成的压缩文件)
def unzip_file(zip_path: 'dir', save_folder=None, delete_zipped=False) -> None:
    """
    解压站点zip压缩文件,默认保存路径为当前目录
    :param zip_path:需要解压站点的压缩包路径
    :param save_folder:解压位置
    :return:None
    """
    if not os.path.exists(zip_path):
        return
    if '.zip' in zip_path.lower():
        z = zipfile.ZipFile(zip_path, "r")
    # 打印zip文件中的文件列表
    file_list = z.namelist()
    if save_folder is None:
        save_folder = os.path.dirname(zip_path)
    writer_folder = os.path.join(save_folder, os.path.basename(zip_path)[:-4])
    if os.path.exists(writer_folder):
        try:
            shutil.rmtree(writer_folder)
            os.mkdir(writer_folder)
        except:
            pass
    else:
        os.mkdir(writer_folder)
    # 剔除子文件夹
    file_list = [file for file in file_list if file.find('/') == -1]
    for file in file_list:
        content = z.read(file)
        with open(writer_folder + '/' + file, 'wb') as f:
            f.write(content)
    # 是否删除压缩文件
    z.close()
    if delete_zipped:
        os.remove(zip_path)


# 解压zip文件包(将一个文件夹压缩形成的压缩文件)
def unzip_folder(zip_path: 'dir', save_folder=None, delete_zipped=False) -> None:
    """
    解压站点zip压缩文件,默认保存路径为当前目录
    :param zip_path:需要解压站点的压缩包路径
    :param save_folder:解压位置
    :return:None
    """
    if not os.path.exists(zip_path):
        return
    if '.zip' not in zip_path.lower():
        return
    if '.zip' in zip_path.lower():
        z = zipfile.ZipFile(zip_path, "r")
    # 打印zip文件中的文件列表
    file_list = z.namelist()
    if save_folder is None:
        save_folder = os.path.dirname(zip_path)
    writer_folder = os.path.join(save_folder, os.path.basename(zip_path)[:-4]).upper()
    if os.path.exists(writer_folder):
        shutil.rmtree(writer_folder)
    os.mkdir(writer_folder)
    # 剔除子文件夹
    file_list = [file for file in file_list if file.find('/') < len(file) - 1]
    for file in file_list:
        content = z.read(file)
        writer_path = os.path.join(writer_folder, os.path.basename(file))
        with open(writer_path, 'wb') as f:
            f.write(content)
    z.close()
    if delete_zipped:
        os.remove(zip_path)


# 将df存储为h5
def write_df_2_h5(df: pd.DataFrame, save_path: 'hdf5', key_name):
    """
    将报表从df格式转换成h5的格式,方便的存取,
    :param df:报表的df格式数据
    :param key_name:报表的df的命名
    :param save_path:保存的路径(.hdf5文件)
    :return:
    """
    if public_function.detect_df(df) is False:
        return None
    if '.hdf5' not in save_path:
        raise ValueError(f"{save_path} is not a h5(.hdf5) file.")
    h5_save_folder = os.path.dirname(save_path)
    if not os.path.exists(h5_save_folder):
        os.makedirs(h5_save_folder)
    df.to_hdf(save_path, key=key_name, mode='a')


# 将h5读写成df
def read_h5_2_df(h5_path: 'path', key_name: str) -> pd.DataFrame:
    """
    将原先保存在h5文件中的df格式数据读出来
    :param h5_path: h5文件路径,以'.hdf5'结尾
    :param key_name: 文件中数据保存的组
    :return:
    """
    if not os.path.exists(h5_path):
        raise ValueError(f'ERROR PATH:{h5_path} DONT EXIST.')
    if '.hdf5' not in h5_path.lower():
        raise ValueError(f'ERROR PATH:{h5_path} IS NOT h5 FILE.')
    f = h5py.File(h5_path, mode='r')
    keys_name = f.keys()
    if key_name not in keys_name:
        raise ValueError(f'ERROR KEY:{key_name} 不在文件中,请输入{keys_name}中的一个.')
    else:
        df = pd.read_hdf(h5_path, key_name)
        return df


# 将df存储成pickle
def write_df_2_pickle(df, pkl_path: 'path'):
    """
    将df存储为pickle格式
    :param df: df数据
    :param pkl_path:存储的pickle路径
    :return: None
    """
    if '.pkl' not in pkl_path.lower():
        raise ValueError(f'ERROR PATH:{pkl_path} IS NOT pkl FILE.')
    if public_function.detect_df(df) is False:
        return None
    df.to_pickle(pkl_path)


# 将存储为pickle格式的df读出来
def read_pickle_2_df(pkl_path: 'path'):
    """
    将存储为pickle格式的df读出来
    :param pkl_path: pickle文件路径(.pkl)
    :return: df
    """
    if not os.path.exists(pkl_path):
        raise ValueError(f'ERROR PATH:{pkl_path} DONT EXIST.')
    if '.pkl' not in pkl_path.lower():
        raise ValueError(f'ERROR PATH:{pkl_path} IS NOT pkl FILE.')
    return pd.read_pickle(pkl_path)


# 将文件存储为pickle格式
def write_file_2_pickle(file_path, pkl_path=None, sheet_name='Sheet1'):
    data = read_file(file_path, sheet_name=sheet_name)
    if pkl_path is None:
        pkl_path = os.path.join(os.path.splitext(file_path)[0], '.pkl')
    write_df_2_pickle(data, pkl_path)


class WriteDf2Excel(Workbook):
    """
        使用pyexcelerate将df读入到excel中
        此方法比pd.ExcelWriter更快速,耗时基本是一半的时间
        Case:
            from pyexcelerate import Workbook
            import gc # 用于内存回收

            writer = WriteDf2Excel()
            writer.to_excel(dfs,sheets_name=None)
            writer.save("output.xlsx")

    """

    def to_excel(self, dfs, sheets_name=None):
        """
        将dfs写入到sheets_name中

        Parameters
        ----------
        dfs : list of pd.DataFrame,pd.DataFrame
            pd.DataFrame或是一组pd.DataFrame
        sheets_name : None,str,list of str,default None
            工作表名
        Returns
        -------

        """
        # 输入参数判断
        if not isinstance(dfs, (list, pd.DataFrame)):
            raise TypeError(f'input type is error,dfs must df or list of df.')
        if not isinstance(sheets_name, (str, list, type(None))):
            raise TypeError(f'input type is error,sheets_name must str or list of str or None.')
        sheet_name_default = 'Sheet'
        # 初始化sheet name
        if isinstance(dfs, list):
            if isinstance(sheets_name, list):
                if len(dfs) != len(sheets_name):
                    raise ValueError(f'dfs number is {len(dfs)} not equal to sheets number {len(sheets_name)}.')
            elif isinstance(sheets_name, str):
                raise ValueError(f'dfs number is {len(dfs)} not equal to sheets number 1.')
            else:
                sheets_name = list(map(lambda x: f'{sheet_name_default}{x}', range(1, len(dfs) + 1)))
        else:
            if isinstance(sheets_name, list):
                raise ValueError(f'dfs number is 1 not equal to sheets number {len(sheets_name)}.')
            elif sheets_name is None:
                sheets_name = f'{sheet_name_default}1'
            # 单个df直接写入到sheet中
            export_value = dfs.values.tolist()
            export_value.insert(0, list(dfs.columns))
            super(WriteDf2Excel, self).new_sheet(sheets_name, data=export_value)
            del export_value
            gc.collect()
        # 输出
        if isinstance(dfs, list):
            for df, sheet_name in zip(dfs, sheets_name):
                export_value = df.values.tolist()
                export_value.insert(0, list(df.columns))
                super(WriteDf2Excel, self).new_sheet(sheet_name, data=export_value)
            del export_value, df
            gc.collect()


# 1 '''把时间戳转化为时间: 1479264792 to 2016-11-16 10:53:12'''
def TimeStampToTime(timestamp):
    timeStruct = time.localtime(timestamp)
    return time.strftime('%Y-%m-%d %H:%M:%S', timeStruct)


def timeStrToTime(timeStr,format='%Y-%m-%d %H:%M:%S'):
    if not isinstance(timeStr,str):
        raise TypeError('timeStr should be str')
    try:
        return datetime.strptime(timeStr,format)
    except:
        raise ValueError('timeStr can not transform to time.')


# 2、'''获取文件的大小,结果保留两位小数，单位为KB'''
def file_size(filePath):
    # filePath = unicode(filePath,'utf8')
    fsize = os.path.getsize(filePath)
    fsize = fsize / float(1024)
    return round(fsize, 4)


# 3、　　'''获取文件的访问时间'''
def file_access_time(filePath):
    # filePath = unicode(filePath,'utf8')
    t = os.path.getatime(filePath)
    return TimeStampToTime(t)


# 4、　　'''获取文件的创建时间'''
def file_create_time(filePath):
    # filePath = unicode(filePath,'utf8')
    t = os.path.getctime(filePath)
    return TimeStampToTime(t)


# 5、　　'''获取文件的修改时间'''
def file_modify_time(filePath):
    # filePath = unicode(filePath,'utf8')
    t = os.path.getmtime(filePath)
    return TimeStampToTime(t)


# 6.最新的表
def newest_file(files,minSize=1):
    """
    保留最新的修改文件
    Parameters
    ----------
    files :

    Returns
    -------

    """
    if not isinstance(files,(list,set)):
        raise TypeError('Input must list or set.')
    if not files:
        return None
    if len(files) ==1:
        return files[0]
    existFiles = [file for file in files if os.path.exists(file)]
    if not existFiles:
        return
    filesModifyTime = {file: file_modify_time(file) for file in existFiles if file_size(file) > minSize*0.00001}
    if len(filesModifyTime) == 0:
        return
    lastTime = max(filesModifyTime.values())
    lastFile = [file for file, modifyTime in filesModifyTime.items() if modifyTime == lastTime]
    if not lastFile:
        return
    else:
        return lastFile[0]


def file_type(files):
    """
    获取文件类型
    :param files:
    :return: Dict
    """
    reportTypeSignDict = {'bulk': 'cp', 'business': 'br', 'search': 'st',
                          'active': 'ac', 'orders': 'ao'}
    FileTypeDict = {}
    for file in files:
        for signWord, type in reportTypeSignDict.items():
            if signWord in file.lower():
                fileTypePath = FileTypeDict.get(type,[])
                fileTypePath.append(file)
                FileTypeDict[type] = fileTypePath
                break
        # ao表需要重新处理
        if os.path.splitext(os.path.basename(file))[0].isdigit():
            fileTypeAo = FileTypeDict.get('ao', [])
            fileTypeAo.append(file)
            FileTypeDict['ao'] = fileTypeAo
    return FileTypeDict


