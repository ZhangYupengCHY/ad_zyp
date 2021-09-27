import os
import re
from datetime import datetime
import pandas as pd
import numpy as np
import pymysql
import time

from my_toolkit import get_time, query_frequently_table_info, sql_write_read, public_function, process_files,change_rate,monthly_reports

"""
计算月数据
"""


class CalcMonthly(object):
    """
    计算月数据:
        站点的月数据分为三种来源:api请求的br,api请求的cp,销售上传的cp+br
        其中状态原始表状态(状态_原始),计算后状态(状态_计算),最后上传库状态(状态_成型)
    """

    def __init__(self):
        # 上月的年,月
        self.last_year, self.last_month = get_time.last_month()
        self._remoteCpSpDbName = 'amazon_ad_api'

    def calc_monthly(self):
        """
        分为远程的br,远程的cp,以及销售上传的
        :return:
        """
        sellerUploadCp,sellerUploadBr = self.seller_upload_cp_br()
        remoteRequestCp = self.calc_remote_cp()
        remoteRequestBr = self._load_remote_br()
        remoteRequestBrRenameColumns = {'sessions':'Sessions','units_ordered':'Units Ordered','ordered_product_sales':'Ordered Product Sales'}
        remoteRequestBr.rename(columns=remoteRequestBrRenameColumns,inplace=True)
        allCp = pd.concat([sellerUploadCp,remoteRequestCp])
        allBr = pd.concat([sellerUploadBr,remoteRequestBr])
        allCp.drop_duplicates(subset=['account','country'],inplace=True,keep='last')
        allBr.drop_duplicates(subset=['account','country'],inplace=True,keep='last')
        return self.__merge_br_cp(allCp,allBr)

    def _load_remote_br(self):
        """
        加载远程请求的br上月月数据
        :return:
        """
        _monthlyBrInfo = query_frequently_table_info.query_remote_request_monthly_br(
            columns=['station_name', 'year', 'month', 'sessions', 'units_ordered', 'ordered_product_sales'])
        _monthlyBrInfo['account'] = _monthlyBrInfo['station_name'].str[:-3]
        _monthlyBrInfo['country'] = _monthlyBrInfo['station_name'].str[-2:]
        _monthlyBrInfo = public_function.init_df(_monthlyBrInfo, change_columns_type={'int': ['year', 'month']})
        return _monthlyBrInfo[
            (_monthlyBrInfo['year'] == self.last_year) & (_monthlyBrInfo['month'] == self.last_month)]

    def calc_remote_cp(self):
        spInfo, sbInfo = self._load_remote_cp()
        needColumns = ["Sales", "Spend", "Orders", "Clicks", "Impressions", "station"]
        sumColumns = needColumns.copy()
        stationColumn = "station"
        sumColumns.remove(stationColumn)
        spInfo = spInfo[needColumns]
        sbInfo = sbInfo[needColumns]
        cpInfo = pd.concat([spInfo, sbInfo])
        # 将计算项转换成数值的格式
        cpInfo = public_function.init_df(cpInfo, change_columns_type={'int': ['Orders', 'Clicks', 'Impressions'],
                                                                      'float': ['Sales', 'Spend']})
        cpInfoSum = cpInfo.groupby(by=[stationColumn])[sumColumns].sum().reset_index()
        cpInfoSum['account'] = [station[:-3] for station in cpInfoSum['station']]
        cpInfoSum['country'] = [station[-2:] for station in cpInfoSum['station']]
        return cpInfoSum

    def _load_remote_cp(self):
        """
        加载远程请求的cp上月月数据中
        :return:
        """
        spTableName = 'station_monthly_data_sp'
        sbTableName = 'station_monthly_data_sb'
        spInfo = self._load_remote_cp_part(spTableName)
        sbInfo = self._load_remote_cp_part(sbTableName)
        cpRenameColumns = self._cp_rename_columns()
        spInfo.rename(columns=cpRenameColumns, inplace=True)
        sbInfo.rename(columns=cpRenameColumns, inplace=True)
        return [spInfo, sbInfo]

    def _cp_rename_columns(self):
        spRenameColumns = {'attributedSales7d': "Sales",
                           'cost': "Spend",
                           'attributedConversions7d': "Orders",
                           'clicks': "Clicks",
                           'Impressions': "Impressions",
                           'impressions': "Impressions",
                           "ad_station": "station",
                           }
        sbRenameColumns = {'attributedSales14d': "Sales",
                           'cost': "Spend",
                           'attributedConversions14d': "Orders",
                           'clicks': "Clicks",
                           'impressions': "Impressions",
                           "ad_station": "station",
                           }
        cpRenameColumns = spRenameColumns
        cpRenameColumns.update(sbRenameColumns)
        return cpRenameColumns

    def _load_remote_cp_part(self, tableName):
        """
        加载远程请求的cp上月月数据中的sp/br部分
        :return:
        """
        monthlyCpPartSQL = "select * from %s where (`year` = %s) and (`month` = %s)" % (
            tableName, self.last_year, self.last_month)
        _connMysql = sql_write_read.QueryMySQL(database=self._remoteCpSpDbName)
        monthlyCpPartInfo = _connMysql.read_table(tableName, monthlyCpPartSQL)
        _connMysql.close()
        return monthlyCpPartInfo

    def seller_upload_cp_br(self):
        # 计算月数据
        sellerUploadFolder = r'F:\sales_upload_monthly_zipped'
        allStationsName = os.listdir(sellerUploadFolder)
        # 测试某个表
        now_datetime = get_time.now_str()
        df_br_all = []
        df_cp_all = []
        # br表有问题的站点
        # brWrongPath =r"C:\Users\Administrator\Desktop\br wrong.txt"
        for stationName in allStationsName:
            # # # todo 测试某个站点
            # if stationName.lower() != 'hu_fr':
            #     continue
            br_file_sign = 'Business'
            campaign_file_sign = 'Campaign'
            account = stationName[:-3]
            country = stationName[-2:]
            stationsFiles = os.listdir(os.path.join(sellerUploadFolder, stationName))
            brList = [file for file in stationsFiles if br_file_sign in file]
            if brList:
                brFile = os.path.join(os.path.join(sellerUploadFolder, stationName, brList[0]))
            else:
                brFile = None
            cpList = [file for file in stationsFiles if campaign_file_sign in file]
            if cpList:
                cpFile = os.path.join(os.path.join(sellerUploadFolder, stationName, cpList[0]))
            else:
                cpFile = None

            # 计算br报表
            if brFile:
                try:
                    df_br = pd.read_csv(brFile)
                except:
                    df_br = process_files.read_file(brFile)
            else:
                df_br = pd.DataFrame()
            if df_br is None:
                df_br = pd.DataFrame()

            df_br.rename(columns={"日期": "Date",
                                  "已订购商品销售额": "Ordered Product Sales",
                                  "已订购商品的销售额 – B2B": "Ordered Product Sales – B2B",
                                  "已订购商品数量": "Units Ordered",
                                  "订购数量 – B2B": "Units Ordered – B2B",
                                  "订单商品种类数": "Total Order Items",
                                  "订单商品总数 – B2B": "Total Order Items – B2B",
                                  "页面浏览次数": "Page Views",
                                  "买家访问次数": "Sessions",
                                  "购买按钮赢得率": "Buy Box Percentage",
                                  "订单商品数量转化率": "Unit Session Percentage",
                                  "商品转化率 – B2B": "Unit Session Percentage – B2B",
                                  "平均在售商品数量": "Average Offer Count",
                                  "平均父商品数量": "Average Parent Items",
                                  "Units ordered": 'Units Ordered',
                                  'Ordered product sales': 'Ordered Product Sales'}, inplace=True)
            df_br['account'] = account
            df_br['country'] = country
            df_br['updatetime'] = now_datetime
            df_br = df_br.reindex(
                columns=['account', 'country', 'Date', 'Ordered Product Sales', 'Units Ordered', 'Buy Box Percentage',
                         'Sessions',
                         'updatetime'])
            df_br_all.append(df_br)

            # 计算cp报表
            # 计算br报表
            if cpFile:
                try:
                    df_cp = pd.read_csv(cpFile)
                except:
                    df_cp = process_files.read_file(cpFile)
            else:
                df_cp = pd.DataFrame()
            if df_cp is None:
                df_cp = pd.DataFrame()
            df_cp['account'] = account
            df_cp['country'] = country
            # campaign 表头标准化
            df_cp.rename(columns=lambda x: re.sub('\\(.*?\\)|\\{.*?}|\\[.*?]', '', x), inplace=True)
            df_cp.rename(columns={'状态': 'State', '广告活动': 'Campaigns', '状态.1': 'Type', '类型': 'Status', '投放': 'Targeting',
                                  '广告活动的竞价策略': 'Campaign bidding strategy', '开始日期': 'Start date', '结束日期': 'End date',
                                  '广告组合': 'Portfolio',
                                  '预算': 'Budget', '曝光量': 'Impressions', '点击次数': 'Clicks', '点击率 ': 'CTR', '花费': 'Spend',
                                  '每次点击费用 ': 'CPC', '订单': 'Orders', '销售额': 'Sales', '广告投入产出比 ': 'ACoS', 'ACOS': 'ACoS'},
                         inplace=True)
            df_cp['updatetime'] = now_datetime
            df_cp = df_cp.reindex(
                columns=['account', 'country', 'State', 'Campaigns', 'Status', 'Type', 'Targeting', 'Start date',
                         'End date', 'Budget',
                         'Impressions', 'Clicks', 'CTR', 'Spend', 'CPC', 'Orders', 'Sales', 'ACoS', 'updatetime'])
            df_cp_all.append(df_cp)

            # business report ,月份/年份/货币金额---------------------------------------------------------------------------------

        def str2month(df):
            if len(re.findall('[0-9]+', df['Date'])[0]) == 4:
                return re.findall('[0-9]+', df['Date'])[1]
            else:
                month = [month  for month in re.findall('[0-9]+', df['Date']) if (int(month) != 1) and (int(month) < 13)]
                if month:
                    return month[0]
                else:
                    return 1
                # if df['country'] in ['US', 'CA', 'JP', 'MX', 'AE']:
                #     return re.findall('[0-9]+', df['Date'])[0]
                # else:
                #     return re.findall('[0-9]+', df['Date'])[1]

        def str2year(df):
            if len(re.findall('[0-9]+', df['Date'])[0]) == 4:
                return re.findall('[0-9]+', df['Date'])[0]
            else:
                return re.findall('[0-9]+', df['Date'])[2]

        df_br_all = pd.concat(df_br_all)

        df_br_all.reset_index(drop=True, inplace=True)
        # 查看是哪个站点有问题
        for account, country, month in zip(df_br_all['account'], df_br_all['country'], df_br_all['Date']):
            try:
                re.findall('[0-9]+', month)
            except:
                # print(f'{account, country}有问题')
                pass
        df_br_all = df_br_all[~pd.isna(df_br_all['Date'])]
        df_br_all['month'] = df_br_all.apply(lambda x: str2month(x), axis=1)
        df_br_all['year'] = df_br_all.apply(lambda x: str2year(x), axis=1)
        df_br_all['month'] = df_br_all['month'].astype('int')
        df_br_all['year'] = df_br_all['year'].astype('int')
        df_br_all['Ordered Product Sales'] = df_br_all['Ordered Product Sales'].str.extract('(\d+,?\d*.\d+)')
        for col in ['Ordered Product Sales', 'Units Ordered', 'Sessions']:
            df_br_all[col] = df_br_all[col].astype('str')
            df_br_all[col] = df_br_all[col].str.replace(',', '').astype('float')
        # 只取当月的br月数据
        df_br_month = df_br_all[(df_br_all['month'] == self.last_month) & ((df_br_all['year'] == self.last_year) | (df_br_all['year'] == int(str(self.last_year)[-2:])))]

        # campaign, 直接汇总生成--------------------------------------------------------------------------------------------
        def money2num(num):
            num = num.rstrip()
            if any(i in num for i in [',', '.']):  # 原数据中含有,.等符号
                res = ''
                for ii in filter(str.isdigit, num):
                    res += ii
                if num[-3].isdigit():
                    return float(res) / 10
                else:
                    return float(res) / 100
            else:
                return float(num + '00') / 100

        def amount2num(num):
            res = ''
            for ii in filter(str.isdigit, num.split('.')[0]):
                res += ii
            return int(res)

        df_cp_all = pd.concat(df_cp_all)
        df_cp_all.dropna(subset=['Spend'], inplace=True)
        for col in ['Spend', 'Sales']:
            df_cp_all[col] = df_cp_all[col].astype('str')
            df_cp_all[col] = df_cp_all[col].apply(lambda x: money2num(x))

        for col in ['Clicks', 'Orders']:
            df_cp_all[col] = df_cp_all[col].astype('str')
            df_cp_all[col] = df_cp_all[col].apply(lambda x: amount2num(x))
        df_cp_all = df_cp_all.groupby(['account', 'country'])['Clicks', 'Spend', 'Orders', 'Sales'].sum().reset_index()
        return df_cp_all, df_br_month


    def __merge_br_cp(self,df_cp_all,df_br_month):
        df_cp_all['ACoS'] = df_cp_all['Spend'] / df_cp_all['Sales']
        df_cp_all['CPC'] = df_cp_all['Spend'] / df_cp_all['Clicks']
        df_cp_all['CR'] = df_cp_all['Orders'] / df_cp_all['Clicks']
        for col in ['ACoS','CPC','CR']:
            df_cp_all[col] = df_cp_all[col].replace([np.inf, -np.inf,np.nan], 0)

        # 广告报表中添加时间两列
        df_cp_all['data_month'] = int(self.last_month)
        df_cp_all['data_year'] = int(self.last_year)

        # 将汇总后的Br表和广告报表上传到广告组服务器中
        # 初始化连接数据库
        df_merge = df_br_month.merge(df_cp_all, on=['account', 'country'], how='outer')
        df_merge['account'] = df_merge['account'].str.upper()
        df_merge['country'] = +df_merge['country'].str.upper()
        df_merge['month'] = str(self.last_month) + '月'
        df_merge['year'] = str(self.last_year) + '年'
        df_merge['account-country'] = df_merge['account']+'_'+df_merge['country']

        # 显示br或是cp不全的站点
        df_lost = df_merge[(pd.isna(df_merge['Sales'])) | (pd.isna(df_merge['Ordered Product Sales']))]
        df_lost_info = df_lost[['account-country','Sales','Ordered Product Sales']]
        # lostPath = r"C:\Users\Administrator\Desktop\monthly Lost.xlsx"
        # df_lost_info.to_excel(lostPath,index=False)
        # 筛选掉br或是cp表不全的站点
        df_merge=df_merge[(~pd.isna(df_merge['Sales'])) & (~pd.isna(df_merge['Ordered Product Sales']))]
        #
        df_merge = public_function.init_df(df_merge,change_columns_type={'float':['Spend','Ordered Product Sales'],'int':['Clicks','Sessions','Orders','Units Ordered']})

        df_merge['spend/sales'] = df_merge['Spend'] / df_merge['Ordered Product Sales']
        df_merge['Clicks/Sessions'] = df_merge['Clicks'] / df_merge['Sessions']
        df_merge['order percentage'] = df_merge['Orders'] / df_merge['Units Ordered']
        df_merge['session percentage'] = df_merge['Units Ordered'] / df_merge['Sessions']
        for col in ['spend/sales','Clicks/Sessions','order percentage','session percentage']:
            df_merge[col] = df_merge[col].replace([np.inf, -np.inf,np.nan], 0)
        df_merge = df_merge.reindex(
            columns=['account', 'country', 'account-country', 'year', 'month', 'Spend', 'ACoS', 'Sales',
                     'Ordered Product Sales', 'spend/sales', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
                     'Clicks', 'Sessions', 'Clicks/Sessions', 'Orders', 'Units Ordered',
                     'order percentage', 'CR', 'session percentage', 'Buy Box Percentage'])
        # 筛选掉广告花费为零的站点
        df_merge = df_merge[df_merge['Spend'] != 0]
        return df_merge



def statistic_user_month_data(user_month_data):
    now_user_data = user_month_data.copy()
    now_year = list(now_user_data['year'].str.strip('年'))[0]
    del now_user_data['year']
    # 获取文件内的月份数据
    now_month_str = list(now_user_data['month'].str.strip('月'))[0]
    if re.search('[0-9]', now_month_str):
        file_month = re.search('[0-9]+', now_month_str.strip()).group()
        now_user_data['month'] = file_month
    else:
        file_month = "未搜索到月份数据"
        print("未搜索到月份数据")
        return
    # 检查当前文件的月份是否正确
    # if now_month - int(file_month) > 2:
    #     return

    # 添加需要缺少的辅助列
    now_user_data.insert(3, '年月', now_year)

    # 匹配汇率数据，将对应的本币换算成美金
    # change_current = {'US': 1, 'CA': 0.7459, 'UK': 1.3015, 'DE': 1.1241,  'IT': 1.1241,
    #                   'ES': 1.1241,  'JP': 0.008997, 'FR': 1.1241, 'MX': 0.0513,
    #                   'IN': 0.01418, 'AU': 0.7080270290}
    change_current = change_rate.change_current()
    # now_user_data['Spend'] = now_user_data.apply(
    #     lambda m_data: m_data['Spend']*change_current[m_data['country']], axis=1)
    # now_user_data['Sales'] = now_user_data.apply(
    #     lambda m_data: m_data['Sales']*change_current[m_data['country']], axis=1)
    now_user_data = now_user_data[now_user_data['country'].isin(change_current.keys())]
    now_user_data['Spend'] = [spend * change_current[country.upper()] for spend, country in
                                      zip(now_user_data['Spend'], now_user_data['country'])]
    now_user_data['Sales'] = [sales * change_current[country.upper()] for sales, country in
                                      zip(now_user_data['Sales'], now_user_data['country'])]
    # print(now_user_data)
    # now_user_data['Ordered Product Sales'] = now_user_data.apply(
    #     lambda m_data: m_data['Ordered Product Sales']*change_current[m_data['country']], axis=1)
    now_user_data['Ordered Product Sales'] = [sales * change_current[country.upper()] for sales, country in
                                      zip(now_user_data['Ordered Product Sales'], now_user_data['country'])]

    # 修改对应列的名字
    now_user_data.rename(columns={'account': '账号', 'country': '站点',
                                  'account-country': '账号站点', 'month': '月份',
                                  'Spend': '广告花费', 'ACoS': 'acos',
                                  'Sales': '广告带来的销售额', 'Ordered Product Sales': '站点总销售额',
                                  'spend/sales': '广告花费占总销售额的比例',
                                  'Clicks': '广告Click', 'Sessions': '店铺session',
                                  'Clicks/Sessions': '广告流量占店铺总流量比例', 'Orders': '广告订单量',
                                  'Units Ordered': '店铺订单量',
                                  'order percentage': '广告订单占店铺总订单比例',
                                  'CR': '广告转化率',
                                  'session percentage': '店铺转化率'}, inplace=True)
    # now_user_data['账号站点'] = now_user_data['账号站点'].str.replace('-','_').replace(' ','_')
    # 取出上月对应站点的销售数据，包括：站点实际毛利，广告利润，广告销售额。匹配到当前表中
    last_month = int(file_month) - 1
    search_year = now_year
    if last_month == 0:
        last_month = 12
        search_year = str(int(now_year) - 1)
    last_month_sql = "SELECT 账号站点,acos,站点实际毛利率,广告花费,站点实际毛利,广告花费占总销售额的比例," \
                     "广告直接带来的销售额占站点总销售额的占比,广告直接带来的利润,广告带来的销售额 FROM station_statistic " \
                     "WHERE 年月='%s' and 月份='%s'" % (search_year, last_month)
    last_month_data = sql_write_read.read_table(last_month_sql)
    last_month_data.rename(columns={'广告花费': '上月广告花费',
                                    'acos': '上月acos',
                                    '广告花费占总销售额的比例': '上月花费比',
                                    '广告直接带来的销售额占站点总销售额的占比': '上月广销比',
                                    '站点实际毛利率': '上月站点实际毛利率',
                                    '站点实际毛利': '上月站点实际毛利',
                                    '广告直接带来的利润': '上月广告直接带来的利润',
                                    '广告带来的销售额': '上月广告带来的销售额'}, inplace=True)
    if not last_month_data.empty:
        last_month_data.fillna(0, inplace=True)
        last_month_data['上月acos'] = last_month_data['上月acos'].apply(
            lambda m_data: float(str(m_data).strip('%'))/100 if re.search('%', str(m_data)) else 0)
        last_month_data['上月花费比'] = last_month_data['上月花费比'].apply(
            lambda m_data: float(str(m_data).strip('%'))/100 if re.search('%', str(m_data)) else 0)
        last_month_data['上月广销比'] = last_month_data['上月广销比'].apply(
            lambda m_data: float(str(m_data).strip('%'))/100 if re.search('%', str(m_data)) else 0)
        last_month_data['上月广告花费'] = last_month_data['上月广告花费'].apply(str_to_num)
        last_month_data['上月站点实际毛利'] = last_month_data['上月站点实际毛利'].apply(str_to_num)
        last_month_data['上月广告直接带来的利润'] = last_month_data['上月广告直接带来的利润'].apply(str_to_num)
        last_month_data['上月广告带来的销售额'] = last_month_data['上月广告带来的销售额'].apply(str_to_num)
        now_user_data = pd.merge(now_user_data, last_month_data, on='账号站点', how='left')
    else:
        now_user_data['上月acos'] = 0
        now_user_data['上月站点实际毛利率'] = '0%'
        now_user_data['上月花费比'] = 0
        now_user_data['上月广销比'] = 0
        now_user_data['上月广告花费'] = 0
        now_user_data['上月站点实际毛利'] = 0
        now_user_data['上月广告直接带来的利润'] = 0
        now_user_data['上月广告带来的销售额'] = 0

    # 取出上上个月的毛利率
    last_last_month = int(file_month) - 2
    search_year_2 = now_year
    if last_last_month == 0:
        last_last_month = 12
        search_year_2 = str(int(now_year) - 1)
    elif last_last_month == -1:
        last_last_month = 11
        search_year_2 = str(int(now_year) - 1)
    last_last_month_sql = "SELECT 账号站点,站点实际毛利率 FROM station_statistic " \
                          "WHERE 年月='%s' and 月份='%d'" % (search_year_2, last_last_month)
    last_last_month_data = sql_write_read.read_table(last_last_month_sql)
    last_last_month_data.rename(columns={'站点实际毛利率': '上上月站点实际毛利率'}, inplace=True)
    if not last_last_month_data.empty:
        last_last_month_data['上上月站点实际毛利率'] = \
            last_last_month_data['上上月站点实际毛利率'].apply(
            lambda m_data: float(str(m_data).strip('%'))/100 if re.search('%', str(m_data)) else 0)
        now_user_data = pd.merge(now_user_data, last_last_month_data, on='账号站点', how='left')
        now_user_data['上上月站点实际毛利率'].fillna(0, inplace=True)
    else:
        now_user_data['上上月站点实际毛利率'] = 0

    # 修改空白列的列名
    now_user_data.rename(columns={'A': '站点实际毛利率', 'B': '非广告部分站点毛利率',
                                  'C': '站点实际毛利', 'D': '不开广告的利润',
                                  'E': '广告直接带来的利润',
                                  'F': '广告直接带来的利润占站点总利润的占比',
                                  'G': '广告直接带来的销售额占站点总销售额的占比',
                                  'H': '实际总毛利对比'}, inplace=True)
    # 填充NaN值
    now_user_data['上月站点实际毛利率'].fillna('0%', inplace=True)
    now_user_data.fillna(0, inplace=True)

    # 计算缺少值的列
    now_user_data['站点实际毛利率'] = ((now_user_data['上月站点实际毛利率'].str.strip()).str.replace('%', '')).astype(float)/100
    now_user_data['非广告部分站点毛利率'] = now_user_data['站点实际毛利率'] + now_user_data['广告花费占总销售额的比例']
    now_user_data['站点实际毛利'] = ((now_user_data['站点总销售额'] * now_user_data['站点实际毛利率']).round()).astype(int)
    # print(now_user_data)
    # print(now_user_data['站点总销售额'].head(5))
    # print(now_user_data['广告带来的销售额'].head(10))
    # print(now_user_data['非广告部分站点毛利率'].head(15))
    for col in ['站点总销售额','广告带来的销售额','非广告部分站点毛利率']:
        now_user_data[col].replace([np.inf, -np.inf], 0, inplace=True)
        now_user_data[col].fillna(0,inplace=True)
    now_user_data['不开广告的利润'] = \
        (((now_user_data['站点总销售额'] - now_user_data['广告带来的销售额']) * now_user_data['非广告部分站点毛利率']).round()).astype(int)
    now_user_data['广告直接带来的利润'] = \
        ((now_user_data['站点实际毛利'] - now_user_data['不开广告的利润']).round()).astype(int)
    now_user_data['广告直接带来的利润占站点总利润的占比'] = now_user_data.apply(
        lambda m_data: float(m_data['广告直接带来的利润']) / m_data['站点实际毛利'] if m_data['站点实际毛利'] else 0, axis=1)
    now_user_data['广告直接带来的销售额占站点总销售额的占比'] = now_user_data.apply(
        lambda m_data: float(m_data['广告带来的销售额']) / m_data['站点总销售额'] if m_data['站点总销售额'] else 0, axis=1)
    now_user_data['实际总毛利对比'] = now_user_data.loc[:, '广告直接带来的利润'].apply(
        lambda m_data: '正' if m_data > 0 else '负')

    # 匹配小组负责人，大部负责人，广告接手人，接手日期
    ad_manger_sql = "SELECT station,ad_manger,accept_time FROM only_station_info"
    near_accept_info = sql_write_read.read_table(ad_manger_sql)
    near_accept_info['小组负责人'] = ""
    near_accept_info['大部负责人'] = ""
    near_accept_info['station'] = [public_function.standardize_station(station,case='upper') for station in near_accept_info['station']]
    near_accept_info.rename(columns={'station': '账号站点', 'ad_manger': '广告接手人',
                                     'accept_time': '接手日期'}, inplace=True)
    old_accept_sql = "SELECT 账号站点,小组负责人,大部负责人,广告接手人,接手日期 FROM station_statistic " \
                     "WHERE 年月='%s' and 月份='%d'" % (now_year, int(file_month)-1)
    old_accept_info = sql_write_read.read_table(old_accept_sql)
    total_ad_station = near_accept_info.append(old_accept_info, ignore_index=True, sort=False)
    total_ad_station.drop_duplicates(subset='账号站点', keep='first', inplace=True)
    total_ad_station['接手日期'] = total_ad_station['接手日期'].astype(str)
    now_user_data['账号站点'] = now_user_data['账号站点'].str.upper()
    total_ad_station['账号站点'] = [public_function.standardize_station(account,case='upper') for account in total_ad_station['账号站点']]
    now_user_data = pd.merge(now_user_data, total_ad_station, on='账号站点', how='left')
    now_user_data.fillna('', inplace=True)
    now_user_data['接手日期'] = now_user_data['接手日期'].apply(lambda m_data: m_data if m_data else '0000-00-00')
    now_user_data['接手日期'] = now_user_data['接手日期'].apply(lambda m_data: '0000-00-00' if m_data == 'nan' else m_data)
    a = {station:ad_manager for station,ad_manager in zip(now_user_data['账号站点'],now_user_data['广告接手人'])}


    # 添加以下列：（整个表单共49列）
    # 站点利润gap，广告利润gap，广告销售额gap，广告销售额增长，
    # 是否有效（ACoS<原始毛利率），广告组，模式，初始月，单站点是否达标，
    # 站点毛利率变化，广告ACoS变化，广销比变化，花费占比变化，广告客单价，
    # 店铺客单价，客单价倍数（广告/站点），转化率倍数（广告/站点），是否计算在内，占比是否达标
    now_user_data['站点利润gap'] = now_user_data['站点实际毛利'] - now_user_data['上月站点实际毛利']
    now_user_data['广告利润gap'] = now_user_data['广告直接带来的利润'] - now_user_data['上月广告直接带来的利润']
    now_user_data['广告销售额gap'] = now_user_data['广告带来的销售额'] - now_user_data['上月广告带来的销售额']
    now_user_data['广告销售额增长'] = ""
    now_user_data['是否有效(ACOS<原始毛利率)'] = now_user_data.apply(
        lambda m_data: '是' if m_data['acos'] <= m_data['站点实际毛利率'] else '否', axis=1)
    now_user_data['广告组'] = ""
    now_user_data['模式'] = ""
    now_user_data['接手日期'] = now_user_data['接手日期'].apply(lambda x:x.replace('_','-'))
    # for station ,date_time in zip(now_user_data['账号站点'],now_user_data['接手日期']):
    #     print(station,date_time,type(date_time))
    now_user_data['初始月'] = now_user_data['接手日期'].apply(generate_init_month)  # 20号及以前算上个月的，20号以后算这个月的
    now_user_data['单站点达标'] = ""
    now_user_data['站点毛利率变化'] = now_user_data['站点实际毛利率'] - now_user_data['上上月站点实际毛利率']
    now_user_data['广告acos变化'] = now_user_data['acos'] - now_user_data['上月acos']
    now_user_data['广销比变化'] = now_user_data['广告直接带来的销售额占站点总销售额的占比'] - now_user_data['上月广销比']
    now_user_data['花费占比变化'] = now_user_data['广告花费占总销售额的比例'] - now_user_data['上月花费比']
    now_user_data['广告客单价'] = now_user_data.apply(
        lambda m_data: round(float(m_data['广告带来的销售额']) / m_data['广告订单量'], 1) if m_data['广告订单量'] else 0, axis=1)
    now_user_data['店铺客单价'] = now_user_data.apply(
        lambda m_data: round(float(m_data['站点总销售额']) / m_data['店铺订单量'], 1) if m_data['店铺订单量'] else 0, axis=1)
    now_user_data['客单价倍数'] = (now_user_data['广告客单价'] / now_user_data['店铺客单价']).round(2)
    now_user_data['转化率倍数'] = (now_user_data['广告转化率'] / now_user_data['店铺转化率']).round(2)
    now_user_data['月两位数'] = now_user_data['月份'].apply(
        lambda m_data: '0'+str(m_data) if len(str(int(m_data))) == 1 else str(m_data))
    now_user_data['是否计算在内'] = now_user_data.apply(
        lambda m_data: '算' if int(m_data['年月'] + str(m_data['月两位数'])) > int(m_data['初始月'].replace('-', '').replace('/',''))
        and m_data['初始月'] != '0000-00' else '不算', axis=1)
    del now_user_data['月两位数']
    now_user_data['占比是否达标'] = ""

    # 将对应的列变成百分数
    now_user_data['acos'] = now_user_data.loc[:, 'acos'].apply(lambda m_data: '{:.1%}'.format(m_data))
    now_user_data['广告花费占总销售额的比例'] = now_user_data.loc[:, '广告花费占总销售额的比例'].apply(
        lambda m_data: '{:.1%}'.format(m_data))
    now_user_data['广告流量占店铺总流量比例'] = now_user_data.loc[:, '广告流量占店铺总流量比例'].apply(
        lambda m_data: '{:.1%}'.format(m_data))
    now_user_data['广告订单占店铺总订单比例'] = now_user_data.loc[:, '广告订单占店铺总订单比例'].apply(
        lambda m_data: '{:.1%}'.format(m_data))
    now_user_data['广告转化率'] = now_user_data.loc[:, '广告转化率'].apply(lambda m_data: '{:.1%}'.format(m_data))
    now_user_data['店铺转化率'] = now_user_data.loc[:, '店铺转化率'].apply(lambda m_data: '{:.1%}'.format(m_data))
    now_user_data['站点实际毛利率'] = now_user_data['站点实际毛利率'].apply(lambda m_data: '{:.1%}'.format(m_data))
    now_user_data['非广告部分站点毛利率'] = now_user_data['非广告部分站点毛利率'].apply(lambda m_data: '{:.1%}'.format(m_data))
    now_user_data['广告直接带来的利润占站点总利润的占比'] = now_user_data['广告直接带来的利润占站点总利润的占比'].apply(
        lambda m_data: '{:.1%}'.format(m_data))
    now_user_data['广告直接带来的销售额占站点总销售额的占比'] = now_user_data['广告直接带来的销售额占站点总销售额的占比'].apply(
        lambda m_data: '{:.1%}'.format(m_data))

    now_user_data['站点毛利率变化'] = now_user_data['站点毛利率变化'].apply(
        lambda m_data: '{:.1%}'.format(m_data))
    now_user_data['广告acos变化'] = now_user_data['广告acos变化'].apply(
        lambda m_data: '{:.1%}'.format(m_data))
    now_user_data['广销比变化'] = now_user_data['广销比变化'].apply(
        lambda m_data: '{:.1%}'.format(m_data))
    now_user_data['花费占比变化'] = now_user_data['花费占比变化'].apply(
        lambda m_data: '{:.1%}'.format(m_data))

    # 将相应的数据进行圆整
    now_user_data['广告花费'] = now_user_data['广告花费'].apply(lambda m_data: int(round(m_data)))
    now_user_data['广告带来的销售额'] = now_user_data['广告带来的销售额'].apply(lambda m_data: int(round(m_data)))
    now_user_data['站点总销售额'] = now_user_data['站点总销售额'].apply(lambda m_data: int(round(m_data)))

    # 只取需要的列
    now_user_data = now_user_data[['账号', '站点', '账号站点', '年月', '月份', '广告花费', 'acos', '广告带来的销售额',
                                   '站点总销售额', '广告花费占总销售额的比例',
                                   '站点实际毛利率', '非广告部分站点毛利率', '站点实际毛利', '不开广告的利润',
                                   '广告直接带来的利润', '广告直接带来的利润占站点总利润的占比',
                                   '广告直接带来的销售额占站点总销售额的占比', '实际总毛利对比', '广告Click', '店铺session',
                                   '广告流量占店铺总流量比例', '广告订单量', '店铺订单量', '广告订单占店铺总订单比例',
                                   '广告转化率', '店铺转化率', '小组负责人', '大部负责人', '广告接手人', '接手日期',
                                   '站点利润gap', '广告利润gap', '广告销售额gap', '广告销售额增长',
                                   '是否有效(ACOS<原始毛利率)', '广告组', '模式', '初始月', '单站点达标', '站点毛利率变化',
                                   '广告acos变化', '广销比变化', '花费占比变化', '广告客单价', '店铺客单价', '客单价倍数',
                                   '转化率倍数', '是否计算在内', '占比是否达标']]

    # 输出测试文件
    # now_user_data.to_excel('123.xlsx')
    now_user_data.reset_index(drop=True, inplace=True)

    # 将数据回传到数据库，若存在则为更新，若不存在则插入
    db = pymysql.connect("127.0.0.1", "marmot", "marmot123", "team_station", charset='utf8')
    # 计算df的列数，总列数为49列
    now_user_data.drop_duplicates(subset='账号站点', keep='first', inplace=True)
    if len(now_user_data.columns) == 49:
        all_row_data = [list(now_user_data.loc[one_index]) for one_index in now_user_data.index]
        for one_row in all_row_data:
            # 获取当前更新月的是否存在于数据库中
            now_month_sql = "SELECT 账号站点,年月,月份 FROM station_statistic " \
                            "WHERE 账号站点='%s' and 年月='%s' and 月份='%d'" % (one_row[2], now_year, int(file_month))
            row_search = search(db, now_month_sql)
            if row_search:
                update_content = "UPDATE station_statistic SET 广告花费='%s',acos='%s',广告带来的销售额='%s'," \
                                 "站点总销售额='%s',广告花费占总销售额的比例='%s',站点实际毛利率='%s'," \
                                 "非广告部分站点毛利率='%s',站点实际毛利='%s',不开广告的利润='%s'" \
                                 ",广告直接带来的利润='%s',广告直接带来的利润占站点总利润的占比='%s'," \
                                 "广告直接带来的销售额占站点总销售额的占比='%s',实际总毛利对比='%s',广告Click='%s'" \
                                 ",店铺session='%s',广告流量占店铺总流量比例='%s',广告订单量='%s',店铺订单量='%s'," \
                                 "广告订单占店铺总订单比例='%s',广告转化率='%s'" \
                                 ",店铺转化率='%s',小组负责人='%s',大部负责人='%s',广告接手人='%s',接手日期='%s'," \
                                 "站点利润gap='%s',广告利润gap='%s',广告销售额gap='%s'" \
                                 ",广告销售额增长='%s',是否有效='%s',广告组='%s',模式='%s',初始月='%s',单站点达标='%s'," \
                                 "站点毛利率变化='%s',广告acos变化='%s',广销比变化='%s',花费占比变化='%s'," \
                                 "广告客单价='%s',店铺客单价='%s'" \
                                 ",客单价倍数='%s',转化率倍数='%s',是否计算在内='%s',占比是否达标='%s'" \
                                 " WHERE 账号站点='%s' and 年月='%s' and 月份='%d'" % (
                                    one_row[5], one_row[6], one_row[7], one_row[8], one_row[9], one_row[10],
                                    one_row[11], one_row[12], one_row[13],
                                    one_row[14], one_row[15], one_row[16], one_row[17], one_row[18], one_row[19],
                                    one_row[20], one_row[21], one_row[22], one_row[23], one_row[24],
                                    one_row[25], one_row[26], one_row[27], one_row[28], one_row[29],
                                    one_row[30], one_row[31], one_row[32], one_row[33], one_row[34],
                                    one_row[35], one_row[36], one_row[37], one_row[38], one_row[39],
                                    one_row[40], one_row[41], one_row[42], one_row[43], one_row[44], one_row[45],
                                    one_row[46], (one_row[47]).strip(), one_row[48],
                                    one_row[2], one_row[3], int(file_month))
                commit_sql(db, update_content)
            else:
                insert_content = "INSERT INTO station_statistic(账号,站点,账号站点,年月,月份,广告花费,acos,广告带来的销售额,站点总销售额," \
                                 "广告花费占总销售额的比例,站点实际毛利率,非广告部分站点毛利率,站点实际毛利,不开广告的利润" \
                                 ",广告直接带来的利润,广告直接带来的利润占站点总利润的占比,广告直接带来的销售额占站点总销售额的占比,实际总毛利对比,广告Click" \
                                 ",店铺session,广告流量占店铺总流量比例,广告订单量,店铺订单量,广告订单占店铺总订单比例,广告转化率" \
                                 ",店铺转化率,小组负责人,大部负责人,广告接手人,接手日期,站点利润gap,广告利润gap,广告销售额gap,广告销售额增长,是否有效," \
                                 "广告组,模式,初始月,单站点达标,站点毛利率变化,广告acos变化,广销比变化,花费占比变化,广告客单价,店铺客单价" \
                                 ",客单价倍数,转化率倍数,是否计算在内,占比是否达标)VALUES(" \
                                 "'%s', '%s', '%s','%s', '%s', '%s','%s', '%s', '%s', '%s','%s', '%s', '%s','%s'," \
                                 " '%s', '%s','%s', '%s', '%s', '%s','%s', '%s', '%s','%s', '%s', '%s','%s', '%s'," \
                                 " '%s', '%s', '%s', '%s', '%s','%s', '%s', '%s','%s', '%s', '%s', '%s','%s', '%s'," \
                                 " '%s', '%s','%s', '%s', '%s','%s', '%s')" % (
                                    one_row[0], one_row[1], one_row[2], one_row[3], one_row[4],
                                    one_row[5], one_row[6], one_row[7], one_row[8], one_row[9],
                                    one_row[10], one_row[11], one_row[12], one_row[13], one_row[14],
                                    one_row[15], one_row[16], one_row[17], one_row[18], one_row[19],
                                    one_row[20], one_row[21], one_row[22], one_row[23], one_row[24],
                                    one_row[25], one_row[26], one_row[27], one_row[28], one_row[29],
                                    one_row[30], one_row[31], one_row[32],
                                    one_row[33], one_row[34],  one_row[35], one_row[36], one_row[37],
                                    one_row[38], one_row[39], one_row[40], one_row[41], one_row[42],
                                    one_row[43], one_row[44], one_row[45], one_row[46], (one_row[47]).strip(),
                                    one_row[48])
                commit_sql(db, insert_content)
        # 关闭数据库连接
        db.close()
    else:
        print('列的数量不匹配')

def str_to_num(m_data):
    try:
        return float(m_data)
    except:
        return 0

def generate_init_month(m_data):
    if m_data is None:
        return '0000-00'
    if m_data == 'None':
        return '0000-00'
    now_data = str(m_data)
    if now_data == "0000-00-00":
        return "0000-00"
    if '-' in now_data:
        day_list = str(now_data).split('-')
    elif '/' in now_data:
        day_list = str(now_data).split('/')
    # print(now_data)
    if int(day_list[2]) > 20:
        return str(now_data)[0:7]
    else:
        month = str(int(day_list[1]) - 1)
        if month == "0":
            year = str(int(day_list[0])-1)
            month = '12'
            return year + "-" + month
        elif len(month) == 1:
            return now_data[0:5] + '0' + month
        else:
            return now_data[0:5] + month

# 数据搜索
def search(db, sql):
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        return cursor.fetchall()
    except:
        print("Error: unable to fecth data")
    finally:
        cursor.close()
    # 关闭数据库连接
    # db.close()

# 数据插入
def commit_sql(db, sql):
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    try:
        # 执行sql语句
        cursor.execute(sql)
        # 提交到数据库执行
        db.commit()
    except:
        # Rollback in case there is any error
        db.rollback()
        print("写入失败")
    finally:
        cursor.close()

if __name__ == '__main__':
    # 更新当前的月数据上传情况
    while 1:
        monthly_reports.SellerMonthlyFile.all_not_upload_report_info()
        a = CalcMonthly()
        monthly = a.calc_monthly()
        # 规范并上传站点月数据
        statistic_user_month_data(monthly)
        print(f"{datetime.now()}:完成月数据上传更新")
        print("休息两个小时")
        time.sleep(2*3600)
    # exportPath = r"C:\Users\Administrator\Desktop\monthly.xlsx"
    # monthly.to_excel(exportPath,index=False)
