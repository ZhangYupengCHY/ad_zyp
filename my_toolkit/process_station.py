"""
处理站点有关方法
"""
import pandas as pd


from my_toolkit import query_frequently_table_info, sql_write_read, public_function, chinese_check

stationUnknown = '未知'


def standardStation(stationName, case='lower'):
    """规范站点的命名"""
    if (stationName is None) or (not isinstance(stationName, str) or (len(stationName.strip()) == 0)):
        return ''
    if case not in ['lower', 'upper']:
        raise ValueError('case must lower or upper')
    if case == 'lower':
        return stationName.strip().replace('-', '_').replace(' ', '_').lower()
    else:
        return stationName.strip().replace('-', '_').replace(' ', '_').upper()


class Station(object):

    def __init__(self, stationName):
        if not isinstance(stationName, (str, list, set)):
            raise TypeError(f'station type must str list or set.')
        if isinstance(stationName, str):
            self.stationName = standardStation(stationName)
        if isinstance(stationName, (list, set)):
            self.stationName = [standardStation(station) for station in stationName]

    def companyNSellerBelong(self) -> list or dict:
        """获取站点的公司与销售花名"""
        # 首先判断账号是不是在易佰的有效账号中,
        # 然后再去其他公司的账号中去判断
        yibaiAmazonStatus = query_frequently_table_info.query_yibai_amazon_account_status(
            columns=['account_name', 'shortName', 'status'])
        # yibaiAmazonValidStation = yibaiAmazonStatus[yibaiAmazonStatus['status'] == '1']
        yibaiAmazonValidStation = yibaiAmazonStatus
        yibaiCompanyName = '易佰'
        unknown = stationUnknown
        _connMysql = sql_write_read.QueryMySQL()
        # 加载易佰的负责人列表
        yibaiStationSellerTableName = 'erp_seller_account'
        yibaiStationSeller = _connMysql.read_table(yibaiStationSellerTableName, columns=['name', 'short_name'])
        # 保留账号最后销售信息
        yibaiStationSeller.drop_duplicates(subset=['short_name'], keep='last', inplace=True)
        yibaiAmazonValidStationSeller = pd.merge(yibaiAmazonValidStation, yibaiStationSeller, how='left',
                                                 left_on='shortName', right_on='short_name')
        yibaiAmazonValidStationSeller = yibaiAmazonValidStationSeller[['account_name', 'shortName', 'name']]
        yibaiAmazonValidStationSeller.drop_duplicates(subset=['account_name'], keep='last', inplace=True)
        yibaiAmazonValidStationSeller.set_index('account_name', inplace=True)
        yibaiAmazonValidStationSeller['name'].fillna(value=unknown, inplace=True)
        otherCompanyStationTableName = 'other_company_belong'
        otherCompanyStationSeller = _connMysql.read_table(otherCompanyStationTableName,
                                                          columns=['department', 'station', 'nickname'])
        otherCompanyStationSeller['station'] = [standardStation(station) for station in
                                                otherCompanyStationSeller['station']]
        otherCompanyStationSeller.drop_duplicates(subset=['station'], keep='last', inplace=True)
        otherCompanyStationSeller.set_index('station', inplace=True)
        _connMysql.close()
        # 首先判断单个站点
        if isinstance(self.stationName, str):
            if self.stationName in yibaiAmazonValidStationSeller.index:
                stationSeller = \
                    yibaiAmazonValidStationSeller['name'][
                        yibaiAmazonValidStationSeller.index == self.stationName].values[0]
                return [yibaiCompanyName, stationSeller]
            elif self.stationName in otherCompanyStationSeller.index:
                otherCompanystationBelongInfo = otherCompanyStationSeller[
                    otherCompanyStationSeller.index == self.stationName]
                return [otherCompanystationBelongInfo['department'].values[0],
                        otherCompanystationBelongInfo['nickname'].values[0]]
            else:
                return [unknown, unknown]
        if isinstance(self.stationName, (set, list)):
            yibaiStationSellerDict = {station: [yibaiCompanyName, yibaiAmazonValidStationSeller['name'][
                yibaiAmazonValidStationSeller.index == station].values[0]]
                                      for station in self.stationName if station in yibaiAmazonValidStationSeller.index}
            otherCompanySellerDict = {
                station: [otherCompanyStationSeller['department'][otherCompanyStationSeller.index == station].values[0],
                          otherCompanyStationSeller['nickname'][otherCompanyStationSeller.index == station].values[0]]
                for station in self.stationName if station in otherCompanyStationSeller.index}
            unknowComanySellerDict = {station: [unknown, unknown] for station in self.stationName if
                                      (station not in yibaiAmazonValidStationSeller.index) and (
                                              station not in otherCompanyStationSeller.index)}
            allComanySellerDict = yibaiStationSellerDict
            allComanySellerDict.update(otherCompanySellerDict)
            allComanySellerDict.update(unknowComanySellerDict)
            return allComanySellerDict

    def station_yibai_id(self):
        """给账号添加id:暂时只给易佰的账号添加id"""
        yibaiAccountIdInfo = query_frequently_table_info.query_amazon_account_index()
        yibaiStationColumnName = 'account_name'
        yibaiStationIdColumnName = 'id'
        if isinstance(self.stationName, str):
            if self.stationName in set(yibaiAccountIdInfo[yibaiStationColumnName]):
                return int(yibaiAccountIdInfo[yibaiStationIdColumnName][
                               yibaiAccountIdInfo[yibaiStationColumnName] == self.stationName].values[0])

            else:
                return {}
        if isinstance(self.stationName, (set, list)):
            return {station: int(id) for station, id in
                    zip(yibaiAccountIdInfo[yibaiStationColumnName], yibaiAccountIdInfo[yibaiStationIdColumnName]) if
                    station in self.stationName}

    def station_yibai_short_name(self):
        """获取账号易佰的账号简称"""
        yibaiAccountShortNameInfo = query_frequently_table_info.query_amazon_account_short_name()
        yibaiStationNameColumnName = 'account_name'
        yibaiStationShortNameColumnName = 'short_name'
        if isinstance(self.stationName, str):
            if self.stationName in set(yibaiAccountShortNameInfo[yibaiStationNameColumnName]):
                return yibaiAccountShortNameInfo[yibaiStationShortNameColumnName][
                    yibaiAccountShortNameInfo[yibaiStationNameColumnName] == self.stationName].values[0]
            else:
                return {}
        if isinstance(self.stationName, (set, list)):
            return {station: shortName for station, shortName in
                    zip(yibaiAccountShortNameInfo[yibaiStationNameColumnName],
                        yibaiAccountShortNameInfo[yibaiStationShortNameColumnName]) if station in self.stationName}

    @staticmethod
    def contain_chinese(stationName):
        return chinese_check.check_contain_chinese(stationName)

    @classmethod
    def cn_2_en_one_station(cls, stationName):
        # 首先需要判断是否包含中文
        if not cls.contain_chinese(stationName):
            return standardStation(stationName)
        else:
            site = public_function.COUNTRY_CN_EN_DICT.get(chinese_check.extract_chinese(stationName).replace('站', ''),
                                                          None)
            if site is None:
                raise ValueError(f'{chinese_check.extract_chinese(stationName)} 无法识别成国家.')
            account = chinese_check.filter_chinese(stationName)
            return standardStation(account + '_' + site)

    @classmethod
    def cn_2_en(cls,stationName):
        if isinstance(stationName,str):
            return cls.cn_2_en_one_station(stationName)
        if isinstance(stationName,(set,list)):
            return dict((station,cls.cn_2_en_one_station(station)) for station in stationName)


    @staticmethod
    def seller_sku(stationName):
        """
        本公司的sellersku
        :return:
        """
        station_sku_map_query_sql = "select `seller-sku` from station_ac_major_data where station = '%s'" % stationName
        return list(sql_write_read.read_table(station_sku_map_query_sql)['seller-sku'])

    @classmethod
    def erp_sku(cls,stationName):
        """
        本公司对应的erpsku
        :return:
        """
        stationSellerSku = cls.seller_sku(stationName)
        station_erpsku_info = cls.query_sku_tied(stationSellerSku)
        return list(station_erpsku_info['erp_sku'])


    def query_sku_tied(sku: str or list, is_seller_sku=True) -> pd.DataFrame:
        """
            查询sku捆绑表 亚马逊平台:根据seller_sku查询erp_sku 或是更新erp_sku查询seller_sku
        :param sku:string or list
            需要查询的sku
        :param is_seller_sku:bool default True
            is false means query sku is erp_sku
        :return:pd.DataFrame or None
            查询的返回结果 seller_sku和erp_sku列
        """
        if not isinstance(sku, (str, list)):
            raise TypeError(f'sku type must be string or list.But sku param you input {sku} type is {type(sku)}.')
        if not isinstance(is_seller_sku, bool):
            raise TypeError(
                f'sku type must be bool.But is_seller_sku param you input {is_seller_sku} type is {type(sku)}.')
        # 初始化数据库连接
        _conn_mysql = sql_write_read.QueryMySQL()
        sku_map_db_table_name = 'yibai_amazon_sku_map'
        if isinstance(sku, str):
            if is_seller_sku:
                query_sku = "SELECT seller_sku,erp_sku from %s where seller_sku = '%s'" % (sku_map_db_table_name, sku)
            else:
                query_sku = "SELECT seller_sku,erp_sku from %s where erp_sku = '%s'" % (sku_map_db_table_name, sku)
        if isinstance(sku, (set,list)):
            sku_query = sql_write_read.query_list_to_str(sku)
            if is_seller_sku:
                query_sku = "SELECT seller_sku,erp_sku from %s where seller_sku in (%s)" % (
                    sku_map_db_table_name, sku_query)
            else:
                query_sku = "SELECT seller_sku,erp_sku from %s where erp_sku in (%s)" % (
                sku_map_db_table_name, sku_query)
        sku_map_data_response = _conn_mysql.read_table(sku_map_db_table_name, query_sku,
                                                       columns=['seller_sku', 'erp_sku'])
        # 关闭数据库连接
        _conn_mysql.close()
        sku_map_data_response.drop_duplicates(inplace=True)
        sku_map_data_response.reset_index(inplace=True, drop=True)
        return sku_map_data_response

    @classmethod
    def erpsku_sellersku_dict(cls,stationName):
        """
        获取本公司负责的sellersku对应的erpsku与本公司的sellersku字典
        :return:
        """
        stationSellerSku = cls.seller_sku(stationName)
        station_erpsku_info = cls.query_sku_tied(stationSellerSku)
        station_erpsku_grouped = station_erpsku_info.groupby(by=['erp_sku'])
        return {erpsku:set(erpskuinfo['seller_sku']) for erpsku,erpskuinfo in station_erpsku_grouped}
        # return {erpsku:set(station_erpsku_info['seller_sku'][station_erpsku_info['erp_sku'] == erpsku]) for erpsku in station_erpsku_info['erp_sku']}


    @staticmethod
    def stationNameBridge(stations,queryType='full'):
        """站点名之间的转换：简称和全称"""
        if stations is None or len(stations) == 0:
            raise ValueError(f'查询信息不能为空。')
        if not isinstance(stations, (str, list, set)):
            raise TypeError(f'查询信息错误。')
        allTypes = {'full','short'}
        if queryType == 'full':
            searchType = 'short'
            stations = [standardStation(station) for station in stations]
        else:
            queryType = 'short'
            searchType = 'full'
        if queryType not in allTypes:
            raise ValueError(f'查询的输入类型必须为{",".join(allTypes)}的一种')
        sellerNameInfo = query_frequently_table_info.query_amazon_account_short_name()
        columnsDict = {'full': 'account_name', 'short': 'short_name'}
        if isinstance(stations, str):
            searchInfo = sellerNameInfo[columnsDict[searchType]][sellerNameInfo[columnsDict[queryType]] == stations]
            if len(searchInfo) > 0:
                return standardStation(searchInfo.values[-1])
            else:
                return
        if isinstance(stations, (list, set)):
            searchInfo = sellerNameInfo[[columnsDict[queryType], columnsDict[searchType]]][
                sellerNameInfo[columnsDict[queryType]].isin(stations)]
            searchInfo.drop_duplicates(subset=[columnsDict[queryType]], keep='last', inplace=True)
            return {queryInfo: standardStation(searchInfo) for queryInfo, searchInfo in
                    zip(searchInfo[columnsDict[queryType]], searchInfo[columnsDict[searchType]])}

    @staticmethod
    def query_account_name_by_id(id:int or list or set or tuple,accounIdTable='account_id_index',accountIdDb='team_station'):
        """
        通过账号id查询账号名
        :param id:
        :return:
        """
        if not isinstance(id,(int,set,list,tuple,str)):
            return
        accountNameColumnName = 'account_name'
        accountIdColumnName = 'id'
        # 查询单个id
        if isinstance(id,(int,str)):
            if isinstance(id,str):
                if not id.isdigit():
                    return
                id = int(id)
            querySql = 'select `%s` from `%s` where `%s`=%s' % (accountNameColumnName,accounIdTable,
                                                                accountIdColumnName,id)
            queryInfo = sql_write_read.QueryMySQL(database=accountIdDb).select(querySql,one=True)
            if queryInfo:
                return queryInfo[accountNameColumnName]
            else:
                return
        # 查询多个id
        if isinstance(id,(set,list,tuple)):
            queryIdstr = sql_write_read.query_list_to_str(id)
            querySql = 'select `%s`,`%s` from `%s` where `%s` in (%s)' % (accountIdColumnName,
            accountNameColumnName, accounIdTable, accountIdColumnName, queryIdstr)
            queryInfo = sql_write_read.QueryMySQL(database=accountIdDb).select(querySql)
            if not queryInfo:
                return {i:None for i in id}
            accountQueryIndex = [v_[accountIdColumnName] for v_ in queryInfo]
            qeuryInfoSeries = pd.Series([v_[accountNameColumnName] for v_ in queryInfo],index=accountQueryIndex)
            return {i:qeuryInfoSeries.get(i if isinstance(i,int) else int(i),None) if ((isinstance(i,int)) or (isinstance(i,str) and i.isdigit())) and (int(i) in accountQueryIndex) else None for i in id}

