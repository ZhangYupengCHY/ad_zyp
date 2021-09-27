"""
处理公司的类
"""
import pandas as pd

from public_function import run_time
from sql_write_read import QueryMySQL, query_list_to_str
import process_station, query_frequently_table_info, type_verify,access_info_query


class Company(object):
    """
    与公司有关的类
    """
    yibaiName = '易佰'

    def __init__(self, name=yibaiName):
        self.allCompany = self.all_company()
        if name != 'all':
            if name not in self.allCompany:
                raise ValueError(f'{name} should one of {",".join(list(self.allCompany))}')
        self.companyName = name
        self.otherCompanyAccessTable = 'other_company_belong'

    def station(self, short_name=False):
        '''
        获取公司的负责的站点
        :return:
        '''
        self.short_name = short_name
        if self.companyName == 'all':
            allCompanyStationsDict = {}
            yibaiStations = {self.yibaiName: self._yibai_station()}
            otherCompanyStations = self._other_company_station()
            allCompanyStationsDict.update(yibaiStations)
            allCompanyStationsDict.update(otherCompanyStations)
            return allCompanyStationsDict
        if self.companyName == self.yibaiName:
            companyStations = self._yibai_station()
        else:
            companyStations = self._other_company_station()
        return companyStations

    def _yibai_station(self, fullName=True):
        _connMysql = QueryMySQL()
        YBstationsTableName = 'account_status'
        if fullName == True:
            YBstationsSQL = "select account_name from %s where status = 1" % YBstationsTableName
            columns = ['account_name']
            YBstationsInfo = _connMysql.read_table(YBstationsTableName, YBstationsSQL, columns=columns)
            companyStations = set(
                [process_station.standardStation(station) for station in set(YBstationsInfo['account_name']) if
                 (station is not None) and (~pd.isna(station))])
        else:
            YBstationsSQL = "select shortName from %s where status = 1" % YBstationsTableName
            columns = ['shortName']
            YBstationsInfo = _connMysql.read_table(YBstationsTableName, YBstationsSQL, columns=columns)
            companyStations = set(
                [process_station.standardStation(station) for station in set(YBstationsInfo['shortName']) if
                 (station is not None) and (~pd.isna(station))])
        _connMysql.close()
        return companyStations

    def _other_company_station(self, degree='company'):
        """
        获取其他公司的站点信息:
            单个公司/全部外公司
        :param degree:
        :return:
        """
        _connMysql = QueryMySQL()
        degrees = ['company', 'depart']
        if degree not in degrees:
            raise ValueError(f'degree must one of {",".join(degrees)}.')
        if self.companyName != 'all':
            otherCompanySQL = "select station from %s where `department` like '%%%%%s%%%%'" % (
                self.otherCompanyAccessTable, self.companyName)
            columns = ['station']
            otherCompanyInfo = _connMysql.read_table(self.otherCompanyAccessTable, otherCompanySQL, columns)
            _connMysql.close()
            companyStations = set(
                [process_station.standardStation(station) for station in set(otherCompanyInfo['station'])])
            return companyStations
        else:
            allOtherCompanySQL = 'select `department`,`station` from %s' % self.otherCompanyAccessTable
            columns = ['department', 'station']
            allOtherCompanyInfo = _connMysql.read_table(self.otherCompanyAccessTable, allOtherCompanySQL, columns)
            _connMysql.close()
            allOtherCompanyInfo.drop_duplicates(keep='last', inplace=True)
            allOtherCompanyInfo['station'] = [process_station.standardStation(station) for station in
                                              allOtherCompanyInfo['station']]
            if degree == 'depart':
                allOtherCompanyDepartStationInfo = allOtherCompanyInfo.groupby(by=['department'])
                allOtherCompanyDepartStationDict = {}
                for departName, stationInfo in allOtherCompanyDepartStationInfo:
                    allOtherCompanyDepartStationDict[departName] = set(stationInfo['station'])
                return allOtherCompanyDepartStationDict
            otherCompanyNameNDepartmentDict = self.other_company_and_department_name_dict()
            otherCompanyStations = {}
            [otherCompanyStations.setdefault(companyName, set()).add(station) for companyName, companyDepartment in
             otherCompanyNameNDepartmentDict.items() for station, department in
             zip(allOtherCompanyInfo['station'], allOtherCompanyInfo['department']) if department in companyDepartment]
            return otherCompanyStations


    def other_company_and_department_name_dict(self):
        allOtherCompanyDepartmentName = self.other_company_department()
        allOtherCompanyDepartmentNameDict = {}
        [allOtherCompanyDepartmentNameDict.setdefault(name_, set()).add(departname) for name_ in self.allCompany for
         departname in allOtherCompanyDepartmentName if name_ in departname]
        return allOtherCompanyDepartmentNameDict

    def other_company_depart_belong(self):
        allOtherCompanyDepartmentName = self.other_company_department()
        allOtherCompanyDepartmentNameDict = {}
        for depart in allOtherCompanyDepartmentName:
            for company in self.allCompany:
                if company in depart:
                    allOtherCompanyDepartmentNameDict[depart] = company
                    break
        return allOtherCompanyDepartmentNameDict

    def other_company_department(self):
        _connMysql = QueryMySQL()
        otherCompanyDepartmentSQL = "select department from %s group by department" % (self.otherCompanyAccessTable)
        otherCompanyDepartmentInfo = _connMysql.read_table(self.otherCompanyAccessTable, otherCompanyDepartmentSQL,
                                                           columns=['department'])
        _connMysql.close()
        return list(otherCompanyDepartmentInfo['department'])

    @staticmethod
    def yibai_mode(stationType='full'):
        """
            易佰的站点模式:按照大部分配
            分为站点全称和简称
        """
        stationTypes = ['full', 'short']
        if stationType not in stationTypes:
            raise ValueError(f'查询站点类型为:{",".join(stationTypes)}.')
        yibaiSellerStationInfo = query_frequently_table_info.query_seller_stations()
        yibaiSellerStationInfo = yibaiSellerStationInfo[['part', 'short_name']][yibaiSellerStationInfo['part'] != '']
        yibaiStationGroup = yibaiSellerStationInfo.groupby(by='part')
        yibaiStationPartDict = {}
        modeSign = {'产品': '产品线', '智': '产品线', '仓': 'FBA/FBM', '精品': '精品'}
        for part, partInfo in yibaiStationGroup:
            if not any(word in part for word in modeSign.keys()):
                raise ValueError(f'无法识别大部:{part}的站点模式.')
            partMode = [mode for word, mode in modeSign.items() if word in part][0]
            yibaiStationPartDict.setdefault(partMode, set()).update(set(partInfo['short_name']))
        if stationType == 'short':
            return yibaiStationPartDict
        else:
            yibaiStationTypeDict = process_station.Station.yibai_station_type_dict()
            yibaiStationTypeDictReverse = {shortName: stationName for stationName, shortName in
                                           yibaiStationTypeDict.items()}
            yibaiStationModeDictTemp = {mode: {yibaiStationTypeDictReverse.get(station, '') for station in stations} for
                                        mode, stations in yibaiStationPartDict.items()}
            yibaiStationModeDict = {}
            for mode, station in yibaiStationModeDictTemp.items():
                station.discard('')
                yibaiStationModeDict[mode] = station
            return yibaiStationModeDict

    @staticmethod
    def other_company_station_mode():
        """
        其他公司的站点模式字典
        :return:
        """
        # 部门与模式的字典
        modeDepartmentDict = {
            "楚晋": "产品线",
            "光迅嘉": '其他',
            '依莱佳': '其他',
            "松华": "产品线",
            "璟嘉": '其他',
            "朗罗": "其他",
            "恩韦逊": '产品线',
            "丰腾": '其他',
            "云羿": '产品线',
            "木星人": '其他',
            "轩行": '其他',
            '云奕': '产品线',
            '黑杉': '其他',
            '东益': '其他',
            "热气球": '其他',
            "加一点": '其他',
            "跃众": "其他", }
        # 通过查询salers_shop表去查询站点模式
        otherCompanyInfo = query_frequently_table_info.query_other_company_belong()
        otherCompanyStationMode = {}
        for depart, departInfo in otherCompanyInfo.groupby('department'):
            mode = [mode for company, mode in modeDepartmentDict.items() if company in depart]
            if not mode:
                raise ValueError(f'部门{depart}找不到公司')
            mode = mode[0]
            otherCompanyStationMode.setdefault(mode, set()).update(
                set([process_station.standardStation(station) for station in departInfo['station']]))

        return otherCompanyStationMode

    @staticmethod
    def company_station_mode():
        """
        全部公司的站点模式字典
        :return:
        """
        yibaiStationModeDict = Company.yibai_mode()
        otherCompanyStationModeDict = Company.other_company_station_mode()
        companyStationModeDict = {}
        companyStationMode = yibaiStationModeDict.keys() | otherCompanyStationModeDict.keys()
        [companyStationModeDict.setdefault(mode_, yibaiStationModeDict.get(mode_, set())).update(
            otherCompanyStationModeDict.get(mode_, set())) for mode_ in companyStationMode]
        return companyStationModeDict

    @staticmethod
    def all_company(includeYB=True, short=True):
        """全部公司"""
        type_verify.TypeVerify.type_valid([includeYB, short], [bool, bool])
        shortColumnName = 'short_name'
        fullColumnName = 'full_name'
        YbShortName = Company.yibaiName
        # allCompanyInfo = query_frequently_table_info.query_company_name(columns=[shortColumnName, fullColumnName])
        allCompanyInfo = QueryMySQL().fast_read_table('company_name', database='team_authority',
                                                      columns=[shortColumnName, fullColumnName])
        if not includeYB:
            allCompanyInfo = allCompanyInfo[allCompanyInfo[shortColumnName] != YbShortName]
        if short:
            return set(allCompanyInfo[shortColumnName])
        else:
            return set(allCompanyInfo[fullColumnName])

    def charged_aders(self):
        # 广告专员权限数据库
        DATABASE__ = 'team_authority'
        # 广告专员基本信息表
        ADERS_BASE_INFO__ = 'user_base_info'
        # 广告专员负责公司信息表
        COMPANT_CHARGED_INFO__ = 'company_station_charged'
        """负责的公司"""
        companyId = self.companyId()
        _connMysql = QueryMySQL(database=DATABASE__)
        companyStationChargedInfo = _connMysql.read_table(COMPANT_CHARGED_INFO__)
        ADersNameIndexInfo = _connMysql.read_table(ADERS_BASE_INFO__, columns=['id', 'login_name'])
        _connMysql.close()
        chargedAders = set(companyStationChargedInfo['user_id'][companyStationChargedInfo['company_id'] == companyId])
        return set(ADersNameIndexInfo['login_name'][ADersNameIndexInfo['id'].isin(chargedAders)])

    def companyId(self):
        # 公司名索引表
        COMPANY_NAME_INDEX__ = 'company_name'
        DATABASE__ = 'team_authority'
        _connMysql = QueryMySQL(database=DATABASE__)
        companyNameInfo = _connMysql.read_table(COMPANY_NAME_INDEX__)
        _connMysql.close()
        companyIdInfo = companyNameInfo['id'][companyNameInfo['short_name'] == self.companyName]
        if not len(companyIdInfo):
            return None
        else:
            return companyIdInfo.values[0]

    @staticmethod
    def other_company_depart_station():
        """获取其他公司和公司部门所拥有的站点"""
        allCompany = Company(name='all')
        allOtherDepartStationDict = allCompany._other_company_station(degree='depart')
        for key in allOtherDepartStationDict.keys():
            if Company.yibaiName in key:
                del allOtherDepartStationDict[key]
        allOtherDepartStationDict = {depart:list(station) for depart,station in allOtherDepartStationDict.items()}
        departStationDf = pd.DataFrame(allOtherDepartStationDict.items(),columns=['depart','station']).explode('station')
        allOtherCompanyDepartDict = allCompany.other_company_depart_belong()
        departStationDf['company'] = [allOtherCompanyDepartDict.get(depart,None) for depart in departStationDf['depart']]
        return departStationDf

    @staticmethod
    def yibai_depart_station():
        """
        获取yibai站点的部门信息
        :return:
        """
        allYibaiStation = query_frequently_table_info.query_seller_stations(columns=['job_number','short_name'])
        allYibaiStation.drop_duplicates(subset='short_name',keep='last',inplace=True)
        # 添加全称
        allStationDict = process_station.Station.stationNameBridge(set(allYibaiStation['short_name']),queryType='short')
        allYibaiStation['station'] = [allStationDict.get(shortName,None) for shortName in allYibaiStation['short_name']]
        # 筛选掉无效的站点
        yibaiStations = Company()._yibai_station()
        allYibaiStation = allYibaiStation[allYibaiStation['station'].isin(yibaiStations)]
        # 添加部门信息
        allYibaiNum = {num for num in allYibaiStation['job_number']}
        allYibaiNumDepartDict = access_info_query.SellerManager.sellerDepart(allYibaiNum)
        allYibaiStation['depart'] = [allYibaiNumDepartDict.get(num,None) for num in allYibaiStation['job_number']]
        return allYibaiStation

    @staticmethod
    def all_company_depart_station():
        yibaiDepartStation = Company.yibai_depart_station()
        yibaiDepartStation['company'] = Company.yibaiName
        otherCompanyDepartStatin = Company.other_company_depart_station()
        chooseColumns = ['company','depart','station']
        allCompanyDepartStation = pd.concat([yibaiDepartStation[chooseColumns],otherCompanyDepartStatin[chooseColumns]])
        allCompanyDepartStation.reset_index(drop=True,inplace=True)
        return allCompanyDepartStation


if __name__ == '__main__':
    companyInfo = Company.all_company_depart_station()
    print(1)
