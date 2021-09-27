"""
与销售有关的类
"""
from datetime import datetime, timedelta
import os
import re
import calendar
import pandas as pd


from my_toolkit import get_time,process_station,query_frequently_table_info,public_function,process_company
from my_toolkit.access_info_query import SellerManager
from my_toolkit.sql_write_read import query_list_to_str, QueryMySQL,to_table_replace


# 销售上传的月数据压缩包保存路径
SELLERUPLOADMONTHFOLDER = r"F:\sales_upload_monthly_zipped"


class SellerMonthlyFile(SellerManager):
    """
    处理销售上传月数据:

    :param object:
    :return:
    """

    def __init__(self, workNum, companyBelong='yibai', host='172.16.128.240', port=3306, username='marmot',
                 password='marmot123',
                 db='team_station', orgTable='company_organization', sellerStationTable='erp_seller_account',
                 otherAccessDistributionTable='salers_shop'):
        # workNum:yibai用工号,其他公司用花名
        SellerManager.__init__(self, workNum=workNum, companyBelong=companyBelong, host=host, port=port,
                               username=username, password=password,
                               db=db, orgTable=orgTable, sellerStationTable=sellerStationTable,
                               otherAccessDistributionTable=otherAccessDistributionTable)

        self.workNum = workNum
        # 销售负责的站点
        self.sellerChargedStations = set(self.chargedStations())
        # 上月的年,月
        self.last_year, self.last_month = get_time.last_month()

    def month_reports_status(self,all_seller=False):
        """
        上月的月数据报表状态
        :return:
        """
        if not all_seller:
            if not self.sellerChargedStations:
                return {}
        # 销售上传的月数据站点列表
        # {'kimiss_uk':{'cp'}}
        sellerUploadStationTypeDict = self._seller_upload(all_seller=all_seller)
        # 通过接口获取的月数据站点列表
        remoteStationTypeDict = self.__remote_request(all_seller=all_seller)
        # 全部的月数据站点列表
        return self.__merge_type_dict(remoteStationTypeDict, sellerUploadStationTypeDict)

    def __remote_request(self,all_seller=False):
        """上月远程请求报表状态"""
        remoteStationCpDict = self.__remote_request_cp(all_seller=all_seller)
        remoteStationBrDict = self.__remote_request_br(all_seller=all_seller)
        return self.__merge_type_dict(remoteStationCpDict, remoteStationBrDict)

    def __remote_request_cp(self,all_seller=False):
        """上月远程Cp报表状态"""
        _monthlyCpDB = 'amazon_ad_api'
        _monthlyCpTable = 'station_monthly_data_sp'
        __columns_ = ['ad_station']
        if not all_seller:
            sellerChargedStation = self.sellerChargedStations
            sellerChargedStationStr = query_list_to_str(sellerChargedStation)
            _monthlySql = "select ad_station from %s where (`ad_station` in (%s)) and (`year` = %s) and (`month` = %s)" % (
            _monthlyCpTable, sellerChargedStationStr, self.last_year, self.last_month)
        else:
            _monthlySql = "select ad_station from %s where (`year` = %s) and (`month` = %s)" % (
                _monthlyCpTable, self.last_year, self.last_month)
        _connMysql = QueryMySQL(database=_monthlyCpDB)
        _monthlyCpInfo = _connMysql.read_table(_monthlyCpTable, _monthlySql, columns=__columns_)
        _connMysql.close()
        if len(_monthlyCpInfo.index) == 0:
            return {}
        return {process_station.standardStation(station): {'cp'} for station in set(_monthlyCpInfo['ad_station'])}

    def __remote_request_br(self,all_seller=False):
        """上月远程Br报表状态"""
        _monthlyBrInfo = query_frequently_table_info.query_remote_request_monthly_br(
            columns=['station_name', 'year', 'month'])
        # 筛选出本月属于自己的站点
        _monthlyBrInfo = public_function.init_df(_monthlyBrInfo,change_columns_type={'int':['year','month']})
        if not all_seller:
            _monthlyBrInfo['station_name'] = _monthlyBrInfo['station_name'][
                (_monthlyBrInfo['station_name'].isin(self.sellerChargedStations)) & (
                        _monthlyBrInfo['year'] == self.last_year) & (
                        _monthlyBrInfo['month'] == self.last_month)]
        else:
            _monthlyBrInfo['station_name'] = _monthlyBrInfo['station_name'][(
                        _monthlyBrInfo['year'] == self.last_year) & (
                        _monthlyBrInfo['month'] == self.last_month)]
        return {process_station.standardStation(station): {'br'} for station in _monthlyBrInfo['station_name']}

    def _seller_upload(self,all_seller=False):
        """
        销售上传的状态：
            销售上传站点缺失的压缩文件,
            cp表格式:Campaigns_Feb_24_2021.csv
            br表格式:BusinessReport-2-24-21.csv
            识别cp报表中的年月用:
        :return:
        """
        monthlyFolderList = set([os.path.join(SELLERUPLOADMONTHFOLDER,file_) for file_ in os.listdir(SELLERUPLOADMONTHFOLDER)])
        monlyFilesISdirDict = {__dir__:os.path.isdir(__dir__) for __dir__ in monthlyFolderList}
        allMonthlyFiles = {process_station.standardStation(os.path.basename(stationFiles_)): stationFiles_ for
                           stationFiles_ in monthlyFolderList if monlyFilesISdirDict.get(stationFiles_)}
        if not all_seller:
            sellerMonthlyZipfilesPath = {stationName: stationFolder for
                                         stationName, stationFolder in allMonthlyFiles.items() if
                                         stationName in self.sellerChargedStations}
        else:
            sellerMonthlyZipfilesPath = allMonthlyFiles
        stationMonthlyDict = {}
       #  stationsFilesDict = os.listdir(set(sellerMonthlyZipfilesPath.values()))
        stationsFilesDict = {__dir__:os.listdir(__dir__) for __dir__ in set(sellerMonthlyZipfilesPath.values())}
        for stationName, stationFolder in sellerMonthlyZipfilesPath.items():
            stationFiles = stationsFilesDict.get(stationFolder)
            if not stationFiles:
                continue
            for file in stationFiles:
                filePath = os.path.join(stationFolder, file)
                monthlyFile = MonthlyFile(filePath)
                fileType_ = monthlyFile.file_type()
                if fileType_ is None:
                    continue
                fileDate = monthlyFile.file_date()
                if monthlyFile._is_date_this_month():
                    stationMonthlyDict.setdefault(stationName, set()).add(fileType_)
        return stationMonthlyDict

    def __merge_type_dict(self, *TypeDict):
        # {'station':{'cp'}}
        typeKeys = set(sum([list(dict_.keys()) for dict_ in TypeDict], []))
        mergedDict = {}
        for dict_ in TypeDict:
            if not isinstance(dict_, dict):
                continue
            for key_, values_ in dict_.items():
                if not isinstance(values_, set):
                    continue
                mergedDict.setdefault(key_, set()).update(values_)
        return mergedDict

    @staticmethod
    def all_upload_reports_status():
        """
        获取上传的全部站点的月数据
        :return:
        """
        return SellerMonthlyFile('1').month_reports_status(all_seller=True)


    @staticmethod
    def all_not_upload_report_info():
        """还没有上传全的月数据的站点"""
        uploadedReports = SellerMonthlyFile.all_upload_reports_status()
        # 全部站点信息
        allDepartStations = process_company.Company.all_company_depart_station()
        # 添加月数据列
        allMonthlyFile = {'br','cp'}
        allDepartStations['monthly_file_lost'] = [allMonthlyFile - uploadedReports.get(station,set()) for station in allDepartStations['station']]
        allDepartStationsLost = allDepartStations[allDepartStations['monthly_file_lost'].str.len() != 0]
        allDepartStationsLost['monthly_file_lost'] = [','.join(list(lostType)) for lostType in allDepartStationsLost['monthly_file_lost']]
        allDepartStationsLost['updatetime'] = datetime.now()
        # 上传到数据库中
        tableName = 'monthly_file_lost_right_now'
        to_table_replace(allDepartStationsLost,tableName)
        print(f'完成更新当前月报表缺失情况,并录入到数据表{tableName}中.')

class MonthlyFile(object):

    def __init__(self, filePath):
        # if not os.path.exists(filePath):
        #     raise FileExistsError(f'{filePath} not exists.')
        self.filePath = filePath
        self.filePath = self._init_monthly_file()
        self.cpFileName = 'cp'
        self.brFileName = 'br'
        self.typeSignWord = {self.cpFileName: 'Campaigns', self.brFileName: 'BusinessReport'}
        self.fileType = self.file_type()
        self.fileName = os.path.splitext(os.path.basename(self.filePath))[0]
        self.fileDate = self.file_date()

    def _init_monthly_file(self):
        return re.sub('\(.*?\)', '', self.filePath).strip()


    def file_type(self):
        """广告报表用:Campaigns识别,br报表用BusinessReport识别"""
        for type, signWord in self.typeSignWord.items():
            if signWord in self.filePath:
                return type
        return

    def file_date(self):
        """
            广告报表用文件名中的英文的月以及最后的年来识别的年月,
            br报表用文件名中的英文的月以及最后的年来识别的年月,
        """
        if self.fileType is None:
            return [13, 9999]
        if self.fileType == self.cpFileName:
            fileMonthName, fileYearNum = self.fileName.split('_')[1], self.fileName.split('_')[-1]
            if fileMonthName in list(calendar.month_abbr):
                fileMonthNum = list(calendar.month_abbr).index(fileMonthName)
            else:
                # 返回无效的月份
                fileMonthNum = 13
            if str(fileYearNum)[-2:] in [str(datetime.now().year)[-2:], str(datetime.now().year - 1)[-2:]]:
                fileYearNum = int(f'20{str(fileYearNum)[-2:]}')
            else:
                fileYearNum = 9999
            return [fileYearNum, fileMonthNum]
        else:
            """考虑到br表的月份不同的国家不同,于是取中间两个均为月份"""
            fileMonthName, fileYearNum = {int(self.fileName.split('-')[1]), int(self.fileName.split('-')[2])}, int(
                self.fileName.split('-')[-1])
            fileMonthSetNum = fileMonthName & set(range(1, 13))
            if not fileMonthSetNum:
                fileMonthSetNum = {13}
            if str(fileYearNum)[-2:] in [str(datetime.now().year)[-2:], str(datetime.now().year - 1)[-2:]]:
                fileYearNum = int(f'20{str(fileYearNum)[-2:]}')
            else:
                fileYearNum = 9999
            return [fileYearNum, fileMonthSetNum]

    def _is_date_this_month(self):
        if self.fileType is None:
            return False
        nowYear, nowMonth = datetime.now().year, datetime.now().month
        if self.fileType == self.cpFileName:
            if self.fileDate == [nowYear, nowMonth]:
                return True
            else:
                return False
        # br的文件日期为[year:int,month:set]
        if self.fileType == self.brFileName:
            if self.fileDate[0] != nowYear:
                return False
            if nowMonth not in self.fileDate[1]:
                return False
            else:
                return True

    @classmethod
    def is_br_valid(cls,brData):
        if isinstance(brData,pd.DataFrame) and len(brData.index) < 8:
            return True
        else:
            return False


if __name__ == '__main__':
    SellerMonthlyFile.all_not_upload_report_info()
