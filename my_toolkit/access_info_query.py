#!/usr/bin/env python
# coding=utf-8
# author:Zyp
import json
import pandas as pd
import traceback
from datetime import datetime

from my_toolkit import sql_write_read, query_frequently_table_info, public_function, process_station, process_company, \
    type_verify

SUPERMANAGER = ['王艳', '张于鹏', '人工智能3', '张立滨', '人工智能2', '陈配配', '人工智能1', '人工智能4', '人工智能5', '人工智能6']


def query_info(request):
    # 创建连接
    _conn_sql = sql_write_read.QueryMySQL()
    # show_df = sql_write_read.read_db_table("SELECT * FROM access_assign", 'team_station')
    show_df = _conn_sql.read_table('access_assign')
    # 关闭连接
    _conn_sql.close()
    show_df.fillna('', inplace=True)
    access_df = show_df.loc[~(show_df['team_member'].isin(['', '[]'])), 'real_name']

    access_list = SUPERMANAGER + list(access_df)

    return {'access_list': access_list}


def teamMember(realname):
    if realname in SUPERMANAGER:
        _conMysql = sql_write_read.QueryMySQL()
        queryTable = "access_assign"
        querySql = "select real_name from `%s`" % queryTable
        teamMemberDf = _conMysql.read_table(queryTable, querySql, columns=['real_name'])
        return list(set(teamMemberDf['real_name']))
    # 获取成员
    _conMysql = sql_write_read.QueryMySQL()
    queryTable = "access_assign"
    querySql = "select team_member from `%s` where real_name = '%s'" % (queryTable, realname)
    teamMemberDf = _conMysql.read_table(queryTable, querySql, columns=['team_member'])
    _conMysql.close()
    if teamMemberDf.empty:
        return [realname]
    else:
        memberStr = teamMemberDf['team_member'].values[0].strip()
        if len(memberStr) == 0:
            return [realname]
        members = json.loads(memberStr)
        members.append(realname)
        return members


def seller_get_member(nickname):
    """
    销售获取下级全部名单
    :param nickname:
        str of seller realname
    :return:
        set of seller username
    """
    _connMysql = sql_write_read.QueryMySQL()
    sellerShop = 'salers_shop'
    sellerShopDF = _connMysql.read_table(sellerShop)

    # 首先判断是不是有特殊权限
    superAccessTable = 'salers_shop_special_access'
    superAccessSql = "select * from %s where nickname = '%s'" % (superAccessTable, nickname)
    superAccessResult = _connMysql.read_table(superAccessTable, superAccessSql)
    if len(superAccessResult.index) != 0:
        # 获得拥有特殊权限的人的队员
        SuperOwnerDepartmentList = set(superAccessResult['department'])
        SuperOwnerDepartmentListStr = sql_write_read.query_list_to_str(SuperOwnerDepartmentList)
        SuperOwnerMemorySql = "select nickname from %s where department in (%s)" % (
            sellerShop, SuperOwnerDepartmentListStr)
        SuperOwnerStationMemoryInfo = _connMysql.read_table(sellerShop, SuperOwnerMemorySql, columns=['nickname'])
        teamNickname = set(SuperOwnerStationMemoryInfo['nickname'])
        selfNickname = set([nickname])
        _connMysql.close()
        return teamNickname | selfNickname
    department_manager = set(sellerShopDF['departmentcharge'])
    owener = set(sellerShopDF['groupcharge'])
    # if name in superOwener:
    #     teamName = set(sellerShopDF['username'][sellerShopDF['super_owner'] == name])
    if nickname in department_manager:
        teamNickname = set(sellerShopDF['nickname'][sellerShopDF['departmentcharge'] == nickname])
    elif nickname in owener:
        teamNickname = set(sellerShopDF['nickname'][sellerShopDF['groupcharge'] == nickname])
    else:
        teamNickname = set(sellerShopDF['nickname'][sellerShopDF['sellersname'] == nickname])
    selfNickname = set([nickname])
    _connMysql.close()
    return teamNickname | selfNickname


def sellers_degree(nickname):
    """
    判断销售的身份等级:
        超级管理员,大部负责人,小组负责人,销售,未知
    :param nickname:
    :return:
    """
    _connMysql = sql_write_read.QueryMySQL()

    # 首先判断是不是有特殊权限
    superAccessTable = 'salers_shop_special_access'
    superAccessSql = "select nickname from %s where nickname = '%s'" % (superAccessTable, nickname)
    superAccessResult = _connMysql.read_table(superAccessTable, superAccessSql)
    if len(superAccessResult.index) != 0:
        # 获得拥有特殊权限的人的队员
        degree = '超级管理员'

    else:
        sellerShop = 'salers_shop'
        superAccessSql = "select departmentcharge,groupcharge,nickname from %s where nickname = '%s'" % (
            sellerShop, nickname)
        sellerShopDF = _connMysql.read_table(sellerShop)

        department_manager = set(sellerShopDF['departmentcharge'])
        groupOwener = set(sellerShopDF['groupcharge'])
        sellersNickname = set(sellerShopDF['nickname'])
        # if name in superOwener:
        #     teamName = set(sellerShopDF['username'][sellerShopDF['super_owner'] == name])
        if nickname in department_manager:
            degree = '大部负责人'
        elif nickname in groupOwener:
            degree = '小组负责人'
        elif nickname in sellersNickname:
            degree = '销售'
        else:
            degree = '未知'

    _connMysql.close()
    return degree


def seller_stations(nickname):
    """
    获取销售负责的全部站点
    :param name:
    :return:
    """
    if nickname == '':
        return set([])
    _connMysql = sql_write_read.QueryMySQL()
    sellerShopTable = 'salers_shop'
    # 首先判断是不是拥有特殊权限
    superOwnerTable = 'salers_shop_special_access'
    querySqlSuperOwner = "select * from `%s` where nickname = '%s' " % (superOwnerTable, nickname)
    queryResultSuperOwnerInfo = _connMysql.read_table(superOwnerTable, querySqlSuperOwner)
    if len(queryResultSuperOwnerInfo.index) != 0:
        # 获得拥有特殊权限的人的站点
        SuperOwnerDepartmentList = set(queryResultSuperOwnerInfo['department'])
        SuperOwnerDepartmentListStr = sql_write_read.query_list_to_str(SuperOwnerDepartmentList)
        SuperOwnerStationSql = "select station from %s where department in (%s)" % (
            sellerShopTable, SuperOwnerDepartmentListStr)
        SuperOwnerStationListInfo = _connMysql.read_table(sellerShopTable, SuperOwnerStationSql, columns=['station'])
        # 关闭数据库连接
        _connMysql.close()
        return set(SuperOwnerStationListInfo['station'])
    # 判断是不是大部负责人
    querySqlDepartmentManager = "select station from %s where departmentcharge = '%s'" % (sellerShopTable, nickname)
    queryResultDepartmentManager = _connMysql.read_table(sellerShopTable, querySqlDepartmentManager,
                                                         columns=['station'])
    if len(queryResultDepartmentManager.index) != 0:
        _connMysql.close()
        return set(queryResultDepartmentManager['station'])
    # 判断是不是小组负责人或销售
    querySqlOwner = "select station from %s where groupcharge = '%s' or nickname = '%s'" % (
        sellerShopTable, nickname, nickname)
    queryResultOwner = _connMysql.read_table(sellerShopTable, querySqlOwner, columns=['station'])
    if len(queryResultOwner.index) != 0:
        _connMysql.close()
        return set(queryResultOwner['station'])
    else:
        # 什么都不是
        return set([])


def stations_and_sellers(realName):
    """
    获取销售负责的全部站点和销售姓名
    :param name:
    :return:
    """
    if realName == '':
        return pd.DataFrame([])
    _connMysql = sql_write_read.QueryMySQL()
    sellerShopTable = 'salers_shop'
    # 首先判断是不是拥有特殊权限
    superOwnerTable = 'salers_shop_special_access'
    querySqlSuperOwner = "select * from `%s` where real_name = '%s' " % (superOwnerTable, realName)
    queryResultSuperOwnerInfo = _connMysql.read_table(superOwnerTable, querySqlSuperOwner)
    if len(queryResultSuperOwnerInfo.index) != 0:
        # 获得拥有特殊权限的人的站点
        SuperOwnerDepartmentList = set(queryResultSuperOwnerInfo['department'])
        SuperOwnerDepartmentListStr = sql_write_read.query_list_to_str(SuperOwnerDepartmentList)
        SuperOwnerStationSql = "select sellersname,station from %s where department in (%s)" % (
            sellerShopTable, SuperOwnerDepartmentListStr)
        SuperOwnerStationListInfo = _connMysql.read_table(sellerShopTable, SuperOwnerStationSql,
                                                          columns=['sellersname', 'station'])
        # 关闭数据库连接
        _connMysql.close()
        SuperOwnerStationListInfo.drop_duplicates(inplace=True)
        return SuperOwnerStationListInfo
    # 判断是不是大部负责人
    querySqlDepartmentManager = "select sellersname,station from %s where departmentcharge = '%s'" % (
        sellerShopTable, realName)
    queryResultDepartmentManager = _connMysql.read_table(sellerShopTable, querySqlDepartmentManager,
                                                         columns=['sellersname', 'station'])
    if len(queryResultDepartmentManager.index) != 0:
        _connMysql.close()
        queryResultDepartmentManager.drop_duplicates(inplace=True)
        return queryResultDepartmentManager
    # 判断是不是小组负责人
    querySqlOwner = "select sellersname,station from %s where groupcharge = '%s'" % (sellerShopTable, realName)
    queryResultOwner = _connMysql.read_table(sellerShopTable, querySqlOwner, columns=['sellersname', 'station'])
    if len(queryResultOwner.index) != 0:
        _connMysql.close()
        queryResultOwner.drop_duplicates(inplace=True)
        return queryResultOwner
    # 判断是不是销售
    querySqlSeller = "select sellersname,station from %s where sellersname = '%s'" % (sellerShopTable, realName)
    queryResultSeller = _connMysql.read_table(sellerShopTable, querySqlSeller, columns=['sellersname', 'station'])
    if len(queryResultSeller.index) != 0:
        _connMysql.close()
        queryResultSeller.drop_duplicates(inplace=True)
        return queryResultSeller
    else:
        # 什么都不是
        return pd.DataFrame([])


def stations_charged_info_by_nickname(nickName, companyBelong='yibai'):
    """
    获取销售负责的全部站点和销售花名
    :param name:
    :return:
    """
    if nickName == '':
        return pd.DataFrame([])
    _connMysql = sql_write_read.QueryMySQL()
    sellerShopTable = 'salers_shop'
    if companyBelong != 'yibai':
        # 判断是不是大部负责人
        querySqlDepartmentManager = "select * from %s where departmentcharge = '%s'" % (sellerShopTable, nickName)
        queryResultDepartmentManager = _connMysql.read_table(sellerShopTable, querySqlDepartmentManager)
        if len(queryResultDepartmentManager.index) != 0:
            _connMysql.close()
            queryResultDepartmentManager.drop_duplicates(subset=['station'], keep='last', inplace=True)
            return queryResultDepartmentManager
        # 判断是不是小组负责人或是小组负责人
        querySqlOwner = "select * from %s where groupcharge = '%s' or nickname = '%s'" % (
            sellerShopTable, nickName, nickName)
        queryResultOwner = _connMysql.read_table(sellerShopTable, querySqlOwner)
        if len(queryResultOwner.index) != 0:
            _connMysql.close()
            queryResultOwner.drop_duplicates(subset=['station'], keep='last', inplace=True)
            return queryResultOwner
    else:
        erpAccountBelong = query_frequently_table_info.query_seller_stations(
            columns=['part', 'group', 'name', 'job_number', 'short_name'])
        sellerAccountBelong = erpAccountBelong[['name', 'short_name']][erpAccountBelong['name'] == nickName]
        # sellerAccountBelong.rename(columns={'name':'nickname','short_name':'station'},inplace=True)
        shortNameInfo = query_frequently_table_info.query_amazon_account_short_name(
            columns=['account_name', 'short_name'])
        sellerAccountBelong = pd.merge(sellerAccountBelong, shortNameInfo, how='left', on='short_name')
        sellerAccountBelong.rename(columns={'name': 'nickname', 'account_name': 'station'}, inplace=True)
        exportColumns = ['nickname', 'station']
        return sellerAccountBelong[exportColumns]


def query_ad_manager_station(ad_manager_name=None, type='station'):
    """
    获取广告专员的负责的账号列表或是站点
    :param ad_manager_name:
    :param type:  station or account
    :return: set
    """
    if type not in ['station', 'account']:
        raise ImportError('type must station or account.')
    _connMysql = sql_write_read.QueryMySQL()
    # 查询站点
    queryTable = 'only_station_info'
    if ad_manager_name is None:
        querySQL = 'select `station` from `%s`' % (queryTable)
    else:
        querySQL = 'select `station` from `%s` where ad_manger = "%s"' % (queryTable, ad_manager_name)
    try:
        queryInfo = _connMysql.read_table(queryTable, querySQL, columns=['station'])
        _connMysql.close()
    except Exception as e:
        # print(e)
        _connMysql.close()
        return set()
    if len(queryInfo.index) == 0:
        return set()
    queryInfo.dropna(inplace=True)
    querystation = set(queryInfo['station'])
    if type == 'station':
        return set([station.lower() for station in querystation])
    elif type == 'account':
        return set([station[:-3].lower() for station in querystation if len(station) > 3])


def queryUserWordID(realName):
    """
    通过真实姓名获取工号
    :param realName:
    :return:
    """
    nickNameTable = 'nickname'
    queryColumns = 'work_number'
    # todo
    connMysql = sql_write_read.read_table(nickNameTable)
    pass


def query_team_login_name(login_man):
    """
    通过erp提供的接口保存后的数据库,通过登录名获取自己的职位从而获取自己负责的组员名
    :param login_man:str
        erp的登录名
    :return:
        list
        自己负责的组员登录名
    """
    if login_man is None:
        return []
    if not isinstance(login_man, str):
        return []
    login_man = login_man.strip()
    if login_man == '':
        return []
    # 连接数据库
    _connMysql = sql_write_read.QueryMySQL()
    # erp的权限表
    accessErpTable = 'company_organization'
    # 获取自己职位与部门
    queryDepNPosSql = 'select department_name,pos_name from %s where login_name = "%s" ' % (accessErpTable, login_man)
    queryDepNPosInfo = _connMysql.read_table(accessErpTable, sql=queryDepNPosSql,
                                             columns=['department_name', 'pos_name'])
    _connMysql.close()
    if len(queryDepNPosInfo.index) == 0:
        return []
    elif len(queryDepNPosInfo.index) == 2:
        raise ValueError(f"erp权限列表中注册名重复:数据库名{accessErpTable},注册名:{login_man}.")
    # 职位为负责人的关键词
    managerSignWord = ['组长', '主管', '经理']
    loginManPos = queryDepNPosInfo['pos_name'].values[0]
    depName = queryDepNPosInfo['department_name'].values[0]
    for signWord in managerSignWord:
        if signWord in loginManPos:
            return query_dep_login_man(depName)
    # 职位为普通专员或是销售等非负责人
    return [login_man]


def query_dep_login_man(dep_name):
    """
    erp权限列表获取某个部门的全部注册人员列表
    :param dep_name:
    :return:
    """
    if dep_name is None:
        return []
    if not isinstance(dep_name, str):
        return []
    dep_name = dep_name.strip()
    if dep_name == '':
        return []
    # 连接数据库
    _connMysql = sql_write_read.QueryMySQL()
    accessErpTable = 'company_organization'
    queryLoginManSQL = 'select login_name from %s where dep_path like "%%%s%%"' % (accessErpTable, dep_name)
    queryLoginManInfo = _connMysql.read_table(accessErpTable, queryLoginManSQL, columns=['login_name'])
    _connMysql.close()
    return list(set(queryLoginManInfo['login_name']))


def query_all_staff_num():
    """
    获取全部员工的工号
    :return:
    """
    # 连接数据库
    _connMysql = sql_write_read.QueryMySQL()
    accessErpTable = 'company_organization'
    queryUserNumSQL = 'select user_number from %s' % accessErpTable
    queryUserNumInfo = _connMysql.read_table(accessErpTable, queryUserNumSQL, columns=['user_number'])
    _connMysql.close()
    return set(queryUserNumInfo['user_number'])


class ADers(object):
    """
    广告专员
    """

    @staticmethod
    def is_ADers(loginName, caseSensitive=True):
        """
        是否是广告专员
        """
        # 验证输入的类型是否正确
        type_verify.TypeVerify.type_valid(loginName, (str, set, list))
        type_verify.TypeVerify.type_valid(caseSensitive, bool)
        # 判断是否是广告专员
        Namecolumn = 'name'
        ADersLoginInfo = query_frequently_table_info.query_ADers_login_user(columns=[Namecolumn])
        if caseSensitive is True:
            ADersLoginName = set(ADersLoginInfo[Namecolumn])
            if isinstance(loginName, str):
                return loginName in ADersLoginName
            else:
                return {oneName: oneName in ADersLoginName for oneName in loginName}
        else:
            ADersLoginName = set([name_.lower() for name_ in ADersLoginInfo[Namecolumn]])
            if isinstance(loginName, str):
                return loginName.lower() in ADersLoginName
            else:
                return {oneName: oneName.lower() in ADersLoginName for oneName in loginName}


class ADersCompanyAccessManage(ADers):
    """
    广告专员的公司权限管理
    """
    # 广告专员权限数据库
    DATABASE__ = 'team_authority'
    # 公司名索引表
    COMPANY_NAME_INDEX__ = 'company_name'
    # 广告专员基本信息表
    ADERS_BASE_INFO__ = 'user_base_info'
    # 广告专员负责公司信息表
    COMPANT_CHARGED_INFO__ = 'company_station_charged'

    def __init__(self, loginName):
        # 判断是否是广告专员
        type_verify.TypeVerify.type_valid(loginName, (str, list, set))
        isADersInfo = ADers.is_ADers(loginName)
        if isinstance(loginName, str):
            if not isADersInfo:
                raise ValueError(f'{loginName} is not ADers.')
        else:
            notAders = {name for name, validADerInfo in isADersInfo.items() if not validADerInfo}
            if notAders:
                raise ValueError(f'{",".join(notAders)} is not ADers.')
        self.loginName = loginName

    def charged_company(self):
        """负责的公司"""
        _connMysql = sql_write_read.QueryMySQL(database=self.DATABASE__)
        companyNameInfo = _connMysql.read_table(self.COMPANY_NAME_INDEX__)
        companyChargedInfo = _connMysql.read_table(self.COMPANT_CHARGED_INFO__)
        allADersBaseInfo = _connMysql.read_table(self.ADERS_BASE_INFO__)
        _connMysql.close()
        if isinstance(self.loginName, str):
            queryADersBaseInfo = allADersBaseInfo[allADersBaseInfo['login_name'] == self.loginName]
        else:
            queryADersBaseInfo = allADersBaseInfo[allADersBaseInfo['login_name'].isin(self.loginName)]
        chargedCompany = pd.merge(queryADersBaseInfo, companyChargedInfo, left_on='id', right_on='user_id', how='inner')
        chargedCompany = pd.merge(chargedCompany, companyNameInfo, left_on='company_id', right_on='id')
        companyColumn = 'short_name'
        if isinstance(self.loginName, str):
            return set(chargedCompany[companyColumn])
        else:
            chargedCompanyGroupByLoginName = chargedCompany.groupby('login_name')
            chargedCompanyADersInfo = {name: nameInfo for name, nameInfo in chargedCompanyGroupByLoginName}
            return {name: set(
                chargedCompanyADersInfo.get(name)[companyColumn]) if name in chargedCompanyADersInfo.keys() else {} for
                    name in self.loginName}


class SellerManager(object):
    """
    销售管理：
    1.销售的归属,属于易佰还是外公司
    2.销售的职位,销售的管辖人员
    3.销售负责的站点
    """

    def __init__(self, workNum, companyBelong='yibai', host='172.16.128.240', port=3306, username='marmot',
                 password='marmot123',
                 db='team_station', orgTable='company_organization', sellerStationTable='erp_seller_account',
                 otherAccessDistributionTable='salers_shop'):
        """
        :param workNum:str
            工号或是花名,当isNickname为真是则是花名(易佰用工号,外公司用花名)
        :param workNumIsNickname: bool
            workNum是登录名:外公司是账号名,易佰是工号
        :param companyBelong: str
            yibai or other
        :param host:
        :param port:
        :param username:
        :param password:
        :param db:
        :param orgTable: str
            公司的组织架构表
        :param sellerStationTable:str
            销售权限表:数据来源是销售上传到erp中的业绩表
        :param otherAccessDistributionTable:str
            自行上传的权限表,主要是外公司
        """
        if companyBelong is None or len(companyBelong) == 0:
            raise ValueError(f'公司名不能为空。')
        if workNum is None or len(workNum) == 0:
            raise ValueError(f'工号不能为空。')
        if not isinstance(companyBelong, str):
            raise TypeError(f'公司名类型错误。')
        if not isinstance(workNum, str):
            raise TypeError(f'工号类型错误。')
        self.companyBelong = companyBelong
        self.workNum = workNum
        self.companyOrgInfo = query_frequently_table_info.query_company_organization()
        self.baseInfoColumns = ['department', 'group', 'short_name', 'nickname', 'station']
        # 获取自定义的权限配置
        if workNum and (companyBelong != 'yibai'):
            # 获取花名
            _connMysql = sql_write_read.QueryMySQL()
            queryTable = 'saler_login'
            querySql = "select nickname from `%s` where username = '%s' " % (queryTable, workNum)
            queryRealNameDf = _connMysql.read_table(queryTable, querySql, columns=['nickname'])
            if len(queryRealNameDf.index) != 0:
                nickname = queryRealNameDf['nickname'].values[0]
            else:
                nickname = None
            sellerShopTable = otherAccessDistributionTable
            # 判断是不是大部负责人
            querySqlDepartmentManager = "select * from `%s` where departmentcharge = '%s'" % (
                sellerShopTable, nickname)
            queryResultDepartmentManager = _connMysql.read_table(sellerShopTable, sql=querySqlDepartmentManager)
            if len(queryResultDepartmentManager.index) != 0:
                _connMysql.close()
                queryResultDepartmentManager.drop_duplicates(subset=['station'], keep='last', inplace=True)
                queryResultDepartmentManager['short_name'] = ''
                queryResultDepartmentManager['station'] = [process_station.standardStation(station) for station in
                                                           queryResultDepartmentManager['station']]
                # queryResultDepartmentManager.rename(columns={'station':'short_name'},inplace=True)
                self.otherAccessInfo = queryResultDepartmentManager
            else:
                # 判断是不是小组负责人或是小组负责人
                querySqlOwner = "select * from %s where groupcharge = '%s' or nickname = '%s'" % (
                    sellerShopTable, nickname, nickname)
                queryResultOwner = _connMysql.read_table(sellerShopTable, querySqlOwner)
                _connMysql.close()
                if len(queryResultOwner.index) != 0:
                    queryResultOwner.drop_duplicates(subset=['station'], keep='last', inplace=True)
                    queryResultOwner['short_name'] = ''
                    queryResultOwner['station'] = [process_station.standardStation(station) for station in
                                                   queryResultOwner['station']]
                    self.otherAccessInfo = queryResultOwner
                else:
                    self.otherAccessInfo = pd.DataFrame(columns=self.baseInfoColumns)

    def is_in_company_origanization(self):
        """
        是否拥有广告部的权限，没有需要在北森系统中配置
        :return:
        """
        if self.companyBelong != 'yibai':
            raise ValueError('只有易佰的才能判断是不是属于公司架构')
        allUserNum = set(self.companyOrgInfo['user_number'])
        if self.workNum in allUserNum:
            return True
        else:
            return False

    def chargedStaff(self):
        """
        易佰的组织架构走公司接口获取的
        其他公司组织架构走自己上传的
        :return:list
        """
        if self.companyBelong == 'yibai':
            pos, depName = self.positionNDepartment()
            """易佰的人员"""
            if pos in ['seller', '']:
                return [self.workNum]
            else:
                # 获取销售的花名
                nickNameInfo = query_frequently_table_info.query_seller_name_bridge(columns=['work_number', 'nickname'])
                nickNameInfo['work_number'] = [workNum.upper() for workNum in nickNameInfo['work_number']]
                nickName = list(nickNameInfo['nickname'][nickNameInfo['work_number'] == self.workNum.upper()])
                if nickName:
                    nickName = nickName[0]
                else:
                    nickName = 'None'
                chargedStaffInfo = self.companyOrgInfo[
                    (self.companyOrgInfo['dep_path'].str.contains(depName, regex=False))
                    | (self.companyOrgInfo['dep_path'].str.contains(nickName, regex=False))]
                if len(chargedStaffInfo.index) == 0:
                    return [self.workNum]
                else:
                    return list(set(chargedStaffInfo['user_number']))
        else:
            if len(self.otherAccessInfo) != 0:
                return set(self.otherAccessInfo['nickname'])
            else:
                return []

    def chargedStations(self, shortName=False):
        """
        负责的站点
        :return:list of stations name
        """
        if self.companyBelong == 'yibai':
            chargedStaff = self.chargedStaff()
            if len(chargedStaff) == 0:
                return []
            allSellerStationInfo = query_frequently_table_info.query_seller_stations()
            queryStationInfo = allSellerStationInfo[allSellerStationInfo['job_number'].isin(chargedStaff)]
            if queryStationInfo is None:
                return []
            else:
                if shortName is True:
                    return list(set(queryStationInfo['short_name']))
                accountShortNameInfo = query_frequently_table_info.query_amazon_account_short_name(
                    columns=['account_name', 'short_name'])
                accountName = accountShortNameInfo['account_name'][
                    accountShortNameInfo['short_name'].isin(list(set(queryStationInfo['short_name'])))]
                return [process_station.standardStation(station) for station in set(accountName)]
        else:
            if len(self.otherAccessInfo) != 0:
                return [process_station.standardStation(station) for station in set(self.otherAccessInfo['station'])]
            else:
                return []

    def baseInfo(self):
        """
        销售的基本信息,包括大部名,小组名,销售花名,站点简称

        :return:pd.DataFrame
            columns = [department,group,short_name,nickname]
        """
        if self.companyBelong != 'yibai':
            if len(self.otherAccessInfo.index) == 0:
                return pd.DataFrame(columns=self.baseInfoColumns)
            return self.otherAccessInfo[self.baseInfoColumns]
        else:
            chargedSellers = self.chargedStaff()
            if chargedSellers:
                allSellerStationInfo = sql_write_read.QueryMySQL.fast_read_table('erp_seller_account',
                                                                                 columns=['part', 'group', 'name',
                                                                                          'job_number', 'short_name'])
                sellerBaseInfo = allSellerStationInfo[['part', 'group', 'name', 'short_name']][
                    allSellerStationInfo['job_number'].isin(chargedSellers)]
                stationInfo = query_frequently_table_info.query_amazon_account_short_name(
                    columns=['account_name', 'short_name'])
                sellerBaseInfo = pd.merge(sellerBaseInfo, stationInfo, how='left', on='short_name')
                sellerBaseInfo.rename(columns={'part': 'department', 'name': 'nickname', 'account_name': 'station'},
                                      inplace=True)
                sellerBaseInfo.drop_duplicates(subset=['short_name'], keep='last', inplace=True)
                sellerBaseInfo = sellerBaseInfo.dropna(subset=['station', 'short_name'])
                sellerBaseInfo.reset_index(drop=True, inplace=True)
                return sellerBaseInfo
            else:
                return pd.DataFrame(columns=self.baseInfoColumns)

    def positionNDepartment(self):
        """
        销售的职位
        :return:list
        """
        if self.companyBelong == 'yibai':
            queryInfo = self.companyOrgInfo[self.companyOrgInfo['user_number'] == self.workNum]
            if queryInfo is None:
                return ['', '']
            managerSignWord = ['经理', '主管', '组长', '部门助理']
            if len(queryInfo.index) != 0:
                posName = queryInfo['job_name'].values[0]
                depName = queryInfo['department_name'].values[0]
                for signWord in managerSignWord:
                    if signWord in posName:
                        return ['manager', depName]
                return ['seller', depName]
            else:
                return ['', '']
        else:
            return [None, None]

    def nickName(self):
        if self.companyBelong != 'yibai':
            _connMysql = sql_write_read.QueryMySQL()
            queryTable = 'saler_login'
            querySql = "select nickname from `%s` where username = '%s' " % (queryTable, self.workNum)
            queryRealNameDf = _connMysql.read_table(queryTable, querySql, columns=['nickname'])
            if len(queryRealNameDf.index) != 0:
                return queryRealNameDf['nickname'].values[0]
            else:
                return None
        else:
            return sellerNameBridge(self.workNum, 'nickName')

    def realName(self):
        if self.companyBelong != 'yibai':
            _connMysql = sql_write_read.QueryMySQL()
            queryTable = 'saler_login'
            querySql = "select name from `%s` where username = '%s' " % (queryTable, self.workNum)
            queryRealNameDf = _connMysql.read_table(queryTable, querySql, columns=['name'])
            if len(queryRealNameDf.index) != 0:
                return queryRealNameDf['name'].values[0]
            else:
                return None
        else:
            return sellerNameBridge(self.workNum, 'realName')

    def fiveReportStatus(self):
        '''销售上传五表的情况'''
        # 添加某站点五表情况
        chargedStation = set(self.chargedStations())
        _connRedis = public_function.Redis_Store(db=1)
        fiveReportSignWord = 'api_request_files'
        fiveReportRedisKey = [key for key in _connRedis.keys() if fiveReportSignWord in key]
        fiveReportStationTypeDict = {}
        for key in fiveReportRedisKey:
            RedisStationName = key[len(fiveReportSignWord) + 1:len(key) - 3]
            if process_station.standardStation(RedisStationName) not in chargedStation:
                continue
            type = key[-2:]
            fiveReportStationTypeDict.setdefault(RedisStationName, []).append(type)
            # 将站点报表redis键值转化为字符串拼接
        _connRedis.close()
        return fiveReportStationTypeDict

    def stations_upload_status(self, interval_days=6):
        """
        需要处理的站点
        :return:
        """
        chargedAllStations = set(self.chargedStations())
        return process_station.Station.seller_upload_status(chargedAllStations)

    def need_process_station_base_info(self):
        '''需要处理的站点基本信息'''
        needUploadFileSignWord = '传表！'
        needProcessStationsDict = self.stations_upload_status()
        needProcessStations = {station for station, stationStatus in needProcessStationsDict.items() if
                               stationStatus == needUploadFileSignWord}
        baseInfo = self.baseInfo()
        exportColumns = ['nickname', 'short_name', 'station']
        return baseInfo[exportColumns][baseInfo['station'].isin(needProcessStations)]

    @staticmethod
    def sellerDepart(sellerNum):
        """
        通过工号查询销售所属的部门
        :param seller:string or list of string or set of string
        :return:
        """
        #todo 通过工号查询部门
        type_verify.TypeVerify.type_valid(sellerNum,(set,list,str))
        companyOrz = query_frequently_table_info.query_company_organization(columns=['user_number', 'department_name'])
        sellerNumDepartDict = {sellerNum:depart for sellerNum,depart in zip(companyOrz['user_number'],companyOrz['department_name'])}
        if isinstance(sellerNum,str):
            return sellerNumDepartDict.get(sellerNum,None)
        if isinstance(sellerNum,(set,list)):
            return {Num:sellerNumDepartDict.get(str(Num).lower(),None) for Num in sellerNum if isinstance(Num,(str,float,int))}



class SellerSkuPerformance(SellerManager):

    def __init__(self, username, companyBelong='yibai'):
        SellerManager.__init__(self, username, companyBelong=companyBelong)
        self.username = username

    @staticmethod
    def access_to_search_sku_performan(usernum):
        """
        是否拥有可以查询搜索sku表现的权限
        :param usernum:
        :return:
        """
        if not isinstance(usernum, str):
            return False
        accessInfo = query_frequently_table_info.query_access_seller_search_sku_performance()
        accessNum = set([str(num).lower() for num in accessInfo['work_num']])
        if usernum.lower() in accessNum:
            return True
        else:
            return False

    @classmethod
    def station_short_name(cls, usernum):
        if not cls.access_to_search_sku_performan(usernum):
            return []
        accessInfo = query_frequently_table_info.query_access_seller_search_sku_performance()
        accessInfo['work_num'] = [num.lower() for num in accessInfo['work_num']]
        usernum = usernum.lower()
        accessInfo['work_num'] = [num.lower() for num in accessInfo['work_num']]
        return set(accessInfo['short_name'][accessInfo['work_num'] == usernum])

    @classmethod
    def station_name(cls, usernum):
        if not cls.access_to_search_sku_performan(usernum):
            return []
        stations_short_name = cls.station_short_name(usernum)
        if not stations_short_name:
            return []
        stationNameInfo = query_frequently_table_info.query_amazon_account_short_name()
        return set(stationNameInfo['account_name'][stationNameInfo['short_name'].isin(stations_short_name)])


def sellerNameBridge(queryInfo, searchType, queryType='workNum'):
    """

    :param queryInfo:str,list
    :param searchType: str
        one of workNum, nickName,realName
    :param queryType: str
        one of workNum, nickName
    :return:
    """
    if queryInfo is None or len(queryInfo) == 0:
        raise ValueError(f'查询信息不能为空。')
    if not isinstance(queryInfo, (str, list, set)):
        raise TypeError(f'查询信息错误。')
    queryTypes = ['workNum', 'nickName']
    searchTypes = ['workNum', 'nickName', 'realName']
    if searchType == queryType:
        return queryInfo
    if queryType not in queryTypes:
        raise ValueError(f'查询的输入类型必须为{",".join(queryTypes)}的一种')
    if searchType not in searchTypes:
        raise ValueError(f'查询的输入类型必须为{",".join(searchTypes)}的一种')
    sellerNameInfo = query_frequently_table_info.query_seller_name_bridge()
    columnsDict = {'workNum': 'work_number', 'nickName': 'nickname', 'realName': 'real_name'}
    if isinstance(queryInfo, str):
        searchInfo = sellerNameInfo[columnsDict[searchType]][sellerNameInfo[columnsDict[queryType]] == queryInfo]
        if len(searchInfo) > 0:
            return searchInfo.values[-1]
        else:
            return
    if isinstance(queryInfo, (list, set)):
        searchInfo = sellerNameInfo[[columnsDict[queryType], columnsDict[searchType]]][
            sellerNameInfo[columnsDict[queryType]].isin(queryInfo)]
        searchInfo.drop_duplicates(subset=[columnsDict[queryType]], keep='last', inplace=True)
        return {queryInfo: searchInfo for queryInfo, searchInfo in
                zip(searchInfo[columnsDict[queryType]], searchInfo[columnsDict[searchType]])}


if __name__ == '__main__':
    # print(SellerManager('14583').chargedStations())
    # print(SellerManager('14583').chargedStaff())
    # a = ADersCompanyAccessManage('人工智能6').charged_company()
    # print(a)
    print(SellerManager.sellerDepart([14583,'C10654','123']))
