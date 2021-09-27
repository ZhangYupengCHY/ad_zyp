import io
import os
import re
import zipfile
from datetime import datetime
import pandas as pd


def upload_show_monthly(request):
    """
    上传以及显示月数据
    :param request:
    :return:
    """
    pstSqlTable = 'ali_express_monthly_super'
    storeSqlTable = 'ali_express_monthly_alliance'
    monthlyDefaultType = '直通车'
    if (request.method == 'POST') and ('monthly_type' in request.POST):
        monthlyDefaultType = request.POST.get('monthly_type')
    if (request.method == "POST") and ('myfile' in request.FILES):
        month_file_zipped = request.FILES.get('myfile', None)
        if month_file_zipped is None:
            print('None')
        # 首先判断是否为rar文件(如果前端做了判断,这里就没有不要再次做判断)
        month_data_path_name = month_file_zipped.name
        myFile_name = month_file_zipped.name
        upload_zipped_file_type = os.path.splitext(month_file_zipped.name)[1]
        zipped_file_sign = '.zip'
        if upload_zipped_file_type.lower() != zipped_file_sign:
            error_msg = '{}不是.zip文件,请上传.zip压缩文件.'.format(myFile_name)
            print(error_msg)

        # 对上传的文件名进行检验
        try:
            [(year,month,manager)] = re.findall('(.*)年(.*)月(.*)',myFile_name[:-4])
        except:
            error_msg = '格式为:2021年3月姓名'
            print(error_msg)
        if (int(year) not in range(2021,2200)) or (int(month) not in range(0,13)):
            error_msg = '月数据输入的年份或是月份不对'
            print(error_msg)
        # 全部的速卖通人员
        allAliexpressManager = {'张峰','曹思楚','谢雨欣','贺正扬'}
        if manager not in allAliexpressManager:
            error_msg = f'你输入的人名:{manager}不在库中,请核查后再次上传'
            print(error_msg)

        now_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 分别计算直通车和联盟月数据
        aliexpressFileSign = {'店铺':'生意参谋','联盟':'store','直通车':'账户报告'}

        def decode_zipfile_name(zip_list):
            new_zip_list = []
            for zip_file in zip_list:
                try:
                    zip_file = zip_file.encode('cp437').decode('gbk')
                except:
                    zip_file = zip_file.encode('utf-8').decode('utf-8')
                new_zip_list.append(zip_file)
            return new_zip_list

        # 计算月数据
        month_file = zipfile.ZipFile(month_file_zipped, "r")
        all_month_files = month_file.namelist()
        # print(all_month_files)

        allStation = set([path.strip('/').split('/')[-1] for path in all_month_files if (path.endswith('/')) and ('/' in path.strip('/'))])
        allStation = [station.lower() for station in allStation]
        # aliexpressAllAccount = [account.lower() for account in frequrntly_params.ALIEXPRESS_ACCOUNT]
        # if not set(allStation).issubset(set(aliexpressAllAccount)):
        #     error_msg = f"有不存在的站点:{','.join(set(allStation)-set(aliexpressAllAccount))}"
        #     print(error_msg)
        allStationPathDict = {}
        for station in allStation:
            allStationPathDict[station] = set([path for path in all_month_files if (not path.endswith('/')) and (station in path.lower())])
        #计算全部站点的直通车和联盟数据
        allStoreDf = pd.DataFrame()
        allPstDf = pd.DataFrame()
        print(allStationPathDict)
        for station,stationPath in allStationPathDict.items():
            stationShopPath = [file for file in stationPath if aliexpressFileSign['店铺'].encode('gbk').decode('cp437') in file]
            print(stationShopPath)
            if not stationShopPath:
                message = f'站点{station}店铺销量表有问题.'
                print(error_msg)
            else:
                stationShopPath = stationShopPath[0]
                stationShopInfo = pd.read_excel(io.BytesIO(month_file.read(stationShopPath)))
                if len(stationShopInfo.index) == 0:
                    continue
                stationShopInfo = init_monthly_df(station,stationShopInfo,'shop')
            # 计算站点联盟月数据
            stationStorePath = [file for file in stationPath if aliexpressFileSign['联盟'] in file]
            if stationStorePath:
                stationStorePath = stationStorePath[0]
                stationStoreInfo = pd.read_csv(io.BytesIO(month_file.read(stationStorePath)))
                stationStoreInfo = init_monthly_df(station,stationStoreInfo,'store')
            else:
                storeNeedColumns = ["访客数", "支付订单数",'支付金额', "预计佣金",'acos','ROI']
                stationStoreInfo = pd.Series([0]*len(storeNeedColumns),index=storeNeedColumns)
            stationStoreMonthlyInfo = calc_store_month(station,stationShopInfo,stationStoreInfo)
            if isinstance(stationStoreMonthlyInfo,str):
                message = stationStoreMonthlyInfo
                print(message)
            else:
                allStoreDf = pd.concat([allStoreDf,pd.DataFrame([stationStoreMonthlyInfo],columns=stationStoreMonthlyInfo.index)])

            # 计算站点直通车月数据
            stationPstPath = [file for file in stationPath if aliexpressFileSign['直通车'].encode('gbk').decode('cp437') in file]
            if stationPstPath:
                stationPstPath = stationPstPath[0]
                stationPstInfo = pd.read_excel(io.BytesIO(month_file.read(stationPstPath)))
                stationPstInfo = init_monthly_df(station,stationPstInfo,'pst')
                # print(stationPstInfo)
            else:
                pstNeedColumns = ["点击量", "P4P下单数" , '花费', "P4P下单金额","平均cpc", "直通车转化率",'acos', "ROI"]
                stationPstInfo = pd.Series([0]*len(pstNeedColumns),index=pstNeedColumns)
            stationPstMonthlyInfo = calc_pst_month(station,stationShopInfo,stationPstInfo)
            if isinstance(stationPstMonthlyInfo,str):
                message = stationPstMonthlyInfo
                print(message)
            else:
                allPstDf = pd.concat([allPstDf,pd.DataFrame([stationPstMonthlyInfo],columns=stationPstMonthlyInfo.index)])


        print("直通车",allPstDf.shape)
        print("联盟",allStoreDf.shape)
        # 将站点信息上传到数据库中
        # 更新这个人这个月的月数据,首先是删除,然后上传
        # 首先更新直通车
        if len(allPstDf.index) != 0:
            #添加时间和负责人
            allPstDf['updatetime'] = datetime.now().replace(microsecond=0)
            allPstDf['ad_manager'] = manager
            allPstDf['year'] = year
            allPstDf['month'] = month

            pstDeleteSql = "delete from %s where `year`=%s and `month`=%s and `ad_manager`= '%s'"%(pstSqlTable,year,month,manager)
            try:
                select_update_sql.commit_sql(pstDeleteSql)
                sql_write_read.to_table_append(allPstDf,pstSqlTable)
            except Exception as e:
                message = f'上传站点{station}月数据失败。原因:{e}.'
                return render(request, (BASE_DIR + '/aliexpress/templates/monthly.html').replace('\\', '/'), locals())
        # 更新联盟
        if len(allStoreDf.index) != 0:
            #添加时间和负责人
            allStoreDf['updatetime'] = datetime.now().replace(microsecond=0)
            allStoreDf['ad_manager'] = manager
            allStoreDf['year'] = year
            allStoreDf['month'] = month

            storeDeleteSql = "delete from %s where `year`=%s and `month`=%s and `ad_manager`= '%s' "%(storeSqlTable,year,month,manager)
            try:
                select_update_sql.commit_sql(storeDeleteSql)
                sql_write_read.to_table_append(allStoreDf,storeSqlTable)
            except Exception as e:
                message = f'上传站点{station}月数据失败。原因:{e}.'
                return render(request, (BASE_DIR + '/aliexpress/templates/monthly.html').replace('\\', '/'), locals())
        message = f'成功上传:直通车站点:{len(allPstDf.index)};' \
                  f'联盟站点:{len(allStoreDf.index)}. '
    # 加载站点月数据
    _connMysql = sql_write_read.QueryMySQL()
    if monthlyDefaultType == '直通车':
    # 直通车月数据
        managerAllPstMonthlySql = "select * from `%s`"%pstSqlTable
        managerAllPstMonthlyInfo = _connMysql.read_table(pstSqlTable,sql = managerAllPstMonthlySql)
        managerAllPstMonthlyInfo = public_function.init_df(managerAllPstMonthlyInfo,change_columns_type={'int':['支付金额', '支付主订单数', '商品加购人数', '点击量',
           '花费', 'P4P下单数', 'P4P下单金额', '广告销售额占比'],'float':['加购订单比','平均cpc','ROI','店铺转化率']})
        managerAllPstMonthlyInfo.rename(columns={'ad_manager':'广告接手人'},inplace=True)
        # managerAllPstMonthlyInfo[['加购订单比','平均cpc','ROI','店铺转化率']] = managerAllPstMonthlyInfo[['加购订单比','平均cpc','ROI','店铺转化率']].applymap(lambda x:round(x,2) if isinstance(x,(int,float)) else 0)
        managerAllPstMonthlyInfo['年月'] = [str(year).zfill(4)+str(month).zfill(2) for year,month in zip(managerAllPstMonthlyInfo['year'],managerAllPstMonthlyInfo['month'])]
        # 添加访问数
        managerAllPstMonthlyInfo['访客数'] = managerAllPstMonthlyInfo['支付主订单数']/managerAllPstMonthlyInfo['店铺转化率']
        managerAllPstMonthlyInfo['访客数'] = managerAllPstMonthlyInfo['访客数'].replace([np.nan,np.inf,-np.inf],0)
        managerAllPstMonthlyInfo['花费(美元)'] = managerAllPstMonthlyInfo['花费']*change_current['CN']
        # 计算求和项
        sumColumns = ['支付金额', '支付主订单数','商品加购人数', '点击量', '花费', '花费(美元)','P4P下单数', 'P4P下单金额','访客数']
        countColumn = ['账户名']
        aggSumExpression = {col:'sum' for col in sumColumns}
        aggCountExpression = {col:'count' for col in countColumn}
        aggSumExpression.update(aggCountExpression)
        managerAllPstMonthlyInfoSum = managerAllPstMonthlyInfo.groupby(by=['年月', '广告接手人']).agg(aggSumExpression).reset_index()
        managerAllPstMonthlyInfoSum['加购订单比'] = managerAllPstMonthlyInfoSum['商品加购人数'] / managerAllPstMonthlyInfoSum[
            '支付主订单数']
        managerAllPstMonthlyInfoSum['直通车转化率'] = managerAllPstMonthlyInfoSum['P4P下单数'] / \
                                            managerAllPstMonthlyInfoSum['点击量']
        managerAllPstMonthlyInfoSum['店铺转化率'] = managerAllPstMonthlyInfoSum['支付主订单数'] / \
                                               managerAllPstMonthlyInfoSum['访客数']
        managerAllPstMonthlyInfoSum['平均cpc(人民币)'] = managerAllPstMonthlyInfoSum['花费'] / managerAllPstMonthlyInfoSum[
            '点击量']

        managerAllPstMonthlyInfoSum['acos'] = managerAllPstMonthlyInfoSum['花费(美元)'] / managerAllPstMonthlyInfoSum['P4P下单金额']
        managerAllPstMonthlyInfoSum['广告销售额占比'] = managerAllPstMonthlyInfoSum['P4P下单金额'] / managerAllPstMonthlyInfoSum[
            '支付金额']
        managerAllPstMonthlyInfoSum['ROI'] = 1/managerAllPstMonthlyInfoSum['acos']
        for col in ['acos', '广告销售额占比', 'ROI', '加购订单比', '平均cpc(人民币)', '直通车转化率','店铺转化率']:
            managerAllPstMonthlyInfoSum[col] = managerAllPstMonthlyInfoSum[col].replace([np.nan, np.inf, -np.inf],0)
        intColumns = ['支付金额', '支付主订单数', '商品加购人数', '点击量',
           '花费(美元)', 'P4P下单数', 'P4P下单金额']
        percentColumns = ['acos', '广告销售额占比', '直通车转化率','店铺转化率']
        floatColumns = ['ROI','平均cpc(人民币)','加购订单比']
        managerAllPstMonthlyInfoSum = public_function.init_df(managerAllPstMonthlyInfoSum,change_columns_type={'int': intColumns,
                                                                                 'float': floatColumns,
                                                                                 'percent': percentColumns})
        managerAllPstMonthlyInfoSum[floatColumns] = managerAllPstMonthlyInfoSum[floatColumns].applymap(lambda x:round(x,2))

        #重命名列名
        managerAllPstMonthlyInfoSum.rename(columns={'账户名':'接手店铺数'},inplace=True)


        managerAllPstMonthlyShowColumns=['年月','广告接手人','接手店铺数', '支付金额', '支付主订单数', '商品加购人数', '加购订单比', '店铺转化率', '直通车转化率', '点击量',
           '花费(美元)', 'P4P下单数', 'P4P下单金额', '平均cpc(人民币)', 'acos', '广告销售额占比', 'ROI']
        managerAllPstMonthlyInfoSum.sort_values(by=['年月','广告接手人'],ascending=[False,False],inplace=True)
        showDf = managerAllPstMonthlyInfoSum[managerAllPstMonthlyShowColumns]
        showColumns = managerAllPstMonthlyShowColumns
    elif monthlyDefaultType == '联盟':
    # 联盟月数据
        managerAllStoreMonthlySql = "select * from `%s`"%storeSqlTable
        # managerAllStoreMonthlySql = "select * from `%s` where ad_manager = '%s'"%(storeSqlTable,manager)
        managerAllStoreMonthlyInfo = _connMysql.read_table(storeSqlTable,sql = managerAllStoreMonthlySql)
        managerAllStoreMonthlyInfo = public_function.init_df(managerAllStoreMonthlyInfo,change_columns_type={'int':['访客数','支付订单数', '支付金额', '店铺支付金额','预计佣金'],'float':['ROI']})
        # managerAllStoreMonthlyInfo['ROI'] = managerAllStoreMonthlyInfo['ROI'].apply(lambda x:round(x,2) if isinstance(x,(int,float)) else 0)
        managerAllStoreMonthlyInfo['联盟花费'] = managerAllStoreMonthlyInfo['支付金额']/managerAllStoreMonthlyInfo['ROI']
        managerAllStoreMonthlyInfo['联盟花费'] = managerAllStoreMonthlyInfo['联盟花费'].replace([np.nan,np.inf,-np.inf],0)
        managerAllStoreMonthlyInfo['年月'] = [str(year).zfill(4)+str(month).zfill(2) for year,month in zip(managerAllStoreMonthlyInfo['year'],managerAllStoreMonthlyInfo['month'])]
        managerAllStoreMonthlyInfo.rename(columns={'ad_manager': '广告接手人','支付订单数':'联盟订单数','支付金额':'联盟销售额(美元)','店铺支付金额':'店销额(美元)'}, inplace=True)
        # 计算求和项
        sumColumns = ['访客数', '联盟订单数', '联盟销售额(美元)','联盟花费', '店销额(美元)', '预计佣金']
        countColumn = ['账户名']
        aggSumExpression = {col: 'sum' for col in sumColumns}
        aggCountExpression = {col: 'count' for col in countColumn}
        aggSumExpression.update(aggCountExpression)
        managerAllStoreMonthlyInfoSum = managerAllStoreMonthlyInfo.groupby(by=['年月','广告接手人']).agg(aggSumExpression).reset_index()

        managerAllStoreMonthlyInfoSum['acos'] = managerAllStoreMonthlyInfoSum['联盟花费']/managerAllStoreMonthlyInfoSum['联盟销售额(美元)']
        managerAllStoreMonthlyInfoSum['广告销售额占比'] = managerAllStoreMonthlyInfoSum['联盟销售额(美元)']/managerAllStoreMonthlyInfoSum['店销额(美元)']
        managerAllStoreMonthlyInfoSum['ROI'] = managerAllStoreMonthlyInfoSum['联盟销售额(美元)']/managerAllStoreMonthlyInfoSum['联盟花费']
        for col in ['acos','广告销售额占比','ROI']:
            managerAllStoreMonthlyInfoSum[col] = managerAllStoreMonthlyInfoSum[col].replace([np.nan,np.inf,-np.inf],0)
        del managerAllStoreMonthlyInfoSum['联盟花费']
        percentColumns = ['acos','广告销售额占比']
        intColumns = ['访客数', '联盟订单数', '联盟销售额(美元)', '店销额(美元)', '预计佣金']
        floatColumns = ['ROI']
        managerAllStoreMonthlyInfoSum = public_function.init_df(managerAllStoreMonthlyInfoSum,change_columns_type={'int':intColumns,'float':floatColumns,'percent':percentColumns})
        managerAllStoreMonthlyInfoSum['ROI'] = [round(roi,2) for roi in managerAllStoreMonthlyInfoSum['ROI']]

        # 重命名
        managerAllStoreMonthlyInfoSum.rename(columns={'账户名':'接手店铺数'},inplace=True)
        #显示的列
        managerAllStoreMonthlyShowColumns = ['年月','广告接手人','接手店铺数','访客数', '联盟订单数', '联盟销售额(美元)','店销额(美元)', '预计佣金','acos','广告销售额占比','ROI']
        managerAllStoreMonthlyInfoSum.sort_values(by=['年月','广告接手人'],ascending=[False,False],inplace=True)
        showDf = managerAllStoreMonthlyInfoSum[managerAllStoreMonthlyShowColumns]
        showColumns = managerAllStoreMonthlyShowColumns

    _connMysql.close()
    return render(request, (BASE_DIR + '/aliexpress/templates/monthly.html').replace('\\', '/'),
                  locals())