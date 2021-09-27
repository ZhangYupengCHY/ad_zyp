"""
远程接口请求5表
"""


"""
同时执行多项任务:

"""
from datetime import datetime, timedelta

import rsa, requests, os, redis, zipfile, time, re, xlsxwriter

import json
import pandas as pd
import threading
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import paramiko as pm

from my_toolkit import chinese_check, sql_write_read, public_function, commonly_params, change_rate, process_files, \
    init_station_report,myZip,station_belong

import api_request_ac_ao_reports


"""
    通过接口请求站点5表,若5表缺失,则需要提醒销售去下载补充
    1.通过接口获取cp,br,st表的接口获取的报表日志,通过日志链接去请求站点报表;通过其他接口获取ao,ac表
    2.将报表获取的报表类型上传到redis中(站点类型以及日期),同时将缺失的部分同步显示到销售端
    3.销售将缺失的报表上传,同时将上传的站点报表信息上传到redis中
    广告报表redis有效时长为两天
    其他报表redis有效时长为四天
    只将距离5~14天的站点开放为销售下载报表
"""

@public_function.run_time
@public_function.loop_func(update_time=14)
def process_five_reports():

    def read_report_file_log(filePath):
        """
        读取报表请求日志:广告报表,br表,st表的格式一致
        只读取时间是昨天的日志
        :param filePath:
        :return:pd.DataFrame or None
        """
        # 首先判断日志文件是否存在
        if not os.path.isfile(filePath):
            return
        try:
            logFileData = pd.read_excel(filePath)
        except Exception as e:
            print(e)
            return
        if (logFileData is None) or (len(logFileData) == 0):
            return
        nowDate = datetime.now()
        lastDate = nowDate - timedelta(days=1)
        lastDateStr = lastDate.strftime('%Y-%m-%d')
        if 'task_run_time' not in logFileData.columns:
            return
        if logFileData['task_run_time'].dtype != 'object':
            logFileData['task_run_time'] = logFileData['task_run_time'].dt.strftime('%Y-%m-%d')
        return logFileData[logFileData['task_run_time'] == lastDateStr]
        # return logFileData

    def request_jwt():
        """
        通过账号和密码的方式获取token
        Parameters
        ----------
        url :
        jss :
        secret :

        Returns
        -------

        """
        url = "http://python2.yibainetwork.com/yibai/python/services/jwt/token?iss=&secret="
        iss = 'sz_sales_ad_data_analysis'
        secret = 'hjaq24.cdta91ldDaqlcdqkb'
        params = {'iss': iss, 'secret': secret}
        response = requests.get(url, params=params)
        return json.loads(response.content)['jwt']

    def station_operator_time(start_day=5, end_day=14):
        """
        获取only_station_info中操作时间在5~12天之间的站点以及接受了还没有操作的站点
        Parameters
        ----------
        start_day :
        end_day :

        Returns
        -------

        """
        # 连接数据库
        _connMysql = sql_write_read.QueryMySQL()
        chooseColumns = ['station', 'update_time', 'accept_time']
        tableName = 'only_station_info'
        erpStation = station_belong.yibai_account_from_erp()
        erpStationStr = sql_write_read.query_list_to_str(erpStation)

        # todo 只请求易佰的站点数据
        selectSql = "select station,update_time,accept_time from %s where `station` in (%s)" % (
        tableName, erpStationStr)
        # selectSql = "select station,update_time,accept_time from %s where (`ad_manger` not like '%%%%人工智能%%%%') and (`company` = '易佰')"% tableName
        queryInfo = _connMysql.read_table(tableName, selectSql, columns=chooseColumns)
        _connMysql.close()
        # 筛选时间段
        queryInfo.drop_duplicates(inplace=True)
        # 接受了但是还没有操作的站点
        takenNotOperatorStations = queryInfo['station'][
            (~pd.isna(queryInfo['accept_time'])) & (pd.isna(queryInfo['update_time']))]
        # 操作时间在5~12天的站点
        OperatorStationsInfo = queryInfo[~pd.isna(queryInfo['update_time'])]
        OperatorStationsInfo['update_time'] = pd.to_datetime(OperatorStationsInfo['update_time'],
                                                             format='%y-%m-%d %H:%M:%S').dt.date
        # 开始天数
        startDay = (datetime.now() - timedelta(start_day)).date()
        endDay = (datetime.now() - timedelta(end_day)).date()
        OperatorStationsInfo = OperatorStationsInfo['station'][
            (OperatorStationsInfo['update_time'] >= endDay) & (OperatorStationsInfo['update_time'] <= startDay)]
        onlyStations = set(takenNotOperatorStations) | set(OperatorStationsInfo)

        onlyStations = set([station.strip().replace('-','_').replace(' ','_').lower() for station in onlyStations])

        stationVaild = station_belong.get_account(status=1)

        return onlyStations & stationVaild

    def request_reports_log():
        """
        五表请求前一天的日志
        """
        jwt = request_jwt()
        #
        nowDate = datetime.now()
        nowDateStr = datetime.strftime(nowDate, '%Y-%m-%d')
        lastDate = nowDate - timedelta(days=1)
        lastDateStr = datetime.strftime(lastDate, '%Y-%m-%d')
        request_url = f"http://bi.yibainetwork.com:8000/bi/store/search_store_log?jwt={jwt}"
        remoteReportsNameDict = {'cp': '广告活动批量电子表格', 'br': '业务报告-详情页面上的销售量与访问量-根据ASIN', 'st': '广告报告-商品推广-搜索词-一览'}
        for fileType, remoteFileTypeSaveName in remoteReportsNameDict.items():
            params = {
                "jwt_id": "sz_sales_ad_data_analysis",
                'content': remoteFileTypeSaveName,
                'start_date': lastDateStr,
                'end_date': lastDateStr,

            }
            response = requests.post(request_url, json=params)
            if response.status_code != 200:
                print(f'{fileType}报表日志请求错误,错误代码:{response.status_code},时间:{datetime.now()}')
                continue
            # 文件流
            html_str = response.content
            # 日志保存文件夹
            logFolderName = 'log'
            logSaveBaseName = f'{fileType}.xlsx'
            logSaveDirname = os.path.join(REMOTE_SAVE_FOLDER, logFolderName)
            if not os.path.exists(logSaveDirname):
                os.mkdir(logSaveDirname)
            outPath = os.path.join(logSaveDirname, logSaveBaseName)
            with open(outPath, 'wb') as f:
                f.write(html_str)
            print(f'{fileType}日志请求完成:{datetime.now()}')

    def redis_store_report_remote_path():
        """
        将报表的远程路径存储到redis数据库中
        :return:
        """
        # 远程报表类型与对应文件夹名称
        remoteFileTypeFolderDict = {'广告报告-商品推广-搜索词-一览': 'Search term report',
                                    '业务报告-详情页面上的销售量与访问量-根据ASIN': 'BusinessReport',
                                    "广告活动批量电子表格": 'bulksheet'}
        request_reports_log()

        # 每天申请的站点名
        global requestFilesQueue
        global todayQueryStations
        requestFilesQueue = public_function.Queue()
        todayQueryStations = station_operator_time(start_day=5,end_day=14)
        # 获取存储日志,存储路径为cp.xlsx,br.xlsx,st.xlsx
        logSaveFolder = os.path.join(REMOTE_SAVE_FOLDER, 'log')
        filesSaveBasenameList = ['cp.xlsx', 'br.xlsx', 'st.xlsx']
        filesSavePathList = [os.path.join(logSaveFolder, basename) for basename in filesSaveBasenameList if
                             os.path.exists(os.path.join(logSaveFolder, basename))]
        allPathDf = pd.DataFrame()
        for path in filesSavePathList:
            pathDf = read_report_file_log(path)
            if pathDf is not None:
                allPathDf = pd.concat([allPathDf, pathDf])
        if len(allPathDf.index) == 0:
            requestFilesQueue.enqueue_items([])
            return
        # 将df转换为路径
        needColumns = set(['type', 'platform', 'task_run_time', 'marketplace', 'store_name', 'file_name'])
        if not needColumns.issubset(set(allPathDf.columns)):
            newColumns = needColumns - set(allPathDf.columns)
            newColumnsStr = ','.join(newColumns)
            raise ValueError(f'远程请求报表,日志文件中有缺失列:{newColumnsStr}')
        # 将df中报表类型转换为文件夹中存储的路径
        allPathDf['type'] = allPathDf['type'].replace(remoteFileTypeFolderDict)
        folderFirstDegree = '/shujubaobiao/report'
        #
        if allPathDf['task_run_time'].dtype != 'object':
            allPathDf['task_run_time'] = allPathDf['task_run_time'].dt.strftime('%Y-%m-%d')
        allPathList = [
            os.path.join(folderFirstDegree, type, platform, marketplace, store_name, task_run_time, file_name)
            for type, platform, marketplace, store_name, task_run_time, file_name in
            zip(allPathDf['type'], allPathDf['platform'], allPathDf['marketplace'], allPathDf['store_name'],
                allPathDf['task_run_time'], allPathDf['file_name'])]
        # todo 筛选出
        allPathListStation = {path:os.path.split(os.path.dirname(os.path.dirname(path)))[-1][:-3].lower().replace(' ',"_").replace('-','_')+'_'+os.path.split(os.path.dirname(os.path.dirname(os.path.dirname(path))))[-1].lower()
                              for path in set(allPathList)}
        filterStations = set([path for path,station in allPathListStation.items() if station in todayQueryStations])
        # todo 选择一个站点测试
        # filterStations = [path for path in filterStations if 'smandy' in path.lower()]
        if not filterStations:
            return

        # 将请求的文件路径列表存储到队列中

        requestFilesQueue.enqueue_items(filterStations)

    def file_redis_expire_time(file_time,expireDay=3):
        """
        文件存在redis的过期时间
        :param file_time:
        :param file_expire_time:
        :return:
        """
        if file_time is None:
            return
        if (not isinstance(file_time,datetime)):
            return
        expireDate = file_time+timedelta(days=expireDay)
        expireDatetime = datetime(expireDate.year,expireDate.month,expireDate.day)
        return int((expireDatetime-datetime.now()).total_seconds())

    @public_function.run_time
    def request_server_files(sftp,remoteDir):
        """
        远程请求下载广告三表
        :return:
        """
        if requestFilesQueue.empty():
            return
        remoteDir = remoteDir.replace('\\', '/')



        # 报表路径与类型字典
        reportFolderTypeDict = {'bulksheet': 'cp', 'BusinessReport': 'br', 'Search term report': 'st'}
        reportFolderTypeDictReverse = {value: key for key, value in reportFolderTypeDict.items()}
        reportType = [value for key, value in reportFolderTypeDict.items() if key in os.path.basename(remoteDir)]
        if reportType:
            reportType = reportType[0]
        else:
            reportType = ''
        # 判断是否存在
        # 连接
        # sftp = pm.SFTPClient.from_transport(tran)
        try:
            sftp.stat(remoteDir)
        except Exception as e:
            print(f'远程文件不存在:{remoteDir}')
            print(e)
        # 将文件保存
        stationName = os.path.basename(os.path.dirname(os.path.dirname(remoteDir)))[:-3] + '_' + \
                      re.findall('_[a-zA-Z]{2}_', os.path.basename(remoteDir))[-1][1:3]
        stationName = re.sub('[ -]', '_', stationName.strip()).lower()
        stationLocalSaveFolder = os.path.join(REMOTE_SAVE_FOLDER, stationName)
        stationLocalSavePath = os.path.join(stationLocalSaveFolder, os.path.basename(remoteDir))

        def _timeout(size, file_size):
            cost_time = int((datetime.now() - startDatetime).total_seconds())
            timeoutSeconds = 300
            if cost_time > timeoutSeconds:
                raise TimeoutError(f'请求报表超时。超过了{timeoutSeconds}秒.')

        try:
            if not os.path.exists(stationLocalSaveFolder):
                os.mkdir(stationLocalSaveFolder)
            startDatetime = datetime.now()
            print(f'{stationName} {reportType}表:开始请求.')
            # 删除本类型4天以前的报表
            yesterDayStr = (datetime.now() - timedelta(days=4)).strftime('%Y-%m-%d')
            [os.remove(os.path.join(stationLocalSaveFolder, file)) for file in os.listdir(stationLocalSaveFolder) if
             (reportFolderTypeDictReverse[reportType] in file)
             and (process_files.file_create_time(os.path.join(stationLocalSaveFolder, file))[:10] <= yesterDayStr)]
            sftp.get(remoteDir, stationLocalSavePath, callback=_timeout)
            print(f'{stationName} {reportType}表:请求完成.')
        except Exception as e:
            print(f'远程文件无法下载:{remoteDir}')
            print(e)
        # 若请求到的表为空则删除
        if (os.path.exists(stationLocalSavePath)) and (process_files.file_size(stationLocalSavePath) == 0):
            os.remove(stationLocalSavePath)
            print(f'{stationName} {reportType}表:数据为空,删除掉。')

    def server_tran():
        # 连接公司远程
        host = 'yiduan.yibai-it.com'
        port = 2022
        # 获取transport传输实例, sftp服务器 ip + 端口号
        tran = pm.Transport((host, port))
        return tran

    def server_sftp(tran):
        username = 'gg'
        password = 'yb@2021asd#!'
        # 连接ssh服务器, user + password
        tran.connect(username=username, password=password)
        sftp = pm.SFTPClient.from_transport(tran)
        return sftp

    @public_function.run_time
    def thread_request_server_files():
        """
        多线程下载远程报表到本地
        :return:
        """
        # 连接公司远程
        tran = server_tran()
        sftp = server_sftp(tran)
        print(f'一共请求:{requestFilesQueue.size()}个报表.')
        while 1:
            if requestFilesQueue.empty():
                # 关闭远程连接
                sftp.close()
                tran.close()
                print('完成今天全部报表请求.')
                break
            remoteDir = requestFilesQueue.dequeue()
            try:
                request_server_files(sftp, remoteDir)
            except Exception as e:
                time.sleep(100)
                print(e)
                # 重新连接
                try:
                    sftp.close()
                    tran.close()
                except Exception as e:
                    print(e)
                    print('无法先关闭sftp')
                    try:
                        tran.close()
                    except Exception as e:
                        print(e)
                        print('无法关闭tran')
                # 重新连接
                tran = server_tran()
                sftp = server_sftp(tran)
                requestFilesQueue.enqueue(remoteDir)
            # request_server_files(sftp, remoteDir)

    def save_five_reports():
        """
        将远程请求的5表同步保留到redis中与
        1.首先将站点报表类型:路径 键值对存储到redis
        2. 将文件压缩至到5表压缩的文件夹中
        :return:
        """
        if not os.path.isdir(REMOTE_SAVE_FOLDER):
            raise FileNotFoundError(f'{REMOTE_SAVE_FOLDER}.')
        # 1.存储到redis中
        # 获取文件中站点存在的文件类型
        # 路径中的类型关键词
        reportTypeSignDict = {'bulk': 'cp', 'business': 'br', 'search': 'st',
                              'active': 'ac', 'orders': 'ao'}
        stationTypeDict = {}
        # 删除两天以前的报表
        # 遍历远程保存文件夹,获取站点类型
        threeDayBeforeStr = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d %H:%M:%S')
        loopStationDictStartTime = datetime.now()
        for root, dirs, files in os.walk(REMOTE_SAVE_FOLDER):
            stationName = os.path.basename(root)
            # 只压缩今天请求的站点
            if stationName.lower() not in todayQueryStations:
                continue
            filesType = {}
            # # # todo 测试一个站点
            # if 'smandy_it' not in root:
            #     continue
            for file in files:
                fileFullpath = os.path.join(root, file)
                if process_files.file_create_time(fileFullpath) < threeDayBeforeStr:
                    if os.path.exists(fileFullpath):
                        os.remove(fileFullpath)
                        continue
                for signWord, type in reportTypeSignDict.items():
                    if signWord in file.lower():
                        fileTypePath = filesType.get(type, [])
                        fileTypePath.append(os.path.join(stationName, file))
                        filesType[type] = fileTypePath
                        break
                # ao表需要重新处理
                if os.path.splitext(file)[0].isdigit():
                    fileTypeAo = filesType.get('ao',[])
                    fileTypeAo.append(os.path.join(stationName, file))
                    filesType['ao'] = fileTypeAo

            if filesType:
                # 各个类型中有可能存在几个表,此时保留最大的报表,同时删除其他报表
                for type, path in filesType.items():
                    if isinstance(path, list):
                        if len(path) == 1:
                            filesType[type] = os.path.join(REMOTE_SAVE_FOLDER,path[0])
                        else:
                            allFileFullPath = [os.path.join(REMOTE_SAVE_FOLDER, onePath) for onePath in path]
                            newestPath = process_files.newest_file(allFileFullPath)
                            filesType[type] = newestPath
                            if newestPath is not None:
                                try:
                                    [os.remove(path) for path in allFileFullPath if (path !=newestPath) and (os.path.exists(path))]
                                except Exception as e:
                                    print(e)
                                    continue
                stationTypeDict[stationName] = filesType
        # # 循环全部文件夹花费的时间
        # costTime = int((datetime.now()- loopStationDictStartTime).total_seconds())
        # # 1.保存到redis中,同时设置过期时间
        # _connRedis = public_function.Redis_Store(db=1)
        # redisSignWord = 'api_request_files'
        # fileTypeRedisExpireTimeDict = {'cp': 3600 * 24 * 2 , 'br': 3600 * 24 * 2, 'st': 3600 * 24 * 2,
        #                                'ac': 3600 * 24 * 2, 'ao': 3600 * 24 * 2}
        # # todo 这里好像需要加个判断?
        # #需要修正一下过期时间,需要把循环站点的时间给减去
        # fileTypeRedisExpireTimeDict = {type:redisTime for type ,redisTime in fileTypeRedisExpireTimeDict.items()}
        # if len(stationTypeDict) > 0:
        #     [_connRedis.set(f'{redisSignWord}:{station}_{type}', path, ex=fileTypeRedisExpireTimeDict.get(type)) for
        #      station, types in stationTypeDict.items() for
        #      type, path in types.items()]
        # # 首先删除压缩文件中存在的类型文件
        # stationsFileTypeRedisSignKey = 'api_request_files'
        # stationsFileTypeKeys = [key for key in _connRedis.keys() if stationsFileTypeRedisSignKey in key]
        # if not stationsFileTypeKeys:
        #     return
        # # 新的文件类型
        # stationFilePathDict = {}
        # for key in stationsFileTypeKeys:
        #     stationFilePath = _connRedis.get(key)
        #     # 销售上传的值为1 ，不是键，故不能当做路径
        #     if stationFilePath == 1:
        #         continue
        #     RedisStationName = key[len(stationsFileTypeRedisSignKey) + 1:len(key) - 3]
        #     stationExistPath = stationFilePathDict.get(RedisStationName, [])
        #     stationExistPath.append(stationFilePath)
        #     stationFilePathDict[RedisStationName] = stationExistPath
        # 判断压缩文件是否存在
        stationsZipFolderPath = r"F:\five_reports_zipped"
        todayWeekDay = datetime.now().weekday()
        for station, stationNewFileTypeDict in stationTypeDict.items():
            stationZipFile = os.path.join(stationsZipFolderPath, station.lower() + '.zip')
            if not os.path.isfile(stationZipFile):
                with zipfile.ZipFile(stationZipFile, 'w') as file:
                    pass
            # zip文件中已经存在的文件类型
            stationExistFile = myZip.zipFileList(stationZipFile)
            stationExistFileTypeDict = process_files.file_type(stationExistFile)
            # 新加入的文件类型
            deleteType = set(stationNewFileTypeDict.keys()) & set(stationExistFileTypeDict.keys())
            try:
                [myZip.zip_delete(stationZipFile, path) for key in deleteType for path in stationExistFileTypeDict.get(key)]
            except Exception as e:
                print(e)
                print(station)
                continue
            # 将新的写入
            with zipfile.ZipFile(stationZipFile, 'a') as wfile:
                for _, file in stationNewFileTypeDict.items():
                    if file is None:
                        continue
                    if os.path.exists(file):
                        station = station.strip().replace('-','_').replace(' ','_').lower()
                        targetPath = os.path.join(station,os.path.basename(file).lower())
                        wfile.write(file, targetPath)

            # 删除压缩文件中两天前的文件
            fileCreateTime = {file:myZip.file_create_time_in_zip(stationZipFile,file) for file in myZip.zipFileList(stationZipFile) if not file.endswith('/')}
            try:
                [myZip.zip_delete(stationZipFile,file) for file,fileTime in fileCreateTime.items() if isinstance(fileTime,datetime) and (datetime.now().date() - fileTime.date()).days > 2]
            except Exception as e:
                print(f'处理{station}的压缩文件有问题')
                print(station)
                print(e)
                continue
            # 删掉redis中本站点的键,然后再添加
            stationsFileTypeRedisSignKey = 'api_request_files'
            _connRedis = public_function.Redis_Store(db=1)
            # [_connRedis.delete(key) for key in _connRedis.keys() if (key.startswith(stationsFileTypeRedisSignKey)) and (station.lower() == key[len(stationsFileTypeRedisSignKey) + 1:len(key) - 3])]
            stationExistFile = myZip.zipFileList(stationZipFile)
            stationExistFileTypeDict = process_files.file_type(stationExistFile)
            for type,files in stationExistFileTypeDict.items():
                for file in files:
                    fileTime = myZip.file_create_time_in_zip(stationZipFile,file)
                    redisExpireTime = file_redis_expire_time(fileTime,expireDay=3)
                    if redisExpireTime is not None:
                        _connRedis.set(f'{stationsFileTypeRedisSignKey}:{station.lower()}_{type}',todayWeekDay,ex=int(redisExpireTime))

        # 添加日志
        stationRequestTypeResult = {station:list(stationTypeValue.keys()) for station, stationTypeValue in stationTypeDict.items()}
        resultTypeDict = {'ac':[],'br':[],'ao':[],'st':[],'cp':[]}
        allTypeMsg = ''
        for type in resultTypeDict.keys():
            for station,stationType in stationRequestTypeResult.items():
                if type in stationType:
                    resultTypeDict[type].append(station)

            typeLen = len(resultTypeDict[type])
            typeMsg = f'{type}表一共请求到{typeLen}个,请求到的比例为:{round(typeLen/len(todayQueryStations)*100,2)}%\n'
            allTypeMsg+=typeMsg
        msg = f'{datetime.now().date()}:请求{len(todayQueryStations)}个站点。\n请求详请如下:\n{allTypeMsg}'
        print(msg)

        # 没有请求到的站点信息
        stationMissedDict = {station:list(set(todayQueryStations)-set(stationList)) for station,stationList in resultTypeDict.items()}
        stationMissedDictMsg = json.dumps(stationMissedDict)
        allRequestStationMsg = json.dumps(list(todayQueryStations))

        logPath = r"F:\five_reports_zipped\request_stations_log.txt"
        with open(logPath,'w+') as f:
            f.write(f'{msg}\n')
            f.write(f'今日请求站点列表:{allRequestStationMsg}\n\n')
            f.write(f'站点缺失详情:{stationMissedDictMsg}\n')


    global REMOTE_SAVE_FOLDER,THREAD_NUM,THREAD_POOL
    REMOTE_SAVE_FOLDER = r'F:\remote_get_five_files'
    THREAD_NUM = 4
    THREAD_POOL = ThreadPoolExecutor(THREAD_NUM)
    # # # # 通过请求远程报表日期来获取报表路径
    redis_store_report_remote_path()
    # # # # 通过路径来请求远程文件到本地
    thread_request_server_files()
    # 将文件夹中的全部文件（3个文件由本py请求,2个文件由其他py请求）压缩到压缩包中,同时保存到redis中
    save_five_reports()
    # 请求到的日志


if __name__ == '__main__':
    process_five_reports()
