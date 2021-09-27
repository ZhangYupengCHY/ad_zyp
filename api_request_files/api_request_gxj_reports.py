# -*- coding: utf-8 -*-
"""
Proj: AD-Helper1
Created on:   2019/12/26 17:21
@Author: RAMSEY

Standard:
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""

import rsa, json, requests, os, redis, zipfile, shutil, time, re, xlsxwriter
import pandas as pd
# import Crypto.PublicKey.RSA
import base64, pymysql
from datetime import datetime, timedelta,date
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import paramiko as pm
from retry import retry

import public_function
from my_toolkit import sql_write_read, process_files, station_belong, myZip,process_company,process_station
from api_request_files import api_request_five_reports

# 远程文件请求路径
SAVEFOLDER = r"F:\gjx_five_reports\remote_get_five_files"
# 压缩文件保存路径
STATIONSZIPFOLDER  = r"F:\five_reports_zipped"

def request_gxj_amazon_account(start_day=5, end_day=14):
    """
        每日请求的楚晋的站点列表
    """
    allAdAccount = all_ad_station(start_day=start_day, end_day=end_day)

    # 筛选除楚晋的站点
    # cjAmazonAccountInfo = cj_amazon_account()
    # cjAmazonAccountInfo = cjAmazonAccountInfo[~pd.isna(cjAmazonAccountInfo['account_name'])]
    # cjAmazonAccountInfo['account_name'] = [public_function.standardize_station(station) for station in cjAmazonAccountInfo['account_name']]
    # cjAmazonAccount = set(cjAmazonAccountInfo['account_name'])
    gxjAmazonAccount = process_company.Company("光迅嘉").station()

    # 每日需要请求的楚晋站点
    return list(allAdAccount & gxjAmazonAccount)


def del_file_by_signword(folder,signWord):
    """
    删除文件夹中有特殊字段的文件
    :param folder:
    :param signWord:
    :return:
    """
    # if all([folder,signWord]):
    #     return
    if not os.path.exists(folder):
        return
    if not isinstance(signWord,(str,list,set)):
        raise TypeError(f'signword must str,list,set')
    files = os.listdir(folder)
    if isinstance(signWord,str):
        try:
            [os.remove(os.path.join(folder,file)) for file in files if (isinstance(signWord,str)) and (signWord.lower() in file.lower())]
        except Exception as e:
            return
    else:
        try:
            [os.remove(os.path.join(folder,file)) for file in files if any(sign.lower() in file.lower() for sign in signWord)]
        except Exception as e:
            return


def all_ad_station(start_day=5, end_day=14):
    """
    获取only_station_info中操作时间在5~12天之间的站点以及接手了还没有操作的站点
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
    selectSql = "select station,update_time,accept_time from %s"% tableName
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

    onlyStations = set([public_function.standardize_station(station) for station in onlyStations])

    return onlyStations


def file_redis_expire_time(file_time, expireDay=3):
    """
    文件存在redis的过期时间
    :param file_time:
    :param file_expire_time:
    :return:
    """
    if file_time is None:
        return
    if (not isinstance(file_time, datetime)):
        return
    expireDate = file_time + timedelta(days=expireDay)
    expireDatetime = datetime(expireDate.year, expireDate.month, expireDate.day)
    return int((expireDatetime - datetime.now()).total_seconds())


def save_five_reports():
    """
    将远程请求的5表同步保留到redis中与
    1.首先将站点报表类型:路径 键值对存储到redis
    2. 将文件压缩至到5表压缩的文件夹中
    :return:
    """
    if not os.path.isdir(SAVEFOLDER):
        raise FileNotFoundError(f'{SAVEFOLDER}.')
    # 1.存储到redis中
    # 获取文件中站点存在的文件类型
    # 路径中的类型关键词
    reportTypeSignDict = {'bulk': 'cp', 'business': 'br', 'search': 'st',
                          'active': 'ac', 'orders': 'ao'}
    stationTypeDict = {}
    # 删除两天以前的报表
    # 遍历远程保存文件夹,获取站点类型y
    threeDayBeforeStr = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d %H:%M:%S')
    for root, dirs, files in os.walk(SAVEFOLDER):
        stationName = os.path.basename(root)
        # todo 取消只压缩今天请求的站点
        if stationName.lower() not in todayQueryStations:
            continue
        filesType = {}
        # # # todo 测试一个站点
        # if 'wanleihu_uk' not in root:
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
                fileTypeAo = filesType.get('ao', [])
                fileTypeAo.append(os.path.join(stationName, file))
                filesType['ao'] = fileTypeAo

        if filesType:
            # 各个类型中有可能存在几个表,此时保留最大的报表,同时删除其他报表
            for type, path in filesType.items():
                if isinstance(path, list):
                    if len(path) == 1:
                        filesType[type] = os.path.join(SAVEFOLDER, path[0])
                    else:
                        allFileFullPath = [os.path.join(SAVEFOLDER, onePath) for onePath in path]
                        newestPath = process_files.newest_file(allFileFullPath)
                        filesType[type] = newestPath
                        if newestPath is not None:
                            try:
                                [os.remove(path) for path in allFileFullPath if
                                 (path != newestPath) and (os.path.exists(path))]
                            except Exception as e:
                                print(e)
                                continue
            stationTypeDict[stationName] = filesType
    # 判断压缩文件是否存在
    todayWeekDay = datetime.now().weekday()
    for station, stationNewFileTypeDict in stationTypeDict.items():
        stationZipFile = os.path.join(STATIONSZIPFOLDER, station.lower() + '.zip')
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
                    station = public_function.standardize_station(station)
                    targetPath = os.path.join(station, os.path.basename(file).lower())
                    wfile.write(file, targetPath)

        # 删除压缩文件中两天前的文件
        fileCreateTime = {file: myZip.file_create_time_in_zip(stationZipFile, file) for file in
                          myZip.zipFileList(stationZipFile) if not file.endswith('/')}
        try:
            [myZip.zip_delete(stationZipFile, file) for file, fileTime in fileCreateTime.items() if
             isinstance(fileTime, datetime) and (datetime.now().date() - fileTime.date()).days > 2]
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
        for type, files in stationExistFileTypeDict.items():
            for file in files:
                fileTime = myZip.file_create_time_in_zip(stationZipFile, file)
                redisExpireTime = file_redis_expire_time(fileTime, expireDay=3)
                if redisExpireTime is not None:
                    _connRedis.set(f'{stationsFileTypeRedisSignKey}:{station.lower()}_{type}', todayWeekDay,
                                   ex=int(redisExpireTime))

    # 添加日志
    stationRequestTypeResult = {station: list(stationTypeValue.keys()) for station, stationTypeValue in
                                stationTypeDict.items()}
    resultTypeDict = {'ac': [], 'br': [], 'ao': [], 'st': [], 'cp': []}
    allTypeMsg = ''
    for type in resultTypeDict.keys():
        for station, stationType in stationRequestTypeResult.items():
            if type in stationType:
                resultTypeDict[type].append(station)

        typeLen = len(resultTypeDict[type])
        typeMsg = f'{type}表一共请求到{typeLen}个,请求到的比例为:{round(typeLen / len(todayQueryStations) * 100, 2)}%\n'
        allTypeMsg += typeMsg
    msg = f'{datetime.now().date()}:请求{len(todayQueryStations)}个站点。\n请求详请如下:\n{allTypeMsg}'
    print(msg)

    # 没有请求到的站点信息
    stationMissedDict = {station: list(set(todayQueryStations) - set(stationList)) for station, stationList in
                         resultTypeDict.items()}
    stationMissedDictMsg = json.dumps(stationMissedDict)
    allRequestStationMsg = json.dumps(list(todayQueryStations))

    logPath = r"F:\five_reports_zipped\request_cj_stations_log.txt"
    with open(logPath, 'a+') as f:
        f.write(f'{msg}\n')
        f.write(f'今日请求站点列表:{allRequestStationMsg}\n\n')
        f.write(f'站点缺失详情:{stationMissedDictMsg}\n')


def server_tran():
    # 连接公司远程
    host = 'yiduan.yibai-it.com'
    port = 2022
    # 获取transport传输实例, sftp服务器 ip + 端口号
    tran = pm.Transport((host, port))
    print(f'连接{host}')
    return tran


def server_sftp(tran):
    username = 'gg'
    password = 'yb@2021asd#!'
    # 连接ssh服务器, user + password
    tran.connect(username=username, password=password)
    sftp = pm.SFTPClient.from_transport(tran)
    sftp.get_channel().settimeout(30)
    print(f'连接session')
    return sftp


def request_jwt(iss, secret):
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
    params = {'iss': iss, 'secret': secret}
    response = requests.get(url, params=params,timeout=(10, 60))
    return json.loads(response.content)['jwt']


@public_function.run_time
def request_ac_file(key_path=r"E:\ad_zyp\api_request_files\public.key"):
    """
    通过接口请求楚晋全部站点ac表
    :return:
    """

    def get_ac_file_dir(station_name, key_path=key_path,
                        download_url=r"http://yunyipub.yibainetwork.com/services/api/advertise/getreport/"):
        with open(key_path, 'r') as fp:
            public_key = fp.read()
        # pkcs8格式
        key = public_key
        password = "Kr51wGeDyBM39Q0REVkXn4lW7ZqCxdPLS8NO6iIfubTJcvsjt2YpmAgzHFUoah"
        pubkey = rsa.PublicKey.load_pkcs1_openssl_pem(key)
        password = password.encode('utf8')
        crypt_password = rsa.encrypt(password, pubkey)
        token = base64.b64encode(crypt_password).decode()
        station_name = station_name[0:-3].replace('_', '-') + station_name[-3:]

        def get_report(station_name):
            post_load = {
                'token': token,
                'data': json.dumps({
                    0: {
                        "account_id": station_name,
                        "report_type": "Active",
                    }
                })
            }
            response = requests.post(download_url, data=post_load,timeout=(10,60)).content
            response = json.loads(response)
            return response['data']

        try:
            data = get_report(station_name)
        except:
            return
        if (not data) & (station_name[-2:] == 'es'):
            station_name = station_name[:-2] + 'sp'
            data = get_report(station_name)
        if not data:
            station_name = station_name[0:-3].replace('-', ' ') + station_name[-3:]
            data = get_report(station_name)

        if data:
            return data[0]

    # 请求并保存请求到的两种类型的报表
    def request_save_ac_file(files_save_dirname=SAVEFOLDER, key_path=key_path):
        station_name = stationsQueue.dequeue()
        if station_name:
            if station_name.lower().endswith('es'):
                station_name = station_name[:-2]+'sp'
            acFileRemoteDir = get_ac_file_dir(station_name, key_path=key_path)
            if acFileRemoteDir is None:
                return
            # 站点数据保存的文件夹不存在,则新建
            if station_name.lower().endswith('sp'):
                station_name = station_name[:-2]+'es'
            station_save_folder = os.path.join(files_save_dirname, station_name)
            if not os.path.exists(station_save_folder):
                os.mkdir(station_save_folder)

            def download_from_api(api_dir: 'dir', files_save_dirname, station_name):
                newest_dir = api_dir
                newest_dir = newest_dir.replace('/mnt/yunyi_manyorg02', 'http://erp.yibainetwork.com')
                file_basename = os.path.basename(newest_dir)
                try:
                    request_file = requests.get(newest_dir, timeout=(10, 60))
                except Exception as e:
                    return
                status_code = request_file.status_code
                if status_code == 200:
                    out_content = request_file.content
                    file_dirname = os.path.join(files_save_dirname, station_name)
                    if not os.path.exists(file_dirname):
                        try:
                            os.makedirs(file_dirname)
                        except Exception as e:
                            print(e)
                            print(f'{file_dirname}:文件存在.')
                    # 删除文件夹中的ac文件
                    del_file_by_signword(file_dirname,'ACTIVE_LISTING')
                    files_save_dirname = os.path.join(file_dirname, file_basename)
                    with open(files_save_dirname, 'wb') as f:
                        f.write(out_content)
                    print(f'完成{station_name}:ac的请求')
                    api_request_five_reports.save_request_log_2_redis('ac',NOWDATE,station_name,signWord=redisSignWord)
                else:
                    print(f'无法请求{newest_dir}报表! \n status_code:{status_code}')

            # 1.得到ac报表
            download_from_api(acFileRemoteDir, files_save_dirname, station_name)

    # todo 修改楚晋请求ac报表的开始和结束时间
    cjAmazonAccountDailyRequest = request_gxj_amazon_account(start_day=5, end_day=14)

    # 生成队列
    global stationsQueue
    stationsQueue = public_function.Queue()
    stationsQueue.enqueue_items(cjAmazonAccountDailyRequest)
    print(f"请求楚晋ac报表。此次请求{stationsQueue.size()}个站点.")

    while stationsQueue.size() != 0:
        request_save_ac_file()
    print(f'完成请求楚晋ac表.')


@public_function.run_time
def request_ao_file():
    """
        获取ao表 jwt用json给的
    """
    def request_ao_file(reqeustUrl, jwt, page=1, size=1, type=2):
        params = {
            'page': page,
            'size': size,
            'type': type,
            'jwt': jwt,
        }
        try:
            response = requests.post(reqeustUrl, params=params,timeout=(10, 60))
        except Exception as e:
            print(e)
            return
        if response.status_code != 200:
            print(f'请求楚讯ao报表的接口无法连接,状态为:{response.status_code}')
            return
        return json.loads(response.content)['data']

    jwt = request_jwt('sz_sales_ad_data_analysis', 'hjaq24.cdta91ldDaqlcdqkb')
    request_url = f"http://bi.yibainetwork.com:8000/bi/report/amazon/amazon_order_report_cx"
    requestLimit = int(1e4)
    requestAllAoInfo = []
    page = 1
    while 1:
        try:
            requestsOneTimeAoInfo = request_ao_file(request_url, jwt, page=page, size=requestLimit)
        except Exception as e:
            requestsOneTimeAoInfo = None
            continue
        if requestsOneTimeAoInfo is not None:
            requestAllAoInfo.extend(requestsOneTimeAoInfo)
        else:
            break
        if len(requestsOneTimeAoInfo) != requestLimit:
            break
        page += 1
    requestAllAoInfo = pd.DataFrame(requestAllAoInfo)
    if 'rn' in requestAllAoInfo.columns:
        del requestAllAoInfo['rn']
    nowDate = datetime.now().strftime('%Y-%m-%d')
    # 获取楚讯账号id和账号
    cjAmazonAccountInfo = cj_amazon_account()
    if cjAmazonAccountInfo['id'].dtype == object:
        cjAmazonAccountInfo['id'] = [int(accountId) if accountId is not None and accountId.isdigit() else None for accountId in cjAmazonAccountInfo['id']]
    cjAmazonAccountDict = {id:account_name for id,account_name in zip(cjAmazonAccountInfo['id'],cjAmazonAccountInfo['account_name']) if id is not None}
    # 将全部站点的ao信息按照站点保存
    if not os.path.exists(SAVEFOLDER):
        os.mkdir(SAVEFOLDER)
    requestStations = []
    for accountId,accountInfo in requestAllAoInfo.groupby(by='account_id'):
        if accountId not in cjAmazonAccountDict.keys():
            continue
        accountName = cjAmazonAccountDict.get(accountId,None)
        if accountName is None:
            print(f'{accountId}找不到account_name')
            continue
        saveDirPath = os.path.join(SAVEFOLDER,accountName)
        if not os.path.exists(saveDirPath):
            os.mkdir(saveDirPath)
        # 删除历史的ao报表
        del_file_by_signword(saveDirPath,'All Orders')
        saveBasename = f'All Orders {nowDate}.csv'
        savePath = os.path.join(saveDirPath,saveBasename)
        del accountInfo['account_id']
        accountInfo.to_csv(savePath,index=False)
        requestStations.append(accountName)
    requestStations = set(requestStations) & set(todayQueryStations)
    # 保存站点请求ao表的记录到redis中
    api_request_five_reports.save_request_log_2_redis('ao',NOWDATE,list(requestStations),redisSignWord)
    print('完成请求楚晋ao表.')

@public_function.run_time
def request_server_files(sftp, remoteDir):
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
    # try:
    #     sftp.stat(remoteDir)
    # except Exception as e:
    #     print(f'远程文件不存在:{remoteDir}')
    #     print(e)
    #     return
    #     print(datetime.now().replace(microsecond=0))
    #     raise ConnectionError(f'{sftp} 无法连接.')
    sftp.stat(remoteDir)

    # 将文件保存
    stationName = savePath2stationName(remoteDir)
    stationLocalSaveFolder = os.path.join(SAVEFOLDER, stationName)
    stationLocalSavePath = os.path.join(stationLocalSaveFolder, os.path.basename(remoteDir))

    def _timeout(startDatetime):
        cost_time = int((datetime.now() - startDatetime).total_seconds())
        timeoutSeconds = 300
        if cost_time > timeoutSeconds:
            raise TimeoutError(f'请求报表超时。超过了{timeoutSeconds}秒.')

    if not os.path.exists(stationLocalSaveFolder):
        os.mkdir(stationLocalSaveFolder)
    startDatetime = datetime.now()
    # 删除本类型4天以前的报表
    yesterDayStr = (datetime.now() - timedelta(days=4)).strftime('%Y-%m-%d')
    [os.remove(os.path.join(stationLocalSaveFolder, file)) for file in os.listdir(stationLocalSaveFolder) if
     (reportFolderTypeDictReverse[reportType] in file)
     and (process_files.file_create_time(os.path.join(stationLocalSaveFolder, file))[:10] <= yesterDayStr)]
    try:
        print(f'{stationName} {reportType}表:开始请求.')
        sftp.get(remoteDir, stationLocalSavePath, _timeout(startDatetime))
        print(f'{stationName} {reportType}表:请求完成.')
        api_request_five_reports.save_request_log_2_redis(reportType.lower(),NOWDATE,stationName,redisSignWord)
    except Exception as e:
        print(e)
        print(datetime.now().replace(microsecond=0))
        raise ValueError(f'远程文件无法下载:{remoteDir}')
    # sftp.get(remoteDir, stationLocalSavePath, callback=_timeout)
    # 若请求到的表为空则删除
    if (os.path.exists(stationLocalSavePath)) and (process_files.file_size(stationLocalSavePath) == 0):
        os.remove(stationLocalSavePath)
        print(f'{stationName} {reportType}表:数据为空,删除掉。')


def savePath2stationName(remoteDir):
    statonName =  os.path.split(os.path.dirname(remoteDir))[-1][:-3] + '_' +os.path.split(os.path.dirname(os.path.dirname(remoteDir)))[-1]
    return process_station.standardStation(statonName)

@public_function.run_time
def request_cp_br_st_reports():


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
        # 判断昨日的日志是否存在
        nowDate = datetime.now()
        lastDate = nowDate - timedelta(days=1)
        # lastDate = nowDate - timedelta(days=0)
        lastDateStr = lastDate.strftime('%Y-%m-%d')
        if 'task_run_time' not in logFileData.columns:
            return
        if logFileData['task_run_time'].dtype != 'object':
            logFileData['task_run_time'] = logFileData['task_run_time'].dt.strftime('%Y-%m-%d')
        return logFileData[logFileData['task_run_time'] == lastDateStr]
        # return logFileData

    def request_reports_log():
        """
        五表请求前一天的日志
        """
        jwt = request_jwt('sz_sales_ad_data_analysis', 'hjaq24.cdta91ldDaqlcdqkb')
        nowDate = datetime.now()
        nowDateStr = datetime.strftime(nowDate, '%Y-%m-%d')
        lastDate = nowDate - timedelta(days=1)
        # lastDate = nowDate - timedelta(days=1)
        lastDateStr = datetime.strftime(lastDate, '%Y-%m-%d')
        request_url = f"http://bi.yibainetwork.com:8000/bi/store/search_store_log?jwt={jwt}"
        remoteReportsNameDict = {'cp': '广告活动批量电子表格', 'br': '业务报告-详情页面上的销售量与访问量-根据ASIN', 'st': '广告报告-商品推广-搜索词-一览'}
        for fileType, remoteFileTypeSaveName in remoteReportsNameDict.items():
            params = {
                "jwt_id": "sz_sales_ad_data_analysis",
                'content': remoteFileTypeSaveName,
                'sys_type': "gjx",
                'start_date': lastDateStr,
                'end_date': lastDateStr,

            }
            response = requests.post(request_url, json=params, timeout=(10, 60))
            if response.status_code != 200:
                print(f'{fileType}报表日志请求错误,错误代码:{response.status_code},时间:{datetime.now()}')
                continue
            # 文件流
            html_str = response.content
            # 日志保存文件夹
            logFolderName = 'log'
            logSaveBaseName = f'gxj_{fileType}.xlsx'
            logSaveDirname = os.path.join(SAVEFOLDER, logFolderName)
            if not os.path.exists(logSaveDirname):
                os.makedirs(logSaveDirname)
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
        requestFilesQueue = public_function.Queue()

        # 获取存储日志,存储路径为cp.xlsx,br.xlsx,st.xlsx
        logSaveFolder = os.path.join(SAVEFOLDER, 'log')
        filesSaveBasenameList = ['gxj_cp.xlsx', 'gxj_br.xlsx', 'gxj_st.xlsx']
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
        cxFolderFirstDegree = '/shujubaobiao/report_new/gjx_report'
        #
        if allPathDf['task_run_time'].dtype != 'object':
            allPathDf['task_run_time'] = allPathDf['task_run_time'].dt.strftime('%Y-%m-%d')
        allPathList = [
            os.path.join(cxFolderFirstDegree, type, platform, marketplace, store_name, task_run_time, file_name)
            for type, platform, marketplace, store_name, task_run_time, file_name in
            zip(allPathDf['type'], allPathDf['platform'], allPathDf['marketplace'], allPathDf['store_name'],
                allPathDf['task_run_time'], allPathDf['file_name'])]
        allPathList = [
            os.path.join(cxFolderFirstDegree, type, task_run_time,platform, marketplace, store_name, file_name)
            for type, platform, marketplace, store_name, task_run_time, file_name in
            zip(allPathDf['type'], allPathDf['platform'], allPathDf['marketplace'], allPathDf['store_name'],
                allPathDf['task_run_time'], allPathDf['file_name'])]

        allPathListStation = {
            path: savePath2stationName(path)
            for path in set(allPathList)}

        filterStationsPath = set(
            [path for path, station in allPathListStation.items() if station in todayQueryStations])


        # todo 选择一个站点测试
        # filterStations = [path for path in filterStations if 'smandy' in path.lower()]
        if not filterStationsPath:
            return

        # 将请求的文件路径列表存储到队列中
        requestFilesQueue.enqueue_items(filterStationsPath)

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
                print(f'关闭session和连接')
                print('完成今天全部报表请求.')
                break
            remoteDir = requestFilesQueue.dequeue()
            try:
                request_server_files(sftp, remoteDir)
            except Exception as e:
                time.sleep(10)
                print(remoteDir)
                print(e)
                # 重新连接
                try:
                    sftp.close()
                    tran.close()
                    print(f'关闭session和连接')
                except Exception as e:
                    print(e)
                    print('无法先关闭sftp')
                    try:
                        tran.close()
                        print(f'关闭连接')
                    except Exception as e:
                        print(e)
                        print('无法关闭tran')
                # 重新连接
                tran = server_tran()
                sftp = server_sftp(tran)
                # requestFilesQueue.enqueue(remoteDir)

    global THREAD_NUM, THREAD_POOL
    THREAD_NUM = 2
    THREAD_POOL = ThreadPoolExecutor(THREAD_NUM)
    # # # # 通过请求远程报表日期来获取报表路径
    redis_store_report_remote_path()
    # # # # 通过路径来请求远程文件到本地
    thread_request_server_files()


def cj_amazon_account():
    """
    楚晋亚马逊全部账号的信息
        从数据库中加载楚晋亚马逊全部账号信息:账号+账号id('id','account_name')
    Returns
    -------

    """
    connMysql = sql_write_read.QueryMySQL()
    cjAmazonAccountTableName = 'cj_amazon_account'
    cjAmazonAccountInfo = connMysql.read_table(cjAmazonAccountTableName)
    connMysql.close()
    return cjAmazonAccountInfo[['id','account_name']]


# def process_cj_reports(ac_ao_start_hour=20, st_br_cp_start_hour=9):
def process_gxj_reports(ac_ao_start_hour=20, st_br_cp_start_hour=8):
    """
    楚晋请求五表
    ac/ao
    br/st/cp
    :return:
    """
    global redisSignWord
    # redis保存识别字段
    redisSignWord = 'request_log_gxj'
    redisDB = 3
    redisReportResult = f'{redisSignWord}:status'
    redisConn = sql_write_read.Redis_Store(db=redisDB)
    while 1:
        global todayQueryStations, NOWDATE
        todayQueryStations = request_gxj_amazon_account(start_day=5, end_day=14)
        halfHour = 600
        # 将今日请求的全部报表存储到redis中
        # NOWDATE = datetime.now().date() - timedelta(days=1)
        NOWDATE = datetime.now().date()
        api_request_five_reports.save_request_log_2_redis('all', NOWDATE, todayQueryStations,
                                                          signWord=redisSignWord,redisDb=redisDB)
        # while 1:
        #     if datetime.now().hour != ac_ao_start_hour:
        #         time.sleep(halfHour)
        #     else:
        #         request_ac_file()
        #         request_ao_file()
        #         break
        while 1:
            if datetime.now().hour != st_br_cp_start_hour:
                time.sleep(halfHour)
            else:
                redisConn.set(redisReportResult, '0')
                request_cp_br_st_reports()
                break
        save_five_reports()
        redisConn.set(redisReportResult, '1')
        time.sleep(3600)


if __name__ == '__main__':
    process_gxj_reports()