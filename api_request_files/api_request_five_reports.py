from datetime import datetime, timedelta,date

import rsa, requests, os, redis, zipfile, time, re, xlsxwriter

from retry import retry

import json
import pandas as pd
import threading
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import paramiko as pm

from my_toolkit import chinese_check, sql_write_read, public_function, commonly_params, change_rate, process_files, \
    init_station_report, myZip, station_belong,process_company

import api_request_ac_ao_reports

RUNPYS = ['api_request_cp_br_st_reports.py', 'api_request_ac_ao_reports.py']

Thread_Pool = ThreadPoolExecutor(len(RUNPYS))
BASE_PATH = os.path.dirname(__file__)


@public_function.run_time
def request_ao_ac_file():
    """
    请求全部站点的ao和ac表
    :return:
    """

    def get_all_files_dir(station_name, key_path=r"E:\ad_zyp\api_request_files\public.key",
                          download_url="http://erp.yibainetwork.com/services/api/advertise/getreport"):
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

        @retry(tries=3,delay=10)
        def get_report(station_name):
            post_load = {
                'token': token,
                # 'data': json.dumps({
                #     0: {
                #         'date_range': 1,
                #         "child_type": 30,
                #         'account_id': station_name,
                #         "report_type": "Campaign"
                #     },
                #     1: {
                #         'date_range': 1,
                #         'account_id': station_name,
                #         "report_type": "ST"
                #     },
                #     2: {
                #         'date_range': 1,
                #         'account_id': station_name,
                #         "report_type": "BR"
                #     },
                #     3: {
                #         'date_range': 30,
                #         "child_type": 30,
                #         "account_id": station_name,
                #         "report_type": "AO"
                #     },
                #     4: {
                #         "account_id": station_name,
                #         "report_type": "Active"
                #     },
                #     5: {
                #         "account_id": station_name,
                #         "report_type": "All"
                #     },
                # })
                'data': json.dumps({
                    3: {
                        'date_range': 30,
                        "child_type": 30,
                        "account_id": station_name,
                        "report_type": "AO"
                    },
                    4: {
                        "account_id": station_name,
                        "report_type": "Active"
                    },
                })
            }
            try:
                response = requests.post(download_url, data=post_load, timeout=(10, 60)).content
            except Exception as e:
                print(f'无法正常连接{download_url}')
                print(e)
                return
            data = []
            try:
                data_basic = json.loads(response)['data']
                data.extend(data_basic)
            except:
                pass
                # red.rpush('station_no_data', station_name)
                # return
            # 单独请求四天的广告报表
            # post_camp_date = [1, 7, 14, 60]
            # all_camp_post_dir = []
            # for post_date in post_camp_date:
            #     post_camp_load = {
            #         'token': token,
            #         'data': json.dumps({
            #             0: {
            #                 'date_range': 1,
            #                 "child_type": post_date,
            #                 'account_id': station_name,
            #                 "report_type": "Campaign"
            #             }
            #         })
            #     }
            #     response_camp = requests.post(download_url, data=post_camp_load).content
            #     try:
            #         camp_data = json.loads(response_camp)['data']
            #     except:
            #         print(f"{station_name}广告报表{post_date}无法请求..")
            #         continue
            #     all_camp_post_dir.extend(camp_data)
            # # data = all_camp_post_dir
            #
            # # 本地接口请求AO
            # '''
            # local_url = 'http://192.168.9.167:8080/services/api/advertise/getreport'
            # post_ao_load = {
            #     'token': token,
            #     'data': json.dumps({
            #         0: {
            #             'date_range': 30,
            #             "child_type": 30,
            #             'account_id': station_name,
            #             "report_type": "AO"
            #         }
            #     })
            # }
            # response_ao = requests.post(local_url,data=post_ao_load).content
            # try:
            #     ao_data = json.loads(response_ao)['data']
            #     ao_data = [data.replace('D:/phpStudy/PHPTutorial/WWW/wwwerp','http://192.168.9.167:8080') for data in ao_data]
            # except:
            #     print(f"{station_name}:AO报表{post_date}本地无法请求..")
            #     ao_data = []
            # data.extend(ao_data)
            # '''
            #
            # data.extend(all_camp_post_dir)
            return data

        data = get_report(station_name)
        if (not data) & (station_name[-2:] == 'es'):
            station_name = station_name[:-2] + 'sp'
            data = get_report(station_name)
        if not data:
            station_name = station_name[0:-3].replace('-', ' ') + station_name[-3:]
            data = get_report(station_name)

        files_keyword_dict = {'ST': 'SearchTerm', 'BR': 'Business', 'AO': 'ORDER', 'AC': 'AVTIVE_LISTING',
                              'AL': 'All_LISTING'}
        camp_keyword_dict = ['Advertising', 'Sponsored']
        all_files_dict = {}
        for report_type, report_kw in files_keyword_dict.items():
            all_files_dict[report_type] = [report for report in data if files_keyword_dict[report_type] in report]
        all_files_dict['CP'] = [report for report in data if
                                (camp_keyword_dict[0] in report) or (camp_keyword_dict[1] in report)]
        return all_files_dict

    # 保留所有站点最新的数据，剔除重复数据
    def keep_newest_file_dir(all_files_dict: 'dict', station_name):
        file_keys = all_files_dict.keys()
        for report_type in file_keys:
            report_type_files = all_files_dict[report_type]
            if report_type == 'ST':
                continue
            if len(report_type_files) > 1:
                try:
                    files_date = [re.findall('[0-9]{4}.[0-9]{2}.[0-9]{2}', os.path.basename(file)) for file in
                                  report_type_files]
                    # 排除没有日期的链接
                    if not files_date[0]:
                        continue
                    last_date = max([max(dates) for dates in files_date])
                    all_files_dict[report_type] = [file for file in report_type_files if
                                                   last_date in os.path.basename(file)]
                except:
                    print(f"{station_name}有文件命名有问题.")
                    pass
        return all_files_dict

    # 请求并保存请求到的两种类型的报表
    def request_save_all_2_files(files_save_dirname=r"F:\remote_get_five_files"):
        # now_date = str(datetime.now().date())
        # files_save_dirname = os.path.join(files_save_dirname, now_date)
        station_name = stationsQueue.dequeue()
        if station_name:
            # print(f"开始请求ac表和ao表: {station_name} ")
            try:
                all_file_dict = get_all_files_dir(station_name)
            except Exception as e:
                print(e)
                return
            if not all_file_dict:
                return
            all_file_key = all_file_dict.keys()
            all_file_dict = keep_newest_file_dir(all_file_dict, station_name)

            # 站点数据保存的文件夹不存在,则新建
            station_save_folder = os.path.join(files_save_dirname, station_name)
            if not os.path.exists(station_save_folder):
                os.mkdir(station_save_folder)

            # 规范命名
            def format_reports(files_folder: 'dirname', station_name) -> dict:
                '''
                广告报表    :账号-国家-30（7/14/30/60）天-bulksheet-月-日-年
                搜索词报告  :Sponsored Products Search term report-月-日-年
                业务报告    :BusinessReport-月-日-年
                在售商品报告:Active+Listings+Report+月-日-年
                全部商品报告:All+Listings+Report+月-日-年
                订单报告    :All Orders-月-日-年
                :param all_file_dict:
                :return:
                '''
                try:
                    all_report_files = os.listdir(files_folder)
                except:
                    return

                # 删掉历史ac表和ao表Active+Listings+Report  All Orders
                [os.remove(os.path.join(files_folder, file)) for file in all_report_files if
                 ('Active+Listings+Report' in file) or ('All Orders' in file) if
                 os.path.exists(os.path.join(files_folder, file))]
                all_report_files = os.listdir(files_folder)
                # 转换文件名称
                account = station_name[:-3]
                site = station_name[-2:]
                date = datetime.now().strftime('%m-%d-%Y')
                if not all_report_files:
                    return
                report_sign_word = {'sevendays': 7, 'fourteendays': 14, 'bulknearlyamonth': 30, 'sixtydays': 60,
                                    'amazonsponsoredproductsbulk': 1,
                                    'amazonsearchtermreportmonthtodate': '(last_month)',
                                    'amazonsearchtermreport': '', 'all_listing': '', 'avtive_listing': '',
                                    'order': ''}
                # 广告报表改名字典
                report_sign_word = {key: f'{account}-{site}-{value}天-bulksheet-{date}' if ('day' in key.lower()) or (
                        'bulk' in key.lower()) else value for key, value in
                                    report_sign_word.items()}
                # 搜索词改名字典
                report_sign_word = {
                    key: f'Sponsored Products Search term report{value}-{date}' if ('search' in key.lower()) else value
                    for
                    key, value in
                    report_sign_word.items()}
                # # 业务报表改名字典
                # report_sign_word = {key: f'BusinessReport-{date}' if
                # 'business' in key.lower() else value for key, value in report_sign_word.items()}
                # # 在售商品改名字典
                report_sign_word = {key: f'Active+Listings+Report+{date}' if
                'avtive_listing' in key.lower() else value for key, value in report_sign_word.items()}

                # # 全部商品改名字典
                # report_sign_word = {key: f'All+Listings+Report+{date}' if
                # 'all_listing' in key.lower() else value for key, value in report_sign_word.items()}
                # 订单报告改名字典
                report_sign_word = {key: f'All Orders-{date}' if
                'order' in key.lower() else value for key, value in report_sign_word.items()}

                # 修改文件名
                for file in all_report_files:
                    for key in report_sign_word.keys():
                        if key in file.lower():
                            new_file_dirname = report_sign_word[key]
                            file_type = os.path.splitext(file)[-1]
                            try:
                                renameFilePath = os.path.join(files_folder, new_file_dirname + file_type)
                                if os.path.exists(renameFilePath):
                                    os.remove(os.path.join(files_folder, file))
                                    break
                                else:
                                    os.rename(os.path.join(files_folder, file),
                                              os.path.join(files_folder, new_file_dirname + file_type))
                                break
                            except:
                                break

            @retry(tries=3, delay=10)
            def download_from_api(api_dir: 'dir', files_save_dirname, station_name, type):
                newest_dir = api_dir
                newest_dir = newest_dir.replace('/mnt/erp', 'http://erp.yibainetwork.com')
                file_basename = os.path.basename(newest_dir)
                try:
                    request_file = requests.get(newest_dir, timeout=(10, 60))
                except Exception as e:
                    print(e)
                    print(f'{station_name}: 请求错误{newest_dir}')
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
                    files_save_dirname = os.path.join(file_dirname, file_basename)
                    # print(file_dirname)
                    with open(files_save_dirname, 'wb') as f:
                        f.write(out_content)
                    print(f'完成{station_name}:{type}的请求')
                    # 将完成后存储到redis中
                    save_request_log_2_redis(type.lower(),NOWDATE,station_name)
                else:
                    if 'MonthToDate' in newest_dir:
                        return
                    print(f'无法请求{newest_dir}报表! \n status_code:{status_code}')

            # 1.得到广告报表
            if set(['ST', 'BR', 'AO', 'AC', 'AL', 'CP']) != all_file_key:
                lost_file = set(['ST', 'BR', 'AO', 'AC', 'AL', 'CP']) - set(all_file_key)
                print(f'{station_name}缺失 {lost_file}报表.')
            else:
                for key in all_file_dict.keys():
                    for i in range(len(all_file_dict[key])):
                        download_from_api(all_file_dict[key][i], files_save_dirname, station_name, key)

            # station_folder_full_dir = os.path.join(files_save_dirname, station_name)
            # 规范命名,以及删除历史站点文件
            format_reports(station_save_folder, station_name)
            # print(f"完成请求ac表和ao表: {station_name}")
            # request_file_complete_detect(station_save_folder, station_name)
            # zip_station_folder(station_save_folder, station_name)
        # print(f'完成{station_name}的ac和ao表请求.')

    # 生成队列
    global stationsQueue
    stationsQueue = public_function.Queue()
    # todo 请求部分站点
    stationsQueue.enqueue_items(todayQueryStations)
    print(f"请求ac和ao表。此次请求{stationsQueue.size()}个站点.")

    while stationsQueue.size() != 0:
        request_save_all_2_files()
    print(f'完成请求ac和ao表.')


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

    onlyStations = set([public_function.standardize_station(station) for station in onlyStations])

    stationVaild = process_company.Company().station()

    return onlyStations & stationVaild


def save_five_reports():
    """
    将远程请求的5表同步保留到redis中与
    1.首先将站点报表类型:路径 键值对存储到redis
    2. 将文件压缩至到5表压缩的文件夹中
    :return:
    """

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
    REMOTE_SAVE_FOLDER = r'F:\remote_get_five_files'
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
                fileTypeAo = filesType.get('ao', [])
                fileTypeAo.append(os.path.join(stationName, file))
                filesType['ao'] = fileTypeAo

        if filesType:
            # 各个类型中有可能存在几个表,此时保留最大的报表,同时删除其他报表
            for type, path in filesType.items():
                if isinstance(path, list):
                    if len(path) == 1:
                        filesType[type] = os.path.join(REMOTE_SAVE_FOLDER, path[0])
                    else:
                        allFileFullPath = [os.path.join(REMOTE_SAVE_FOLDER, onePath) for onePath in path]
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
    stationsZipFolderPath = r"F:\five_reports_zipped"
    todayWeekDay = datetime.now().weekday()
    for station, stationNewFileTypeDict in stationTypeDict.items():
        stationZipFile = os.path.join(stationsZipFolderPath, station.lower() + '.zip')
        if not os.path.isfile(stationZipFile):
            with zipfile.ZipFile(stationZipFile, 'w') as file:
                pass
        # zip文件中已经存在的文件类型
        try:
            stationExistFile = myZip.zipFileList(stationZipFile)
        except Exception as e:
            print(f'{stationZipFile}有问题')
            os.remove(stationZipFile)
            continue
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
                    station = station.strip().replace('-', '_').replace(' ', '_').lower()
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
    #重新请求全部报表缺失的站点
    requestStation = 1

    logPath = r"F:\five_reports_zipped\request_stations_log.txt"
    with open(logPath, 'a+') as f:
        f.write(f'{msg}\n')
        f.write(f'今日请求站点列表:{allRequestStationMsg}\n\n')
        f.write(f'站点缺失详情:{stationMissedDictMsg}\n')


@public_function.run_time
@retry(tries=3,delay=5)
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
    #     print(datetime.now().replace(microsecond=0))\
    #     raise ConnectionError(f'{sftp} 无法连接.')
    sftp.stat(remoteDir)

    # 将文件保存
    stationName = os.path.split(os.path.dirname(remoteDir))[-1][:-3].lower().replace(' ', "_").replace('-','_') + '_' +os.path.split(os.path.dirname(os.path.dirname(remoteDir)))[-1].lower()
    stationLocalSaveFolder = os.path.join(REMOTE_SAVE_FOLDER, stationName)
    stationLocalSavePath = os.path.join(stationLocalSaveFolder, os.path.basename(remoteDir))

    def _timeout(size, file_size):
        cost_time = int((datetime.now() - startDatetime).total_seconds())
        timeoutSeconds = 300
        if cost_time > timeoutSeconds:
            raise TimeoutError(f'请求报表超时。超过了{timeoutSeconds}秒.')

    if not os.path.exists(stationLocalSaveFolder):
        os.mkdir(stationLocalSaveFolder)
    startDatetime = datetime.now()
    print(f'{stationName} {reportType}表:开始请求.')
    # 删除本类型4天以前的报表
    yesterDayStr = (datetime.now() - timedelta(days=4)).strftime('%Y-%m-%d')
    [os.remove(os.path.join(stationLocalSaveFolder, file)) for file in os.listdir(stationLocalSaveFolder) if
     (reportFolderTypeDictReverse[reportType] in file)
     and (process_files.file_create_time(os.path.join(stationLocalSaveFolder, file))[:10] <= yesterDayStr)]
    try:
        sftp.get(remoteDir, stationLocalSavePath, callback=_timeout)
        print(f'{stationName} {reportType}表:请求完成.')
        # 将成功请求存储到redis中
        save_request_log_2_redis(reportType.lower(),NOWDATE,stationName)
    except Exception as e:
        print(e)
        print(datetime.now().replace(microsecond=0))
        raise ValueError(f'远程文件无法下载:{remoteDir}')
    # sftp.get(remoteDir, stationLocalSavePath, callback=_timeout)
    # 若请求到的表为空则删除
    if (os.path.exists(stationLocalSavePath)) and (process_files.file_size(stationLocalSavePath) == 0):
        os.remove(stationLocalSavePath)
        print(f'{stationName} {reportType}表:数据为空,删除掉。')


@public_function.run_time
def request_st_bt_cp_file():
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
        if 'task_run_date' not in logFileData.columns:
            return
        if logFileData['task_run_date'].dtype != 'object':
            logFileData['task_run_date'] = logFileData['task_run_date'].dt.strftime('%Y-%m-%d')
        return logFileData[logFileData['task_run_date'] == lastDateStr]
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
        response = requests.get(url, params=params, timeout=(10, 60))
        return json.loads(response.content)['jwt']

    def request_reports_log():
        """
        五表请求前一天的日志
        """
        try:
            jwt = request_jwt()
        except Exception as e:
            print('获取jwt连接超时')
            print(e)
            return
        #
        nowDate = datetime.now()
        # nowDateStr = datetime.strftime(nowDate, '%Y-%m-%d')
        lastDate = nowDate - timedelta(days=1)
        lastDateStr = datetime.strftime(lastDate, '%Y-%m-%d')
        request_url = f"http://bi.yibainetwork.com:8000/bi/services/report/log/items?jwt={jwt}"
        remoteReportsNameDict = {'cp': '广告活动批量电子表格', 'br': '业务报告-详情页面上的销售量与访问量-根据ASIN', 'st': '广告报告-商品推广-搜索词-一览'}
        for fileType, remoteFileTypeSaveName in remoteReportsNameDict.items():
            params = {
                "jwt_id": "sz_sales_ad_data_analysis",
                'content': remoteFileTypeSaveName,
                'start_date': lastDateStr,
                'end_date': lastDateStr,
                'page':1,
                'limit':100000,
            }
            response = requests.post(request_url, json=params,timeout=(10,60))
            if response.status_code != 200:
                print(f'{fileType}报表日志请求错误,错误代码:{response.status_code},时间:{datetime.now()}')
                continue
            # 文件流
            html_str = response.content
            # 日志保存文件夹
            responseInfo = json.loads(html_str)
            statusCode =responseInfo['status_code']
            if statusCode != 200:
                continue
            responseData = responseInfo['data']

            responseDataPd = pd.DataFrame(responseData)

            logFolderName = 'log'
            logSaveBaseName = f'{fileType}.xlsx'
            logSaveDirname = os.path.join(REMOTE_SAVE_FOLDER, logFolderName)
            outPath = os.path.join(logSaveDirname, logSaveBaseName)
            responseDataPd.to_excel(outPath,index=False)
            print(f'{fileType}日志请求完成:{datetime.now()}')

    def redis_store_report_remote_path(folderFirstDegree = '/shujubaobiao/report_new/report'):
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
        needColumns = set(['task_report_name', 'platform','task_run_date', 'marketplace', 'store_name', 'file_name'])
        if not needColumns.issubset(set(allPathDf.columns)):
            newColumns = needColumns - set(allPathDf.columns)
            newColumnsStr = ','.join(newColumns)
            raise ValueError(f'远程请求报表,日志文件中有缺失列:{newColumnsStr}')
        # 将df中报表类型转换为文件夹中存储的路径
        allPathDf['task_report_name'] = allPathDf['task_report_name'].replace(remoteFileTypeFolderDict)
        #
        if allPathDf['task_run_date'].dtype != 'object':
            allPathDf['task_run_date'] = allPathDf['task_run_date'].dt.strftime('%Y-%m-%d')
        allPathList = [
            os.path.join(folderFirstDegree, type, task_run_date,platform, marketplace, store_name, file_name)
            for type,task_run_date,platform, marketplace, store_name,file_name in
            zip(allPathDf['task_report_name'], allPathDf['task_run_date'], allPathDf['platform'], allPathDf['marketplace'],
                allPathDf['store_name'], allPathDf['file_name'])]
        # todo 筛选出
        allPathListStation = {
            path: os.path.split(os.path.dirname(path))[-1][:-3].lower().replace(' ', "_").replace('-',
                                                                                                                   '_') + '_' +
                  os.path.split(os.path.dirname(os.path.dirname(path)))[-1].lower()
            for path in set(allPathList)}
        filterStations = set([path for path, station in allPathListStation.items() if station in todayQueryStations])
        # todo 选择一个站点测试
        # filterStations = [path for path in filterStations if 'smandy' in path.lower()]
        if not filterStations:
            return

        # 将请求的文件路径列表存储到队列中

        requestFilesQueue.enqueue_items(filterStations)

    @retry(tries=3, delay=10)
    def server_tran():
        # 连接公司远程
        host = 'yiduan.yibai-it.com'
        port = 2022
        # 获取transport传输实例, sftp服务器 ip + 端口号
        tran = pm.Transport((host, port))
        return tran

    @retry(tries=3,delay=10)
    def server_sftp(tran):
        username = 'gg'
        password = 'yb@2021asd#!'
        # 连接ssh服务器, user + password
        print(f'使用{username}连接{tran.hostname}.')
        tran.connect(username=username, password=password)
        sftp = pm.SFTPClient.from_transport(tran)
        sftp.get_channel().settimeout(60)
        print(f'使用{username}连接{tran.hostname}成功.')
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
                time.sleep(10)
                print(remoteDir)
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
                # requestFilesQueue.enqueue(remoteDir)
            # request_server_files(sftp, remoteDir)

    global REMOTE_SAVE_FOLDER, THREAD_NUM, THREAD_POOL
    REMOTE_SAVE_FOLDER = r'F:\remote_get_five_files'
    THREAD_NUM = 4
    THREAD_POOL = ThreadPoolExecutor(THREAD_NUM)
    # # # # 通过请求远程报表日期来获取报表路径
    redis_store_report_remote_path()
    # # # # 通过路径来请求远程文件到本地
    thread_request_server_files()
    # 将文件夹中的全部文件（3个文件由本py请求,2个文件由其他py请求）压缩到压缩包中,同时保存到redis中


def save_request_log_2_redis(requestType,requestDate,value,signWord='request_log',redisDb=3):
    """
    将请求的报表日志存储到redis中
    :param requestType:
    :param requestDate:
    :param value:
    :param signWord:
    :return:
    """
    requestAllType = ['ac','ao','cp','st','br','all']
    if requestType not in requestAllType:
        raise ValueError(f'requestType must one of {",".join(requestAllType)}')
    if not isinstance(requestDate,date):
        return TypeError(f'requestDate type must date.')
    if not isinstance(value,(str,list,set,tuple)):
        return TypeError(f'value type error.')
    redisKey = f'{signWord}:{requestDate}:{requestType}'
    redisObj = sql_write_read.Redis_Store(db=redisDb)
    if not value:
        return
    if isinstance(value,str):
        redisObj.sadd(redisKey,value)
    if isinstance(value,(set,list,tuple)):
        redisObj.sadd(redisKey,*list(value))


# def request_five_reports(ac_ao_start_hour=10, st_br_cp_start_hour=12):
def request_five_reports(ac_ao_start_hour=18, st_br_cp_start_hour=6):
    """
    易佰请求五表
    ac/ao
    br/st/cp
    :return:
    """
    # redis保存识别字段
    redisSignWord  ='request_log'
    redisDB = 3
    redisReportResult = f'{redisSignWord}:status'
    redisConn = sql_write_read.Redis_Store(db=redisDB)
    while 1:
        global todayQueryStations,NOWDATE
        todayQueryStations = list(station_operator_time(start_day=4, end_day=15))
        # 将今日请求的全部报表存储到redis中
        NOWDATE = datetime.now().date()
        # NOWDATE = (datetime.now()-timedelta(days=1)).date()
        save_request_log_2_redis('all',NOWDATE,todayQueryStations,signWord=redisSignWord,redisDb=redisDB)
        # # todo 测试特定站点
        halfHour = 600
        while 1:
            if datetime.now().hour != ac_ao_start_hour:
                time.sleep(halfHour)
            else:
                request_ao_ac_file()
                break
        while 1:
            # 将请求状态设置为0

            redisConn.set(redisReportResult,'0')

            if datetime.now().hour != st_br_cp_start_hour:
                time.sleep(halfHour)
            else:
                request_st_bt_cp_file()
                break
        save_five_reports()
        redisConn.set(redisReportResult, '1')


if __name__ == "__main__":
    request_five_reports()
