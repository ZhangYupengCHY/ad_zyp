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
from datetime import datetime,timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

import public_function
from my_toolkit import sql_write_read,process_files,station_belong


@public_function.run_time
@public_function.loop_func(update_time=9)
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
            response = requests.post(download_url, data=post_load).content
            data = []
            try:
                data_basic = json.loads(response)['data']
                data.extend(data_basic)
            except:
                pass
                # red.rpush('station_no_data', station_name)
                # return
            # 单独请求四天的广告报表
            post_camp_date = [1, 7, 14, 60]
            all_camp_post_dir = []
            for post_date in post_camp_date:
                post_camp_load = {
                    'token': token,
                    'data': json.dumps({
                        0: {
                            'date_range': 1,
                            "child_type": post_date,
                            'account_id': station_name,
                            "report_type": "Campaign"
                        }
                    })
                }
                response_camp = requests.post(download_url, data=post_camp_load).content
                try:
                    camp_data = json.loads(response_camp)['data']
                except:
                    print(f"{station_name}广告报表{post_date}无法请求..")
                    continue
                all_camp_post_dir.extend(camp_data)
            # data = all_camp_post_dir

            # 本地接口请求AO
            '''
            local_url = 'http://192.168.9.167:8080/services/api/advertise/getreport'
            post_ao_load = {
                'token': token,
                'data': json.dumps({
                    0: {
                        'date_range': 30,
                        "child_type": 30,
                        'account_id': station_name,
                        "report_type": "AO"
                    }
                })
            }
            response_ao = requests.post(local_url,data=post_ao_load).content
            try:
                ao_data = json.loads(response_ao)['data']
                ao_data = [data.replace('D:/phpStudy/PHPTutorial/WWW/wwwerp','http://192.168.9.167:8080') for data in ao_data]
            except:
                print(f"{station_name}:AO报表{post_date}本地无法请求..")
                ao_data = []
            data.extend(ao_data)
            '''

            data.extend(all_camp_post_dir)
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
            all_file_dict = get_all_files_dir(station_name)
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

            def download_from_api(api_dir: 'dir', files_save_dirname, station_name,type):
                newest_dir = api_dir
                newest_dir = newest_dir.replace('/mnt/erp', 'http://erp.yibainetwork.com')
                file_basename = os.path.basename(newest_dir)
                try:
                    request_file = requests.get(newest_dir)
                except Exception as e:
                    # print(e)
                    # print(f'{station_name}: 请求的链接{newest_dir}')
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
                        download_from_api(all_file_dict[key][i], files_save_dirname, station_name,key)


            # station_folder_full_dir = os.path.join(files_save_dirname, station_name)
            # 规范命名,以及删除历史站点文件
            format_reports(station_save_folder, station_name)
            # print(f"完成请求ac表和ao表: {station_name}")
            # request_file_complete_detect(station_save_folder, station_name)
            # zip_station_folder(station_save_folder, station_name)
        # print(f'完成{station_name}的ac和ao表请求.')
    # 其他公司站点
    def get_other_company_station(path):
        station_list = pd.read_excel(path)['广告后台店铺名']
        station_list = [station.replace('-','_').replace(' ','_').lower() for station in station_list]
        return station_list

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
        selectSql = "select station,update_time,accept_time from %s where `station` in (%s)"% (tableName,erpStationStr)
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



    # stations_name_n_manger = db_download_station_names()
    # stations_name = stations_name_n_manger['station_name']
    # # 非易佰站点名
    # path = r"E:\ad_zyp\api_request_files\非易佰站点.xlsx"
    # other_company_station = get_other_company_station(path)
    #
    # # 冻结的站点
    # fz_stations = pd.read_excel(r"E:\ad_zyp\api_request_files\冻结站点.xlsx")
    # fz_stations= fz_stations['冻结站点'].apply(lambda x:x.lower())
    #
    # #请求的站点数
    # stations_name=set(stations_name) - set(other_company_station)
    # stations_name = set(stations_name - set(fz_stations))
    # 每天申请的站点名

    stations_name = station_operator_time(start_day=4, end_day=15)


    # 生成队列
    global stationsQueue
    stationsQueue = public_function.Queue()
    #todo 请求部分站点
    stationsQueue.enqueue_items(stations_name)
    print(f"请求ac和ao表。此次请求{stationsQueue.size()}个站点.")

    while stationsQueue.size() != 0:
        request_save_all_2_files()
    print(f'完成请求ac和ao表.')



if __name__ == '__main__':
    request_ao_ac_file()