# -*- coding: utf-8 -*-
"""
Proj: AD-Helper1
Created on:   2019/12/26 17:21
@Author: RAMSEY



transform server stations folder to individual pc
"""
import gc

import requests, os, shutil
import zipfile
import uuid
from tkinter import LEFT,BOTTOM,RIGHT,TOP,CENTER,BOTH,YES,NO,X,Y,ttk
import tkinter.messagebox
import pandas as pd
import pymysql
from datetime import datetime,timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed


reportTypeSignDict = {'bulk': 'cp', 'business': 'br', 'search': 'st',
                      'active': 'ac', 'orders': 'ao'}


def init_log(logFilePath):
    if not isinstance(logFilePath,str):
        raise TypeError(f'文件应该为路径。{logFilePath}')
    if os.path.splitext(logFilePath)[1].lower() != '.txt':
        raise TypeError(f'文件保存应该为txt。{os.path.splitext(logFilePath)[1]}')
    if not os.path.exists(os.path.dirname(logFilePath)):
        os.mkdir(os.path.dirname(logFilePath))
    if not os.path.exists(logFilePath):
        with open(logFilePath,mode='a+') as f:
            f.write('时间\t站点名\t是否请求到\n')


def file_type(files):
    """
    获取文件类型
    :param files:
    :return: Dict
    """
    FileTypeDict = {}
    for file in files:
        for signWord, type in reportTypeSignDict.items():
            if signWord in file.lower():
                fileTypePath = FileTypeDict.get(type,[])
                fileTypePath.append(file)
                FileTypeDict[type] = fileTypePath
                break
        # ao表需要重新处理
        if os.path.splitext(os.path.basename(file))[0].isdigit():
            fileTypeAo = FileTypeDict.get('ao', [])
            fileTypeAo.append(file)
            FileTypeDict['ao'] = fileTypeAo
    return FileTypeDict


def zipFileList(oldZipFile):
    """
    获取压缩文件中的全部文件
    :param oldZipFile:
    :return:
    """
    if not zipfile.is_zipfile(oldZipFile):
        return []
    with zipfile.ZipFile(oldZipFile, 'r') as zipfileOpen:
        return [item.filename for item in zipfileOpen.infolist() if item.file_size !=0]


class Queue(object):
    # 定义一个空队列
    def __init__(self):
        self.items = []

    # 队列(只能在队尾)添加一个元素
    def enqueue(self, item):
        self.items.append(item)

    # 删除队列（只能在对头）一个元素
    def dequeue(self):
        return self.items.pop(0)

    # 判断队列是否为空
    def isEmpty(self):
        return (self.items == [])

    # 清空队列
    def clear(self):
        del (self.items)  # 该队列就不存在了，而不是清空元素

    # 添加数组
    def enqueue_items(self, items):
        for item in items:
            self.items.append(item)

    # 返回队列项的数量
    def size(self):
        return (len(self.items))

    # 打印队列
    def print(self):
        print(self.items)


# 加载全部的站点名和广告专员
def db_download_station_names(manager,db='team_station', table='only_station_info', ip='127.0.0.1', port=3306,
                              user_name='marmot', password='marmot123',start_day=5, end_day=None) -> pd.DataFrame:
    """
    加载广告组接手的站点名
    :return: 所有站点名
    """
    conn = pymysql.connect(
        host=ip,
        user=user_name,
        password=password,
        database=db,
        port=port,
        charset='UTF8')
    # 创建游标
    cursor = conn.cursor()
    # 写sql
    sql = """SELECT station,update_time,accept_time FROM {} where ad_manger = '{}' """.format(table,manager)
    # 执行sql语句
    cursor.execute(sql)
    stations_name_n_manger = cursor.fetchall()
    queryInfo = pd.DataFrame([list(station) for station in stations_name_n_manger],
                                          columns=['station','update_time','accept_time'])
    queryInfo.drop_duplicates(inplace=True)
    conn.commit()
    cursor.close()
    conn.close()

    if start_day == 0:
        return queryInfo

    # 操作时间在5~12天的站点
    OperatorStationsInfo = queryInfo[~pd.isna(queryInfo['update_time'])]
    OperatorStationsInfo['update_time'] = pd.to_datetime(OperatorStationsInfo['update_time'],
                                                         format='%y-%m-%d %H:%M:%S').dt.date
    # 开始天数
    startDay = (datetime.now() - timedelta(start_day)).date()
    if end_day is not None:
        endDay = (datetime.now() - timedelta(end_day)).date()
        OperatorStationsInfo = OperatorStationsInfo[['station']][
            (OperatorStationsInfo['update_time'] >= endDay) & (OperatorStationsInfo['update_time'] <= startDay)]
    else:
        OperatorStationsInfo = OperatorStationsInfo[['station']][(OperatorStationsInfo['update_time'] <= startDay)]
    return OperatorStationsInfo


def request_station_report(request_url="http://172.16.128.240:8848/"):
    """
    通过api请求站点数据
    Parameters
    ----------
    request_url :

    Returns
    -------

    """
    if 'save_folder_path' not in globals().keys():
        # 选择的路径
        chooseFolder = addressEntered.get()
        chooseFolder = chooseFolder.strip('"').strip("'").strip('“').strip("‘")
        print(chooseFolder)
        chooseFolder = r'{}'.format(chooseFolder)
        if not os.path.isdir(chooseFolder):
            tkinter.messagebox.showinfo('五表请求结果',
                                        f'非法路径:{chooseFolder}\n路径不存在,请创建文件夹\n')
    else:
        chooseFolder = save_folder_path
    if 'stations_queue' in globals().keys():
        if stations_queue.isEmpty():
            return
        else:
            station = stations_queue.dequeue()
    else:
        station = stationChosen.get()
    station = station.strip().replace("-", '_').replace(' ', '_').upper()
    logFile = os.path.join(chooseFolder, 'log.txt')
    if len(station)<4:
        with open(logFile, mode='a+') as f:
            f.write(f'{datetime.now().replace(microsecond=0)}\t{station}\t否\n')
        if 'stations_queue' not in globals().keys():
            root = tkinter.Tk()
            root.withdraw()
            root.wm_attributes('-topmost', 1)
            tkinter.messagebox.showinfo('五表请求结果', f'{station}报表文件不全.请求结果请查看生产的日志文件.\n')
    account = station[:-3].upper()
    request_url = os.path.join(request_url,f'{station}.zip')
    response = requests.get(request_url)
    if response.status_code != 200:
        with open(logFile, mode='a+') as f:
            f.write(f'{datetime.now().replace(microsecond=0)}\t{station}\t否\n')

        if 'stations_queue' not in globals().keys():
            root = tkinter.Tk()
            root.withdraw()
            root.wm_attributes('-topmost', 1)
            tkinter.messagebox.showinfo('五表请求结果', f'{station}报表不存在.请求结果请查看生产的日志文件.\n')
    unzipPathTempFolder = os.path.join(chooseFolder,'TEMP')
    unzipPathTemp = os.path.join(unzipPathTempFolder, station + '.zip')
    if os.path.exists(unzipPathTemp):
        try:
            os.remove(unzipPathTemp)
        except:
            pass
    if not os.path.exists(unzipPathTempFolder):
        try:
            os.makedirs(unzipPathTempFolder)
        except:
            pass
    write_stream_into_file(response,unzipPathTemp)
    stationSaveFolder = os.path.join(chooseFolder,account)
    if os.path.exists(os.path.join(stationSaveFolder,station)):
        try:
            shutil.rmtree(os.path.join(stationSaveFolder,station))
        except:
            pass
    # 判断是否五表齐全
    unzipFiles = zipFileList(unzipPathTemp)
    fileTypesDict = file_type(unzipFiles)
    if len(fileTypesDict) == len(reportTypeSignDict):
        # 解压
        try:
            unzip(unzipPathTemp,stationSaveFolder)
            os.rename(os.path.join(stationSaveFolder,station.lower()),os.path.join(stationSaveFolder,station.upper()))
            # 将日志写入到文件中
            if not os.path.exists(logFile):
                init_log(logFile)
            if fileTypesDict:
                with open(logFile,mode='a+') as f:
                    f.write(f'{datetime.now().replace(microsecond=0)}\t{station}\t是\n')
        except Exception as e:
            with open(logFile, mode='a+') as f:
                f.write(f'{datetime.now().replace(microsecond=0)}\t{station}\t否\n')
    else:
        with open(logFile, mode='a+') as f:
            f.write(f'{datetime.now().replace(microsecond=0)}\t{station}\t否\n')
    # 删除临时文件
    if os.path.exists(unzipPathTemp):
        try:
            os.remove(unzipPathTemp)
        except:
            pass
    if 'stations_queue' not in globals().keys():
        root = tkinter.Tk()
        root.withdraw()
        root.wm_attributes('-topmost', 1)
        tkinter.messagebox.showinfo('五表请求结果', f'{station}请求完成.文件存储在{chooseFolder}下.请求结果请查看生产的日志文件.\n')


def write_stream_into_file(streamResponse,file):
    with open(file, 'wb') as f:
        for chunk in streamResponse.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)


def unzip(zipfilePath,targetFile):
    zFile = zipfile.ZipFile(zipfilePath)
    for fileM in zFile.namelist():
        zFile.extract(fileM, targetFile)
    zFile.close()


def thread_read_file():
    THREAD_POOL = ThreadPoolExecutor(THREADNUM)
    if not os.path.exists(save_folder_path):
        os.mkdir(save_folder_path)
    while 1:
        all_task = []
        for one_page in range(THREADNUM):
            all_task.append(THREAD_POOL.submit(request_station_report))
        for future in as_completed(all_task):
            future.result()
        if stations_queue.isEmpty():
            break


def get_manager():
    # 通过数据库获取mac地址表,返回manager,mac两列
    def db_download_manager_mac(db='ad_db', table='login_user', ip='127.0.0.1', port=3306,
                                user_name='marmot', password='marmot123') -> pd.DataFrame:
        """
        加载所有用户的mac地址
        :return: 用户的mac地址
        """
        conn = pymysql.connect(
            host=ip,
            user=user_name,
            password=password,
            database=db,
            port=port,
            charset='UTF8')
        # 创建游标
        cursor = conn.cursor()
        # 写sql
        sql = """SELECT real_name,pc_mac FROM {} """.format(table)
        # 执行sql语句
        cursor.execute(sql)
        all_manager_mac = cursor.fetchall()
        all_manager_mac = pd.DataFrame([list(mac) for mac in all_manager_mac],
                                       columns=['manager', 'mac'])
        all_manager_mac.drop_duplicates(inplace=True)
        conn.commit()
        cursor.close()
        conn.close()
        return all_manager_mac

    # 控制每人只获取自己的站点数据
    self_mac = ':'.join(['{:02x}'.format((uuid.getnode() >> ele) & 0xff) for ele in range(0, 8 * 6, 8)][::-1])
    # self_mac = 'F0:2F:74:34:03:40'
    all_manager_mac = db_download_manager_mac()
    manager_name = [manager for manager, mac in zip(all_manager_mac['manager'], all_manager_mac['mac']) if
                    self_mac.lower() in mac.lower()]

    if not manager_name:
        root = tkinter.Tk()
        root.withdraw()
        root.wm_attributes('-topmost', 1)
        tkinter.messagebox.showinfo('五表请求结果', f'五表请求错误: {self_mac} 不在mac库中,请联系管理员添加mac.')
        raise ('quit')
    return manager_name[0]


# 通过mac地址匹配获取请求的站点名
def get_stations(manager_name):
    # 请求的站点数
    # stations_name = stations_name_n_manger['station_name']
    stations_name_n_manger = db_download_station_names(manager_name,start_day=6,end_day=None)
    stations_name = stations_name_n_manger['station']
    if stations_name.empty:
        root = tkinter.Tk()
        root.withdraw()
        root.wm_attributes('-topmost', 1)
        tkinter.messagebox.showinfo('五表请求结果', f'五表请求错误: {manager_name} 错误.\n请联系管理员核查mac地址中的姓名和only_station_info中的姓名是否一致.')
        raise ('quit')
    # 规范站点数据
    stations_name = [station.strip().replace("-",'_').replace(' ','_').lower() for station in stations_name]
    return stations_name


def main_station_flow():
    global THREADNUM,save_folder_path,stations_queue
    # 选择的路径
    save_folder_path = addressEntered.get()
    save_folder_path = save_folder_path.strip('"').strip("'").strip('“').strip("‘")
    save_folder_path = r'{}'.format(save_folder_path)
    if not os.path.isdir(save_folder_path):
        tkinter.messagebox.showinfo('五表请求结果',
                                    f'非法路径:{save_folder_path}\n路径不存在,请创建文件夹\n')
    speedValed = speedValue.get()
    if speedValed == 1:
        THREADNUM = 4
    else:
        THREADNUM = 2
    queryStation = get_stations(manager)
    stations_queue = Queue()
    stations_queue.enqueue_items(queryStation)
    stations_num = stations_queue.size()
    thread_read_file()
    root = tkinter.Tk()
    root.withdraw()
    root.wm_attributes('-topmost', 1)
    tkinter.messagebox.showinfo('五表请求结果', f'五表请求完成. 一共请求{stations_num}个站点.文件存储在{save_folder_path}下.请求结果请查看生产的日志文件.\n')
    del stations_queue
    gc.collect()


def tk_window():
    global stationChosen,manager,addressEntered,speedValue
    win = tkinter.Tk()
    win.geometry("360x200")
    win.resizable(False,False)
    win.title("files flow")  # 添加标题
    manager = get_manager()
    # 获取全部站点
    allStationInfo = db_download_station_names(manager,start_day=0)
    if len(allStationInfo.index) != 0:
        allStation = sorted(set(allStationInfo['station']))
    else:
        tkinter.messagebox.showinfo(f'没有接手站点','你暂时没有接手站点')
        raise ('quit')
    frameTop = ttk.Frame(win)
    frame1 = ttk.Frame(win)
    frame2 = ttk.Frame(win)
    # 是否选择了单个站点
    frameTop.pack(side=TOP,padx=0,pady=0,expand=NO)
    frame1.pack(side=LEFT,padx=10,pady=10,expand=YES,fill=BOTH)
    frame2.pack(side=RIGHT,padx=10,pady=10,expand=YES,fill=BOTH)
    speedValue = tkinter.IntVar()
    speedRadio = tkinter.Checkbutton(frameTop,text="加速请求",variable=speedValue,onvalue = 1, offvalue = 0)
    speedRadio.pack(side=LEFT,padx=(20,0),pady=0,expand=NO)
    addressLab = tkinter.Label(frameTop,text='文件保存地址:')
    addressLab.pack(side=LEFT, padx=(20,0), pady=0, expand=NO)
    address = tkinter.StringVar(value=r'D:\待处理')# StringVar是Tk库内部定义的字符串变量类型，在这里用于管理部件上面的字符；不过一般用在按钮button上。改变StringVar，按钮上的文字也随之改变。
    addressEntered = tkinter.Entry(frameTop,textvariable=address)
    addressEntered.pack(side=RIGHT,padx=(0,20),pady=0,expand=NO)
    downAboveFiveBtn = ttk.Button(frame1,text='下载大于5天',command=main_station_flow)
    downAboveFiveBtn.pack(side=LEFT,padx=20,pady=20,expand=YES,fill=BOTH)
    # Name 下拉列表
    singleStation = tkinter.StringVar()
    stationChosen = ttk.Combobox(frame2, width=20, textvariable=singleStation)
    allStation.insert(0,"请选择站点")
    stationChosen['values'] = allStation  # 设置下拉列表的值
    stationChosen.current(0)  # 设置下拉列表默认显示的值，0为 numberChosen['values'] 的下标值
    stationChosen.pack(side=TOP,padx=(0,30),pady=(10,0),expand=YES,fill=X)
    downloadOneSubmit = ttk.Button(frame2,text='下载',command=request_station_report)
    downloadOneSubmit.pack(side=BOTTOM,padx=(0,30),pady=(0,20),expand=YES,fill=BOTH)
    win.mainloop()  # 当调用mainloop()时,窗口才会显示出来


if __name__ == '__main__':
    tk_window()

