
"""
将每日请求的报表情况写入到redis中
"""
from datetime import datetime
import os


from my_toolkit import public_function


def request_result(file_folder=r"F:\remote_get_five_files"):
    """
    每日请求报表的结果,并将结果上传到Redis中
    :param file_folder:
    :return:
    """
    errorMsg = '完成今天报表请求日志出错.'
    if not os.path.exists(file_folder):
        print(errorMsg)
        return

    def filename_2_type(filepath):
        """
        通过文件的关键词将文件名修改为文件类型
        :param fileBasename:
        :return:
        """
        # 文件类型的关键词
        fileTypeSignDict = {'cp': 'bulk', 'ac': 'active', 'st': 'search', 'ao': 'order', 'br': "business"}
        for type,sign in fileTypeSignDict.items():
            if sign in os.path.basename(filepath).lower():
                return type
        return

    allStationsTypeDict = {}
    for root, dirs, files in os.walk(file_folder):  # 将os.walk在元素中提取的值，分别放到root（根目录），dirs（目录名），files（文件名）中。
        for file in files:
            stationName = os.path.basename(root).lower()
            filePath = os.path.join(root, file)
            # 将文件路径转化为文件类型
            fileType = filename_2_type(filePath)
            if stationName not in allStationsTypeDict.keys():
                allStationsTypeDict[stationName] = [fileType]
            else:
                stationExistFiles = allStationsTypeDict[stationName]
                stationExistFiles.append(fileType)
                allStationsTypeDict[stationName] = stationExistFiles
    # 将文件的请求结果上传到redis中
    if len(allStationsTypeDict) == 0:
        print(errorMsg)
        return
    else:
        red_1 = public_function.Redis_Store(db=1)
        # 上传到redis hset 键为api_request_files:station_name
        # 键包括文件的数据表的个数,以及每个表是否存在:存在为1，不存在为0
        allNeedIncludeTypes = ['ac','ao','br','cp','st']
        redisApiRequestFileSign = 'api_request_files'
        # 删除历史数据
        [red_1.delete(key) for key in red_1.keys()]
        for station,requestResult in allStationsTypeDict.items():
            if not requestResult:
                continue
            redisApiRequestFileKey = f'{redisApiRequestFileSign}:{station}'
            # # 个数
            # reqeustNum = len(requestResult)
            # red_1.hset(redisApiRequestFileKey,'num',reqeustNum)
            # 文件类型
            fileTypeResult = {}
            for type in allNeedIncludeTypes:
                if type in requestResult:
                    fileTypeResult[type] = 1
                else:
                    fileTypeResult[type] = 0
            red_1.hmset(redisApiRequestFileKey, fileTypeResult)
    print('完成今日的站点请求日志.')
