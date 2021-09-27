"""
处理销售上传的五表
"""

# 上传到新的zip文件中,并需要将老的同类型的文件给删掉
import os
import re
import shutil
import zipfile
from datetime import datetime,timedelta
import time

from my_toolkit import public_function,process_files,process_station


def process_seller_upload_stations_files():
    """从redis中取出需要处理的单个站点"""

    def process_one_station(stationBasePath,detele_source=True):

        def zipFileList(oldZipFile):
            """
            获取压缩文件中的全部文件
            :param oldZipFile:
            :return:
            """
            if not zipfile.is_zipfile(oldZipFile):
                return []
            with zipfile.ZipFile(oldZipFile, 'r') as zipfileOpen:
                return [item.filename for item in zipfileOpen.infolist()]

        def zip_delete(oldZipFile, file):
            """
            压缩文件删除文件
            Parameters
            ----------
            zipfile :
            file :

            Returns
            -------

            """
            if not os.path.isfile(oldZipFile):
                return
            zipfileTemp = os.path.join(os.path.dirname(oldZipFile),
                                       os.path.splitext(os.path.basename(oldZipFile))[0] + '_TEMP' +
                                       os.path.splitext(os.path.basename(oldZipFile))[1])

            zin = zipfile.ZipFile(oldZipFile, 'r')
            zout = zipfile.ZipFile(zipfileTemp, 'w')
            for item in zin.infolist():
                buffer = zin.read(item.filename)
                if item.filename != file:
                    zout.writestr(item, buffer)
            zout.close()
            zin.close()
            shutil.move(zipfileTemp, oldZipFile)

        sellerSaveFolder = "F:/sales_upload_zipped"
        sellerSavePath = os.path.join(sellerSaveFolder, stationBasePath.upper())
        sellerUploadFiles = zipFileList(sellerSavePath)
        uploadFilesType = process_files.file_type(sellerUploadFiles)
        stationExistFolder = "F:/five_reports_zipped"
        stationZip = os.path.join(stationExistFolder, stationBasePath.lower())
        reportTypeSignDict = {'bulk': 'cp', 'business': 'br', 'search': 'st',
                              'active': 'ac', 'orders': 'ao'}
        reportTypeSignDictReverse = {value: key for key, value in reportTypeSignDict.items()}

        if os.path.exists(stationZip):
            if 'ao' in uploadFilesType:
                aoFile = [file for file in zipFileList(stationZip) if re.findall('^\d', os.path.basename(file))]
                if aoFile:
                    try:
                        [zip_delete(stationZip, file) for file in aoFile]
                    except Exception as e:
                        print(e)
                # aoFile = [ for file in existTypeFile if ]
            existTypeFile = zipFileList(stationZip)
            for type in uploadFilesType:
                typeSignWord = reportTypeSignDictReverse.get(type, 'None')
                deleteTypeFiles = [file for file in existTypeFile if typeSignWord.lower() in file.lower()]
                try:
                    [zip_delete(stationZip, file) for file in deleteTypeFiles]
                except Exception as e:
                    print(e)

        # 将临时压缩文件中的全部文件写入到压缩文件中
        z1 = zipfile.ZipFile(stationZip, 'a')
        z2 = zipfile.ZipFile(sellerSavePath, 'r')
        [z1.writestr(process_station.standardStation(t[0]), t[1].read()) for t in
         ((n, z2.open(n)) for n in z2.namelist())]
        z2.close()
        z1.close()
        # 将上传的报表类型上传到redis数据库中
        _connRedis = public_function.Redis_Store(db=1)
        requestedFilesRedisSignKey = 'api_request_files'
        uploadStationName = process_station.standardStation(os.path.splitext(stationBasePath)[0])
        stationrequestedFilesRedisKey = f"{requestedFilesRedisSignKey}:{uploadStationName}"
        expireDate = (datetime.now() + timedelta(days=3)).date()
        expireDatetime = datetime(expireDate.year, expireDate.month, expireDate.day)
        expireTime = int((expireDatetime - datetime.now()).total_seconds())
        if uploadFilesType:
            for type in uploadFilesType:
                fileTypeRedisKey = stationrequestedFilesRedisKey + '_' + type
                # print(fileTypeRedisKey,expireTime)
                _connRedis.set(fileTypeRedisKey, 10, ex=expireTime)

        if detele_source:
            try:
                sellerSavePath = os.path.join(sellerSaveFolder, stationBasePath)
                if os.path.exists(sellerSavePath):
                    os.remove(sellerSavePath)
                else:
                    pass
            except:
                pass

    while 1:
        redis = public_function.Redis_Store(db=0)
        sellerStoreKey = 'seller_upload_station'
        if sellerStoreKey not in redis.keys():
            # print('暂时没有更新,休息10秒.')
            time.sleep(10)
            continue
        processFolder = redis.blpop(sellerStoreKey)
        if processFolder is None:
            # print('暂时没有更新,休息10秒.')
            time.sleep(10)
        else:
            try:
                process_one_station(processFolder[1])
            except Exception as e:
                print(e)
                print(f'{processFolder[1]}:将销售上传的表压缩到五表有问题')


if __name__ == '__main__':
    process_seller_upload_stations_files()