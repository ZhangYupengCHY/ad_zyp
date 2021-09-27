"""
处理压缩文件

"""
import os
import shutil
import zipfile
from datetime import datetime,timedelta

import process_files


def zip_delete(oldZipFile,file):
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
    zipfileTemp = os.path.join(os.path.dirname(oldZipFile),os.path.splitext(os.path.basename(oldZipFile))[0]+'_TEMP'+os.path.splitext(os.path.basename(oldZipFile))[1])
    # if not os.path.exists(zipfileTemp):
    #     os.makedirs(zipfileTemp)
    zin = zipfile.ZipFile(oldZipFile,'r')
    zout = zipfile.ZipFile(zipfileTemp,'w')
    for item in zin.infolist():
        buffer = zin.read(item.filename)
        if item.filename != file:
            zout.writestr(item,buffer)
    zout.close()
    zin.close()
    # 用新文件覆盖旧文件
    shutil.move(zipfileTemp, oldZipFile)


def file_create_time_in_zip(zipFile,filename):
    """
    zip文件中的文件的创建时间
    :param mode:
    :return:
    """
    if not os.path.isfile(zipFile):
        return
    fileList = zipFileList(zipFile)
    if not fileList:
        return
    if filename not in fileList:
        return
    rZip = zipfile.ZipFile(zipFile,'r')
    fileCreateTime = rZip.getinfo(filename).date_time
    rZip.close()
    return datetime(fileCreateTime[0],fileCreateTime[1],fileCreateTime[2],fileCreateTime[3],fileCreateTime[4],fileCreateTime[5])



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


def delete_folder_older_zip_files(folder_path,days=1):
    """
    删除文件夹中的压缩文件中时间大于四天的文件
    Parameters
    ----------
    folder_path :
    days :

    Returns
    -------

    """
    zipFiles = [os.path.join(folder_path,zipFile) for zipFile in os.listdir(folder_path) if os.path.splitext(zipFile)[1].lower() == '.zip']
    if not zipFiles:
        return
    olderTime = datetime.now().date() - timedelta(days=days)
    for zipFile in zipFiles:
        try:
            fileModifyTime = process_files.timeStrToTime(process_files.file_modify_time(zipFile)).date()
        except:
            continue
        if fileModifyTime <= olderTime:
            try:
                os.remove(zipFile)
            except:
                pass


def unzip_file(zipFilePath,saveFolder=None):
    """
    将压缩文件解压到指定文件夹中
    Parameters
    ----------
    zipFilePath :
    saveFolder :

    Returns
    -------

    """
    zip_file = zipfile.ZipFile(zipFilePath)
    zip_list = zip_file.namelist()  # 得到压缩包里所有文件
    if saveFolder is None:
        saveFolder = os.path.dirname(zipFilePath)
    if not os.path.exists(saveFolder):
        os.mkdir(saveFolder)
    for f in zip_list:
        zip_file.extract(f, saveFolder)  # 循环解压文件到指定目录
    zip_file.close()