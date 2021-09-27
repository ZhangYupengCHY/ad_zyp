"""

请求五表压缩包中的很久之前的站点数据
"""
from datetime import datetime


from my_toolkit import myZip,public_function



@public_function.loop_func(update_time=22)
def clear_station_older_files(folderInfo):
    print(f'{datetime.now().replace(microsecond=0)}:开始删除过期的压缩文件夹')
    for oneFolderInfo in folderInfo:
        myZip.delete_folder_older_zip_files(oneFolderInfo['folder'],days=oneFolderInfo['day'])
    print(f'{datetime.now().replace(microsecond=0)}处理完成')


if __name__ == '__main__':
    deleteFolderInfo  =[{'folder':r"F:/five_reports_zipped",'day':3},{'folder':r"F:/sales_upload_zipped",'day':7}]
    clear_station_older_files(deleteFolderInfo)