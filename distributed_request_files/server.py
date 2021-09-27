# -*- coding: utf-8 -*-
"""
Proj: recommend
Created on:   2020/1/17 11:34
@Author: RAMSEY

Standard:  
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""

from socket import *
import os
import json
import struct
import threading
import time
from datetime import datetime
import shutil


def on_new_client(client_socket, addr, filename):
    # filename = input('请输入要传送的文件名加后缀>>>').strip()
    if not os.path.exists(filename):
        return
    filesize_bytes = os.path.getsize(filename)  # 得到文件的大小,字节
    dirc = {
        'filename': filename,
        'filesize_bytes': filesize_bytes,
    }
    # 发送表头信息
    head_info = json.dumps(dirc)  # 将字典转换成字符串
    head_info_len = struct.pack('i', len(head_info))  # 将字符串的长度打包
    #   先将报头转换成字符串(json.dumps), 再将字符串的长度打包
    #   发送报头长度,发送报头内容,最后放真是内容
    #   报头内容包括文件名,文件信息,报头
    #   接收时:先接收4个字节的报头长度,
    #   将报头长度解压,得到头部信息的大小,在接收头部信息, 反序列化(json.loads)
    #   最后接收真实文件
    client_socket.send(head_info_len)  # 发送head_info的长度
    client_socket.send(head_info.encode('utf-8'))
    #   发送真实信息
    old = time.time()
    with open(filename, 'rb') as f:
        data = f.read()
        client_socket.sendall(data)
    now = time.time()
    stamp = int(now - old)
    # print(f'{client_socket}发送成功!!!')
    print('总共用时%ds' % stamp)
    client_socket.shutdown(2)
    client_socket.close()
    # os.remove(filename)


def socket_service():
    print("服务器启动...\n开始发送文件...")
    # 客户端和端口
    client_address = '0.0.0.0'
    port = 3553
    buffsize = 1024
    # socker初始化
    my_socket = socket(AF_INET, SOCK_STREAM)
    # 绑定ip和端口
    my_socket.bind((client_address, port))
    # 监听客户端的个数，最大连接数
    my_socket.listen(8)
    # 每个人电脑ip
    ip_name_dict = {'192.168.7.203': '李舜禹', '192.168.7.222': '何莲', '192.168.7.108': '张于鹏', '192.168.7.27': '贾晨阳',
                    '192.168.7.122': '廖丹'}
    while True:
        client_socket, client_address = my_socket.accept()
        request_ip = client_address[0]
        if request_ip not in ip_name_dict.keys():
            continue
            print(f"{request_ip} 不在ip列表中.")
        request_name = ip_name_dict[request_ip]
        now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        file_path = f"F:/api_request_all_files/{now_time[:10]}_zipfiles/{request_name}.zip"
        if not os.path.exists(file_path):
            message = f'ip:{request_ip}\tname:{request_name}\ttime: {now_time}\treport: NO report'
            print(message)
            print(f"{request_ip}:{request_name} 请查看对应的姓名是否正确.")
            pass
        else:
            message = f'ip:{request_ip}\tname:{request_name}\ttime: {now_time}\treport: HAVE report'
            print(message)
        ip_request_log = r"E:\ad_zyp\distributed_request_files\request_log.txt"
        with open(ip_request_log, 'a+') as f:
            f.write(f'{message}\n')
        threading.Thread(target=on_new_client, args=(client_socket, client_address, file_path)).start()


if __name__ == '__main__':
    socket_service()
