#!/usr/bin/env python
# coding=utf-8
# author:marmot
import sys
# reload(sys)
# sys.setdefaultencoding('utf-8')

import openpyxl
import re
import pandas as pd
import os

campaign_sheet_name = {'US':'Sponsored Products Campaigns 1','UK':'Sponsored Products Campaigns 1',
                        'CA':'Sponsored Products Campaigns 1','MX':u'Campañas de Sponsored Products',
                        'ES':u'Campañas de Sponsored Products' ,'JP':'uスポンサープロダクトキャンペーン 1',
                        'IT':'Campagne Sponsored Products 1','FR':'Campagnes Sponsored Products 1',
                        'DE':'Sponsored Products Kampagnen 1'}

campaign_sheet_name_en = {'US':'Sponsored Products Campaigns','UK':'Sponsored Products Campaigns',
                        'CA':'Sponsored Products Campaigns','MX':u'Campañas de Sponsored Products',
                        'ES':u'Campañas de Sponsored Products' ,'JP':u'スポンサープロダクトキャンペーン',
                        'IT':'Campagne Sponsored Products','FR':'Campagnes Sponsored Products',
                        'DE':'Sponsored Products Kampagnen','IN':'Sponsored Products Campaigns'}


def read_campaign(file_dir, site):
    station_abbr = site.upper()
    if station_abbr == 'SP':
        station_abbr = 'ES'
    # print station_abbr
    cam = pd.DataFrame()
    # 获取当前的campaign表名
    us_cam = campaign_sheet_name_en['US']
    now_cam = campaign_sheet_name_en[station_abbr]
    # 获取所有表单的数据到字典
    cam_dict = pd.read_excel(file_dir, sheet_name=None)
    if '商品推广活动' in cam_dict.keys():
        cam = cam_dict['商品推广活动']
        return cam
    if 'Sponsored Products Campaigns' in cam_dict.keys():
        cam = cam_dict['Sponsored Products Campaigns']
        return cam
    for one_key in cam_dict.keys():
        if re.search(us_cam, one_key):
            cam = cam_dict[one_key]
            break
        elif re.search(now_cam, one_key):
            cam = cam_dict[one_key]
            break
    return cam


def read_campaign_together(file_dir, station_abbr):
    cam = pd.DataFrame()
    try:
        wb = openpyxl.load_workbook(file_dir)
        # 获取workbook中所有的表格
        sheets = wb.sheetnames
        wb.close()
        flag = 0
    except Exception as err:
        flag = 0
        sheets =[]
        print("无法读取通过openpyxl获取表单信息")
        print(err)
    # 获取当前的campaign表名
    us_cam = campaign_sheet_name_en['US']
    now_cam = campaign_sheet_name_en[station_abbr]
    # 通过openpyxl获取表单信息
    if flag and sheets:
        print("通过openpyxl获取表单信息")
        for one_sheet in sheets:
            if re.search(us_cam,one_sheet):
                cam = pd.read_excel(file_dir, sheet_name=us_cam)
            elif re.search(now_cam,one_sheet):
                cam = pd.read_excel(file_dir, sheet_name=now_cam)
            break
    # 如果无法通过openpyxl获取表单信息则直接读取campaign中所有表单
    elif flag == 0:
        print("因无法通过openpyxl获取表单，直接读取campaign中所有表单")
        cam_dict = pd.read_excel(file_dir, sheet_name=None)
        for one_key in cam_dict.keys():
            if re.search(us_cam, one_key):
                cam = cam_dict[us_cam]
            elif re.search(now_cam, one_key):
                cam = cam_dict[now_cam]
            break

    return cam

