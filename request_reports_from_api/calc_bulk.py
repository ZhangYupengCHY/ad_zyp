import pandas as pd
import re
import glob
from datetime import datetime
import numpy as np
from request_reports_from_api.plot_account import show_info
from request_reports_from_api.stg_calc_2_sql import write_to_mysql
import sys

sys.path.append('D:\\AD-Helper1\\ad_helper\\init_proc')
from .read_campaign import read_campaign
from .init_campaign import init_campaign
import threading


# 计算每天的广告数据，曝光、点击、花费等
def every_day_campaign(one_account_dir):
    file_date = parse_date(one_account_dir)
    one_dir = one_account_dir.replace("\\", "/")
    # 读取excel内容
    file_data = read_campaign(one_dir, site)
    file_data = init_campaign(file_data, site.upper(), 'empty')
    # print("稍等!!!")
    file_data[['Clicks', 'Orders', 'Impressions']] = file_data[['Clicks', 'Orders', 'Impressions']].applymap(
        lambda x: int(x))
    file_data[['Spend', 'Sales']] = file_data[['Spend', 'Sales']].applymap(lambda x: float(x))
    one_day_clicks = sum(file_data['Clicks'])
    one_day_impression = sum(file_data['Impressions'])
    one_day_orders = sum(file_data['Orders'])
    one_day_spend = sum(file_data['Spend'])
    one_day_sales = sum(file_data['Sales'])
    if one_day_sales == 0:
        acos = 0
    else:
        acos = one_day_spend / one_day_sales
    if one_day_clicks == 0:
        cpc, cr = 0, 0
    else:
        cpc = one_day_spend / one_day_clicks
        cr = one_day_orders / one_day_clicks
    one_day_info = [account, site, file_date, acos, cpc, cr, one_day_spend, one_day_sales, one_day_impression,
                    one_day_clicks]
    return one_day_info
    # # 数据入到redis
    # pass
    # upload_to_redis(all_info)


# 获得日期字符串
def parse_date(file_name):
    file_date = re.findall('2019.[0-9]{2}.[0-9]{2}', file_name)
    file_date = file_date[-1]
    if len(file_date) == 10:
        file_date = file_date[0:4] + '-' + file_date[5:7] + '-' + file_date[-2:]
    if len(file_date) == 8:
        file_date = file_date[0:4] + '-' + file_date[4:6] + '-' + file_date[-2:]
    return file_date


def add_list(list_a):
    list_a = [round(i) if not isinstance(i, int) else i for i in list_a]
    s = []
    for i in range(len(list_a)):
        s.append(sum(list_a[:i + 1]))
    return s


def parse_date(file_name):
    file_date = re.findall('2019.[0-9]{2}.[0-9]{2}', file_name)
    file_date = file_date[-1]
    if len(file_date) == 10:
        file_date = file_date[0:4] + '-' + file_date[5:7] + '-' + file_date[-2:]
    if len(file_date) == 8:
        file_date = file_date[0:4] + '-' + file_date[4:6] + '-' + file_date[-2:]
    return file_date


# 计算每天的广告数据
def get_imp_cpc(file_dir, account_site):
    global account, site
    all_file_dir = glob.glob(file_dir + account_site.upper() + '/*Bulk*.xlsx')
    account = account_site[:-3]
    site = account_site[-2:]
    all_result = pd.DataFrame(
        columns=['account', 'site', 'date', 'acos', 'cpc', 'cr', 'spend', 'sales', 'impression', 'clicks'])
    for one_dir in all_file_dir:
        # print(one_dir)
        try:
            result = every_day_campaign(one_dir)
            if result[8] == 0:
                continue
            all_result.loc[all_result.shape[0] + 1] = result
        except Exception as e:
            # with open(r'C:\Users\Administrator\Desktop\camp_request_lost_dates.txt', 'a+') as f:
            #     f.write('{}\t{}\t{}\n'.format(account, site, date))
            #     f.close()
            print(e)
            pass
    # print('read_calc_all_files cost {}'.format(datetime.now()-t0))
    all_result.sort_values(['account', 'site', 'date'], inplace=True)
    return all_result


"""重新定义带返回值的线程类"""


class MyThread(threading.Thread):
    def __init__(self, func, args=()):
        super(MyThread, self).__init__()
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            return self.result
        except Exception:
            return None
