import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
import datetime as dt
import matplotlib.dates as mdate
import threading
from itertools import accumulate


# 计算每个站点的积累值
def add_accumulate(one_account_data):
    one_account_data['add_sales'] = list(accumulate(one_account_data['sales']))
    one_account_data['add_spend'] = list(accumulate(one_account_data['spend']))
    one_account_data['add_impression'] = list(accumulate(one_account_data['impression']))
    one_account_data['add_clicks'] = list(accumulate(one_account_data['clicks']))
    one_account_data.reset_index(drop=True, inplace=True)
    all_acos = []
    for i in range(len(one_account_data['add_spend'])):
        if one_account_data['add_sales'][i] > 0:
            temp_acos = one_account_data['add_spend'][i]/one_account_data['add_sales'][i]
        else:
            temp_acos = 0
        all_acos.append(temp_acos)
    one_account_data['add_acos'] = all_acos
    return one_account_data


def save_accounts_png(all_accounts_df):
    all_accounts_df.replace('None', 0, inplace=True)
    all_accounts_df['date'] = [datetime.strptime(i, '%Y-%m-%d') for i in all_accounts_df['date'] if isinstance(i, str)]
    # start_date = datetime.now() - dt.timedelta(days=interval_days)
    # all_accounts_df = all_accounts_df[all_accounts_df['date'] > start_date]
    all_accounts_df = all_accounts_df[['account','site','date','acos','cpc','cr','spend','sales','impression','clicks']]
    all_accounts_df[['acos', 'cpc', 'cr', 'spend', 'sales']] = all_accounts_df[['acos', 'cpc', 'cr', 'spend', 'sales']].astype('float')
    all_accounts_df[['impression', 'clicks']] = all_accounts_df[['impression', 'clicks']].astype('int')
    all_accounts_sites = all_accounts_df[['account', 'site']]
    all_accounts_sites.drop_duplicates(inplace=True)
    for account, site in zip(all_accounts_sites['account'], all_accounts_sites['site']):
        try:
            show_info(all_accounts_df, account, site)
        except Exception as e:
            print(e)
            print(account,site)


# 获取redis中的数据进行展示
def show_info(all_accounts_df, account, site, size=(1920, 984), my_dpi=100, interval_days=31):
    one_account_site_info = all_accounts_df[(all_accounts_df['account'] == account) & (all_accounts_df['site'] == site)]
    last_day = one_account_site_info['date'].max()
    start_day = last_day - dt.timedelta(days=interval_days)
    one_account_site_info = one_account_site_info[one_account_site_info['date'] > start_day]
    one_account_site_info.drop_duplicates(inplace=True)
    one_account_site_info = add_accumulate(one_account_site_info)
    bar_width = 0.25
    cc = one_account_site_info
    # 实例化
    fig = plt.figure(figsize=(size[0]/my_dpi, size[1]/my_dpi), dpi=my_dpi)
    x = cc['date']
    # x = [datetime.strptime(i, "%Y-%m-%d") for i in x]
    x1 = [i + dt.timedelta(hours=6) for i in x]
    x2 = [i + dt.timedelta(hours=12) for i in x]
    x3 = [i + dt.timedelta(hours=18) for i in x]
    # 创造子图
    ax1 = fig.add_subplot(211)
    # 绘图
    ax1.plot(x, cc['acos'], color='blue', label='acos')
    # 添加图例acos
    for a, b in zip(x, cc['acos']):
        if b != 0:
            plt.text(a, b + 0.005, '{:0.2f}%'.format(b * 100), ha='center', va='bottom', fontsize=7)

    ax1.bar(x, cc['cpc'], color='lightcoral', label='cpc', width=bar_width)
    # 添加图例cpc
    for a, b in zip(x, cc['cpc']):
        if b != 0:
            plt.text(a, b + 0.005, '{:0.2f}'.format(b), ha='center', va='bottom', fontsize=7)
            # 添加图例cr
    for a, b in zip(x1, cc['cr']):
        if b != 0:
            plt.text(a, b + 0.01, '{:0.2f}%'.format(b * 100), ha='center', va='bottom', fontsize=7)
    ax1.bar(x1, cc['cr'], color='moccasin', label='cr', width=bar_width)
    # 设置横纵标签
    ax1.set_ylabel('acos/cpc/cr')
    ax1.set_title("{}_{}".format(account,site))
    # 图例
    plt.legend(loc="upper left")
    ax1.set_xlabel('date')
    # 设置X轴的刻度
    plt.xticks(pd.date_range(min(x), max(x)))  # 设置时间标签显示格式
    ax1.xaxis.set_major_formatter(mdate.DateFormatter('%m-%d'))  # 设置时间标签显示格式

    # 副坐标 绘制总的acos
    ax4 = ax1.twinx()  # this is the important function
    ax4.set_ylabel("ADD acos")
    ax4.plot(x, cc['add_acos'], color='red', label='add_acos')
    for a, b in zip(x, cc['add_acos']):
        if b != 0:
            plt.text(a, b + 0.001, '{:0.2f}%'.format(b * 100), ha='center', va='bottom', fontsize=7)
    plt.legend(loc="upper right")
    # 设置X轴的刻度
    plt.xticks(pd.date_range(min(x), max(x)))  # 设置时间标签显示格式
    ax4.xaxis.set_major_formatter(mdate.DateFormatter('%m-%d'))  # 设置时间标签显示格式

    ax2 = fig.add_subplot(212)

    # 设置横纵标签
    ax2.set_ylabel('spend/sales/(impression/100)/clicks')
    # 绘制spend/sales/ipmression/clicks
    ax2.bar(x, cc['spend'], color='deepskyblue', width=bar_width, label='spend')
    ax2.bar(x1, cc['sales'], color='palegreen', width=bar_width, label='sales')
    ax2.bar(x2, cc['impression'] / 100, color='linen', width=bar_width, label='impression/100')
    ax2.bar(x3, cc['clicks'], color='lightgray', width=bar_width, label='clicks')
    # 图例
    plt.legend(loc="upper left")
    ax2.set_xlabel('date')
    # 设置X轴的刻度
    plt.xticks(pd.date_range(min(x), max(x)))  # 设置时间标签显示格式
    ax2.xaxis.set_major_formatter(mdate.DateFormatter('%m-%d'))  # 设置时间标签显示格式
    # 添加图例spend/sales/ipmression/clicks等图例
    for a, b in zip(x, cc['spend']):
        if b != 0:
            plt.text(a, b + 0.01, '{:0.0f}'.format(b), ha='center', va='bottom', fontsize=7)
    for a, b in zip(x1, cc['sales']):
        if b != 0:
            plt.text(a, b + 0.01, '{:0.0f}'.format(b), ha='center', va='bottom', fontsize=7)
    for a, b in zip(x2, cc['impression'] / 100):
        if b != 0:
            plt.text(a, b + 0.01, '{:0.0f}'.format(b), ha='center', va='bottom', fontsize=7)
    for a, b in zip(x3, cc['clicks']):
        if b != 0:
            plt.text(a, b + 0.01, '{:0.0f}'.format(b), ha='center', va='bottom', fontsize=7)

            # 副坐标
    ax3 = ax2.twinx()  # this is the important function
    ax3.set_ylabel("ADD spend/sales/(impression/100)/clicks")
    # 绘制总 spend/sales/ipmression/clicks等图例
    ax3.plot(x, cc['add_spend'], color='blue', label='add_spend')
    ax3.plot(x, cc['add_sales'], color='green', label='add_sales')
    ax3.plot(x, cc['add_impression'] / 100, color='red', label='add_impression/100')
    ax3.plot(x, cc['add_clicks'], color='black', label='add_clicks')
    # 添加图例总 spend/sales/ipmression/clicks等图例
    for a, b in zip(x, cc['add_spend']):
        if b != 0:
            plt.text(a, b + 0.01, '{:0.0f}'.format(b), ha='center', va='bottom', fontsize=7)
    for a, b in zip(x, cc['add_sales']):
        if b != 0:
            plt.text(a, b + 0.01, '{:0.0f}'.format(b), ha='center', va='bottom', fontsize=7)
    for a, b in zip(x, cc['add_impression'] / 100):
        if b != 0:
            plt.text(a, b + 0.01, '{:0.0f}'.format(b), ha='center', va='bottom', fontsize=7)
    for a, b in zip(x, cc['add_clicks']):
        if b != 0:
            plt.text(a, b + 0.01, '{:0.0f}'.format(b), ha='center', va='bottom', fontsize=7)

    plt.legend(loc="upper right")
    # 设置X轴的刻度
    plt.xticks(pd.date_range(min(x), max(x)))  # 设置时间标签显示格式
    ax3.xaxis.set_major_formatter(mdate.DateFormatter('%m-%d'))  # 设置时间标签显示格式
    # plt.show()
    dir = 'D:/AD-Helper1/show_img/static/img/{}_{}.jpg'.format(account, site)
    fig.savefig(dir, dpi=fig.dpi)
    plt.close()


