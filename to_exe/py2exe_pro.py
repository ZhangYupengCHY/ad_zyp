import os
import glob
import pandas as pd
import re
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
import traceback
from datetime import datetime
from sqlalchemy import create_engine
from retry import retry

##############################################################GUI##############################################################################
win = tk.Tk()
win.title("Monthly Combine")  # 添加标题

ttk.Label(win, text="Choose Your Name:").grid(column=0, row=0)  # 设置其在界面中出现的位置  column代表列   row 代表行
ttk.Label(win, text="Folder Address:").grid(column=0, row=1)  # 添加一个标签，并将其列设置为1，行设置为0

# Name 下拉列表
person = tk.StringVar()
personChosen = ttk.Combobox(win, width=20, textvariable=person)
personChosen['values'] = ("请选择姓名",
                          "陈梦文", "刘艳", "何莲", "李晨", "李萌", "李娜",
                          "廖凯锋", "刘银萍", "马良", "毛汉芬", "苏雅丽", "汪维", "贾晨阳", "陈悦", "李舜禹", "赵天钰",
                          "王艳", "向江燕", "张立滨", "待加入姓名")  # 设置下拉列表的值
personChosen.grid(column=1, row=0)  # 设置其在界面中出现的位置  column代表列   row 代表行
personChosen.current(0)  # 设置下拉列表默认显示的值，0为 numberChosen['values'] 的下标值

# Address 文本框
address = tk.StringVar()  # StringVar是Tk库内部定义的字符串变量类型，在这里用于管理部件上面的字符；不过一般用在按钮button上。改变StringVar，按钮上的文字也随之改变。
addressEntered = ttk.Entry(win, width=40,
                           textvariable=address)  # 创建一个文本框，定义长度为12个字符长度，并且将文本框中的内容绑定到上一句定义的name变量上，方便clickMe调用
addressEntered.grid(column=1, row=1)  # 设置其在界面中出现的位置  column代表列   row 代表行
addressEntered.focus()  # 当程序运行时,光标默认会出现在该文本框中

# 提示信息 Text
tip = tk.Label(win, background='seashell', foreground='red',
               text='地址类似于' + r'D:\工作\2020年2月' + ', 只能包含一个\'年\'一个\'月\'')
tip.grid(column=1, row=2)




##############################################################Functions#################################################################################
# button被点击之后会被执行
def old_clickMe():  # 当acction被点击时,该函数则生效
    action.configure(text='Hello ' + personChosen.get())  # 设置button显示的内容
    lab3.delete(0.0, tk.END)
    # action.configure(state='disabled')  # 将按钮设置为灰色状态，不可使用状态
    for i in ['mad', 'angry', 'sad']:
        showtxt = i
        lab3.insert('insert', showtxt + '\n')
        lab3.update()


def clickMe():
    action.configure(text='Hello ' + personChosen.get())  # 设置button显示的内容
    # action.configure(state='disabled')  # 将按钮设置为灰色状态，不可使用状态
    lab3.delete(0.0, tk.END)

    folder_path = address.get()
    month_flag = int(re.findall('.*年(.*)月.*', folder_path)[0])
    year_flag = int(re.findall('[0-9]{4}', folder_path)[0])
    # print("Month %s,Processing..." % month_flag)
    showtxt = "Month %s, Now Is Processing..." % month_flag
    lab3.insert('insert', showtxt + '\n')
    lab3.update()
    os.chdir(folder_path)
    now_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # every station country get data------------------------------------------------------------------------------------
    df_br = pd.DataFrame()
    df_br_all = pd.DataFrame()
    df_cp = pd.DataFrame()
    df_cp_all = pd.DataFrame()

    # 添加df到数据库中
    @retry(tries=3,delay=0.5)
    def to_table_append(df, db_name):
        con = engine.connect()  # 创建连接
        df.to_sql(name=db_name, con=con, if_exists='append', index=False)
        con.close()

    for name in os.listdir(folder_path):
        # account, country = (name.split("~")[0], name.split("~")[1].upper())
        account = re.findall('[A-Za-z0-9]+', name)[0].lower()
        country = re.findall('[A-Za-z0-9]+', name)[1].upper()
        files_path = folder_path + r'\\' + name
        os.chdir(files_path)
        for file in glob.glob('*BusinessReport*.csv'):
            df_br = pd.DataFrame(pd.read_csv(file, engine='python', encoding='utf_8_sig'))
            df_br.rename(columns={"日期": "Date",
                                  "已订购商品销售额": "Ordered Product Sales",
                                  "已订购商品的销售额 – B2B": "Ordered Product Sales – B2B",
                                  "已订购商品数量": "Units Ordered",
                                  "订购数量 – B2B": "Units Ordered – B2B",
                                  "订单商品种类数": "Total Order Items",
                                  "订单商品总数 – B2B": "Total Order Items – B2B",
                                  "页面浏览次数": "Page Views",
                                  "买家访问次数": "Sessions",
                                  "购买按钮赢得率": "Buy Box Percentage",
                                  "订单商品数量转化率": "Unit Session Percentage",
                                  "商品转化率 – B2B": "Unit Session Percentage – B2B",
                                  "平均在售商品数量": "Average Offer Count",
                                  "平均父商品数量": "Average Parent Items"}, inplace=True)
            df_br['account'] = account
            df_br['country'] = country
            df_br['updatetime'] = now_datetime
            df_br = df_br.reindex(
                columns=['account', 'country', 'Date', 'Ordered Product Sales', 'Units Ordered', 'Sessions',
                         'updatetime'])
            df_br_all = df_br_all.append(df_br)


        # print(account + "_" + country + ",Business Report Done!!!")
        showtxt = account + "_" + country + ",Business Report Done!!!"
        lab3.insert('insert', showtxt + '\n')
        # lab3.update()
        lab3.see(tk.END)
        for file in glob.glob('*CAMPAIGN*'):
            if file[-3:] == 'csv':
                try:
                    df_cp = pd.DataFrame(pd.read_csv(file, engine='python', encoding='utf_8_sig'))
                except:
                    df_cp = pd.DataFrame(pd.read_csv(file, engine='python', encoding='ANSI'))
            else:
                df_cp = pd.DataFrame(pd.read_excel(file))
            df_cp['account'] = account
            df_cp['country'] = country
            # campaign 表头标准化
            df_cp.rename(columns=lambda x: re.sub('\\(.*?\\)|\\{.*?}|\\[.*?]', '', x), inplace=True)
            df_cp.rename(columns={'状态': 'State', '广告活动': 'Campaigns', '状态.1': 'Type', '类型': 'Status', '投放': 'Targeting',
                                  '广告活动的竞价策略': 'Campaign bidding strategy', '开始日期': 'Start date', '结束日期': 'End date',
                                  '广告组合': 'Portfolio',
                                  '预算': 'Budget', '曝光量': 'Impressions', '点击次数': 'Clicks', '点击率 ': 'CTR', '花费': 'Spend',
                                  '每次点击费用 ': 'CPC', '订单': 'Orders', '销售额': 'Sales', '广告投入产出比 ': 'ACoS','ACOS':'ACoS'}, inplace=True)
            df_cp['updatetime'] = now_datetime
            df_cp = df_cp.reindex(
                columns=['account', 'country', 'State', 'Campaigns', 'Status', 'Type', 'Targeting', 'Start date',
                         'End date', 'Budget',
                         'Impressions', 'Clicks', 'CTR', 'Spend', 'CPC', 'Orders', 'Sales', 'ACoS','updatetime'])
            df_cp_all = df_cp_all.append(df_cp)
        # print(account + "_" + country + ",Campaign Done!!!")

        showtxt = account + "_" + country + ",Campaign Done!!!"
        lab3.insert('insert', showtxt + '\n')
        lab3.see(tk.END)

    # business report ,月份/年份/货币金额---------------------------------------------------------------------------------
    def str2month(df):
        if len(re.findall('[0-9]+', df['Date'])[0]) == 4:
            return re.findall('[0-9]+', df['Date'])[1]
        else:
            if df['country'] in ['US', 'CA', 'JP', 'MX', 'AE']:
                return re.findall('[0-9]+', df['Date'])[0]
            else:
                return re.findall('[0-9]+', df['Date'])[1]

    def str2year(df):
        if len(re.findall('[0-9]+', df['Date'])[0]) == 4:
            return re.findall('[0-9]+', df['Date'])[0]
        else:
            return re.findall('[0-9]+', df['Date'])[2]

    df_br_all = df_br_all.reset_index(drop=True)
    try:
        df_br_all['month'] = df_br_all.apply(lambda x: str2month(x), axis=1)
        df_br_all['year'] = df_br_all.apply(lambda x: str2year(x), axis=1)
        df_br_all['month'] = df_br_all['month'].astype('int')
        df_br_all['year'] = df_br_all['year'].astype('int')
        df_br_all['Ordered Product Sales'] = df_br_all['Ordered Product Sales'].str.extract('(\d+,?\d*.\d+)')
        for col in ['Ordered Product Sales', 'Units Ordered', 'Sessions']:
            df_br_all[col] = df_br_all[col].astype('str')
            df_br_all[col] = df_br_all[col].str.replace(',', '').astype('float')
        df_br_month = df_br_all[(df_br_all['month'] == month_flag) & (df_br_all['year'] == year_flag)]
    except Exception as e:
        info = traceback.format_exc()
        error_r = int(info[info.find('occurred at index ') + 18:][:-3])
        showtxt = df_br_all.iloc[error_r]['account'] + "_" + df_br_all.iloc[error_r]['country'] + ",BusinessReport有误"
        lab3.insert('insert', showtxt + '\n')
        lab3.see(tk.END)

    # campaign, 直接汇总生成--------------------------------------------------------------------------------------------
    def money2num(num):
        num = num.rstrip()
        if any(i in num for i in [',', '.']):  # 原数据中含有,.等符号
            res = ''
            for ii in filter(str.isdigit, num):
                res += ii
            if num[-3].isdigit():
                return float(res) / 10
            else:
                return float(res) / 100
        else:
            return float(num + '00') / 100

    def amount2num(num):
        res = ''
        for ii in filter(str.isdigit, num.split('.')[0]):
            res += ii
        return int(res)

    df_cp_all.dropna(subset=['Spend'], inplace=True)
    for col in ['Spend', 'Sales']:
        df_cp_all[col] = df_cp_all[col].astype('str')
        df_cp_all[col] = df_cp_all[col].apply(lambda x: money2num(x))

    for col in ['Clicks', 'Orders']:
        df_cp_all[col] = df_cp_all[col].astype('str')
        df_cp_all[col] = df_cp_all[col].apply(lambda x: amount2num(x))
    df1 = df_cp_all.groupby(['account', 'country'])['Clicks', 'Spend', 'Orders', 'Sales'].sum().reset_index()
    df1['ACoS'] = df1['Spend'] / df1['Sales']
    df1['CPC'] = df1['Spend'] / df1['Clicks']
    df1['CR'] = df1['Orders'] / df1['Clicks']

    # 广告报表中添加时间两列
    df_cp_all['data_month'] = month_flag
    df_cp_all['data_year'] = year_flag

    # 将汇总后的Br表和广告报表上传到广告组服务器中
    # 初始化连接数据库
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(
        'marmot', 'marmot123', '127.0.0.1', 3306, 'team_station', 'utf8'))
    export_df_dict = {'station_monthly_data_campaignreport':df_cp_all,'station_monthly_data_businessreport':df_br_all}
    for table_name,df in export_df_dict.items():
        if not df.empty:
            # 将df中的列名修改为用下划线连接
            new_df = df.copy()
            new_df.columns = [col.replace(' ','_') for col in df.columns]
            try:
                to_table_append(new_df,table_name)
            except Exception as e:
                engine.dispose()
                raise messagebox.showinfo('连接广告组服务器错误',f'{e}')
    engine.dispose()

    # business merge campaign-------------------------------------------------------------------------------------------
    df_merge = df_br_month.merge(df1, on=['account', 'country'], how='outer')
    df_merge['account'] = df_merge['account'].str.upper()
    df_merge['month'] = df_merge['month'].astype('str') + '月'
    df_merge['year'] = str(year_flag) + '年'
    df_merge['account-country'] = df_merge['account'] + '-' + df_merge['country']
    df_merge['spend/sales'] = df_merge['Spend'] / df_merge['Ordered Product Sales']
    df_merge['Clicks/Sessions'] = df_merge['Clicks'] / df_merge['Sessions']
    df_merge['order percentage'] = df_merge['Orders'] / df_merge['Units Ordered']
    df_merge['session percentage'] = df_merge['Units Ordered'] / df_merge['Sessions']
    df_merge = df_merge.reindex(
        columns=['account', 'country', 'account-country', 'year', 'month', 'Spend', 'ACoS', 'Sales',
                 'Ordered Product Sales', 'spend/sales', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
                 'Clicks', 'Sessions', 'Clicks/Sessions', 'Orders', 'Units Ordered',
                 'order percentage', 'CR', 'session percentage'])
    os.chdir(folder_path[:folder_path.rfind('\\')])
    df_merge.to_excel('alldata' + f'{year_flag}年' + str(month_flag) + '月-' + personChosen.get() + '.xlsx', index=False)
    showtxt = "All Files Are Done!!! 请关闭窗口并到地址的上一级查看汇总文件"
    lab3.insert('insert', showtxt + '\n')
    lab3.update()
    lab3.see(tk.END)
    # lab3.mark_set('insert',1.5)
    return None


# 按钮
action = ttk.Button(win, text="Ready? Go!", command=clickMe)  # 创建一个按钮, text：显示按钮上面显示的文字, command：当这个按钮被点击之后会调用command函数
action.grid(column=2, row=1)  # 设置其在界面中出现的位置  column代表列   row 代表行

# 输出框
showtxt = tk.StringVar()
lab3 = tk.Text(win, fg='blue')
# lab3 = tk.Label(win,textvariable = showtxt,height=10, width=50,fg='blue',bg='yellow')
lab3.grid(row=3, column=0, columnspan=3)

win.mainloop()  # 当调用mainloop()时,窗口才会显示出来
