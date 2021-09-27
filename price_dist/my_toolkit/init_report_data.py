"""
初始化五表的数据,主要包括数据的表头
"""

import pandas as pd


def init_report(report_data):
    """
    1.首先初始化ST报表
    :param report_data:
    :return:
    """
    if (report_data is None) or (report_data.empty) or (not isinstance(report_data, pd.DataFrame)):
        return
    # 删除列标题中的空格
    report_data.columns = [col.strip(' ') for col in report_data.columns]
    report_columns = report_data.columns
    # st 表需要的列
    st_need_columns = ['7 Day Total Sales', 'Advertising Cost of Sales (ACoS)']
    for col in st_need_columns:
        st_ori_col = [ori_col for ori_col in report_columns if col in ori_col]
        if len(st_ori_col) == 1:
            report_data.rename(columns={st_ori_col[0]: col}, inplace=True)