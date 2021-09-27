import time
from datetime import datetime

import pandas as pd
from retry import retry
from sqlalchemy import create_engine

from my_toolkit import public_function
from my_toolkit import sql_write_read,commonly_params

"""
    获取sku捆绑表,将sku捆绑表一个星期一次从公司服务器的erp上更新到广告组服务器
"""


# @public_function.loop_func(update_time=16)
@public_function.loop_func(update_time=21)
@public_function.run_time
def sku_map_update(update_interval_day=7):
    """
        定时更新广告组服务器中sku捆绑表(team_station.yibai_amazon_sku_map)
        将公司服务器中的sku捆绑表按照一定频率更新到广告组服务器中,供查询
    Returns:
    """

    def sku_map_get_from_company_server_mysql(query_num_interval=100):
        """
        从mysql数据库中获取数据：
            考虑到数据库表限流,于是通过account_id字段(索引)来分段获取
                每次获取200个站点的
        Returns:pd.DataFrame
            sku捆绑表
        """
        # 连接数据库
        engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(
            'amazonaduser', 'Whamazond2020+++', commonly_params.COMPANY_ERP_IP, 3306, 'yibai_product', 'utf8'))
        conn = engine.connect()
        # 每次查询的站点个数
        query_num_interval = query_num_interval
        # 站点总数
        account_max_num_query_lang = "select max(account_id) as account_max_num from yibai_amazon_sku_map"
        account_max_num = pd.read_sql(account_max_num_query_lang, conn)
        account_max_num = account_max_num['account_max_num'].values[0]

        # 每次请求站点的范围
        query_times = int(account_max_num / query_num_interval)

        @retry(tries=3, delay=10)
        def upload_sku_map(start_account, end_account, account_start_id):
            """
            请求站点sku捆绑表,并将捆绑表上传到数据库中
            :param start_account:
            :param end_account:
            :return:
            """
            sku_map_query = "select seller_sku,sku from yibai_amazon_sku_map where account_id <= '%s' and account_id > '%s'" % (
                end_account, start_account)
            print(f'正在请求站点id从{start_account + 1}到{end_account}的sku捆绑表.')
            sku_map_one_time = pd.read_sql(sku_map_query, conn)
            sku_map_one_time.rename(columns={'sku': 'erp_sku'}, inplace=True)
            sku_map_one_time['update_time'] = datetime.now().date()
            account_end_id = len(sku_map_one_time) + account_start_id
            sku_map_one_time['id'] = range(account_start_id, account_end_id)
            sku_map_one_time = sku_map_one_time[['id', 'seller_sku', 'erp_sku', 'update_time']]
            sku_map_table = 'yibai_amazon_sku_map'
            sql_write_read.to_table_append(sku_map_one_time, sku_map_table)
            print(f'完成了站点id从{query_num_range_lower}到{query_num_range_upper}的sku捆绑表的上传数据库.')
            return account_end_id

        account_start_id = 0
        for query_time in range(query_times):
            query_num_range_lower = query_time * query_num_interval
            query_num_range_upper = query_num_range_lower + query_num_interval
            account_start_id = upload_sku_map(query_num_range_lower, query_num_range_upper, account_start_id)

        # 请求站点sku捆绑表的小尾巴
        if query_times * query_num_interval != account_max_num:
            account_start_id = upload_sku_map(query_times * query_num_interval, account_max_num, account_start_id)

        conn.close()

    # 1.首先判断广告组服务器是否为空
    sku_map_table_name_ad_server = 'yibai_amazon_sku_map'
    sku_map_ad_server_is_valid_query_lang = "select * from %s limit 10" % sku_map_table_name_ad_server
    _conn_mysql = sql_write_read.QueryMySQL()
    sku_map_data_get_from_ad_server = _conn_mysql.read_table(sku_map_table_name_ad_server,sku_map_ad_server_is_valid_query_lang)
    _conn_mysql.close()
    if sku_map_data_get_from_ad_server.empty:
        # 将从公司获取的sku捆绑表数据写入到广告组服务器中
        sku_map_get_from_company_server_mysql()
        return
    # 2.定时更新数据,每七天更新一次
    last_update_time = sku_map_data_get_from_ad_server.iloc[0, -1]
    # 转换为日期格式
    if isinstance(last_update_time,str):
        last_update_time = datetime.strptime(last_update_time,'%Y-%m-%d').date()
    interval_day_last_update = (datetime.today().date() - last_update_time).days
    if interval_day_last_update >= update_interval_day:
        # 超过7天更新一次
        # # 先清空数据库,再添加数据
        # sql = "delete from {}".format(sku_map_table_name_ad_server)
        # sql_write_read.delete_table(sql)
        # 1.先复制数据库表结构
        sql_copy = "CREATE TABLE {}_temp like {}".format(sku_map_table_name_ad_server,sku_map_table_name_ad_server)
        sql_write_read.query_sql(sql_copy)
        print("复制完成.")
        # 2.清空数据库
        sql_drop = "DROP table {}".format(sku_map_table_name_ad_server)
        sql_write_read.query_sql(sql_drop)
        print("清空完成.")
        # 3.重命名
        sql_rename = "RENAME TABLE {}_temp to {}".format(sku_map_table_name_ad_server,sku_map_table_name_ad_server)
        sql_write_read.query_sql(sql_rename)
        print("重命名完成.")
        # 先清空数据库,再添加数据
        sku_map_get_from_company_server_mysql()
        print(f'{datetime.now()}: 完成sku map表的更新.')
        return
    else:
        print(f'{datetime.now()}: sku map表还很新,不需要更新.')
        return


if __name__ == '__main__':
    sku_map_update()
