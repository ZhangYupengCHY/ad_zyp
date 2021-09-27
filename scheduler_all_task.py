"""
管理全部的定时任务
使用框架:APScheduler
"""
import time
import gc
import json
import requests
from datetime import datetime
import pandas as pd
from sqlalchemy.dialects.mysql import MEDIUMINT, VARCHAR, DATETIME

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
# from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
# 时间监听器
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import logging

from java_interface import query_nickname, account_id_index
from my_toolkit import chinese_check, sql_write_read, public_function

LOGGNAME = 'scheduler.txt'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename=LOGGNAME,
                    filemode='a')

REDIS = {
    'host': '127.0.0.1',
    'port': '6379',
    'db': 4,
    'password': '',

}


def scheduler_listener(event):
    """事件监听器"""
    if event.exception:
        print(f'{event.job_id}:任务出错了！！！')
    else:
        print(f'{event.job_id}:任务照常运行,完成...')


if __name__ == '__main__':
    # 触发器配置项:作业存储后台设置,执行器执行方式,调度器
    global mScheduler
    jobstores = {
        'redis': RedisJobStore(jobs_key='dispatched_jobs', run_times_key='dispatched_running', **REDIS),
        # 'default':MemoryJobStore()
    }
    executors = {
        'threadpool': ThreadPoolExecutor(10),
        'processpool': ProcessPoolExecutor(5)
    }
    job_defaults = {
        'coalesce': False,
        'max_instances': 5
    }
    mScheduler = BackgroundScheduler(timezone='Asia/Shanghai', jobstores=jobstores, executors=executors,
                                     job_defaults=job_defaults)
    # scheduler = BackgroundScheduler(timezone='UTC', jobstores=jobstores, job_defaults=job_defaults)
    # 添加任务
    mScheduler.add_executor('processpool')
    mScheduler.add_job(query_nickname.request_nickname, trigger='cron', jobstore='redis', id='query_nickname',
                       hour='14', replace_existing=True)
    mScheduler.add_job(account_id_index.station_id_index_from_api, trigger='cron', jobstore='redis',
                       id='query_account_id_index', hour='14', replace_existing=True)

    # 添加事件监听器
    mScheduler.add_listener(scheduler_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    # 添加日志
    mScheduler._logger = logging
    # 开始进程
    mScheduler.start()

    while 1:
        time.sleep(1000)
