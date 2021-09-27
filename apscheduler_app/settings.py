import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.jobstores.memory import MemoryJobStore
from flask_apscheduler import APScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from sqlalchemy import create_engine
from datetime import datetime,timedelta

'''flask_apscheduler的JOBS可以在Config中配置，
也可以通过装饰器调用，还可以通过flask_apschedule的api进行添加
job stores 默认为内存， 可以下在flask的Config中配置为存储在数据库中
'''
from apscheduler_app import task

class Config(object):
    # JOBS可以在配置里面配置
    nowDataTime = str(datetime.now().replace(microsecond=0))
    NextSecondDataTime = str(datetime.now().replace(microsecond=0)+timedelta(seconds=1))
    JOBS = [
        {
        'id': f'CORN-job1',
        'func': 'apscheduler_app.task:job1',
        'args': (10, 20),
        'trigger': 'cron',
        'replace_existing':True,
        'misfire_grace_time':3600,
        'second': "*/10"
    },
        {
        'id': f'INTERVAL-job2',
        'func': 'apscheduler_app.task:job2',
        'args': '',
        'trigger': 'interval',
        'replace_existing': True,
        'misfire_grace_time': 3600,
        'seconds': 15
    },
    # {
    #     'id': f'DATE-job2',
    #     'func': 'apscheduler_app.task:job2',
    #     'args': '',
    #     'trigger': 'date',
    #     'replace_existing': True,
    #     'misfire_grace_time': 3600,
    #     'run_date': '2021-09-23 18:56:45',
    # },
    {
        'id': f'INTERVAL-query_nickname',
        'func': 'java_interface.query_nickname:request_nickname',
        'args': '',
        'trigger': 'interval',
        'replace_existing': True,
        'misfire_grace_time': 3600,
        'days': 1,
    },
    ]
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(
        'root', 'chy910624', '127.0.0.1', 3306, 'apscheduler', 'utf8'))
    # 存储位置
    SCHEDULER_JOBSTORES = {
        'default': SQLAlchemyJobStore(engine=engine)
        # 'default': MemoryJobStore()
    }
    # 线程池配置
    SCHEDULER_EXECUTORS = {
        'default': {'type': 'threadpool', 'max_workers': 20}
    }

    SCHEDULER_JOB_DEFAULTS = {
        'coalesce': False,
        'max_instances': 3
    }
    SCHEDULER_TIMEZONE = 'Asia/Shanghai'  # 配置时区
    SCHEDULER_API_ENABLED = True  # 添加API

    # 添加监听



    # 任务日志
    log = logging.getLogger('apscheduler.executors.default')
    log.setLevel(logging.DEBUG)  # DEBUG
    fmt = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
    h = logging.StreamHandler()
    h = logging.FileHandler('/task_scheduler.log')

    h.setFormatter(fmt)
    log.addHandler(h)


    @staticmethod
    def scheduler_listener(event):
        """事件监听器"""
        if event.exception:
            #todo 可以写入到MySQL中或是弹框,发短信都可以的.
            print(f'{event.job_id}:任务出错了！！！')
        else:
            pass
            # print(f'{event.job_id}:任务照常运行,完成...')