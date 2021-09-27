from apscheduler.schedulers.background import BackgroundScheduler
from flask_apscheduler import APScheduler

# 实例APScheduler定时任务
scheduler = APScheduler(BackgroundScheduler(timezone="Asia/Shanghai"))