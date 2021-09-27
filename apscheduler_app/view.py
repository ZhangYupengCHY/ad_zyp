import requests
import jsonify
from flask import request, jsonify, json
from apscheduler_app.extensions import scheduler
from main import flask_app
# 新增job

#
# @flask_app.route('/addCron', methods=['post'])
# def add_cron():
#     jobargs = request.json
#     print(jobargs)
#     id = jobargs['task_id']
#     trigger_type = jobargs['trigger_type']
#     if trigger_type == "date":
#         run_time = jobargs['run_time']
#         job = scheduler.add_job(func="task:my_job",
#                                 trigger=trigger_type,
#                                 run_date=run_time,
#                                 replace_existing=True,
#                                 coalesce=True,
#                                 id=id)
#         print("添加一次性任务成功---[ %s ] " % id)
#     elif trigger_type == 'interval':
#         seconds = jobargs['interval_time']
#         seconds = int(seconds)
#         if seconds <= 0:
#             raise TypeError('请输入大于0的时间间隔！')
#         scheduler.add_job(func="task:my_job",
#                           trigger=trigger_type,
#                           seconds=seconds,
#                           replace_existing=True,
#                           coalesce=True,
#                           id=id)
#     elif trigger_type == "cron":
#         day_of_week = jobargs["run_time"]["day_of_week"]
#         hour = jobargs["run_time"]["hour"]
#         minute = jobargs["run_time"]["minute"]
#         second = jobargs["run_time"]["second"]
#         scheduler.add_job(func="apscheduler_app.task:add_job", id=id, trigger=trigger_type, day_of_week=day_of_week,
#                           hour=hour, minute=minute,
#                           second=second, replace_existing=True)
#         print("添加周期执行任务成功任务成功---[ %s ] " % id)
#     return jsonify(msg="新增任务成功")
#
# # 暂停
# @flask_app.route('/<task_id>/pause',methods=['GET'])
# def pause_job(task_id):
#     response = {'status': False}
#     try:
#         scheduler.pause_job(task_id)
#         response['status'] = True
#         response['msg'] = "job[%s] pause success!" % task_id
#     except Exception as e:
#         response['msg'] = str(e)
#     return jsonify(response)
#
# #启动
# @flask_app.route('/<task_id>/resume',methods=['GET'])
# def resume_job(task_id):
#     response = {'status': False}
#     try:
#         scheduler.resume_job(task_id)
#         response['status'] = True
#         response['msg'] = "job[%s] resume success!" % task_id
#     except Exception as e:
#         response['msg'] = str(e)
#     return jsonify(response)
#
# #删除
# @flask_app.route('/<task_id>/remove',methods=['GET'])
# def remove_job(task_id):
#     response = {'status': False}
#     try:
#         scheduler.remove_job(task_id)
#         response['status'] = True
#         response['msg'] = "job[%s] remove success!" % task_id
#     except Exception as e:
#         response['msg'] = str(e)
#     return jsonify(response)
#
#
# # @flask_app.route('/')
# # def index():
# #     print(111111111111111111111111111111111111111111111111111111111111)
# #     return '<h1>Hello World!</h1>'
#
# @flask_app.route('/123')
# def index123():
#     print(111111111111111111111111111111111111111111111111111111111111)
#     return '<h1>Hello World!</h1>'