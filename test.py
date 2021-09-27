from apscheduler_app import extensions
import json
import requests
#
# # 实现业务
# 1.添加任务

# url = 'http://172.16.16.114:5000/addCron'
# json = json.dumps({'run_time':{'second':"*/10"},'job_name':'job1','func':'apscheduler_app.task:job1','task_id':'CORN-job1'})
#
# headers = {
#     "Content-Type": "application/json;charset=utf8"
# }
# response = requests.post(url=url,data=json, headers=headers)
# print(response)

# 2.查询任务

# url = 'http://172.16.16.114:5000/get_jobs'
# headers = {
#     "Content-Type": "application/json;charset=utf8"
# }
# response = requests.get(url=url, headers=headers)
# print(json.loads(response.content))
# print(len(json.loads(response.content)))

# # 3.暂停任务
# url = 'http://172.16.16.114:5000/CORN-job1/pause'
# headers = {
#     "Content-Type": "application/json;charset=utf8"
# }
# response = requests.get(url=url, headers=headers)
# print(json.loads(response.content))
# print(len(json.loads(response.content)))


# # 4.恢复任务

url = 'http://172.16.16.114:5000/CORN-job1/resume'
headers = {
    "Content-Type": "application/json;charset=utf8"
}
response = requests.get(url=url, headers=headers)
print(json.loads(response.content))
print(len(json.loads(response.content)))

# from app import extensions,settings
#
# db = settings.Config.SCHEDULER_JOBSTORES
# print(extensions.scheduler.get_jobs(jobstore='default'))

# gmtStr = 'Thu, 16 Sep 2021 09:51:00 GMT'
# def gmt_to_shanghai(gmtStr):
#     from datetime import datetime,timedelta
#     GMT_FORMAT = '%a, %d %b %Y %H:%M:%S GMT'
#     return str(datetime.strptime(gmtStr, GMT_FORMAT)+timedelta(hours=8))
# print(gmt_to_shanghai(gmtStr))


