import pandas as pd


class FormatJob(object):
    """处理job格式"""
    TriggerSignWords = {'cron': ['year', 'month', 'week', 'day_of_week', 'day', 'hour', 'minute', 'second'],
                        'interval': ['weeks', 'days', 'hours', 'minutes', 'seconds'],
                        'date': ['run_date']
                        }

    def __init__(self, jobInfo):
        if not isinstance(jobInfo, list):
            raise TypeError('jobinfo type must list')
        for oneJob in jobInfo:
            if not isinstance(oneJob, dict):
                raise TypeError('one jobinfo type must dict')
            if not {'args', 'func', 'id', 'trigger'}.issubset(set(oneJob.keys())):
                raise TypeError('jobInfo not task message')
            self.triggerType = oneJob['trigger']
            if self.triggerType not in FormatJob.TriggerSignWords.keys():
                raise ValueError(f'new trigger type show:{self.triggerType}')
        self.jobInfo = jobInfo

    @staticmethod
    def to_dataframe(jobInfo):
        """将任务参数信息转换为df"""
        if (not isinstance(jobInfo, list)) or (len(jobInfo) == 0):
            return pd.DataFrame()
        return pd.DataFrame.from_records(jobInfo)

    def to_show(self):
        """将任务参数显示成前端展示的样式
        ID,函数名,函数位置,状态,下一次执行时间,循环逻辑,操作
        """
        self.jobInfo = self.add_trigger_logic_message()
        jobInfoDF = self.to_dataframe(self.jobInfo)
        jobInfoDF['fun_location'] = [func.split(':')[0] if ':' in func else '' for func in jobInfoDF['func']]
        jobInfoDF['fun_name'] = [func.split(':')[1] if ':' in func else '' for func in jobInfoDF['func']]
        jobInfoDF['status'] = ['运行中' if next_run is not None else '暂停' for next_run in jobInfoDF['next_run_time']]
        exportColumns = ['id','fun_location','fun_name','status','next_run_time','running_logic']
        return jobInfoDF[exportColumns]

    def add_trigger_logic_message(self):
        """"""
        for index, oneJob in enumerate(self.jobInfo):
            loginMessage = []
            triggerType = oneJob['trigger']
            triggerKws = FormatJob.TriggerSignWords[triggerType]
            for kw in triggerKws:
                if kw in oneJob.keys():
                    if triggerType != 'date':
                        loginMessage.append(f'{kw}:{oneJob[kw]}')
                    else:
                        loginMessage.append(f'{kw}:{oneJob["next_run_time"]}')
            self.jobInfo[index]['running_logic'] = ','.join(loginMessage)
        return self.jobInfo


# if __name__ == '__main__':
#     a = [{'args': [10, 20], 'func': 'apscheduler_app.task:job1', 'id': 'CORN-job1', 'kwargs': {}, 'max_instances': 3,
#       'misfire_grace_time': 3600, 'name': 'CORN-job1', 'next_run_time': '2021-09-23 18:41:10',
#       'running_logic': 'second:*/10', 'second': '*/10', 'trigger': 'cron'},
#      {'args': [], 'func': 'apscheduler_app.task:job2', 'id': 'INTERVAL-job2', 'kwargs': {}, 'max_instances': 3,
#       'misfire_grace_time': 3600, 'name': 'INTERVAL-job2', 'next_run_time': '2021-09-23 18:41:13',
#       'running_logic': 'seconds:15', 'seconds': 15, 'start_date': '2021-09-23 18:40:58', 'trigger': 'interval'},
#      {'args': [], 'func': 'apscheduler_app.task:job2', 'id': 'DATE-job2', 'kwargs': {}, 'max_instances': 3,
#       'misfire_grace_time': 3600, 'name': 'DATE-job2', 'next_run_time': '2021-09-23 18:56:45',
#       'run_date': 'Thu, 23 Sep 2021 10:56:45 GMT', 'running_logic': 'run_date:2021-09-23 18:56:45+08:00',
#       'trigger': 'date'}]
#     print(FormatJob(a).to_show())
#     print(FormatJob(a).to_show().columns)
