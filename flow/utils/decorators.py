import datetime
import os
from datetime import timedelta

from apscheduler.schedulers.background import BackgroundScheduler
# 定时任务调度器
scheduler = BackgroundScheduler()

def time_decorator():
    def func_outer(func_param):
        def func_inner(*args,**kwargs):
            """
            修改开始和结束时间
            :param args:
            :param kwargs:
            :return:
            """
            kwargs['start']=(datetime.datetime.today()-timedelta(days=365*3 +30)).strftime('%Y%m%d')
            kwargs['end'] = datetime.datetime.today().strftime('%Y%m%d')
            func_param(*args,**kwargs)
            return func_inner
        return func_outer

# 以下函数是定时任务调度装饰器的使用demo和限定指标开始时间和结束时间demo
@scheduler.scheduled_job('cron',id = 'func1' , kwargs={'start':'20150102',
                                                       'end':'20200102'},second = '*/1')
@time_decorator()
def func1(start,end):
    """

    :param start:
    :param end:
    :return:
    """
    print("this is demo")
