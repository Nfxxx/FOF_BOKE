import pandas as pd
import numpy as np

def slice(raw_data , cmbno , start,end):
    """切片函数，基于起止时间将dataframe切片，假设基础数据完美

    :param raw_data(dataframe):贴模型基础数据 colums:{'the_date':timestamp , 'cmbno':str , 'daily_rate':float}
    :param cmbno(str):组合或者基准代码
    :param start(TimeStamp):开始时间
    :param end(TimeStamp):结束时间
    :return:
        array:每日收益率
    """
    sector1 = (raw_data['the_date'] >= start)
    sector2 = (raw_data['the_date'] <= end)
    sector3 = (raw_data['the_date'] == cmbno)
    daily_rate_df = raw_data[sector1&sector2&sector3]
    daily_return = daily_rate_df['daily_rate']
    daily_return = np.array(daily_return)

    return daily_return

def accumulate():
    pass

def commingle():
    pass

def period(array=None,start=None,end=None,calendar=None,mars=False):
    """获取区间天数

    :param array(array):收益率序列
    :param start:
    :param end:
    :param calendar:
    :param mars:
    :return:
        float：起止时间区间内的天数
    """
    if mars :
        period =len(calendar[start:end])
    else:
        period = len(array)
    return period