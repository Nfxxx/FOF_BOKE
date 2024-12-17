"""

个股成交量按时间段统计和预测：
9:15-9:35
9.35-9.45
9.45-10.00
10.00-10.15
10.15-10.30
10.30-11.00
11.00-11.30
13.00-13.30
13.30-14.00
14.00-14.25
14.25-14.45
14.45-14.57

TODO 性能提升
一天的分钟线成交量 volume pd.pivot转成 240*5000 的矩阵
时间范围转成index
0, 5, 15, 30, 45, 60, 90, 120, 150, 180, 205, 225, 237
使用numpy向量计算
这个数据量没必要用GPU

"""

import datetime
import os.path
import numpy as np
import pandas as pd
import ta
from sqlalchemy import create_engine

import common
import jq_data as jqd


data_path = './data'
if not os.path.exists(data_path):
    os.mkdir(data_path)


def prepare_data(the_day, bars=240):
    df = common.load_stocks_kline(bars=bars, freq='1m', end_date=the_day)
    df['time'] = df['date'].apply(lambda x: x.time())
    df['date'] = df['date'].apply(lambda x: x.date())
    df = df[df['date'] == the_day.date()]
    df['code'] = df['jq_code'].apply(lambda x: jqd.add_code_suffix(x.split('.')[0]))
    return df


def make_interval(begin_time, end_time):
    time1 = datetime.datetime.strptime(begin_time, '%H:%M').time()
    time2 = datetime.datetime.strptime(end_time, '%H:%M').time()
    return time1, time2


def make_stats_intervals():
    all = []
    t1, t2 = make_interval('9:15', '9:35')
    all.append((t1, t2))
    t1, t2 = make_interval('9:35', '9:45')
    all.append((t1, t2))
    t1, t2 = make_interval('9:45', '10:00')
    all.append((t1, t2))
    t1, t2 = make_interval('10:00', '10:15')
    all.append((t1, t2))
    t1, t2 = make_interval('10:15', '10:30')
    all.append((t1, t2))
    t1, t2 = make_interval('10:30', '11:00')
    all.append((t1, t2))
    t1, t2 = make_interval('11:00', '11:30')
    all.append((t1, t2))
    t1, t2 = make_interval('13:00', '13:30')
    all.append((t1, t2))
    t1, t2 = make_interval('13:30', '14:00')
    all.append((t1, t2))
    t1, t2 = make_interval('14:00', '14:25')
    all.append((t1, t2))
    t1, t2 = make_interval('14:25', '14:45')
    all.append((t1, t2))
    t1, t2 = make_interval('14:45', '14:57')
    all.append((t1, t2))
    return all

#
# DoneTODO 性能提升
# 一天的分钟线成交量 volume pd.pivot转成 240*5000 的矩阵
# 时间范围转成index
# 0, 5, 15, 30, 45, 60, 90, 120, 150, 180, 205, 225, 237
# 使用numpy向量计算
# 这个数据量没必要用GPU
#
def calc_vol_stats_numpy(day_df):
    intervals = [0, 5, 15, 30, 45, 60, 90, 120, 150, 180, 205, 225, 237]
    pivot_volume_df = day_df.pivot(index='seq', columns='code', values='volume')
    pivot_volume_df.fillna(0, inplace=True)
    av = pivot_volume_df.values
    pivot_amount_df = day_df.pivot(index='seq', columns='code', values='money')
    pivot_amount_df.fillna(0, inplace=True)
    am = pivot_amount_df.values
    sum_rows = []
    row_names = []
    for i in range(len(intervals) - 1):
        start = intervals[i]
        end = intervals[i+1]
        row_names.append('amount_' + str(i))
        row_names.append('volume_' + str(i))
        interval_sum = np.sum(am[start:end, :], axis=0)
        sum_rows.append(interval_sum)
        interval_sum = np.sum(av[start:end, :], axis=0)
        sum_rows.append(interval_sum)
    result_volume_df = pd.DataFrame(sum_rows, columns=pivot_amount_df.columns, index=row_names)
    result_df = result_volume_df.T
    return result_df


def calc_vol_stats(day_df):
    """
    股票按时间段成交额统计
    :param day_df:  one day 1min data for all stocks
    :return:
    """
    intervals = make_stats_intervals()

    def group_func(df):
        result = dict()
        result['code'] = df['code'].iloc[0]
        result['date'] = df['date'].iloc[0]
        for i in range(len(intervals)):
            t1 = intervals[i][0]
            t2 = intervals[i][1]
            s_df = df[df['time'] <= t2]
            s_df = s_df[s_df['time'] > t1]
            money = s_df['money'].sum()
            volume = s_df['volume'].sum()
            result['amount_'+str(i)] = money
            result['volume_'+str(i)] = volume
        return pd.DataFrame([result])

    day_result_df = day_df.groupby(by=['code'], group_keys=False).apply(group_func)
    return day_result_df


def calc_vol_stats_from(start_date):
    """
    从某个日期开始计算
    """
    sh_index_df = jqd.fetch([jqd.SH_INDEX], start_date=start_date)
    trade_days = list(sh_index_df['time'].unique())
    all_df_list = []
    for day in trade_days:
        the_date = pd.to_datetime(day)
        day_df = prepare_data(the_day=the_date)
        print(the_date, 'prepare data done.', len(day_df))
        if len(day_df) == 0:
            continue
        # result_df = calc_vol_stats(day_df)
        result_df = calc_vol_stats_numpy(day_df)
        result_df.insert(0, 'date', [the_date] * len(result_df))
        result_df.reset_index(inplace=True)
        print('vol_stats done.', len(result_df))
        save_data_to_db(result_df)
        print('save to db done.', len(result_df))
        all_df_list.append(result_df)
    all_df = pd.concat(all_df_list)

    return all_df


def ma_stats(result_df, n=5):
    """
    移动平均平滑化
    :param result_df:
    :param n:
    :return:
    """
    def group_func(df):
        for i in range(12):
            df['ma' + str(n) + '_volume_' + str(i)] = ta.ema(df['volume_' + str(i)], n)
            df['ma' + str(n) + '_amount_' + str(i)] = ta.ema(df['amount_' + str(i)], n)
        return df
    result_df.sort_values(by=['date'], ascending=True, inplace=True)
    result_df = result_df.groupby(by=['code'], group_keys=False).apply(group_func)
    return result_df


def save_data_to_db(df, tb_name='tb_volume_stats'):
    if len(df) > 0:
        conn = create_engine('mysql+pymysql://admin:admin@172.0.0.1:3309/soe?charset=utf8')
        pd.io.sql.to_sql(df, tb_name, con=conn, if_exists='append',
                         index=False, index_label='date')
        conn.dispose()


if __name__ == "__main__":
    start_date = "2023-06-01"
    df = calc_vol_stats_from(start_date)
    df = ma_stats(df, n=5)
    df.to_csv(os.path.join(data_path, 'vol_stats_%s.csv' % start_date))
    df.to_pickle(os.path.join(data_path, 'vol_stats_%s.pkl' % start_date))
