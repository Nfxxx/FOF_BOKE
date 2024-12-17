import inspect
import flow.indicator.core.calculate as calculate
import flow.indicator.common.common as common
import pandas as pd
import numpy as np

def cal_indicator(indicator,**kwargs):
    """根据传入的指标名称，调用相应的指标计算flow函数，返会计算结果

    :param indicator: str 指标名称 "sortino"
    :param kwargs: dict 参数字典
    :return: result: float 被调用flow函数怼计算结果
    """
    indicator_to_func = {'hpr':hpr,
                         'nav':nav,
                         'beta':beta,
                         'win_ratio':win_ratio,
                         'volatility':volatility,
                         'downside_volatility':downside_volatility,
                         # 'sortino':sortino_ratio,
                         # 'sharpe':sharp_ratio,
                         # 'ptf_max_drawdown':max_drawdown,
                         # 'ptf_volatility':volatility,
                         # 'ptf_annual_return':annual_return,
                         # 'ptf_current_drawdown':current_drawdown,
                         # 'calmar':calmar_ratio,
                         # 'jansen_ratio':jansen_ratio,
                         # 'treynor_ratio':treynor_ratio,
                         # 'information_ratio':information_ratio
                         }
    params_list = inspect.getargspec(indicator_to_func[indicator])[0]
    params = {k:v for k ,v in kwargs.items() if k in params_list}
    result = indicator_to_func[indicator](**params)
    return result


def daily_return(data,cmbno,start,end):
    """计算每日收益率

    :param data(dataframe):原始数据
    :param cmbno(str):组合编号/基准编号
    :param start(timestamp):开始时间
    :param end(timestamp):结束时间
    :return:
        array:每日收益率
    """
    array = common.slice(data,cmbno,start,end)
    daily_return = calculate.daily_return(array)
    return daily_return


def acc_return(data,cmbno,start,end):
    """计算每日累计收益率

    :param data(dataframe):原始数据
    :param cmbno(str): 组合编号/基准编号
    :param start(timestamp): 开始时间
    :param end(timestamo): 结束时间
    :return:
        array：每日累计收益率
    """
    array = common.slice(data,cmbno,start,end)
    acc_return = calculate.acc_return(array)
    return acc_return

def hpr(data,cmbno,start,end):
    """计算持有期收益率

    :param data(dataframe):原始数据
    :param cmbno(str): 组合编号/基准编号
    :param start(timestamp): 开始时间
    :param end(timestamo): 结束时间
    :return:
        float：持有期收益率
    """
    array = common.slice(data,cmbno,start,end)
    acc_return = calculate.acc_return(array)
    hpr = calculate.hpr(acc_return)
    return hpr

def nav(data,cmbno,start,end):
    """计算净值

       :param data(dataframe):原始数据
       :param cmbno(str): 组合编号/基准编号
       :param start(timestamp): 开始时间
       :param end(timestamo): 结束时间
       :return:
           float：净值
    """
    array = common.slice(data, cmbno, start, end)
    acc_return = calculate.acc_return(array)
    nav = calculate.nav(acc_return)
    return nav


def beta(data,cmbno,start,end):
    """计算贝塔系数

       :param data(dataframe):原始数据
       :param cmbno(str): 组合编号/基准编号
       :param start(timestamp): 开始时间
       :param end(timestamo): 结束时间
       :return:
           float：贝塔系数
    """
    ptf_array = common.slice(data, cmbno, start, end)
    bm_array = common.slice(data, cmbno, start, end)
    ptf_daily_return = calculate.daily_return(ptf_array)
    bm_daily_return = calculate.daily_return(bm_array)
    beta = calculate.beta(ptf = ptf_daily_return,bm=bm_daily_return)
    return beta


def win_ratio(data,ptf_cmbno,bm_cmbno,start,end):
    """计算胜率

       :param data(dataframe):原始数据
       :param cmbno(str): 组合编号/基准编号
       :param start(timestamp): 开始时间
       :param end(timestamo): 结束时间
       :return:
           float：胜率
    """
    ptf_array = common.slice(data, ptf_cmbno, start, end)
    bm_array = common.slice(data, bm_cmbno, start, end)
    ptf_daily_return = np.array(calculate.daily_return(ptf_array))
    bm_daily_return = np.array(calculate.daily_return(bm_array))
    win_ratio = calculate.win_ratio(ptf = ptf_daily_return,bm=bm_daily_return)
    return win_ratio


def volatility(data,cmbno,start,end):
    """计算波动率

       :param data(dataframe):原始数据
       :param cmbno(str): 组合编号/基准编号
       :param start(timestamp): 开始时间
       :param end(timestamo): 结束时间
       :return:
           float：波动率
    """
    ptf_array = common.slice(data, cmbno, start, end)
    ptf_daily_return = calculate.acc_return(ptf_array)
    volatility = calculate.volatility(ptf_daily_return)
    return volatility


def downside_volatility(data,cmbno,start,end):
    """计算波动率

       :param data(dataframe):原始数据
       :param cmbno(str): 组合编号/基准编号
       :param start(timestamp): 开始时间
       :param end(timestamo): 结束时间
       :return:
           float：波动率
    """
    ptf_array = common.slice(data, cmbno, start, end)
    ptf_daily_return = calculate.acc_return(ptf_array)
    volatility = calculate.volatility(ptf_daily_return)
    return volatility


def annual_return(data,cmbno,start,end):
    """计算年化收益率

       :param data(dataframe):原始数据
       :param cmbno(str): 组合编号/基准编号
       :param start(timestamp): 开始时间
       :param end(timestamo): 结束时间
       :return:
           float：年化收益率
    """
    array = common.slice(data,cmbno,start,end)
    acc_return= calculate.acc_return(array)
    hpr= calculate.hpr(acc_return)
    period = common.period(array,start,end)
    annual_return = calculate.acc_return(hpr,period)
    return annual_return