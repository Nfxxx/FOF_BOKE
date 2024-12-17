import numpy as np


class calculate(object):
    name = 'calculate'

def daily_return(*args, omega=None, bp=None):
    """计算每日收益率

    :param args(tuple): 子基准1的每日收益率为 array1 ， 元组为(array1,)
    :param omega(list): 自基准1的权重为float
    :param bp(list): 自基准1的基点偏移量为float
    :return:
            array:复合基准每日收益率
    """


def acc_return(*args,omega=None,bp=None,
               interest_calculation=None,
               diameter_calculation=None):
    """计算复合每日累计收益率

    :param args(tuple): 各个子基准的每日收益率
    :param omega(list): 子基准的权重
    :param bp(list): bps偏移量
    :param interest_calculation(str):计息方式 ，单例simple/复利compound
    :param diameter_calculation(str): 计算口径，分拆split/合并combine
    :return:
            array:复合基准每日累计收益率
    """

def calmar_ratio(Rp,max_drawdown):
    """计算卡玛比率

    :param Rp(float): 组合收益率(年化/期间均值)
    :param max_drawdown(float): 最大回撤率
    :return:
            float:卡玛比率

    """

def excess_volatility(ptf,bm):
    """计算超额波动率，也称为追踪误差

    :param ptf(array): 组合每日收益率
    :param bm(array): 基准每日收益率
    :return:
            float :超额波动率
    """

def infomation_ratio(alpha,TE):
    """计算信息比率

    :param alpha(float):超额收益率(年化/期间均值)
    :param TE(float): 超额波动率，也叫追踪误差(年化/期间均值)
    :return:
            float :信息比率
    """
def excess_annual_return(ptf,bm):
    """计算超额年化收益率

    :param ptf(array): 组合每日收益率
    :param bm(array): 基准每日收益率
    :return:

    """
def treynor_ratio(annual_return,beta,Rf=0.03):
    """计算特雷诺比率

    :param annual_return(float):年化收益率
    :param beta(float): 贝塔系数
    :return:
        float :特雷诺比率
    """

def sortino_ratio(annual_return,annual_volatioity,Rf=0.03):
    """计算索提诺比率

    :param annual_return(float):组合年化收益率
    :param annual_volatioity(float): 组合年化下行标准
    :param Rf(float): 无风险收益率，默认0.03
    :return:
            float:索提诺比率
    """

def jansen_ratio(ptf,bm,beta,Rf=0.03):
    """计算詹森比率

    :param ptf(float):年化收益率
    :param bm(float): 年化收益率
    :param beta(float): 贝塔系数
    :param Rf(float): 无风险收益率，默认0.03
    :return:
            float:詹森比率
    """

def sharp_ratio(annual_return,annual_volatioity,Rf=0.03):
    """计算夏普比率

    :param ptf(float):年化收益率
    :param bm(float): 年化波动率
    :param Rf(float): 无风险收益率，默认0.03
    :return:
        float:夏普比率
    """

def current_drawdown(NAV):
    """计算当前回撤率

    :param NAV(array): 净值
    :return:
        float:当前回撤率
    """


def max_drawdown(NAV):
    """计算最大回撤

    :param NAV(array): 净值
    :return:
        float:最大回撤
    """

def annual_volatioity(volatioity,annual=252):
    """计算年化波动率

    :param volatioity(float): 波动率
    :param annual(int): 年化天数，默认252
    :return:
            float:年化波动率
    """

def annual_return(HPR,period,annual=252):
    """计算期间年化收益率

    :param HPR(float): 持有期收益率
    :param period(float): 区间天数
    :param annual(float): 年化天数，默认为252
    :return:
            float:期间年化收益率
    """


def downside_volatility(daily_return,Rf=0.03):
    """计算下行标准差

    :param daily_return(array):年化收益率
    :param Rf(float): 无风险收益率，默认0.03
    :return:
            float:下行标准差
    """

def volatility(daily_return):
    """计算波动率

    :param daily_return(array): 每日收益率
    :return:
            float:波动率
    """


def win_ratio(ptf,bm):
    """计算胜率

    :param ptf(array): 组合的每日收益率
    :param bm(array): 基准的每日收益率
    :return:
            array:胜率
    """

def beta(ptf,bm):
    """计算贝塔系数

    :param ptf(array): 组合的每日收益率
    :param bm(array): 基准的每日收益率
    :return:
            array:贝塔系数
    """

def nav(acc_return):
    """计算净值

    :param acc_return(array): 累计收益率
    :return:
            array:净值
    """


def hpr(acc_return):
    """计算持有期收益率

    :param acc_return(array): 累计收益率
    :return:
            array:持有期收益率
    """

