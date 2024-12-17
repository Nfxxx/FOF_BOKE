import math
import numpy as np
import pandas as pd


def average_cal(Ture_num,list):
    """
    针对出现碎股的状况 将母单的真实成交数据拆分给各个子单

    """
    new_list=[]
    target_num=np.sum(list)
    for i in list:
        m=Ture_num*i/target_num
        new_list.append(round(m))
    new_true=np.sum(new_list)
    if new_true!=Ture_num:
        new_list = []
        print('无法切分 报错11111')
        return new_list
    else :
        return new_list

def marketCode():
    a=pd.read_csv('file/eqt_1mbar/prevclose.csv').drop(labels=['Unnamed: 0'],axis=1).columns.values
    output=pd.DataFrame(columns=['symbol','market','MarketCode'])
    for i in a:
        q=i.split('.')
        symbol=q[0]
        market=q[1]
        MarketCode=i
        m=[symbol,market,MarketCode]
        output = output._append(pd.Series(m, index=output.columns), ignore_index=True)
    output.to_csv('file/eqt_1mbar/MarketCode2024.csv')
    print(output)


# calculate twap value
def calc_twap(marketDataTable):
    n = len(marketDataTable) - 1
    price_sum = 0.0
    for i in range(1, n + 1):
        high_price = float(marketDataTable[i][9])
        low_price = float(marketDataTable[i][10])
        price = (high_price + low_price) / 2
        price_sum += price

    return price_sum / n


def Close_price(daily):
    # df=pd.read_pickle(f'file/eqt_1mbar/{daily}/Close.pkl')
    # df=df.fillna(method='ffill').tail(1)
    # return df
    df = pd.read_csv(f'file/eqt_1mbar/close.csv')
    # print(df)
    df = df.fillna(method='ffill').tail(1)
    return df


def Open_price(daily):
    df=pd.read_pickle(f'file/eqt_1mbar/{daily}/Open.pkl')
    df = df.fillna(method='ffill').tail(1)
    return df

def cal_index():
    df = pd.read_csv(f'file/eqt_1mbar/index_close.csv')['000905.SH']
    rtn_preclose_to_close = ((df - df.shift(1)) / df.shift(1)*100).tail(1).values[0]
    # df = df.fillna(method='ffill')
    return round(rtn_preclose_to_close,2)


if __name__ == '__main__':
   a= cal_index()

   print(a)
   # print(rtn_preclose_to_close)
   # print(a['300791.SZ'].values[0])
   # import numpy as np, numpy.random
   # print(np.random.dirichlet(np.ones(19), size=1))
   # average_cal(5100,[300, 4800])