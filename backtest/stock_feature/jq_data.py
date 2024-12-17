import os
import copy
import json
import datetime
import jqdatasdk as jq
import pandas as pd

from functools import wraps

#
SH_INDEX = '000001.XSHG'
SZ_INDEX = '399001.XSHE'
CY_INDEX = '399006.XSHE'
HS300_INDEX = '000300.XSHG'
ZZ500_INDEX = '000905.XSHG'
ZZ800_INDEX = '000906.XSHG'
SZ50_INDEX = '000016.XSHG'
ZZ1000_INDEX = '000852.XSHG'


def assert_auth(func):
    @wraps(func)
    def _wrapper(*args, **kwargs):
        if not jq.is_auth():
            jq.auth('', '')

        return func(*args, **kwargs)
    return _wrapper


def to_date(x):
    return x.date()

@assert_auth
def login():
    try:
        print(jq.get_query_count()) 
    except Exception as e:
        print(e)


# 股票代码加上市场后缀, 同花顺接口格式
def add_code_suffix(code):
    if code.find('.') > 0:
        return code
    if code.startswith('60') or code.startswith('68'):
        code = code + '.SH'
    elif code.startswith('90'):
        code = code + '.SH'
    elif code.startswith('SH'):
        code = code[2:len(code)] + '.SH'
    elif code.startswith('SZ'):
        code = code[2:len(code)] + '.SZ'
    else:
        code = code + '.SZ'
    return code


# 股票代码加上市场后缀
def add_norm_suffix(code):
    if code.find('.') > 0:
        code = code.split('.')[0]
    if code.startswith('60') or code.startswith('68'):
        code = code + '.XSHG'
    elif code.startswith('90'):
        code = code + '.XSHG'
    elif code.startswith('SH'):
        code = code[2:len(code)] + '.XSHG'
    elif code.startswith('SZ'):
        code = code[2:len(code)] + '.XSHE'
    else:
        code = code + '.XSHE'
    return code


# 将股票转化为聚宽股票codes形式
# @assert_auth
def normalize_code(codes):
    if type(codes) is str:
        return add_norm_suffix(codes)
    if codes is None or len(codes) == 0:
        return codes
    codes = [add_norm_suffix(c) for c in codes]
    return codes
    # return jq.normalize_code(codes)


@assert_auth
def get_all_stocks(date=None):
    df = jq.get_all_securities(types=['stock'], date=date)
    return df


# 沪深300成分股
@assert_auth
def get_hs300_stocks(date=None):
    index = HS300_INDEX
    stocks = jq.get_index_stocks(index, date)
    return stocks


# zz800成分股
@assert_auth
def get_zz800_stocks(date=None):
    index = ZZ800_INDEX
    stocks = jq.get_index_stocks(index, date)
    return stocks

# zz1000成分股
@assert_auth
def get_zz1000_stocks(date=None):
    index = ZZ1000_INDEX
    stocks = jq.get_index_stocks(index, date)
    return stocks

# 中证500成分股
@assert_auth
def get_zz500_stocks(date=None):
    index = ZZ500_INDEX
    stocks = jq.get_index_stocks(index, date)
    return stocks


# 上证50成分股
@assert_auth
def get_sz50_stocks(date=None):
    index = SZ50_INDEX
    stocks = jq.get_index_stocks(index, date)
    return stocks

# 融资标的股
def get_margincash_stocks(date=None):
    stocks = jq.get_margincash_stocks(date=date)
    return stocks

# 融券标的股
def get_marginsec_stocks(date=None):
    stocks = jq.get_marginsec_stocks(date=date)
    return stocks


# 取概念股
def get_concept_stocks(concept_code, date=None):
    stocks = jq.get_concept_stocks(concept_code, date=date)
    return stocks


# 取msci概念股
@assert_auth
def get_mscilarge_stocks(date=None):
    return get_concept_stocks('GN240', date=date)


# 取msci概念股
@assert_auth
def get_mscimid_stocks(date=None):
    return get_concept_stocks('GN1189', date=date)


# 取龙虎榜股票
def get_billboard_stocks(days=20, end_date=None):
    if end_date is None:
        end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    stocks = jq.get_billboard_list(end_date=end_date, count=days)
    if len(stocks) > 0:
        stock_codes = stocks['code'].unique()
        return stock_codes
    else:
        return []


# 取指定时间段内的交易日
def get_trade_days(start_date=None, end_date=None, count=None):
    days = jq.get_trade_days(start_date=start_date, end_date=end_date, count=count)
    return days


# 取指定时间段内的交易日
def get_stocks_pools(start_date, end_date=None, msci=None, index='300'):
    #
    return {}

# 取指定时间段内的交易日
def fetch_date_serial_hs300(stocks, start_date, end_date=None):
    #
    return None


# 取指定时间段内的交易日
def fetch_feature(stock, start_date):
    #
    return None

# 取指定时间段内的交易日
def get_hs300_stocks_pool(date, margin='c', msci='l'):
    #
    stocks = get_hs300_stocks(date)
    # print('no margin/MSCI')
    margincash_stocks = get_margincash_stocks(date)
    marginsec_stocks = get_marginsec_stocks(date)
    msci_stocks = get_mscilarge_stocks(date)
    if len(marginsec_stocks) > 0:
        s1 = set(stocks)
        stocks = list(s1.intersection(set(marginsec_stocks)))
        # print('融券')
    if len(margincash_stocks) > 0:
        s1 = set(stocks)
        stocks = list(s1.intersection(set(margincash_stocks)))
        # print('融资')
    if len(msci_stocks) > 0:
        s1 = set(stocks)
        stocks = list(s1.intersection(set(msci_stocks)))
        # print('MSCI')
    return stocks


# 提取一组股票所属的行业
def hot_industries(stocks, top_n, industry_prefix='zjw'):
    """
    industry hits ranking in a list of stock
    :param stocks:
    :param industry_prefix: sw: 申万行业分类, zjw: 证监会行业分类，jq: 聚宽行业分类
    :return:
    """
    industry_hits = dict()
    stock_industries = jq.get_industry(security=list(stocks), date=datetime.datetime.now().strftime('%Y-%m-%d'))
    for k, v in stock_industries.items():
        for industry_key in v.keys():
            if industry_key.startswith(industry_prefix):
                industry_obj = v[industry_key]
                industry_code = industry_obj['industry_code']
                if industry_code in industry_hits:
                    hit_obj = industry_hits[industry_code]
                    hit_obj['hits'] = hit_obj['hits'] + 1
                else:
                    hit_obj = copy.deepcopy(industry_obj)
                    hit_obj['hits'] = 1
                    industry_hits[industry_code] = hit_obj
    df = pd.DataFrame(list(industry_hits.values()))
    df.sort_values(by=['hits'], ascending=False, inplace=True)
    hot_industry_codes = df['industry_code'].values
    return hot_industry_codes[:top_n]


# 通过行业过滤股票
def filter_stocks(stocks, industry_codes, industry_prefix='zjw'):
    """
    filter stocks by industries
    :param stocks:
    :param industry_codes:
    :param industry_prefix:
    :return:
    """
    stock_industries = jq.get_industry(security=list(stocks), date=datetime.datetime.now().strftime('%Y-%m-%d'))
    result_stocks = []
    for k, v in stock_industries.items():
        for industry_key in v.keys():
            if industry_key.startswith(industry_prefix):
                industry_obj = v[industry_key]
                industry_code = industry_obj['industry_code']
                if industry_code in industry_codes:
                    result_stocks.append(k)
                    break
    return result_stocks


@assert_auth
def fetch(security, start_date, end_date=None, frequency='daily', fq='pre'):
    """
    fetch future data from jqdatasdk
    :param security:
    :param start_date:
    :param end_date:
    :param frequency: 'daily' or 'minute'
    :return:
    """
    fields = ['open', 'close', 'low', 'high', 'volume', 'money', 'factor', 'high_limit', 'low_limit', 'avg', 'pre_close',
     'paused']
    if end_date is None:
        now = datetime.datetime.now()
        end_date = now.strftime('%Y-%m-%d')
    df = jq.get_price(security, start_date=start_date, end_date=end_date, frequency=frequency, fq=fq, fields=fields,
                      skip_paused=False)
    df.dropna(inplace=True)
    return df


@assert_auth
def fetch_latest(security, fq='post'):
    fields = ['open', 'close', 'low', 'high', 'volume', 'money', 'factor', 'high_limit', 'low_limit', 'avg', 'pre_close',
     'paused']
    now = datetime.datetime.now()
    end_date = now.strftime('%Y-%m-%d')
    df = jq.get_price(security, end_date=end_date, frequency='daily', fields=fields, skip_paused=False, panel=False,
                      fq=fq, count=1)
    return df


@assert_auth
def fetch_bars(security, bars, end_date=None, frequency='1w'):
    """
    fetch bars data from jqdatasdk
    :param security:
    :param bars:
    :param end_date:
    :param frequency: '1m', '5m', '15m', '30m', '60m', '120m', '1d', '1w', '1M'
    :return:
    """
    # fields supports: 'date', 'open', 'close', 'high', 'low', 'volume', 'money', 'open_interest'
    fields = ['date', 'open', 'close', 'low', 'high', 'volume', 'money']
    df = jq.get_bars(security, count=bars, end_dt=end_date, unit=frequency, fields=fields, include_now=True)
    df.dropna(inplace=True)
    return df



def get_industries_local(local_path):
    df = pd.read_csv(local_path, index_col=0)
    df.index = df.index.map(str)
    return df


@assert_auth
def get_industry_stocks(industry_code):
    df = jq.get_industry_stocks(industry_code)
    return df


@assert_auth
def get_stock_industries(stocks, industry_type):
    result_dict = dict()
    stock_industries = jq.get_industry(security=list(stocks), date=datetime.datetime.now().strftime('%Y-%m-%d'))
    for k, v in stock_industries.items():
        if industry_type in v:
            code = v[industry_type]['industry_code']
            result_dict[k] = code
    return result_dict


# 获取聚宽行业信息
@assert_auth
def get_industries_from_mysql(ms, name='sw_l1', date=None):
    sql = "select industry_id, name, start_date " \
          "from tb_jq_industries " \
          "where date = '%s' and type = '%s' " % (date, name)
    df = ms.exeQuery(sql, ['industry_id', 'name', 'start_date'])
    if df is None:
        indu_code = jq.get_industries(name=name, date=date)
        sql = 'replace into tb_jq_industries(date, type, industry_id, name, start_date) values'
        for index, row in indu_code.iterrows():
            sql += " ('%s','%s','%s','%s','%s')," % (date, name, index, row['name'], str(row['start_date'])[:10])
        sql = sql[0:-1] + ";"
        ms.exeNonQuery(sql)
        return indu_code
    else:
        df.set_index("industry_id", inplace=True)
        return df


# 获取聚宽行业下的成分股票
@assert_auth
def get_industry_stocks_from_mysql(ms, industry_id, date):
    sql = "select constituent_stocks " \
          "from tb_jq_industry_constituent_stocks " \
          "where date = '%s' and industry_id = '%s' " % (date, industry_id)
    df = ms.exeQuery(sql, ['constituent_stocks'])
    if df is None:
        stocks = jq.get_industry_stocks(industry_id, date)
        sql = "replace into tb_jq_industry_constituent_stocks(date, industry_id, constituent_stocks) " \
              "values ('%s', '%s', \"%s\");" % (date, industry_id, stocks)
        ms.exeNonQuery(sql)
        return stocks
    else:
        return eval(df.iloc[0]['constituent_stocks'])


#
# # import sqlalchemy.orm.query
from jqdatasdk import finance
# from jqdatasdk import jy
# # from jqdatasdk import s
#
#
# def get_sw_quote(code,end_date=None,count=None,start_date=None):
#     '''获取申万指数行情,返回panel结构'''
#     if isinstance(code,str):
#         code=[code]
#     days = get_trade_days(start_date,end_date,count)
#     code_df = jy.run_query(jq.query(
#          jy.SecuMain.InnerCode,jy.SecuMain.SecuCode,jy.SecuMain.ChiName
#         ).filter(
#         jy.SecuMain.SecuCode.in_(code)))
#
#     df = jy.run_query(jq.query(
#          jy.QT_SYWGIndexQuote).filter(
#         jy.QT_SYWGIndexQuote.InnerCode.in_(code_df.InnerCode),
#         jy.QT_SYWGIndexQuote.TradingDay.in_(days),
#         ))
#     df2  = pd.merge(code_df, df, on='InnerCode').set_index(['TradingDay','SecuCode'])
#     df2.drop(['InnerCode','ID','UpdateTime','JSID'],axis=1,inplace=True)
#     return df2.to_panel()


if __name__ == '__main__':
    # start = '2016-01-01'
    # fetch_and_save_all(start_date=start)

    # all_securities = get_all_stocks()
    # print(all_securities)
    # hs300_stocks = get_hs300_stocks()
    # print(hs300_stocks)
    login()

    code = '070002.0F'
    q = jq.query(jq.finance.FUND_PORTFOLIO_STOCK).filter(
        jq.finance.FUND_PORTFOLIO_STOCK.code == code.split('.')[0]).order_by(
        jq.finance.FUND_PORTFOLIO_STOCK.pub_date.desc()).order_by(jq.finance.FUND_PORTFOLIO_STOCK.rank.asc()).limit(2000)
    df = jq.finance.run_query(q)
    print(df)
    df.to_csv('嘉实增长.csv')

    exit(0)

    df = jq.get_all_securities(types=['stock'], date=None)
    stock_codes = list(df.index)

    _stocks = ['603999.XSHG', '603738.XSHG', '603619.XSHG', '603530.XSHG', '603121.XSHG',
     '601717.XSHG', '600896.XSHG', '600638.XSHG', '600630.XSHG', '513100.XSHG', '603007.XSHG']

    # latest_df = fetch_latest(stock_codes)
    # print(latest_df.close)
    d = get_stock_industries(_stocks, industry_type='sw_l2')
    print(d)

    for code in _stocks:
        si = jq.get_security_info(code)
        print(si)

    df = finance.run_query(
        jq.query(finance.SW1_DAILY_VALUATION).filter(finance.SW1_DAILY_VALUATION.code == '801010').limit(10))
    print(df)

    # stocks_list = list(_stocks)
    # d = hot_industries(stocks_list)
    # hot_industry_codes = d['industry_code'].values
    # top_industries = hot_industry_codes[:20]
    # print(top_industries)
    # stocks_in_hot_industry = filter_stocks(stocks_list, top_industries)
    # print(stocks_in_hot_industry)

    # industries = jq.get_industries(name='sw_l2')
    # print(industries)
    # print(industries.index[0])
    # stocks = get_industry_stocks(industries.index[1])
    # print(stocks)

    # 查询'000001.XSHE'的所有市值数据, 时间是2015-10-15
    # q = query(
    #     valuation
    # ).filter(
    #     valuation.code == '000001.XSHE'
    # )
    # df = get_fundamentals(q, '2015-10-15')
    # # 打印出总市值
    # print(df['market_cap'][0])


    # 注意申万指数在2014年有一次大改,聚源使用的是为改变之前的代码,官网包含更改前和更改后的代码,如果遇到找不到的标的可以根据需求自行查找
    # 如801124 >>801121食品加工II

    # code = get_industries(name='sw_l2').index[:5]
    # df = get_sw_quote('801021')
    # df.to_frame(False).tail()


