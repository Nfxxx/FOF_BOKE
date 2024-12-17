import datetime
import time
import os.path


import numpy as np
import pandas as pd

import common
import model
import jq_data as jqd
from sqlalchemy import create_engine, Table, MetaData, select, and_
from sqlalchemy.orm import sessionmaker


tick_data_path = 'C:\\tick_data\\level2\\202309\\%s\\%s'
tick_file = "%s-TickAB-%s.CSV"
trans_file = "%s-Tran-%s.CSV"

data_path = './data'
if not os.path.exists(data_path):
    os.mkdir(data_path)


def int_to_date(int_date):
    return datetime.date(int_date//10000, (int_date % 10000)//100, int_date % 100)


def int_to_time(int_time):
    int_time = int_time // 1000
    return datetime.time(int_time//10000, (int_time % 10000)//100, int_time % 100)


def load_tick_file(file_path, date):
    df = pd.read_csv(file_path)
    df['Date'] = df['Date'].apply(lambda x: int_to_date(x))
    df['Time'] = df['Time'].apply(lambda x: int_to_time(x))
    trade_date = datetime.datetime.strptime(date, '%Y%m%d')
    df['Time'] = df['Time'].apply(lambda x: datetime.datetime.combine(trade_date, x))
    return df


# load tick data from file and calc tick features
def load_cal_tick_features(code, date):
    # load tick file
    file_path = os.path.join(tick_data_path % (date, date), tick_file % (code, date))
    if not os.path.exists(file_path):
        print('tick file not exist, %s' % file_path)
        return
    df = load_tick_file(file_path, date)
    # load trans file
    trans_df = load_trans_features(code, date)
    if df is None or trans_df is None:
        return
    trade_date = datetime.datetime.strptime(date, '%Y%m%d')
    tf = cal_f2_tick_feature(df, trans_df, trade_date=trade_date)
    return tf


def load_trans_features(code, date):
    # load trans file
    file_path = os.path.join(tick_data_path % (date, date), trans_file % (code, date))
    if not os.path.exists(file_path):
        print('trans file not exist, %s' % file_path)
        return
    df = load_tick_file(file_path, date)
    return df


def cal_f2_tick_feature(df_l2_tick, trans_df, trade_date):
    start_time = datetime.datetime.combine(trade_date, datetime.time(hour=9, minute=30))
    stop_time = datetime.datetime.combine(trade_date, datetime.time(hour=14, minute=57))
    open_auction_df = df_l2_tick[df_l2_tick['Time'] < start_time]
    close_auction_df = df_l2_tick[df_l2_tick['Time'] >= stop_time]
    # l2_tick 取连续竞价 停牌不计算
    df_l2_tick = df_l2_tick[df_l2_tick['Time'] >= start_time]
    df_l2_tick = df_l2_tick[df_l2_tick['Time'] < stop_time]

    df_l2_tick = df_l2_tick[df_l2_tick["BidVolume1"] > 0]
    df_l2_tick = df_l2_tick[df_l2_tick["AskVolume1"] > 0]
    if len(df_l2_tick) == 0:
        return

    ask_size = df_l2_tick["AskVolume1"].mean()
    bid_size = df_l2_tick["BidVolume1"].mean()
    quote_size = (ask_size + bid_size) / 2
    # 过滤涨跌停，没有买单或卖单的情况
    spread = ((df_l2_tick["AskPrice1"] - df_l2_tick["BidPrice1"]) / df_l2_tick["BidPrice1"]).mean()

    # auction time volumes
    pre_close_price = df_l2_tick['PreClose'].iloc[0]
    open_auction_vol = open_auction_df['AccVolume'].iloc[-1]
    mdv = close_auction_df['AccVolume'].iloc[-1]
    close_auction_vol = mdv - close_auction_df['AccVolume'].iloc[0]

    tick_period = get_tick_period(df_l2_tick, trade_date)
    quote_size5 = get_quote_size(df_l2_tick, 5)
    quote_size10 = get_quote_size(df_l2_tick, 10)
    sparsity1 = get_sparsity(df_l2_tick, 5)
    sparsity2 = get_sparsity(df_l2_tick, 10)
    trade_period, trade_size = cal_trans_feature(trans_df, trade_date)
    tf = model.StockTickFeature(symbol=df_l2_tick['Code'].iloc[0], trade_date=time.mktime(trade_date.timetuple()),
                                ask_size=ask_size, bid_size=bid_size, quote_size=quote_size, spread=spread,
                                open_auction_vol=open_auction_vol, close_auction_vol=close_auction_vol,
                                tick_period=tick_period, sparsity1=sparsity1, sparsity2=sparsity2, pre_close_price=pre_close_price,
                                quote_size5=quote_size5, quote_size10=quote_size10, mdv=mdv, trade_period=trade_period, trade_size=trade_size)
    return tf


def get_tick_period(df_l2_tick, trade_date):
    df_l2_tick["middle_price"] = (df_l2_tick["BidPrice1"] + df_l2_tick["AskPrice1"]) / 2
    df_l2_tick = df_l2_tick[~df_l2_tick.middle_price.isna()]
    current_middle_price = df_l2_tick.iloc[0]["middle_price"]
    first_time = df_l2_tick.iloc[0]["Time"]
    current_time = df_l2_tick.iloc[0]["Time"]
    change_list = []
    for _, row in df_l2_tick.iterrows():
        if row["middle_price"] != current_middle_price:
            change_list.append((row["Time"] - current_time).seconds)
            current_time = row["Time"]
            current_middle_price = row["middle_price"]
    time_1300 = datetime.datetime.combine(trade_date, datetime.time(13, 0))
    sum_time = sum(change_list)
    if first_time < time_1300 < current_time:
        sum_time -= 90 * 60
    return sum_time / len(change_list) if len(change_list) > 0 else 0


def get_quote_size(df_l2_tick, level_num):
    assert level_num in {5, 10}
    df_l2_tick["sum_ask_volume"] = 0
    for i in range(1, level_num + 1):
        df_l2_tick["sum_ask_volume"] += df_l2_tick[f"AskVolume{i}"]
    df_l2_tick["sum_bid_volume"] = 0
    for i in range(1, 11):
        df_l2_tick["sum_bid_volume"] += df_l2_tick[f"BidVolume{i}"]
    if level_num == 5:
        div_num = 10
    else:
        div_num = 20
    df_l2_tick[f"quote_size{level_num}"] = (df_l2_tick["sum_ask_volume"] + df_l2_tick["sum_bid_volume"]) / div_num
    con1 = df_l2_tick[f"AskVolume{level_num}"] > 0
    con2 = df_l2_tick[f"BidVolume{level_num}"] > 0
    df_l2_tick_filter = df_l2_tick[con1 & con2]
    return df_l2_tick_filter[f"quote_size{level_num}"].mean()


def get_sparsity(df_l2_tick, level_num):
    assert level_num in {5, 10}
    if level_num == 5:
        div_num = 0.09
    else:
        div_num = 0.19
    df_l2_tick[f"sparsity{level_num}"] = (df_l2_tick[f"AskPrice{level_num}"] - df_l2_tick[
        f"BidPrice{level_num}"]) / div_num
    con1 = df_l2_tick[f"AskPrice{level_num}"] > 0
    con2 = df_l2_tick[f"BidPrice{level_num}"] > 0
    df_l2_tick_filter = df_l2_tick[con1 & con2]
    return df_l2_tick_filter[f"sparsity{level_num}"].mean()


def cal_trans_feature(df_trans, trade_date):
    start_time = datetime.datetime.combine(trade_date, datetime.time(hour=9, minute=30))
    stop_time = datetime.datetime.combine(trade_date, datetime.time(hour=14, minute=57))
    con1 = df_trans.Time >= start_time
    con2 = df_trans.Time <= stop_time
    con3 = df_trans.TradePrice > 0
    # 把撤单排除
    con4 = df_trans.AskOrder > 0
    con5 = df_trans.BidOrder > 0
    df_trans_filter = df_trans[con1 & con2 & con3 & con4 & con5]
    zhongwu = 90 * 60
    time_diff = df_trans_filter.iloc[-1].Time - df_trans_filter.iloc[0].Time
    trade_period = (time_diff.total_seconds() - zhongwu) / len(df_trans_filter)
    trade_size = df_trans_filter.TradeVolume.mean()
    return trade_period, trade_size


def prepare_day_kline(date):
    """
    取日线数据，用于波动率计算
    """
    df = common.load_stocks_kline(bars=60, end_date=date, freq='1d')
    df['code'] = df['jq_code'].apply(lambda x: jqd.add_code_suffix(x.split('.')[0]))
    print(len(df))
    return df[['code', 'date', 'close']]


def get_volatility(df_close):
    # 取60个交易日, df按日期升序
    # df_close = df_close.iloc[::-1]
    df_close["pre_close"] = df_close["close"].shift(1)
    df_close.dropna(inplace=True)
    df_close["ret"] = np.log(df_close["close"] / df_close["pre_close"])
    return df_close["ret"].std() * np.sqrt(252)


def cal_volatility_all_market(date):
    """
    计算全市场股票某一天的波动率。
    看最近60个交易日。
    """
    def group_func(code_df):
        if len(code_df) < 3:
            return 0
        return get_volatility(code_df)

    df = prepare_day_kline(date)
    df.dropna(inplace=True)
    series_vola = df.groupby(by=['code'], group_keys=False).apply(group_func)
    df_vola = series_vola.reset_index()
    df_vola.rename(columns={0: 'volatility'}, inplace=True)
    df_vola['date'] = date
    df_vola.to_csv(os.path.join(data_path, 'stock_volatility_%s.csv' % datetime.datetime.strftime(date, '%Y%m%d')))
    save_volatility_data_to_db(df_vola)
    return df_vola


def load_data_from_db(date: datetime.datetime):
    engine = create_engine('mysql+pymysql://admin:admin@172.0.0.1:3309/soe?charset=utf8')

    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()
    # Define the start and end dates for the range
    start_date = date - datetime.timedelta(days=30)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = date.strftime('%Y-%m-%d')

    # load tick features table
    metadata = MetaData()
    table = Table('tb_tick_features', metadata, autoload_with=engine)
    # Create a select statement with a date range filter
    stmt = select(table).where(and_(
        table.c.date >= start_date_str,
        table.c.date <= end_date_str
    ))
    # Execute the query and fetch the results
    results = session.execute(stmt).fetchall()
    # Convert the results to a DataFrame
    tick_features_df = pd.DataFrame(results, columns=table.columns.keys())

    # load stock volatility table
    metadata = MetaData()
    table = Table('tb_stock_volatility', metadata, autoload_with=engine)
    # Create a select statement with a date range filter
    stmt = select(table).where(and_(
        table.c.date >= end_date_str,
        table.c.date <= end_date_str
    ))
    # Execute the query and fetch the results
    results = session.execute(stmt).fetchall()
    # Convert the results to a DataFrame
    stock_volatility_df = pd.DataFrame(results, columns=table.columns.keys())

    # Close the session
    session.close()

    stock_volatility_df.rename(columns={'code': 'symbol'}, inplace=True)
    # combined_df = pd.merge(tick_features_df, stock_volatility_df,  how='left', on=['symbol', 'date'])

    print(len(tick_features_df), len(stock_volatility_df))
    print(tick_features_df.head())
    print(stock_volatility_df.head())

    # print(len(combined_df))
    # print(combined_df.head())
    return tick_features_df, stock_volatility_df


def cal_stock_basic(date):
    tick_features_df, volatility_df = load_data_from_db(date)

    def group_func(tick_features):
        ask_size = tick_features["ask_size"].median()
        bid_size = tick_features["bid_size"].median()
        quote_size = (ask_size + bid_size) / 2
        spread = tick_features["spread"].head(5).median()
        open_auction_vol = tick_features["open_auction_vol"].median()
        close_auction_vol = tick_features["close_auction_vol"].median()
        mdv = tick_features["mdv"].median()
        tick_period = tick_features["tick_period"].median()
        pre_close_price = tick_features["pre_close_price"].iloc[-1]
        trade_period = tick_features["trade_period"].median()
        trade_size = tick_features["trade_size"].median()
        turnover_period = quote_size / trade_size * trade_period
        quote_thickness = quote_size / trade_size
        sparsity1 = tick_features["sparsity1"].median()
        sparsity2 = tick_features["sparsity2"].median()
        quote_size5 = tick_features["quote_size5"].median()
        quote_size10 = tick_features["quote_size10"].median()
        cluster = 1
        if tick_period <= 4 and trade_period <= 2:
            cluster = 1
        elif (tick_period <= 4 and trade_period > 2) or quote_size10 < 1000 or sparsity2 >= 4:
            cluster = 2
        elif tick_period > 12:
            cluster = 3
        elif 7 < tick_period <= 12:
            cluster = 4
        elif 4 < tick_period <= 7:
            cluster = 5
        tf = model.StockFeatureSummary(symbol=tick_features['symbol'].iloc[0], trade_date=time.mktime(date.timetuple()),
                                       ask_size=ask_size, bid_size=bid_size, quote_size=quote_size, spread=spread,
                                       open_auction_vol=open_auction_vol, close_auction_vol=close_auction_vol,
                                       mdv=mdv, tick_period=tick_period, pre_close_price=pre_close_price,
                                       sparsity1=sparsity1, sparsity2=sparsity2,
                                       trade_period=trade_period, trade_size=trade_size,
                                       turnover_period=turnover_period, quote_thickness=quote_thickness,
                                       quote_size5=quote_size5, quote_size10=quote_size10,
                                       cluster=cluster
                                       )
        return pd.DataFrame([tf.dict()])

    tick_summary_df = tick_features_df.groupby(by=['symbol'], group_keys=False).apply(group_func)
    tick_summary_df = tick_summary_df.drop('volatility', axis=1)
    combined_df = pd.merge(tick_summary_df, volatility_df[['symbol', 'volatility', 'date']],  how='left', on=['symbol'])
    return combined_df


def get_all_stocks():
    path = './data/stocks_kline_1m_240_2023-09-01.pkl'
    df = pd.read_pickle(path)
    codes = df['jq_code'].unique()
    return codes


def cal_tick_features_all_market(date):
    """
    计算某一天全市场股票的tick features
    结果保存到表: soe.tb_tick_features
    """
    all_stocks = get_all_stocks() # jqd.get_all_stocks(date)
    all_codes = [jqd.add_code_suffix(s.split('.')[0]) for s in all_stocks]
    features = []
    for code in all_codes:
        tf = load_cal_tick_features(code, date)
        if tf is None:
            print('done, tf is None', code, datetime.datetime.now())
            continue
        print('done,', code, datetime.datetime.now())
        print(tf)
        features.append(tf)
        # if len(features) > 10:
        #     break

    features_dict_list = [tf.dict() for tf in features]
    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(features_dict_list)
    df['date'] = pd.to_datetime(date)
    df.to_csv(os.path.join(data_path, 'tick_features_%s.csv' % date))
    save_tick_features_data_to_db(df)
    return df


def save_tick_features_data_to_db(df, tb_name='tb_tick_features'):
    if len(df) > 0:
        df.fillna(0, inplace=True)
        conn = create_engine('mysql+pymysql://admin:admin@172.0.0.1:3309/soe?charset=utf8')
        pd.io.sql.to_sql(df, tb_name, con=conn, if_exists='append',
                         index=False, index_label='date')
        conn.dispose()


def save_volatility_data_to_db(df, tb_name='tb_stock_volatility'):
    if len(df) > 0:
        df.fillna(0, inplace=True)
        conn = create_engine('mysql+pymysql://admin:admin@172.0.0.1:3309/soe?charset=utf8')
        pd.io.sql.to_sql(df, tb_name, con=conn, if_exists='append',
                         index=False, index_label='date')
        conn.dispose()


def save_stock_basic_data_to_db(df, tb_name='tb_stock_basic'):
    if len(df) > 0:
        df.fillna(0, inplace=True)
        conn = create_engine('mysql+pymysql://admin:admin@172.0.0.1:3309/soe?charset=utf8')
        pd.io.sql.to_sql(df, tb_name, con=conn, if_exists='append',
                         index=False, index_label='date')
        conn.dispose()


def load_tf():
    df = pd.read_csv(os.path.join(data_path, 'tick_features_%s.csv' % '20230901'), index_col=False)
    df['date'] = datetime.datetime.strptime('20230901', '%Y%m%d')
    # save_data_to_db(df)
    # print(df.describe())
    desc = df.describe()
    print(desc)
    pass


if __name__ == "__main__":
    cal_tick_features_all_market('20230911')
    cal_volatility_all_market(datetime.datetime.strptime('20230911', '%Y%m%d'))

    # dates = ['20230901', '20230904', '20230905', '20230906', '20230907', '20230908']
    # for date in dates:
    #     cal_volatility_all_market(datetime.datetime.strptime('20230908', '%Y%m%d'))

    # basic_df = cal_stock_basic(datetime.datetime.strptime('20230908', '%Y%m%d'))
    # save_stock_basic_data_to_db(basic_df)
