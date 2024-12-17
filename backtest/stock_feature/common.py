import datetime
import pandas as pd
import os.path
import jq_data as jqd

data_path = './data'
if not os.path.exists(data_path):
    os.mkdir(data_path)


def pull_stocks_kline(bars=100, end_date=None, freq='1w'):
    """
    jq 获取全市场股票k线数据， 保存到pkl文件
    前复权数据
    :return: DataFrame
    """
    jqd.login()

    next_date = end_date
    if end_date is not None:
        next_date = end_date + datetime.timedelta(days=1)

    all_stocks_df = jqd.get_all_stocks()
    stock_codes = list(all_stocks_df.index)
    max_fetch_rows = 500000
    batch = round(max_fetch_rows / bars)
    # batch = 500
    if batch < len(stock_codes):
        df_list = []
        for i in range(batch, len(stock_codes), batch):
            batch_codes = stock_codes[i-batch:i]
            if len(batch_codes) > 0:
                print('batch:', len(batch_codes))
                df = jqd.fetch_bars(batch_codes, bars, end_date=next_date, frequency=freq)
                df_list.append(df)
        batch_codes = stock_codes[i:]
        if len(batch_codes) > 0:
            print('batch:', len(batch_codes))
            df = jqd.fetch_bars(batch_codes, bars, end_date=next_date, frequency=freq)
            df_list.append(df)
        df = pd.concat(df_list)
    else:
        df = jqd.fetch_bars(stock_codes, bars, end_date=next_date, frequency=freq)
    df.reset_index(inplace=True)
    df.rename(columns={'level_0': 'jq_code', 'level_1': 'seq'}, inplace=True)
    if end_date is None:
        str_end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    else:
        str_end_date = end_date.strftime('%Y-%m-%d')
    df.to_pickle(os.path.join(data_path, 'stocks_kline_%s_%d_%s.pkl' % (freq, bars, str_end_date)))
    print(df.tail())
    return df


def load_stocks_kline(bars=100, end_date=None, freq='1w'):
    if end_date is None:
        str_end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    else:
        str_end_date = end_date.strftime('%Y-%m-%d')
    pkl_path = os.path.join(data_path, 'stocks_kline_%s_%d_%s.pkl' % (freq, bars, str_end_date))
    if os.path.exists(pkl_path):
        return pd.read_pickle(pkl_path)
    else:
        return pull_stocks_kline(bars, end_date, freq)


if __name__ == "__main__":

    pass
