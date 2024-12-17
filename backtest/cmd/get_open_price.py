from utils.tools import read_snapshot, get_symbol_list
from logger import logger
import pandas as pd


def main(path, date):
    symbols_sz, symbols_sh = get_symbol_list(path, date)
    ret_list = []
    for symbol in symbols_sh | symbols_sz:
        df_snap, _, _ = read_snapshot(f"{path}/{date}", date, symbol)
        open_price = df_snap.iloc[-1].Open
        assert open_price != 0
        ret_list.append({
            "symbol": symbol,
            "open_price": open_price
        })
    pd.DataFrame(ret_list).to_csv(f"{path}/{date}/instructions/Open-{date}.csv", index=None)


if __name__ == "__main__":
    file_path = "D:/backtest/data"
    input_date = "20230601"
    main(file_path, input_date)
