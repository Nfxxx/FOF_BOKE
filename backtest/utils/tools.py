import pandas as pd
import os
from collections import Counter
import datetime


def isin_auc(exec_time):
    """
    是否集合竞价时间
    :param exec_time:
    :return:
    """
    # print(exec_time.dtype)
    try:
        return (exec_time < 93000000) | (exec_time >= 145700000)
    except:
        return (exec_time < '09:30:00.000') | (exec_time >= '14:57:00.000')


def isin_am_auc(exec_time):
    """
    是否上午集合竞价时间
    :param exec_time:
    :return:
    """

    try:
        return exec_time < 93000000
    except:
        return exec_time < '09:30:00.000'


def isin_pm_auc(exec_time):
    """
    是否下午集合竞价时间
    :param exec_time:
    :return:
    """

    try:
        return exec_time >= 145700000
    except:
        return exec_time >= '14:57:00.000'


def isin_trad(exec_time):
    """
    是否交易时间
    :param exec_time:
    :return:
    """
    return (
            ((exec_time >= 93000000) & (exec_time < 113000000))
            | ((exec_time >= 130000000) & (exec_time < 145700000))
    )


def get_fill_buy_sell(df_trans, df_orders):
    """
    将一笔撮合成交数据拆分成两笔
    :param df_trans:
    :param df_orders:
    :return:
    """
    df_fill = df_trans[df_trans.ExecType.eq("F")]
    df_fill_b = df_fill[
        ["EventID", "ExecType", "BidOrder", "Price", "Qty", "Time", "TradPhase"]]
    df_fill_b["OrderNO"] = df_fill_b.BidOrder
    df_fill_b["OrdType"] = df_fill_b.BidOrder.map(df_orders.set_index("OrderNO").OrdType)
    df_fill_b["Side"] = "B"
    df_fill_s = df_fill[
        ["EventID", "ExecType", "AskOrder", "Price", "Qty", "Time", "TradPhase"]]
    df_fill_s["OrderNO"] = df_fill_s.AskOrder
    df_fill_s["OrdType"] = df_fill_s.AskOrder.map(df_orders.set_index("OrderNO").OrdType)
    df_fill_s["Side"] = "S"
    return df_fill_b, df_fill_s


def read_snapshot(path, date_str, symbol):
    """
    读取快照数据
    :param path:
    :param date_str:
    :param symbol:
    :return:
    """
    df_orders = pd.read_csv(f"{path}/{symbol}-TickAB-{date_str}.csv")
    bid_book_index = [
        stag + str(idx)
        for idx in range(1, 10 + 1)
        for stag in ["BidPrice", "BidVolume", ]
    ]
    ask_book_index = [
        stag + str(idx)
        for idx in range(1, 10 + 1)
        for stag in ["AskPrice", "AskVolume", ]
    ]
    return df_orders[
        ["Time", "Price", "Volume", "Open"] + bid_book_index + ask_book_index], bid_book_index, ask_book_index


def get_level_from_snap(snap_ser, bid_book_index, ask_book_index):
    """
    从快照得到10档信息
    :param snap_ser:
    :param bid_book_index:
    :param ask_book_index:
    :return:
    """
    bid_list = []
    ask_list = []
    for i in range(0, len(bid_book_index), 2):
        bid_list.append((snap_ser[bid_book_index[i]], int(snap_ser[bid_book_index[i + 1]])))
    for i in range(0, len(ask_book_index), 2):
        ask_list.append((snap_ser[ask_book_index[i]], int(snap_ser[ask_book_index[i + 1]])))
    return bid_list, ask_list


def assert_order_book(order_book):
    """
    检查order_book
    :param order_book:
    :return:
    """
    assert len(set(order_book.OrderNO)) == len(order_book), "OrderNO 必须唯一"
    assert set(order_book.ExecType) == {"A"}
    assert all(order_book["LeftQty"] > 0)


def convert_message_book(message_book):
    """
    转换message_book 到 order_book
    :param message_book:
    :return:
    """
    left_qty_map = message_book.groupby("OrderNO").LeftQty.min()
    message_book["LeftQty"] = message_book["OrderNO"].map(left_qty_map)
    order_book = message_book[message_book.ExecType.eq("A") & message_book.LeftQty.gt(0)]
    return order_book


def get_level10_info(order_book):
    """
    从order_book提取10档信息
    :param order_book:
    :return:
    """
    assert_order_book(order_book)
    queue_b = order_book[order_book.Side.eq("B")]
    queue_s = order_book[order_book.Side.eq("S")]
    b_map = {}
    for price, group in queue_b.groupby("Price"):
        b_sum_vol = group["LeftQty"].sum()
        b_map[price] = b_sum_vol
    b_rank_map = sorted(b_map.items(), key=lambda x: x[0], reverse=True)
    b_rank_map = b_rank_map[:10]
    s_map = {}
    for price, group in queue_s.groupby("Price"):
        s_sum_vol = group["LeftQty"].sum()
        s_map[price] = s_sum_vol
    s_rank_map = sorted(s_map.items(), key=lambda x: x[0])
    s_rank_map = s_rank_map[:10]
    return b_rank_map, s_rank_map


def get_price_and_vol(message_book):
    """
    从message提取最新价 成交量
    :param message_book:
    :return:
    """
    message_book = message_book[message_book.ExecType.eq("F")]
    if message_book.empty:
        return 0, 0
    buy_vol = message_book[message_book.Side.eq("B")]["Qty"].sum()
    sell_vol = message_book[message_book.Side.eq("S")]["Qty"].sum()
    assert buy_vol == sell_vol
    last_price = message_book.sort_values(by="Time", ascending=False).iloc[0].Price
    return buy_vol, last_price


def print_compare_result(path, date, symbol, df_mb_all):
    """
    输出message_book和快照的对比结果
    :param path:
    :param date:
    :param symbol:
    :param df_mb_all:
    :return:
    """
    df_snap, bid_book_index, ask_book_index = read_snapshot(f"{path}/{date}", date, symbol)
    df_snap["Time"] = df_snap.Time.astype(int)
    for _, row in df_snap.iterrows():
        if row["Volume"] == 0:
            continue
        snapshot_time = int(row["Time"])
        if 93000000 <= snapshot_time < 93000000 + 3000:
            continue
        bid_list, ask_list = get_level_from_snap(row, bid_book_index, ask_book_index)
        df_mb = df_mb_all[df_mb_all.Time <= snapshot_time]
        if snapshot_time < 93000000 + 3000:
            last_snapshot_time = 0
        else:
            last_snapshot_time = snapshot_time - 3000
        vol, price = get_price_and_vol(
            df_mb_all[(df_mb_all.Time > last_snapshot_time) & (df_mb_all.Time <= snapshot_time)])
        ob = convert_message_book(df_mb)
        assert_order_book(ob)
        b_rank_map, s_rank_map = get_level10_info(ob)
        print(f"timestamp:{snapshot_time}")
        print(f"bid from  file:{bid_list}")
        print(f"bid from merge:{b_rank_map}")
        print(f"ask from  file:{ask_list}")
        print(f"ask from merge:{s_rank_map}")
        print(f"from file:price {row['Price']}, vol: {row['Volume']}")
        print(f"from merge:price {price}, vol:{vol}")


def print_open_level_10(path, date, symbol, order_book, match_vol):
    """
    打印开盘时10档信息，对比order_book和快照数据
    :param path:
    :param date:
    :param symbol:
    :param order_book:
    :param match_vol:
    :return:
    """
    b_rank_map, s_rank_map = get_level10_info(order_book)
    df_snap, bid_book_index, ask_book_index = read_snapshot(f"{path}/{date}", date, symbol)
    if match_vol > 0:
        snap_time = df_snap[df_snap.Volume > 0].iloc[0].Time
    else:
        df_snap = df_snap[df_snap.Time >= 92500000]
        snap_time = df_snap[df_snap.AskVolume1 > 0].iloc[0].Time
    df_snap["Time"] = df_snap.Time.astype(int)
    row = df_snap[df_snap.Time == snap_time].iloc[0]
    bid_list, ask_list = get_level_from_snap(row, bid_book_index, ask_book_index)
    print(f"timestamp:{snap_time}")
    print(f"bid from  file:{bid_list}")
    print(f"bid from merge:{b_rank_map}")
    print(f"ask from  file:{ask_list}")
    print(f"ask from merge:{s_rank_map}")
    if len(b_rank_map) != 10:
        for i in range(10 - len(b_rank_map)):
            b_rank_map.append((0, 0))
    if len(s_rank_map) != 10:
        for i in range(10 - len(s_rank_map)):
            s_rank_map.append((0, 0))
    return (bid_list == b_rank_map) & (ask_list == s_rank_map)


def get_symbol_list(path, date):
    """
    从文件中得到股票列表
    :param path:
    :param date:
    :return:
    """
    files = os.listdir(f"{path}/{date}")
    symbols = [file.split("-")[0] for file in files]
    symbols_sz = [symbol for symbol in symbols if
                  symbol.endswith("SZ") and (symbol.startswith("0") | symbol.startswith("3"))]
    sz_count_mp = Counter(symbols_sz)
    symbols_sz = {key for key, value in sz_count_mp.items() if value == 4}
    symbols_sh = [symbol for symbol in symbols if symbol.endswith("SH") and symbol.startswith("6")]
    sh_count_mp = Counter(symbols_sh)
    symbols_sh = {key for key, value in sh_count_mp.items() if value == 4}
    return symbols_sz, symbols_sh


def remove_auc_matched(df_ins, match_vol):
    """
    去除竞合竞价撮合成功的委托
    :param df_ins:
    :param match_vol:
    :return:
    """
    df_ins["QtySum"] = df_ins.Qty.cumsum()
    df_ins["PreQtySum"] = df_ins["QtySum"].shift(1)
    df_ins["PreQtySum"].fillna(0, inplace=True)
    df_ins = df_ins[df_ins.QtySum > match_vol]
    df_ins["Qty"] = df_ins.apply(
        lambda x: x["Qty"] - (match_vol - x["PreQtySum"]) if x["PreQtySum"] < match_vol <= x["QtySum"] else x["Qty"],
        axis=1).astype(int)
    del df_ins["QtySum"], df_ins["PreQtySum"]
    return df_ins


def get_open_order_book(instructions, open_price):
    """
    基于委托指令和开盘价得到开盘order_book
    :param instructions:
    :param open_price:
    :return:
    """
    instructions = instructions[instructions.Time < 93000000]
    cancel_nos = set(instructions[instructions.OrdType.eq("D")].OrderNO)
    instructions = instructions[~instructions.OrderNO.isin(cancel_nos)]
    buy_ins = instructions[instructions.Side.eq("B")].sort_values(["Price", "EventID"], ascending=[False, True])
    sell_ins = instructions[instructions.Side.eq("S")].sort_values(["Price", "EventID"], ascending=[True, True])
    buy_vol = buy_ins[buy_ins.Price.ge(open_price)].Qty.sum()
    sell_vol = sell_ins[sell_ins.Price.le(open_price)].Qty.sum()
    match_vol = min(buy_vol, sell_vol)
    buy_ins = remove_auc_matched(buy_ins, match_vol)
    sell_ins = remove_auc_matched(sell_ins, match_vol)
    order_book = pd.concat([buy_ins, sell_ins])
    order_book["LeftQty"] = order_book["Qty"]
    order_book["ExecType"] = "A"
    order_book["EventID"] = order_book["OrderNO"]
    order_book["TradPhase"] = "A"
    return order_book.sort_values(by="EventID"), match_vol


def get_time_diff(time_x, time_y):
    """
    时间差 秒
    :param time_x: begin_time
    :param time_y: end_time
    :return:
    """
    begin_time = datetime.datetime.strptime(f"{int(time_x)}", "%H%M%S%f")
    end_time = datetime.datetime.strptime(f"{int(time_y)}", "%H%M%S%f")
    if (time_x - 113000000) * (time_y - 113000000) > 0:
        return (end_time - begin_time).seconds
    noon_time = (datetime.datetime.strptime("130000000", "%H%M%S%f") -
                 datetime.datetime.strptime("113000000", "%H%M%S%f")).seconds
    return (end_time - begin_time).seconds - noon_time
