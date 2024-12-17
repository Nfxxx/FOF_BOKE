import sys
import pandas as pd
import numpy as np
from message_book.message_book_sh import format_orders_datayes, format_trans_datayes, check_orders, construct_mkt_order_entrust, \
    fill_mkt_order_price, merge_mkt_order_entrust
from message_book.message_book_sz import format_order_trade_data, fill_auto_cxl, assert_mkt_ord, assert_type_u, \
    fill_mkt_ord_pri


def assert_instructions(instructions):
    """
    检查指令
    :param instructions:
    :return:
    """
    instructions_a = instructions[instructions.OrdType.ne("D")]
    assert len(set(instructions_a.OrderNO)) == len(instructions_a), "OrderNO唯一"
    assert len(set(instructions.EventID)) == len(instructions), "EventID必须唯一"
    df_event_id = instructions.groupby("Time").EventID.min().reset_index()
    sort_by_id = df_event_id.sort_values(by="EventID")
    sort_by_time = df_event_id.sort_values(by="Time")
    assert all(sort_by_id.EventID == sort_by_time.EventID), "EventID大小与时间正比"


def get_instructions_sz(df_orders, df_trans):
    """
    得到深交所委托指令
    :param df_orders:
    :param df_trans:
    :return:
    """
    df_cancels = df_trans[df_trans.ExecType.eq("D")]
    df_cancels["OrderNO"] = df_cancels[["AskOrder", "BidOrder"]].max(axis=1).astype(int)
    df_cancels["Side"] = np.where(df_cancels.BidOrder.eq(0), "S", "B")
    df_cancels["OrdType"] = "D"
    instruction_columns = ["EventID", "Time", "OrderNO", "OrdType", "Side", "Price", "Qty", "TradPhase"]
    instructions = pd.concat([
        df_cancels[instruction_columns], df_orders[instruction_columns]
    ])
    ad_instruction_nos = set(instructions[instructions.OrdType.eq("AD")].OrderNO)
    instructions = instructions[~instructions.OrderNO.isin(ad_instruction_nos)]
    assert set(instructions.OrdType) <= {"A", "L", "M", "U", "D"}
    assert_instructions(instructions)
    instructions = instructions.sort_values(by="EventID")
    return instructions


def get_instructions_sh(df_orders):
    """
    得到上交所委托指令0
    :param df_orders:
    :return:
    """
    instructions = df_orders.copy()
    assert set(instructions.ExecType) == {"A", "D"}
    instructions.loc[instructions.ExecType.eq("D"), "OrdType"] = "D"
    assert set(instructions.OrdType) <= {"A", "L", "M", "D"}
    instruction_columns = ["EventID", "Time", "OrderNO", "OrdType", "Side", "Price", "Qty", "TradPhase"]
    instructions = instructions[instruction_columns]
    assert_instructions(instructions)
    instructions = instructions.sort_values(by="EventID")
    return instructions


def gen_instructions(path, date,symbol):
    """
    生成修正后的委托指令
    :param path:
    :param date:
    :param symbol:
    :return:
    """
    df_SH_order = pd.read_csv(f"{path}/{date}/mdl_4_24_0.csv")
    df_SH_order = df_SH_order[~df_SH_order['TickBSFlag'].isin(['CLOSE', 'CCALL', 'SUSP', 'N', 'ENDTR'])]
    df_SZ_orders = pd.read_csv(f"{path}/{date}/mdl_3_33_0.csv")
    df_SZ_trans = pd.read_csv(f"{path}/{date}/mdl_3_36_0.csv")
    if symbol.endswith("SH"):
        df_SH_orders = format_orders_datayes(df_SH_order)
        df_SH_trans = format_trans_datayes(df_SH_order)
        check_orders(df_SH_orders, df_SH_trans)
        df_mkt_order = construct_mkt_order_entrust(df_SH_trans)
        fill_mkt_order_price(df_mkt_order, df_SH_trans)
        df_SH_orders = merge_mkt_order_entrust(df_mkt_order, df_SH_trans)
        instructions = get_instructions_sh(df_SH_orders)
        # print(instructions)

    else:
        df_orders, df_trans = format_order_trade_data(df_SZ_orders, df_SZ_trans)
        df_orders["EventID"] = df_orders["OrderNO"]
        fill_auto_cxl(df_orders, df_trans)
        assert_mkt_ord(df_orders, df_trans)
        assert_type_u(df_orders, df_trans)
        fill_mkt_ord_pri(df_orders, df_trans)
        instructions = get_instructions_sz(df_orders, df_trans)
    instructions.to_csv(f"{path}/{date}/instructions/{symbol}-Instruction-{date}.csv", index=None)


if __name__ == "__main__":
    path = "C:/Users/niefanxiang/Desktop/flow-master/backtest/data"
    date = "20240726"
    gen_instructions(path, date, "SH")


