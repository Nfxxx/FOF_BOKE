import pandas as pd
import numpy as np
from utils.tools import isin_auc, get_fill_buy_sell
from logger import logger


def format_orders(df_orders):
    """
    托
    :param df_orders:
    :return:格式化上交所逐笔委
    """
    df_orders = df_orders[
        ["BizIndex", "OrderNo", "OrderKind", "FunctionCode", "OrderPrice", "OrderVolume", "Time"]].rename(
        columns={"BizIndex": "EventID", "OrderNo": "OrderNO", "OrderKind": "ExecType", "FunctionCode": "Side",
                 "OrderPrice": "Price",
                 "OrderVolume": "Qty"})
    assert set(df_orders["ExecType"]) == {"A", "D"}
    df_orders["TradPhase"] = np.where(isin_auc(df_orders.Time), "A", "C")
    df_orders["OrdType"] = np.where(isin_auc(df_orders.Time), "A", "L")
    df_orders["LeftQty"] = df_orders.Qty.where(df_orders.ExecType.eq("A"), -df_orders.Qty)
    columns = ["EventID", "ExecType", "OrderNO", "OrdType", "Side", "Price", "Qty", "LeftQty", "Time", "TradPhase"]
    return df_orders[columns]



def format_orders_datayes(df_orders):
    """

    :param df_orders:
    :return:格式化上交所逐笔委托
    [ 'Channel', 'SecurityID', 'TickTime', 'Type', 'BuyOrderNO', 'SellOrderNO', 'Price', 'TradeMoney', 'TickBSFlag', 'LocalTime']
    """

    # df_orders=df_orders[~df_orders['TickBSFlag'].isin(['CLOSE','CCALL','SUSP','N','ENDTR'])]
    df_orders=df_orders[df_orders['Type'].isin(['A','D'])]
    df_orders = df_orders[

        ["BizIndex", "SeqNo","Type", "SecurityID", "Price", "Qty", "TickTime"]].rename(
        columns={"BizIndex": "EventID", "SeqNo": "OrderNO", "Type": "ExecType", "SecurityID": "Side",
                 "TickTime": "Time"
                 })

    assert set(df_orders["ExecType"]) == {"A", "D"}
    # print(df_orders.TickTime)
    df_orders["TradPhase"] = np.where(isin_auc(df_orders.Time), "A", "C")
    df_orders["OrdType"] = np.where(isin_auc(df_orders.Time), "A", "L")
    df_orders["LeftQty"] = df_orders.Qty.where(df_orders.ExecType.eq("A"), -df_orders.Qty)
    columns = ["EventID", "ExecType", "OrderNO", "OrdType", "Side", "Price", "Qty", "LeftQty", "Time", "TradPhase"]
    return df_orders[columns]



def format_trans(df_trans):
    """
    格式化上交所逐笔成交
    :param df_trans:
    :return:
    """
    df_trans = df_trans[
        ["BizIndex", "BSFlag", "BidOrder", "AskOrder", "TradePrice", "TradeVolume", "Time"]].rename(
        columns={"BizIndex": "EventID", "BSFlag": "Side", "TradePrice": "Price", "TradeVolume": "Qty"})
    df_trans["ExecType"] = "F"
    df_trans["TradPhase"] = np.where(isin_auc(df_trans.Time), "A", "C")
    df_trans["LeftQty"] = -df_trans["Qty"]
    return df_trans

def format_trans_datayes(df_trans):
    """
    格式化上交所逐笔成交
    :param df_trans:
    :return:
    """

    df_trans = df_trans[df_trans['Type'].isin(['T'])]
    df_trans = df_trans[
        ["BizIndex", "BSFlag", "BidOrder", "AskOrder", "TradePrice", "TradeVolume", "Time"]].rename(
        columns={"BizIndex": "EventID", "BSFlag": "Side", "TradePrice": "Price", "TradeVolume": "Qty"})
    df_trans["ExecType"] = "F"
    df_trans["TradPhase"] = np.where(isin_auc(df_trans.Time), "A", "C")
    df_trans["LeftQty"] = -df_trans["Qty"]
    print(df_trans)
    return df_trans

def construct_mkt_order_entrust(df_trans):
    """
    重构上交所市价委托
    :param df_trans:
    :return:
    """
    cond_cont_fill = df_trans.TradPhase.eq("C")
    df_active_order = df_trans[cond_cont_fill]
    df_active_order["ExecType"] = "A"
    df_active_order["OrderNO"] = df_active_order.BidOrder.where(df_active_order.Side.eq("B"),
                                                                df_active_order.AskOrder)
    df_active_order["Qty"] = df_active_order.OrderNO.map(df_active_order.groupby( "OrderNO").Qty.sum())
    df_active_order["Price"] = 0
    df_active_order["LeftQty"] = df_active_order["Qty"]
    df_active_order["OrdType"] = "M"
    df_active_order = df_active_order.drop_duplicates(subset="OrderNO")
    columns = ["EventID", "ExecType", "OrderNO", "OrdType", "Side", "Price", "Qty", "LeftQty", "Time", "TradPhase"]
    return df_active_order[columns]


def fill_mkt_order_price(df_active_order, df_trans):
    """
    给市价委托填充价格
    :param df_active_order:
    :param df_trans:
    :return:
    """
    df_trans_bid = df_trans.groupby("BidOrder").Price.max()
    df_trans_ask = df_trans.groupby("AskOrder").Price.min()
    price_map = pd.concat([df_trans_bid, df_trans_ask])
    df_active_order["Price"] = df_active_order.OrderNO.map(price_map)
    assert df_active_order[df_active_order.Price.isnull()].empty
    assert df_active_order[df_active_order.Price.eq(0)].empty


def assert_df_duplicate(df_duplicate):
    """
    合并市价委托到原始委托时, 检查重复的委托
    :param df_duplicate:
    :return:
    """
    if df_duplicate.empty:
        return
    df_ord_type = df_duplicate.groupby("OrderNO").OrdType.agg(["min", "max"])
    assert set(df_ord_type["min"]) == {"L"}
    assert set(df_ord_type["max"]) == {"M"}
    df_time = df_duplicate.groupby("OrderNO").Time.agg(["min", "max"])
    assert all(df_time["min"] == df_time["max"])
    df_side = df_duplicate.groupby("OrderNO").Side.agg(["min", "max"])
    assert all(df_side["min"] == df_side["max"])
    df_price = df_duplicate.groupby("OrderNO").Price.agg(["min", "max"])
    # assert all(df_price["min"] == df_price["max"])


def merge_mkt_order_entrust(df_active_order, df_orders):
    """
    合并市价委托到原始委托
    :param df_active_order:
    :param df_orders:
    :return:
    """
    df_orders_d = df_orders[df_orders.ExecType.eq("D")]
    df_orders_a = df_orders[df_orders.ExecType.eq("A")]
    assert len(set(df_orders_a.OrderNO)) == len(df_orders_a), "委托编号唯一"
    df_orders_all = pd.concat([
        df_orders_a, df_active_order
    ])
    inner_order_nos = set(df_orders_a.OrderNO) & set(df_active_order.OrderNO)
    df_duplicate = df_orders_all.loc[df_orders_all.OrderNO.isin(inner_order_nos)]
    assert_df_duplicate(df_duplicate)
    df_orders_all.loc[df_orders_all.OrderNO.isin(inner_order_nos), "EventID"] = df_orders_all.loc[
        df_orders_all.OrderNO.isin(inner_order_nos), "OrderNO"].map(df_duplicate.groupby("OrderNO").EventID.min())
    df_orders_all.loc[df_orders_all.OrderNO.isin(inner_order_nos), "OrdType"] = "M"

    df_bid = df_duplicate[df_duplicate.Side.eq("B")].groupby("OrderNO").Price.max()
    df_ask = df_duplicate[df_duplicate.Side.eq("S")].groupby("OrderNO").Price.min()
    price_map = pd.concat([df_bid, df_ask])
    df_orders_all.loc[df_orders_all.OrderNO.isin(inner_order_nos), "Price"] = df_orders_all.loc[
        df_orders_all.OrderNO.isin(inner_order_nos), "OrderNO"].map(price_map)
    assert df_orders_all[df_orders_all.Price.isnull()].empty
    assert df_orders_all[df_orders_all.Price.eq(0)].empty
    df_orders_all.loc[df_orders_all.OrderNO.isin(inner_order_nos), "Qty"] = df_orders_all.loc[
        df_orders_all.OrderNO.isin(inner_order_nos), "OrderNO"].map(df_duplicate.groupby("OrderNO").Qty.sum())
    df_orders_all["LeftQty"] = df_orders_all["Qty"]
    df_orders_all = df_orders_all.drop_duplicates(subset="OrderNO")
    df_orders_all = pd.concat([
        df_orders_d, df_orders_all
    ])
    df_orders_all = df_orders_all.sort_values(by="EventID")
    assert len(set(df_orders_all.EventID)) == len(df_orders_all), "EventID编号唯一"
    return df_orders_all


def check_orders(df_orders, df_trans):
    """
    检查上交所的委托数据
    :param df_orders:
    :param df_trans:
    :return:
    """
    assert all(df_orders[df_orders["EventID"] > df_orders["OrderNO"]])
    df_trans_auc = df_trans[df_trans.TradPhase.eq("A")]
    df_trans_cont = df_trans[df_trans.TradPhase.eq("C")]
    assert all(
        df_trans_cont.apply(lambda x: x["EventID"] > max(x["BidOrder"], x["AskOrder"]), axis=1)), "连续竞价不存在异常EventID"
    # assert not all(df_trans_auc.apply(lambda x: x["EventID"] > max(x["BidOrder"], x["AskOrder"]), axis=1)), \
    #     "集合竞价存在异常EventID"
    order_no_set_in_entrust = set(df_orders["OrderNO"])
    order_no_set_in_auc_fill = set(df_trans_auc["AskOrder"]) | set(df_trans_auc["BidOrder"])
    assert order_no_set_in_auc_fill < order_no_set_in_entrust, "集合竞价的成交一定存在对应的委托"
    assert all(df_trans.apply(lambda x: min(x["BidOrder"], x["AskOrder"]) > 0, axis=1)), "委托ID均大于0"

    df_trans_b = df_trans[df_trans.Side.eq("B")]
    df_trans_s = df_trans[df_trans.Side.eq("S")]
    assert all(df_trans_b.BidOrder > df_trans_b.AskOrder), "外盘主动单ID大于被动单ID"
    assert all(df_trans_s.AskOrder > df_trans_s.BidOrder), "内盘主动单ID大于被动单ID"

    active_order_no_set = set(df_trans_b.BidOrder) | set(df_trans_s.AskOrder)
    passive_order_no_set = set(df_trans_b.AskOrder) | set(df_trans_s.BidOrder)
    assert passive_order_no_set < order_no_set_in_entrust, "被动单一定存在对应的委托"
    df_orders_d = df_orders[df_orders.ExecType.eq("D")]
    mkt_to_limit_nos = active_order_no_set & passive_order_no_set
    if len(mkt_to_limit_nos) > 0:
        assert mkt_to_limit_nos < order_no_set_in_entrust, "市转限一定存在对应的委托"
        # logger.info(f"市转限委托:{mkt_to_limit_nos}")
    mkt_to_cancel_nos = active_order_no_set & set(df_orders_d.OrderNO)
    if len(mkt_to_cancel_nos) > 0:
        assert mkt_to_cancel_nos < order_no_set_in_entrust, "市转撤一定存在对应的委托"
        # logger.info(f"市转撤委托:{mkt_to_cancel_nos}")
    special_mkt_to_limit_nos = (active_order_no_set - passive_order_no_set - set(
        df_orders_d.OrderNO)) & order_no_set_in_entrust
    if len(special_mkt_to_limit_nos) > 0:
        logger.info(f"市转限未成交委托:{special_mkt_to_limit_nos}")


def get_message_book_sh(df_orders, df_trans):
    """
    生成上交所全天的message_book
    :param df_orders:
    :param df_trans:
    :return:
    """
    check_orders(df_orders, df_trans)
    df_mkt_order = construct_mkt_order_entrust(df_trans)
    fill_mkt_order_price(df_mkt_order, df_trans)
    df_orders = merge_mkt_order_entrust(df_mkt_order, df_orders)
    df_orders.loc[(df_orders.ExecType.eq("A") & df_orders.TradPhase.eq("C")), "EventID"] = df_orders.loc[
        (df_orders.ExecType.eq("A") & df_orders.TradPhase.eq("C")), "OrderNO"]
    df_fill_b, df_fill_s = get_fill_buy_sell(df_trans, df_orders[df_orders.ExecType.eq("A")])
    df_fill_b["LeftQty"] = -df_fill_b["Qty"]
    df_fill_s["LeftQty"] = -df_fill_s["Qty"]
    columns = ["EventID", "ExecType", "OrderNO", "OrdType", "Side", "Price", "Qty", "LeftQty", "Time", "TradPhase"]
    df_msg_book = pd.concat([
        df_orders[columns],
        df_fill_b[columns], df_fill_s[columns],
    ])
    df_msg_book = df_msg_book.sort_values("EventID").reset_index(drop=True)
    df_msg_book_unique = df_msg_book.drop_duplicates(subset=["EventID", "OrderNO"], keep=False)
    assert len(df_msg_book_unique) == len(df_msg_book), "主键不能重复"
    df_msg_book["LeftQty"] = df_msg_book.groupby("OrderNO").LeftQty.cumsum()
    assert all(df_msg_book["LeftQty"] >= 0)
    return df_msg_book[columns].reset_index(drop=True)
