import numpy as np
import pandas as pd
from utils.tools import isin_auc, isin_am_auc, isin_pm_auc, get_fill_buy_sell
from logger import logger


def assert_empty_type_data(df_orders_empty, df_trans):
    assert not (set(df_orders_empty.OrderNO) & (set(df_trans.AskOrder) | set(df_trans.BidOrder)))


def format_order_trade_data(df_orders, df_trans):
    """
    格式化深交所逐笔委托 逐笔成交
    :param df_orders:
    :param df_trans:
    :return:
    """
    df_orders = df_orders[["Index", "OrderKind", "FunctionCode", "OrderPrice", "OrderVolume", "Time"]].rename(
        columns={"Index": "OrderNO", "OrderKind": "OrdType", "FunctionCode": "Side", "OrderPrice": "Price",
                 "OrderVolume": "Qty"})
    df_orders["TradPhase"] = np.where(isin_auc(df_orders.Time), "A", "C")
    if not df_orders[df_orders["OrdType"].isnull()].empty:
        logger.info(f"warning,逐笔数据异常,存在空类型数据:{df_orders[df_orders['OrdType'].isnull()].to_dict()}")
        assert_empty_type_data(df_orders[df_orders["OrdType"].isnull()], df_trans)
    df_orders = df_orders[~df_orders["OrdType"].isnull()]  # 未知委托类型的订单删除
    assert len(df_orders) == len(set(df_orders.OrderNO)), "委托编号不唯一"
    df_orders["OrdType"] = df_orders["OrdType"].apply(lambda x: x if isinstance(x, str) else str(int(x)))
    assert set(df_orders["OrdType"]) <= {"1", "2", "U"}, "未知委托类型"
    df_orders["OrdType"] = df_orders.OrdType.map(
        {"1": "M", "2": "L", "U": "U"})
    assert set(df_orders[df_orders.TradPhase.eq("A")]["OrdType"]) == {"L"}, "集合竞价应该只有限价委托"
    df_orders.loc[df_orders.TradPhase.eq("A"), "OrdType"] = "A"  # 集合竞价给特定委托类型
    df_orders["Side"] = df_orders["Side"].apply(lambda x: int(x))
    assert set(df_orders.Side) == {1, 2}, "未知交易方向"
    df_orders["Side"] = np.where(df_orders.Side.eq(1), "B", "S")
    df_trans = df_trans[
        ["Index", "OrderKind", "BidOrder", "AskOrder", "TradePrice", "TradeVolume", "Time"]].rename(
        columns={"Index": "EventID", "OrderKind": "ExecType", "TradePrice": "Price", "TradeVolume": "Qty"})
    df_trans["TradPhase"] = np.where(isin_auc(df_trans.Time), "A", "C")
    assert set(df_trans["ExecType"]) == {"4", "F"}, "未知执行类型"
    df_trans["ExecType"] = df_trans.ExecType.map({"4": "D", "F": "F"})
    return df_orders, df_trans


def fill_fst_upd_id(df_orders, df_trans):
    """
    找到委托的第一次更新序号
    :param df_orders:
    :param df_trans:
    :return:
    """
    fst_upd_b = df_trans.groupby("BidOrder").EventID.min()
    if 0 in fst_upd_b.index:
        fst_upd_b = fst_upd_b.drop(labels=[0])
    fst_upd_s = df_trans.groupby("AskOrder").EventID.min()
    if 0 in fst_upd_s.index:
        fst_upd_s = fst_upd_s.drop(labels=[0])
    fst_upd = pd.concat([fst_upd_b, fst_upd_s])
    df_orders["fst_upd_id"] = df_orders.OrderNO.map(fst_upd)
    df_orders["fst_upd_id"] = df_orders["fst_upd_id"].fillna(df_trans.EventID.max() + 1).astype(int)

    auc_am_f_min_id, auc_am_f_max_id = (df_trans[df_trans.ExecType.eq("F") & isin_am_auc(df_trans.Time)].
                                        EventID.agg(["min", "max"]).values)
    auc_pm_f_min_id, auc_pm_f_max_id = (df_trans[df_trans.ExecType.eq("F") & isin_pm_auc(df_trans.Time)].
                                        EventID.agg(["min", "max"]).values)
    df_orders.loc[df_orders.fst_upd_id.ge(auc_am_f_min_id) & df_orders.fst_upd_id.le(auc_am_f_max_id), "fst_upd_id"] \
        = auc_am_f_min_id
    df_orders.loc[df_orders.fst_upd_id.ge(auc_pm_f_min_id) & df_orders.fst_upd_id.le(auc_pm_f_max_id), "fst_upd_id"] \
        = auc_pm_f_min_id


def fill_next_fill(df_orders, df_trans):
    """
    找到委托的下一个成交价
    :param df_orders:
    :param df_trans:
    :return:
    """
    df_fill = df_trans.loc[df_trans.ExecType.eq("F"), ["EventID", "Price"]]
    df_fill["EID_next_fill"] = df_fill["EventID"]
    df_orders = pd.merge_asof(
        left=df_orders,
        right=df_fill,
        on="EventID",
        suffixes=("", "_next_fill"),
        direction="forward",
    )
    df_orders["EID_next_fill"] = df_orders["EID_next_fill"].fillna(-1).astype(int)
    df_orders["Price_next_fill"] = df_orders.Price_next_fill.fillna(-1)
    assert all((df_orders["OrderNO"] < df_orders["EID_next_fill"]) | (df_orders["EID_next_fill"] == -1))
    return df_orders


def assert_price_cage(df_orders, df_trans):
    """
    断言不存在价格笼子, 委托价>下一次成交价却没有成交,说明存在价格笼子
    :param df_orders:
    :param df_trans:
    :return:
    """
    df_orders_tmp = df_orders.copy()
    fill_fst_upd_id(df_orders_tmp, df_trans)
    df_orders_tmp = fill_next_fill(df_orders_tmp, df_trans)
    cond_b = df_orders_tmp.Side.eq("B") & df_orders_tmp.Price.gt(df_orders_tmp.Price_next_fill)
    cond_s = df_orders_tmp.Side.eq("S") & df_orders_tmp.Price.lt(df_orders_tmp.Price_next_fill)
    cond_ord = (df_orders_tmp.OrdType.isin(["A", "L"]) & df_orders_tmp.EID_next_fill.gt(0) &
                df_orders_tmp.fst_upd_id.gt(df_orders_tmp.EID_next_fill) & (cond_b | cond_s))
    df_orders_tmp["is_price_cage"] = np.where(cond_ord, True, False)
    df_price_cage = df_orders_tmp[df_orders_tmp["is_price_cage"]]
    assert df_price_cage.empty


def assert_mkt_ord(df_orders, df_trans):
    """
    检查深交所市价单
    :param df_orders:
    :param df_trans:
    :return:
    """
    df_orders_tmp = df_orders[["EventID", "OrdType", "OrderNO", "Qty"]]
    df_trans_tmp = df_trans[["EventID", "ExecType", "BidOrder", "AskOrder", "Qty"]]

    df_concat = pd.concat([df_orders_tmp, df_trans_tmp])
    df_concat = df_concat.sort_values("EventID")
    df_concat["next_eid"] = df_concat["EventID"].shift(-1)
    df_concat["next_bid_order"] = df_concat["BidOrder"].shift(-1)
    df_concat["next_ask_order"] = df_concat["AskOrder"].shift(-1)
    df_concat["next_exec_type"] = df_concat["ExecType"].shift(-1)
    df_m = df_concat[df_concat.OrdType.eq("M")]  # 注意 此处的M已经剔除了AD

    assert all(df_m["next_eid"] == df_m["EventID"] + 1)
    assert set(df_m["next_exec_type"]) <= {"F"}
    assert all(df_m.apply(lambda x: x["OrderNO"] in {x["next_bid_order"], x["next_ask_order"]}, axis=1))
    mkt_to_cancel_nos = set(df_m.OrderNO.astype(int)) & set(
        df_trans[df_trans.ExecType.eq("D")][["BidOrder", "AskOrder"]].max(axis=1))
    if len(mkt_to_cancel_nos) > 0:
        logger.info(f"市转撤委托:{mkt_to_cancel_nos}")
    mkt_order_no_set = set(df_m.OrderNO)
    df_trans_tmp["OrderNO"] = df_trans_tmp[["BidOrder", "AskOrder"]].max(axis=1)
    df_trans_mkt = df_trans_tmp[df_trans_tmp.OrderNO.isin(mkt_order_no_set)]
    df_m["filled_vol"] = df_m.OrderNO.map(df_trans_mkt.groupby("OrderNO").Qty.sum())
    # assert all(df_m.Qty == df_m.filled_vol), "市价单全部主动成交"
    mkt_to_limit_nos = set(df_m[df_m.Qty != df_m.filled_vol].OrderNO)
    if len(mkt_to_limit_nos) > 0:
        logger.info(f"市转限委托:{mkt_to_limit_nos}")


def assert_type_u(df_orders, df_trans):
    """
    检查深交所 本方最优 委托
    :param df_orders:
    :param df_trans:
    :return:
    """
    df_orders_u = df_orders[df_orders.OrdType.eq("U")]
    if not df_orders_u.empty:
        df_trans_u = df_trans.loc[
            df_trans.AskOrder.isin(set(df_orders_u.OrderNO)) | df_trans.BidOrder.isin(set(df_orders_u.OrderNO))]
        assert set(df_trans_u.ExecType) <= {"D", "F"}
        df_trans_u_f = df_trans_u.loc[df_trans_u.ExecType.eq("F")]
        if not df_trans_u_f.empty:
            assert set(df_trans_u_f[["BidOrder", "AskOrder"]].min(axis=1)) <= set(df_orders_u.OrderNO), "Type U 以被动单成交"


def fill_auto_cxl(df_orders, df_trans):
    """
    找到立即撤掉的委托
    :param df_orders:
    :param df_trans:
    :return:
    """
    df_cxl = df_trans[df_trans.ExecType.eq("D")]
    df_cxl["OrderNO"] = df_cxl[["BidOrder", "AskOrder"]].max(axis=1)
    ad_order_nos = set(df_cxl[df_cxl.EventID.sub(df_cxl.OrderNO).eq(1)].OrderNO)
    df_orders.loc[df_orders.OrderNO.isin(ad_order_nos), "OrdType"] = "AD"


def fill_mkt_ord_pri(df_orders, df_trans):
    """
    填充深交所市价委托的价格
    :param df_orders:
    :param df_trans:
    :return:
    """
    zero_order = df_orders[df_orders["Price"] == 0]
    assert set(zero_order["OrdType"]) <= {"M", "U", "AD"}, "非市价单价格不为0"
    df_trans_bid = df_trans[df_trans["BidOrder"].ne(0) & df_trans.ExecType.ne("D")]
    df_trans_bid = df_trans_bid.groupby("BidOrder").Price.max()
    df_trans_ask = df_trans[df_trans["AskOrder"].ne(0) & df_trans.ExecType.ne("D")]
    df_trans_ask = df_trans_ask.groupby("AskOrder").Price.min()
    price_map = pd.concat([df_trans_bid, df_trans_ask])
    cond_mkt_ord = df_orders["OrdType"].isin(["M", "U"])
    df_orders.loc[cond_mkt_ord, "Price"] = df_orders.loc[cond_mkt_ord, "OrderNO"].map(price_map)
    # df_orders[df_orders.Price.isnull()]
    df_orders.Price.fillna(0, inplace=True)
    zero_order = df_orders[df_orders["Price"] == 0]
    assert set(zero_order["OrdType"]) <= {"U", "AD"}, "市价单应该有成交价"


def get_cxl(df_trans, df_orders):
    """
    得到深交所撤单数据
    :param df_trans:
    :param df_orders:
    :return:
    """
    df_cxl = df_trans[df_trans.ExecType.eq("D")]
    df_cxl["OrderNO"] = df_cxl[["BidOrder", "AskOrder"]].max(axis=1)
    df_cxl["OrdType"] = df_cxl.OrderNO.map(df_orders.set_index("OrderNO")["OrdType"])
    df_cxl["Side"] = np.where(df_cxl.BidOrder.eq(0), "S", "B")
    assert set(df_cxl["OrdType"]) <= {"A", "M", "L", "U", "AD"}
    assert set(df_cxl["Price"]) == {0}
    return df_cxl


def fill_left_qty(df_orders, df_fill_b, df_fill_s, df_cxl):
    """
    添加字段 未成交数量
    :param df_orders:
    :param df_fill_b:
    :param df_fill_s:
    :param df_cxl:
    :return:
    """
    df_orders["LeftQty"] = df_orders["Qty"]
    df_cxl["LeftQty"] = -df_cxl["Qty"]
    df_fill_b["LeftQty"] = -df_fill_b["Qty"]
    df_fill_s["LeftQty"] = -df_fill_s["Qty"]


def get_message_book_sz(df_orders, df_trans):
    """
    得到深交所全天的message_book
    :param df_orders:
    :param df_trans:
    :return:
    """
    df_orders["EventID"] = df_orders["OrderNO"]
    fill_auto_cxl(df_orders, df_trans)
    assert_price_cage(df_orders, df_trans)
    assert_mkt_ord(df_orders, df_trans)
    assert_type_u(df_orders, df_trans)
    fill_mkt_ord_pri(df_orders, df_trans)
    df_cxl = get_cxl(df_trans, df_orders)
    df_fill_b, df_fill_s = get_fill_buy_sell(df_trans, df_orders)
    fill_left_qty(df_orders, df_fill_b, df_fill_s, df_cxl)
    l_msg = ["EventID", "ExecType", "OrderNO", "OrdType", "Side", "Price", "Qty", "LeftQty", "Time", "TradPhase"]
    df_orders["ExecType"] = "A"
    df_msg_book = pd.concat([
        df_orders[l_msg],
        df_cxl[l_msg],
        df_fill_b[l_msg], df_fill_s[l_msg],
    ]).sort_values(by=["EventID", "OrderNO"], ascending=[True, True]).reset_index(drop=True)
    assert len(df_msg_book.drop_duplicates(subset=["EventID", "OrderNO"], keep=False)) == len(df_msg_book), "主键重复"
    df_msg_book["LeftQty"] = df_msg_book.groupby("OrderNO").LeftQty.cumsum()
    assert all(df_msg_book["LeftQty"] >= 0)
    return df_msg_book[l_msg].reset_index(drop=True)
