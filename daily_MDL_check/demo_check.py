# 上交所和深交所的委托和成交的BizIndex是不相交的
# 深交所的OrderID一定包含成交里的BuyOrderID和SellOrderID
# 同一个通道的bizindex是从1开始的连续序列
# 成交量不得大于委托量
# 撤单一定能找到对应的委托，并且撤单量加成交量等于委托量
# 买方成交价格不高于委托价格，卖方成交价格不低于委托价格
import pandas as pd
import itertools

from datetime import datetime




def channelno_run_sz(order,trader):
    for i in order['ChannelNo'].sort_values().unique():
        sz_order_BO = order[order['ChannelNo']==i]
        sz_trader_BO = trader[trader['ChannelNo']==i]
        check_Bid_Offer_continuity(sz_order_BO,sz_trader_BO,i)

        sz_order_BizIndex = sz_order_BO['ApplSeqNum'].sort_values().unique()
        sz_trader_BizIndex = sz_trader_BO['ApplSeqNum'].sort_values().unique()
        missing_indices = check_BizIndex_continuity(sz_order_BizIndex, sz_trader_BizIndex)
        print(f"{i}缺失的OrderIndex:", missing_indices)
        print(len(missing_indices))


def check_BizIndex_continuity(order,trader):
    # 获取BizIndex列
    max_order_BizIndex = max(order)
    max_trader_BizIndex = max(trader)
    set_order = set(order)
    set_trader = set(trader)
    # 获取两个最大值中的较大值
    max_BizIndex = max(max_order_BizIndex, max_trader_BizIndex)
    # 生成完整的BizIndex序列
    expected_order_indices = range(1,max_BizIndex + 1)
    # 找出缺失的BizIndex
    missing_order_indices = set(expected_order_indices) - set_order-set_trader

    # 检查委托和成交数据的 BizIndex 是否不相交
    is_disjoint = set_order.intersection(set_trader)
    # 打印结果
    if is_disjoint:
        print("相交的 BizIndex 值:", is_disjoint)
    else:
        print("委托和成交数据的 BizIndex 不相交")
    return missing_order_indices


def check_Bid_Offer_continuity(df_order,df_trade,channelno):
    # 获取逐笔委托数据中的 ApplSeqNum 集合
    appl_seq_nums = set(df_order['ApplSeqNum'])
    df_trade = df_trade[(df_trade['BidApplSeqNum'] != 0) & (df_trade['OfferApplSeqNum'] != 0)]
    # 检查逐笔成交数据中的 BidApplSeqNum 和 OfferApplSeqNum 是否存在于逐笔委托数据中的 ApplSeqNum 集合中
    bid_appl_seq_nums = set(df_trade['BidApplSeqNum'])
    offer_appl_seq_nums = set(df_trade['OfferApplSeqNum'])
    # 找出不在逐笔委托数据中的 ApplSeqNum 中的 BidApplSeqNum 和 OfferApplSeqNum
    missing_bid_appl_seq_nums = bid_appl_seq_nums - appl_seq_nums
    missing_offer_appl_seq_nums = offer_appl_seq_nums - appl_seq_nums

    # 打印结果
    if missing_bid_appl_seq_nums:
        print(f"{channelno}不在逐笔委托数据中的 ApplSeqNum 中的 BidApplSeqNum:", missing_bid_appl_seq_nums)
    else:
        print("所有 BidApplSeqNum 都在逐笔委托数据中的 ApplSeqNum 中")

    if missing_offer_appl_seq_nums:
        print(f"{channelno}不在逐笔委托数据中的 ApplSeqNum 中的 OfferApplSeqNum:", missing_offer_appl_seq_nums)
    else:
        print("所有 OfferApplSeqNum 都在逐笔委托数据中的 ApplSeqNum 中")


def channelno_run_sh(order,trader):
    for i in order['OrderChannel'].sort_values().unique():
        sh_order_BO = order[order['OrderChannel']==i]
        sh_trader_BO = trader[trader['TradeChan']==i]
        sh_order_BizIndex = sh_order_BO['BizIndex'].sort_values().unique()
        sh_trader_BizIndex = sh_trader_BO['BizIndex'].sort_values().unique()
        missing_indices = check_BizIndex_continuity(sh_order_BizIndex, sh_trader_BizIndex)
        print(f"{i}缺失的OrderIndex:", missing_indices)

if __name__ == '__main__':

    # # #上海委托
    # sh_order = pd.read_csv('20231212/mdl_4_19_0.csv')
    # # print(sh_order['OrderChannel'].sort_values().unique())
    # # #上海成交
    # sh_trader = pd.read_csv('20231212/20231212_Transaction.csv', index_col=False)
    # channelno_run_sh(sh_order, sh_trader)
    # 深圳委托
    sz_order_1 = pd.read_csv('20200528/mdl_6_33_0.csv', index_col=False)
    sz_order_2 = pd.read_csv('20200528/mdl_6_33_0_rebuild.csv', index_col=False)
    sz_order = pd.concat([sz_order_1, sz_order_2], ignore_index=True)
    # print(sz_order)
    # #深圳成交
    sz_trader_1 = pd.read_csv('20200528/mdl_6_36_0.csv', index_col=False)
    sz_trader_2 = pd.read_csv('20200528/mdl_6_36_0_rebuild.csv', index_col=False)
    sz_trader = pd.concat([sz_trader_1, sz_trader_1], ignore_index=True)
    # print(sz_trader)

    # 检查并打印缺失的BizIndex
    channelno_run_sz(sz_order,sz_trader)
