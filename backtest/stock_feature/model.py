from pydantic import BaseModel
from typing import Optional
import datetime


class StockTickFeature(BaseModel):
    symbol: Optional[str]
    trade_date: Optional[float] = 0.0
    # 卖一平均值
    ask_size: Optional[float] = 0.0
    # 买一平均值
    bid_size: Optional[float] = 0.0
    # 买一卖一平均值
    quote_size: Optional[float] = 0.0
    # 买一卖一平均价差 (askprice1 - bidprice1)/bidprice1
    spread: Optional[float] = 0.0
    # 平均开盘集合竞价数量
    open_auction_vol: Optional[int] = 0
    # 平均收盘集合竞价数量
    close_auction_vol: Optional[int] = 0
    # 日成交量
    mdv: Optional[int] = 0
    # middle_price变动时间均值
    tick_period: Optional[int] = 0
    # 昨日收盘价
    pre_close_price: Optional[float] = 0.0
    # 5档价格稀疏度 (askprice5-bidprice5)/0.09 然后求均值
    sparsity1: Optional[float] = 0.0
    # 10档价格稀疏度 (askprice10-bidprice10)/0.19 然后求均值
    sparsity2: Optional[float] = 0.0
    # 5档平均报单数量 quote_size5 = sum(askvolume1...askvolume5...bidvolume1...bidvolume5)/10
    quote_size5: Optional[float] = 0.0
    # 10档平均报单数量 quote_size5 = sum(askvolume1...askvolume10...bidvolume1...bidvolume10)/20
    quote_size10: Optional[float] = 0.0
    # 日均单笔成交间隔, 每笔成交发生时间求均值
    trade_period: Optional[float] = 0.0
    # 日均单笔成交量 每次成家量求均值
    trade_size: Optional[float] = 0.0


class StockFeatureSummary(BaseModel):
    symbol: Optional[str]
    trade_date: Optional[float] = 0.0
    # 默认取最近21个交易日的中位数
    ask_size: Optional[float] = 0.0
    bid_size: Optional[float] = 0.0
    # (ask_size + bid_size)/2
    quote_size: Optional[float] = 0.0
    open_auction_vol: Optional[int] = 0
    close_auction_vol: Optional[int] = 0
    mdv: Optional[int] = 0
    tick_period: Optional[float] = 0.0
    trade_period: Optional[float] = 0.0
    trade_size: Optional[float] = 0.0
    # 历史波动率 最近60日收盘价
    volatility: Optional[float] = 0.0
    turnover_period: Optional[float] = 0.0
    pre_close_price: Optional[float] = 0.0
    quote_thickness: Optional[float] = 0.0
    sparsity1: Optional[float] = 0.0
    sparsity2: Optional[float] = 0.0
    quote_size5: Optional[float] = 0.0
    quote_size10: Optional[float] = 0.0
    cluster: Optional[int] = 0
