from pydantic import BaseModel
from typing import Optional
from enum import Enum


class AlgoTradingParams(BaseModel):
    name: Optional[str] = ""
    cluster_id: Optional[int] = 0
    buffering_avg_lvl_num: Optional[int] = 0  # 把n档盘口看做一层
    # vol_sum/pre_vol_sum 的对照百分比 盘口放量增速指标使用
    increased_pct: Optional[float] = 0.0  # vol_sum增加多少
    max_slice1_num: Optional[int] = 0  # slice1的数量上限 这一层允许挂单手数
    max_slice_num_in_lvl: Optional[int] = 0  # 一个level slice的数量上限 上一次允许存留的挂单手数
    max_slice_num: Optional[int] = 0
    max_lvl_num: Optional[int] = 0  # 最多观测几档
    looking_depth: Optional[int] = 0  # 日内用的


class StockBasicData(BaseModel):
    symbol: Optional[str] = ""
    work_date: Optional[str] = ""
    # 默认取最近21个交易日的中位数
    ask_size: Optional[float] = 0.0
    bid_size: Optional[float] = 0.0
    # (ask_size + bid_size)/2
    quote_size: Optional[float] = 0.0
    spread: Optional[float] = 0.0
    open_auction_vol: Optional[int] = 0
    close_auction_vol: Optional[int] = 0
    mdv: Optional[int] = 0
    tick_period: Optional[float] = 0.0
    trade_period: Optional[float] = 0.0
    trade_size: Optional[float] = 0.0
    turnover_period: Optional[float] = 0.0
    # 历史波动率 最近60日收盘价
    volatility: Optional[float] = 0.0
    pre_close_price: Optional[float] = 0.0
    quote_thickness: Optional[float] = 0.0
    sparsity1: Optional[float] = 0.0
    sparsity2: Optional[float] = 0.0
    quote_size5: Optional[float] = 0.0
    quote_size10: Optional[float] = 0.0
    cluster: Optional[int] = 0


class RequestParams(BaseModel):
    end_time: Optional[int] = 0
    max_pov: Optional[float] = 0.0
    max_vol: Optional[float] = 0.0
    min_vol: Optional[float] = 0.0
    catchup_vol: Optional[float] = 0.0
    catchup_trigger: Optional[float] = 0.0
    post_1lot_magnifier: Optional[float] = 0.0


class MarketSide(Enum):
    buy = 1
    sell = 2


class OrderType(Enum):
    market: Optional[int] = 0
    limit: Optional[int] = 1


class OrderAction(Enum):
    unknown = 0
    buy_to_close = 1
    buy_to_open = 2
    sell_to_close = 3
    sell_to_open = 4


class StockCluster(Enum):
    unknown = 0
    ultrafast = 1
    sparse = 2
    slow = 3
    normal = 4
    fast = 5
