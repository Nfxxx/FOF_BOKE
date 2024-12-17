from typing import Optional

import pandas as pd

import algo.const.const_var as const_var
import random
from algo.model.tick import Tick
from algo.model.model import RequestParams


def get_market(symbol: Optional[str]) -> Optional[str]:
    if symbol[:3] == "300":
        return const_var.market_ge
    if symbol[:3] == "688":
        return const_var.market_star
    return const_var.market_mb


def get_limit_ret(symbol: Optional[str]) -> Optional[float]:
    market = get_market(symbol)
    if market in (const_var.market_ge, const_var.market_star):
        return 0.2
    return 0.1


def get_limit_price(symbol: Optional[str], pre_close: Optional[float], direction: Optional[int]) -> Optional[float]:
    ret = get_limit_ret(symbol)
    px = pre_close * (1 + ret * direction)
    px = round(px, 2)
    return px


def get_lots(symbol: Optional[str]):
    if get_market(symbol) == const_var.market_star:
        return 200
    return 100


def round_by_prob(symbol: Optional[str], qty: Optional[int], magnifier: Optional[float]):
    lots = get_lots(symbol)
    if qty >= lots:
        return int(qty / 100) * 100
    if random.random() < (qty / lots * magnifier):
        return lots
    return 0


def get_middle_price(tick: Optional[Tick]) -> Optional[float]:
    if tick.ask_price1 == 0:
        return tick.bid_price1 / const_var.ten_thousand
    if tick.bid_price1 == 0:
        return tick.ask_price1 / const_var.ten_thousand
    return (tick.ask_price1 + tick.bid_price1) / 20000


def get_near_price(tick: Optional[Tick], direction: Optional[int]) -> Optional[float]:
    if direction == 1:
        return tick.bid_price1 / const_var.ten_thousand
    return tick.ask_price1 / const_var.ten_thousand


def get_far_price(tick: Optional[Tick], direction: Optional[int]) -> Optional[float]:
    if direction == 1:
        return tick.ask_price1 / const_var.ten_thousand
    return tick.bid_price1 / const_var.ten_thousand


def parse_algo_params(params: Optional[str]) -> RequestParams:
    if params == "":
        raise Exception("params is empty")
    req_params_dict = eval(params)
    req_params = RequestParams()
    req_params.end_time = int(req_params_dict["endtime"])
    req_params.max_pov = req_params_dict["maxpov"]
    req_params.min_vol = req_params_dict["minvol"]
    req_params.max_vol = req_params_dict["maxvol"]
    req_params.catchup_vol = req_params_dict["catchupvol"]
    req_params.catchup_trigger = req_params_dict["catchuptrigger"]
    req_params.post_1lot_magnifier = req_params_dict["post_1lot_magnifier"]
    if req_params.min_vol > req_params.max_vol:
        raise Exception("invalid params, minvol > maxvol")
    if req_params.min_vol <= 0 or req_params.min_vol > 0.5:
        raise Exception("invalid params")
    if req_params.max_vol <= 0 or req_params.max_vol > 0.5:
        raise Exception("invalid params")
    if req_params.catchup_trigger <= 0 or req_params.catchup_trigger > 0.5:
        raise Exception("invalid params")
    if req_params.catchup_vol <= 0 or req_params.catchup_vol > 0.5:
        raise Exception("invalid params")
    return req_params


def format_symbol(symbol: Optional[str]) -> Optional[str]:
    assert symbol[0] in {"0", "3", "6"}
    if symbol.startswith("6"):
        if not symbol.endswith("SH"):
            symbol = symbol + ".SH"
    else:
        if not symbol.endswith("SZ"):
            symbol = symbol + ".SZ"
    return symbol


def covert_ser_to_tick(tick_ser: Optional[pd.Series]) -> Optional[Tick]:
    tick = Tick()
    tick.code = str(tick_ser.Code)
    tick.date = int(tick_ser.Date)
    tick.time = int(tick_ser.Time)
    tick.price = int(tick_ser.Price * const_var.ten_thousand)
    tick.volume = int(tick_ser.Volume)
    tick.turnover = int(tick_ser.Turover)
    tick.match_items = int(tick_ser.MatchItems)
    tick.interest = int(tick_ser.Interest)
    tick.trade_flag = str(tick_ser.TradeFlag)
    tick.bs_flag = str(tick_ser.BSFlag)
    tick.acc_volume = int(tick_ser.AccVolume)
    tick.acc_turnover = int(tick_ser.AccTurover)
    tick.high = int(tick_ser.High * const_var.ten_thousand)
    tick.low = int(tick_ser.Low * const_var.ten_thousand)
    tick.open = int(tick_ser.Open * const_var.ten_thousand)
    tick.pre_close = int(tick_ser.PreClose * const_var.ten_thousand)
    tick.ask_avg_price = int(tick_ser.AskAvPrice * const_var.ten_thousand)
    tick.bid_avg_price = int(tick_ser.BidAvPrice * const_var.ten_thousand)
    tick.total_ask_volume = int(tick_ser.TotalAskVolume)
    tick.total_bid_volume = int(tick_ser.TotalBidVolume)
    tick.ask_price1 = int(tick_ser.AskPrice1 * const_var.ten_thousand)
    tick.ask_price2 = int(tick_ser.AskPrice2 * const_var.ten_thousand)
    tick.ask_price3 = int(tick_ser.AskPrice3 * const_var.ten_thousand)
    tick.ask_price4 = int(tick_ser.AskPrice4 * const_var.ten_thousand)
    tick.ask_price5 = int(tick_ser.AskPrice5 * const_var.ten_thousand)
    tick.ask_price6 = int(tick_ser.AskPrice6 * const_var.ten_thousand)
    tick.ask_price7 = int(tick_ser.AskPrice7 * const_var.ten_thousand)
    tick.ask_price8 = int(tick_ser.AskPrice8 * const_var.ten_thousand)
    tick.ask_price9 = int(tick_ser.AskPrice9 * const_var.ten_thousand)
    tick.ask_price10 = int(tick_ser.AskPrice10 * const_var.ten_thousand)
    tick.ask_volume1 = int(tick_ser.AskVolume1)
    tick.ask_volume2 = int(tick_ser.AskVolume2)
    tick.ask_volume3 = int(tick_ser.AskVolume3)
    tick.ask_volume4 = int(tick_ser.AskVolume4)
    tick.ask_volume5 = int(tick_ser.AskVolume5)
    tick.ask_volume6 = int(tick_ser.AskVolume6)
    tick.ask_volume7 = int(tick_ser.AskVolume7)
    tick.ask_volume8 = int(tick_ser.AskVolume8)
    tick.ask_volume9 = int(tick_ser.AskVolume9)
    tick.ask_volume10 = int(tick_ser.AskVolume10)
    tick.bid_price1 = int(tick_ser.BidPrice1 * const_var.ten_thousand)
    tick.bid_price2 = int(tick_ser.BidPrice2 * const_var.ten_thousand)
    tick.bid_price3 = int(tick_ser.BidPrice3 * const_var.ten_thousand)
    tick.bid_price4 = int(tick_ser.BidPrice4 * const_var.ten_thousand)
    tick.bid_price5 = int(tick_ser.BidPrice5 * const_var.ten_thousand)
    tick.bid_price6 = int(tick_ser.BidPrice6 * const_var.ten_thousand)
    tick.bid_price7 = int(tick_ser.BidPrice7 * const_var.ten_thousand)
    tick.bid_price8 = int(tick_ser.BidPrice8 * const_var.ten_thousand)
    tick.bid_price9 = int(tick_ser.BidPrice9 * const_var.ten_thousand)
    tick.bid_price10 = int(tick_ser.BidPrice10 * const_var.ten_thousand)
    tick.bid_volume1 = int(tick_ser.BidVolume1)
    tick.bid_volume2 = int(tick_ser.BidVolume2)
    tick.bid_volume3 = int(tick_ser.BidVolume3)
    tick.bid_volume4 = int(tick_ser.BidVolume4)
    tick.bid_volume5 = int(tick_ser.BidVolume5)
    tick.bid_volume6 = int(tick_ser.BidVolume6)
    tick.bid_volume7 = int(tick_ser.BidVolume7)
    tick.bid_volume8 = int(tick_ser.BidVolume8)
    tick.bid_volume9 = int(tick_ser.BidVolume9)
    tick.bid_volume10 = int(tick_ser.BidVolume10)
    return tick
