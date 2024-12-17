from pydantic import BaseModel
from typing import Optional, List
import algo.const.const_var as const_var
from algo.model.tick import Tick


class LevelPair(BaseModel):
    price: Optional[float] = 0.0
    volume: Optional[int] = 0


class OrderBook(object):
    def __init__(self, tick: Optional[Tick]):
        self.asks: Optional[List[LevelPair]] = []
        self.bids: Optional[List[LevelPair]] = []
        self.gen_order_book(tick)

    def gen_order_book(self, tick: Optional[Tick]):
        self.asks.append(
            LevelPair(price=tick.ask_price1 / const_var.ten_thousand, volume=tick.ask_volume1 * const_var.one_hundred))
        self.asks.append(
            LevelPair(price=tick.ask_price2 / const_var.ten_thousand, volume=tick.ask_volume2 * const_var.one_hundred))
        self.asks.append(
            LevelPair(price=tick.ask_price3 / const_var.ten_thousand, volume=tick.ask_volume3 * const_var.one_hundred))
        self.asks.append(
            LevelPair(price=tick.ask_price4 / const_var.ten_thousand, volume=tick.ask_volume4 * const_var.one_hundred))
        self.asks.append(
            LevelPair(price=tick.ask_price5 / const_var.ten_thousand, volume=tick.ask_volume5 * const_var.one_hundred))
        self.asks.append(
            LevelPair(price=tick.ask_price6 / const_var.ten_thousand, volume=tick.ask_volume6 * const_var.one_hundred))
        self.asks.append(
            LevelPair(price=tick.ask_price7 / const_var.ten_thousand, volume=tick.ask_volume7 * const_var.one_hundred))
        self.asks.append(
            LevelPair(price=tick.ask_price8 / const_var.ten_thousand, volume=tick.ask_volume8 * const_var.one_hundred))
        self.asks.append(
            LevelPair(price=tick.ask_price9 / const_var.ten_thousand, volume=tick.ask_volume9 * const_var.one_hundred))
        self.asks.append(
            LevelPair(price=tick.ask_price10 / const_var.ten_thousand,
                      volume=tick.ask_volume10 * const_var.one_hundred))
        self.bids.append(
            LevelPair(price=tick.bid_price1 / const_var.ten_thousand, volume=tick.bid_volume1 * const_var.one_hundred))
        self.bids.append(
            LevelPair(price=tick.bid_price2 / const_var.ten_thousand, volume=tick.bid_volume2 * const_var.one_hundred))
        self.bids.append(
            LevelPair(price=tick.bid_price3 / const_var.ten_thousand, volume=tick.bid_volume3 * const_var.one_hundred))
        self.bids.append(
            LevelPair(price=tick.bid_price4 / const_var.ten_thousand, volume=tick.bid_volume4 * const_var.one_hundred))
        self.bids.append(
            LevelPair(price=tick.bid_price5 / const_var.ten_thousand, volume=tick.bid_volume5 * const_var.one_hundred))
        self.bids.append(
            LevelPair(price=tick.bid_price6 / const_var.ten_thousand, volume=tick.bid_volume6 * const_var.one_hundred))
        self.bids.append(
            LevelPair(price=tick.bid_price7 / const_var.ten_thousand, volume=tick.bid_volume7 * const_var.one_hundred))
        self.bids.append(
            LevelPair(price=tick.bid_price8 / const_var.ten_thousand, volume=tick.bid_volume8 * const_var.one_hundred))
        self.bids.append(
            LevelPair(price=tick.bid_price9 / const_var.ten_thousand, volume=tick.bid_volume9 * const_var.one_hundred))
        self.bids.append(
            LevelPair(price=tick.bid_price10 / const_var.ten_thousand,
                      volume=tick.bid_volume10 * const_var.one_hundred))
