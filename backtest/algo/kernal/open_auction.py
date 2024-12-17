import algo.const.const_var as const_var
from algo.model.tick import Tick
from typing import Optional, List
from algo.model.parent_order import ParentOrder
import algo.utils.tools as utils
import algo.model.model as model
from algo.model.instruction import Instruction, InstructionOperate
from algo.model.sub_order import SubOrder
import datetime


class OpenAuction(object):
    def __init__(self):
        self.name = const_var.module_open_auction
        self.p_order: Optional[ParentOrder] = None
        self.direction: Optional[int] = 0
        self.limit_up: Optional[float] = 0.0
        self.limit_down: Optional[float] = 0.0
        self.req_params: Optional[model.RequestParams] = None

    def init_data(self, tick: Optional[Tick], p_order: Optional[ParentOrder],
                  req_params: Optional[model.RequestParams]):
        self.p_order = p_order
        self.direction = p_order.get_direction()
        self.limit_up: Optional[float] = utils.get_limit_price(p_order.symbol, tick.pre_close / const_var.ten_thousand,
                                                               1)
        self.limit_down: Optional[float] = utils.get_limit_price(p_order.symbol,
                                                                 tick.pre_close / const_var.ten_thousand, -1)
        self.req_params: Optional[model.RequestParams] = req_params

    def on_data(self, tick: Optional[Tick], p_order: Optional[ParentOrder],
                req_params: Optional[model.RequestParams]) -> List[Instruction]:
        self.init_data(tick, p_order, req_params)
        price = self.get_price(tick)
        if price <= 0:
            return []
        qty = self.get_qty(tick)
        qty = min(self.p_order.remain_vol(), qty)
        qty = int(qty / 100) * 100
        if qty >= utils.get_lots(self.p_order.symbol):
            s = SubOrder()
            s.req_time = datetime.datetime.now()
            s.price = price
            s.qty = qty
            s.remark = self.name
            s.insert_time = tick.time
            s.latest_update_time = tick.time
            return [Instruction(s, InstructionOperate.trade)]
        return []

    def get_price(self, tick: Optional[Tick]):
        mkt_px = tick.ask_price1 / const_var.ten_thousand  # ask_price = bid_price
        price = mkt_px * (1 + self.direction * 0.02)
        limit_price = self.limit_up if self.direction == 1 else self.limit_down
        if self.direction * price - self.direction * limit_price > -0.01:
            return -1
        return round(price, 2)

    def get_qty(self, tick: Optional[Tick]):
        mkt_qty = tick.ask_volume1  # ask_volume = bid_volume
        qty = self.req_params.open_auction_vol * mkt_qty
        return int(qty)
