from typing import Optional, List
from algo.model.tick import Tick
import algo.utils.tools as utils
import algo.model.model as model
from algo.model.parent_order import ParentOrder
import algo.const.const_var as const_var
from algo.model.instruction import Instruction, InstructionOperate
from algo.manager.slice_manager import SliceManager
from algo.model.sub_order import SubOrder
import datetime


class Catchup(object):
    def __init__(self, tick: Optional[Tick], req_params: Optional[model.RequestParams], p_order: Optional[ParentOrder],
                 stk_basic_data: Optional[model.StockBasicData]):
        self.arrival_px: Optional[float] = utils.get_middle_price(tick)
        self.arrival_vol: Optional[int] = tick.acc_volume
        self.vol: Optional[float] = req_params.catchup_vol
        self.trigger: Optional[float] = req_params.catchup_trigger
        self.p_order: Optional[ParentOrder] = p_order
        self.direction: Optional[float] = p_order.get_direction()
        self.filled_vol_before_open: Optional[int] = 0
        self.name = const_var.module_catchup
        self.cluster = stk_basic_data.cluster

    def on_data(self, tick: Optional[Tick], slc_mgr: Optional[SliceManager]) -> Optional[List[Instruction]]:
        ins_list = []
        if self.vol <= 0:
            return []
        if self.trigger <= 0:
            return []
        near_price = utils.get_near_price(tick, self.direction)
        if near_price <= 0:
            return []
        if near_price * self.direction < self.arrival_px * self.direction:
            return []
        slices = slc_mgr.get_unfilled_slices_by_module(self.name)
        for sub_order in slices:
            if near_price * self.direction >= sub_order.price * self.direction:
                ins = Instruction(sub_order, InstructionOperate.cancel)
                ins_list.append(ins)
        if len(slices) > 0 or len(ins_list) > 0:
            return ins_list
        filled_qty = slc_mgr.filled
        # hexing 占比<目标
        if (filled_qty - self.filled_vol_before_open) / (tick.acc_volume - self.arrival_vol) < self.trigger:
            trade_price = self.get_trade_price(tick)
            trade_qty = self.get_trade_qty(tick, filled_qty)
            remain_qty = self.p_order.input_vol - slc_mgr.qty
            qty = min(remain_qty, trade_qty)
            qty = int(qty/100)*100
            if qty >= utils.get_lots(self.p_order.symbol):
                s = SubOrder()
                s.req_time = datetime.datetime.now()
                s.price = trade_price
                s.qty = qty
                s.remark = self.name
                ins = Instruction(s, InstructionOperate.trade)
                ins_list.append(ins)
        return ins_list

    def get_trade_price(self, tick: Optional[Tick]) -> Optional[float]:
        far_price = utils.get_far_price(tick, self.direction)
        t = 0.02
        if self.cluster == 3:
            t = 0
        return far_price + self.direction * t

    def get_trade_qty(self, tick: Optional[Tick], filled_qty: Optional[int]):
        qty = (tick.acc_volume - self.arrival_vol) * self.vol - filled_qty
        qty = utils.round_by_prob(self.p_order.symbol, qty, 1)
        return qty
