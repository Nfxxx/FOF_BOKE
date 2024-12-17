import algo.const.const_var as const_var
from typing import Optional, List
from algo.manager.slice_manager import SliceManager
from algo.model.tick import Tick
from algo.model.instruction import Instruction, InstructionOperate
import algo.model.model as model
from algo.model.parent_order import ParentOrder
import algo.utils.tools as utils
from algo.model.sub_order import SubOrder
import datetime


class BeforeOpen(object):
    def __init__(self):
        self.name = const_var.module_before_open
        self.p_order: Optional[ParentOrder] = None
        self.direction: Optional[int] = 0
        self.req_params: Optional[model.RequestParams] = None

    def init_data(self, p_order: Optional[ParentOrder], req_params: Optional[model.RequestParams]):
        self.p_order: Optional[ParentOrder] = p_order
        self.direction: Optional[int] = p_order.get_direction()
        self.req_params: Optional[model.RequestParams] = req_params

    def on_data(self, tick: Optional[Tick], p_order: Optional[ParentOrder], req_params: Optional[model.RequestParams],
                slc_mgr: SliceManager) -> List[Instruction]:
        self.init_data(p_order, req_params)
        ins_list = []
        slices = slc_mgr.get_unfilled_slices()
        if len(slices) > 0:
            for s in slices:
                ins_list.append(Instruction(s, InstructionOperate.cancel))
            return ins_list
        if tick.open == 0:
            return []
        open_px = tick.open / const_var.ten_thousand
        vol = tick.bid_volume1 if self.direction == 1 else tick.ask_volume1
        vol = int(vol * (self.req_params.min_vol + self.req_params.max_vol) / 2)
        vol = min(vol, self.p_order.remain_vol())
        vol = int(vol / 100) * 100
        if vol >= utils.get_lots(self.p_order.symbol):
            s = SubOrder()
            s.req_time = datetime.datetime.now()
            s.price = open_px
            s.qty = vol
            s.remark = self.name
            s.insert_time = tick.time
            s.latest_update_time = tick.time
            ins_list.append(Instruction(s, InstructionOperate.trade))
            return ins_list
        return []
