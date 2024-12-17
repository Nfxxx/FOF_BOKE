import datetime
from typing import Optional, List
import algo.model.model as model
from algo.model.parent_order import ParentOrder
import algo.const.const_var as const_var
from algo.model.tick import Tick
from algo.model.instruction import Instruction, InstructionOperate
from logger import logger
import algo.utils.tools as utils
from algo.model.order_book import OrderBook, LevelPair
from algo.manager.slice_manager import SliceManager
from algo.model.sub_order import SubOrder
from algo.kernal.base_algo import BaseAlgo


class CornerStone(BaseAlgo):
    def __init__(self, tick: Optional[Tick], symbol: Optional[str], p_order: Optional[ParentOrder],
                 stk_basic_data: Optional[model.StockBasicData], trading_params: Optional[model.AlgoTradingParams],
                 req_params: Optional[model.RequestParams]):
        self.limit_up: Optional[float] = utils.get_limit_price(symbol, tick.pre_close / const_var.ten_thousand, 1)
        self.limit_down: Optional[float] = utils.get_limit_price(symbol, tick.pre_close / const_var.ten_thousand, -1)
        self.trading_params: Optional[model.AlgoTradingParams] = trading_params
        self.stk_basic_data: Optional[model.StockBasicData] = stk_basic_data
        self.req_params: Optional[model.RequestParams] = req_params
        self.p_order: Optional[ParentOrder] = p_order
        self.direction: Optional[int] = self.p_order.get_direction()
        self.name = const_var.module_cornerstone

    def on_data(self, tick: Optional[Tick], slc_mgr: SliceManager) -> List[Instruction]:
        new_slices = []
        if self.is_limit(tick):
            return new_slices
        orderbook = OrderBook(tick)
        if self.p_order.is_buy():
            levels = orderbook.bids
        else:
            levels = orderbook.asks
        for i in range(self.trading_params.max_lvl_num):
            level = levels[i]
            if level.price == 0:
                continue
            if level.volume * 10 < self.stk_basic_data.quote_size:
                continue
            sub_orders = slc_mgr.get_unfilled_slices_by_px(level.price)
            if len(sub_orders) >= self.trading_params.max_slice_num_in_lvl:
                continue
            trade_vol = self.get_trade_vol(sub_orders, level)
            if len(sub_orders) > 0 and trade_vol <= sub_orders[-1].qty:
                continue
            s = SubOrder()
            s.req_time = datetime.datetime.now()
            s.price = level.price
            s.qty = trade_vol
            s.remark = self.name
            if s.qty >= utils.get_lots(self.p_order.symbol):
                new_slices.append(s)
        new_slices.extend(slc_mgr.get_unfilled_slices())
        new_slices.sort(key=lambda x: x.req_time, reverse=True)  # shijian从da dao xiao排序
        new_slices.sort(key=lambda x: x.price * self.direction, reverse=True)  # 从大到小排序
        return self.get_instructions(new_slices, slc_mgr)

    def get_instructions(self, sub_orders: Optional[List[SubOrder]], slc_mgr: SliceManager) -> Optional[List[Instruction]]:
        qty_readable = self.p_order.input_vol - slc_mgr.filled
        cancel = False
        s1_map = {}
        asn = 0
        qty_sum = 0
        first_slice = True
        ins_list = []
        for sub_order in sub_orders:
            if cancel:
                cancel = True
            elif len(s1_map) + 1 > self.trading_params.max_slice1_num:
                cancel = True
            elif asn + 1 > self.trading_params.max_slice_num:
                cancel = True
            elif sub_order.qty > (qty_readable - qty_sum):
                if first_slice:
                    sub_order.qty = min(qty_readable - qty_sum, sub_order.qty)
                    sub_order.qty = int(sub_order.qty / 100) * 100
                    if sub_order.qty < utils.get_lots(self.p_order.symbol):
                        cancel = True
                else:
                    cancel = True
            if cancel:
                if sub_order.send_to_out():
                    ins = Instruction(sub_order, InstructionOperate.cancel)
                    ins_list.append(ins)
            else:
                if not sub_order.send_to_out():
                    ins = Instruction(sub_order, InstructionOperate.trade)
                    ins_list.append(ins)
                s1_map[sub_order.price] = 1
                asn += 1
                qty_sum += sub_order.qty
                first_slice = False
        return ins_list

    def get_trade_vol(self, sub_orders: Optional[List[SubOrder]], lvl: Optional[LevelPair]):
        input_price = self.p_order.input_price
        if self.p_order.group == "momentum":
            x = self.calc_x2(lvl.price, self.direction, input_price, self.stk_basic_data.volatility,
                             self.req_params.min_vol, self.req_params.max_vol)
        else:
            x = self.calc_x(lvl.price, self.direction, input_price, self.stk_basic_data.volatility,
                            self.req_params.min_vol, self.req_params.max_vol)
        posted_qty = sum(sub_order.unfilled_vol() for sub_order in sub_orders)
        all_qty = (lvl.volume * x - posted_qty) / (1 - x)
        qty = all_qty - posted_qty
        qty = qty if qty > 0 else 0
        return utils.round_by_prob(self.p_order.symbol, qty, self.req_params.post_1lot_magnifier)


