import random
from typing import Optional, List
from algo.model.tick import Tick
import algo.model.model as model
from algo.model.parent_order import ParentOrder
import algo.utils.tools as utils
import algo.const.const_var as const_var
from logger import logger
from algo.manager.slice_manager import SliceManager
from algo.model.instruction import InstructionOperate, Instruction
from algo.model.order_book import OrderBook, LevelPair
from algo.kernal.base_algo import BaseAlgo
from algo.model.sub_order import SubOrder
import datetime


class Capstone(BaseAlgo):
    def __init__(self, tick: Optional[Tick], symbol: Optional[str], p_order: Optional[ParentOrder],
                 stk_basic_data: Optional[model.StockBasicData],
                 trading_params: Optional[model.AlgoTradingParams],
                 req_params: Optional[model.RequestParams]):
        self.limit_up: Optional[float] = utils.get_limit_price(symbol, tick.pre_close / const_var.ten_thousand, 1)
        self.limit_down: Optional[float] = utils.get_limit_price(symbol, tick.pre_close / const_var.ten_thousand, -1)
        self.trading_params: Optional[model.AlgoTradingParams] = trading_params
        self.stk_basic_data: Optional[model.StockBasicData] = stk_basic_data
        self.req_params: Optional[model.RequestParams] = req_params
        self.p_order: Optional[ParentOrder] = p_order
        self.direction: Optional[int] = self.p_order.get_direction()
        self.name: Optional[str] = const_var.module_capstone
        self.pre_vol_sum: Optional[int] = 0

    def on_data(self, tick: Optional[Tick], slc_mgr: SliceManager) -> List[Instruction]:
        new_slices = []
        if self.is_limit(tick):
            return new_slices
        orderbook = OrderBook(tick)
        # 同方向看
        if self.p_order.is_buy():
            levels = orderbook.bids
        else:
            levels = orderbook.asks
        if levels[0].price == 0:
            return []
        # 前面x档有没有我未成交的单子
        slices = slc_mgr.get_unfilled_slices()
        has_order = False
        for i in range(self.trading_params.buffering_avg_lvl_num):
            lvl = levels[i]
            s = slc_mgr.get_unfilled_slices_by_px(lvl.price)
            if len(s) > 0:
                has_order = True
                aggressive_level = lvl
                break
        vol_sum = self.vol_sum(levels)
        pre_vol_sum = self.pre_vol_sum if self.pre_vol_sum != 0 else vol_sum
        self.pre_vol_sum = vol_sum
        input_price = self.p_order.input_price
        if self.p_order.group == "momentum":
            x = self.calc_x2(levels[0].price, self.direction, input_price, self.stk_basic_data.volatility,
                             self.req_params.min_vol, self.req_params.max_vol)
        else:
            x = self.calc_x(levels[0].price, self.direction, input_price, self.stk_basic_data.volatility,
                            self.req_params.min_vol, self.req_params.max_vol)
        qty = self.get_trade_qty(x, vol_sum, slices, min(levels[0].price, levels[-1].price),
                                 max(levels[0].price, levels[0].price))
        if has_order:
            if vol_sum / pre_vol_sum >= self.trading_params.increased_pct:
                qty = utils.round_by_prob(self.p_order.symbol, qty, self.req_params.post_1lot_magnifier)
                if qty >= utils.get_lots(self.p_order.symbol):
                    s = SubOrder()
                    s.req_time = datetime.datetime.now()
                    s.price = aggressive_level.price
                    s.qty = qty
                    s.remark = self.name
                    new_slices.append(s)
        else:
            rand_lvl1 = random.randint(0, self.trading_params.buffering_avg_lvl_num - 1 - 1)
            rand_lvl2 = random.randint(0, self.trading_params.buffering_avg_lvl_num - 2 - 1)
            if rand_lvl2 >= rand_lvl1:
                rand_lvl2 += 1
            else:
                rand_lvl1, rand_lvl2 = rand_lvl2, rand_lvl1
            theta = random.randint(0, 400-1) / 1000 + 0.3
            s1 = SubOrder()
            s1.req_time = datetime.datetime.now()
            s1.price = levels[rand_lvl1].price
            s1.qty = utils.round_by_prob(self.p_order.symbol, int(qty * theta), self.req_params.post_1lot_magnifier)
            s1.remark = self.name
            if s1.qty >= utils.get_lots(self.p_order.symbol):
                new_slices.append(s1)
            s2 = SubOrder()
            s2.req_time = datetime.datetime.now()
            s2.price = levels[rand_lvl2].price
            s2.qty = int(int(qty * (1 - theta)) / 100) * 100
            s2.remark = self.name
            if s2.qty >= utils.get_lots(self.p_order.symbol):
                new_slices.append(s2)
        new_slices.extend(slices)
        new_slices.sort(key=lambda sub_order: sub_order.req_time,reverse=True)  # 时间从大到小排序

        new_slices.sort(key=lambda sub_order: sub_order.price * self.direction, reverse=True)  # 从大到小排序
        return self.get_instructions(new_slices, slc_mgr)

    #che dan zhi ling
    def get_instructions(self, sub_orders: Optional[List[SubOrder]], slc_mgr: SliceManager) -> List[Instruction]:
        ins_list = []
        qty_readable = self.p_order.input_vol - slc_mgr.filled
        qty_sum = 0
        count = 0
        for sub_order in sub_orders:
            if not sub_order.send_to_out():
                qty = qty_readable - qty_sum
                qty = utils.round_by_prob(self.p_order.symbol, qty, self.req_params.post_1lot_magnifier)
                if (qty < sub_order.qty) and (qty >= utils.get_lots(self.p_order.symbol)):
                    sub_order.qty = qty
            if (count >= self.trading_params.max_lvl_num) or (qty_sum + sub_order.unfilled_vol()) > qty_readable:
                if sub_order.send_to_out():
                    ins = Instruction(sub_order, InstructionOperate.cancel)
                    ins_list.append(ins)
                continue
            qty_sum += sub_order.qty
            if not sub_order.send_to_out():
                ins = Instruction(sub_order, InstructionOperate.trade)
                ins_list.append(ins)
            count += 1
        return ins_list

    @staticmethod
    def vol_sum(levels: Optional[List[LevelPair]]) -> Optional[float]:
        vol_sum = 0
        for lvl in levels:
            vol_sum += lvl.volume
        return vol_sum

    @staticmethod
    def get_trade_qty(x: Optional[float], vol_sum: Optional[int], slices: List[SubOrder],
                      min_px: Optional[float], max_px: Optional[float]) -> Optional[int]:
        posted_qty = 0
        for sub_order in slices:
            if min_px <= sub_order.price <= max_px:
                posted_qty += sub_order.unfilled_vol()
        all_qty = (x * vol_sum - posted_qty) / (1 - x)
        qty = all_qty - posted_qty
        return int(qty)
