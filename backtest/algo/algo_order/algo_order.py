from typing import Optional, List, Dict
from algo.model.tick import Tick
from algo.model.parent_order import ParentOrder, ParentOrderStatus, RunningStatus
import algo.model.model as model
import algo.const.const_var as const_var
import algo.utils.tools as utils
from algo.kernal.catchup import Catchup
from algo.kernal.cornerstone import CornerStone
from algo.kernal.capstone import Capstone
from algo.manager.slice_manager import SliceManager
from algo.model.instruction import Instruction, InstructionOperate
from logger import logger
from algo.model.sub_order import SubOrder, SubOrderStatus
import uuid
from match.order_match import OrderMatch
from algo.model.match_result import MatchResult, DealInfo


class AlgoOrder(object):
    def __init__(self, p_order: Optional[ParentOrder],
                 stk_basic_data: Optional[model.StockBasicData],
                 trading_params: Optional[model.AlgoTradingParams],
                 req_params: Optional[model.RequestParams], order_match: Optional[OrderMatch]):
        self.limit_up: Optional[float] = 0
        self.limit_down: Optional[float] = 0
        self.trading_params: Optional[model.AlgoTradingParams] = trading_params
        self.stk_basic_data: Optional[model.StockBasicData] = stk_basic_data
        self.req_params: Optional[model.RequestParams] = req_params
        self.p_order: Optional[ParentOrder] = p_order
        self.direction: Optional[int] = self.p_order.get_direction()
        self.catchup: Optional[Catchup] = None
        self.corner_stone: Optional[CornerStone] = None
        self.capstone = None
        self.arrival_px: Optional[float] = 0.0
        self.arrival_vol: Optional[int] = 0
        self.slice_mgr = SliceManager(p_order.origin_id)
        self.pre_tick_time: Optional[int] = 0
        self.match_result_map: Dict[int, MatchResult] = {}
        self.max_cant_fill_price: Optional[float] = 0.0
        self.order_match: Optional[order_match] = order_match

    def init_algo(self, tick: Optional[Tick]):
        if self.arrival_px == 0:
            symbol = self.p_order.symbol
            self.limit_up: Optional[float] = utils.get_limit_price(symbol, tick.pre_close / const_var.ten_thousand, 1)
            self.limit_down: Optional[float] = utils.get_limit_price(symbol, tick.pre_close / const_var.ten_thousand,
                                                                     -1)
            self.arrival_px: Optional[float] = utils.get_middle_price(tick)
            self.arrival_vol: Optional[int] = tick.acc_volume
            self.p_order.input_price = utils.get_middle_price(tick)
            self.p_order.order_status = ParentOrderStatus.started
            self.p_order.running_status = RunningStatus.inited
            self.p_order.insert_time = tick.time
            self.p_order.latest_update_time = tick.time
            self.catchup = Catchup(tick, self.req_params, self.p_order, self.stk_basic_data)
            self.corner_stone = CornerStone(tick, symbol, self.p_order,
                                            self.stk_basic_data, self.trading_params,
                                            self.req_params)
            self.capstone = Capstone(tick, symbol, self.p_order,
                                     self.stk_basic_data, self.trading_params, self.req_params)

    def on_tick_update(self, tick: Optional[Tick]):
        self.init_algo(tick)
        if self.p_order.input_vol < utils.get_lots(self.p_order.symbol):
            raise Exception("invalid order quantity")
        # 涨跌停不交易
        if tick.ask_price1 == 0 or tick.bid_price1 == 0:
            logger.info("symbol is limited")
            return
        while True:
            instructions = self.get_instructions(tick)
            is_cancel, cancel_instructions = self.on_instructions(instructions, tick.time)
            if is_cancel:
                self.on_order_update_cancel(cancel_instructions, tick.time)
            else:
                self.on_order_update_trade(self.pre_tick_time, tick.time)
                break
        self.pre_tick_time = tick.time

    def get_instructions(self, tick: Optional[Tick]) -> Optional[List[Instruction]]:
        instructions = self.catchup.on_data(tick, self.slice_mgr)
        if len(instructions) == 0:
            if model.StockCluster(self.stk_basic_data.cluster) in (
                    model.StockCluster.ultrafast, model.StockCluster.sparse, model.StockCluster.fast):
                instructions = self.capstone.on_data(tick, self.slice_mgr)
            elif model.StockCluster(self.stk_basic_data.cluster) in (
                    model.StockCluster.slow, model.StockCluster.normal):
                instructions = self.corner_stone.on_data(tick, self.slice_mgr)
            else:
                raise Exception("unknown algo cluster")
        return instructions

    def init_sub_order(self, sub_order: Optional[SubOrder]):
        sub_order.sub_order_id = str(uuid.uuid4())
        sub_order.account = self.p_order.account_id
        sub_order.order_type = model.OrderType.limit
        sub_order.symbol = self.p_order.symbol
        sub_order.trader = self.p_order.trader
        sub_order.broker = self.p_order.broker
        sub_order.side = self.p_order.get_side()
        sub_order.strategy = self.p_order.strategy_id
        sub_order.parent_id = self.p_order.origin_id
        sub_order.status = SubOrderStatus.staged

    def on_instructions(self, instructions: List[Instruction], tick_time: Optional[int]) -> (
            Optional[bool], List[Instruction]):
        cancel_instructions = [ins for ins in instructions if ins.operation == InstructionOperate.cancel]
        if cancel_instructions:
            return True, cancel_instructions
        trade_instructions = [ins for ins in instructions if ins.operation == InstructionOperate.trade]
        for ins in trade_instructions:
            self.init_sub_order(ins.sub_order)
            logger.info(f"send trade instructions:{ins}, tick_time:{tick_time}")
            self.slice_mgr.update_sub_order(ins.sub_order)

            if self.max_cant_fill_price > 0 and ins.sub_order.price * self.direction <= self.max_cant_fill_price * self.direction:
                logger.info(
                    f"cant fill instructions:{ins},max cant fill price:{self.max_cant_fill_price}, tick_time:{tick_time}")
                continue
            match_result = self.order_match.continuous_match(ins.sub_order, tick_time)
            if match_result:
                match_result_obj = MatchResult(ins.sub_order.sub_order_id)
                for time, value in match_result.items():
                    match_result_obj.result_by_time[time] = DealInfo(price=value[0], volume=value[1])
                self.match_result_map[ins.sub_order.sub_order_id] = match_result_obj
            else:
                if self.max_cant_fill_price == 0:
                    self.max_cant_fill_price = ins.sub_order.price
                else:
                    if ins.sub_order.price * self.direction > self.max_cant_fill_price * self.direction:
                        self.max_cant_fill_price = ins.sub_order.price
        return False, None

    def on_order_update_trade(self, pre_tick_time: Optional[int], tick_time: Optional[int]):
        for sub_order_id, match_result in self.match_result_map.items():
            deal_vol, deal_amt = 0, 0
            for time, deal_info in match_result.result_by_time.items():
                if pre_tick_time < time <= tick_time:
                    price, vol = deal_info.price, deal_info.volume
                    deal_vol += vol
                    deal_amt += price * vol
            deal_price = deal_amt / deal_vol if deal_vol > 0 else 0
            if deal_vol > 0:
                sub_order = self.slice_mgr.get_sub_order(sub_order_id)
                if sub_order_id not in self.slice_mgr.unfilled_map:
                    logger.info(f"{sub_order_id} has been canceled!")
                    continue
                filled_amt = sub_order.filled_amt() + deal_vol * deal_price
                sub_order.exec_avg_price = filled_amt / (sub_order.filled_vol + deal_vol)
                sub_order.filled_vol += deal_vol
                if sub_order.filled_vol == sub_order.qty:
                    sub_order.status = SubOrderStatus.filled
                else:
                    sub_order.status = SubOrderStatus.partially_filled
                self.update_parent_order(deal_vol, sub_order.exec_avg_price)
                self.slice_mgr.update_sub_order(sub_order)
                logger.info(f"update sub_order:{sub_order},tick_time:{tick_time}")
                logger.info(f"update parent_order:{self.p_order}, tick_time:{tick_time}")

    def on_order_update_cancel(self, instructions: List[Instruction], tick_time: Optional[int]):
        for ins in instructions:
            sub_order = ins.sub_order
            sub_order.status = SubOrderStatus.cancelled
            self.slice_mgr.update_sub_order(sub_order)
            logger.info(f"cancel sub order:{sub_order}, tick_time:{tick_time}")

    def update_parent_order(self, deal_vol: Optional[int], deal_price: Optional[float]):
        filled_amt = self.p_order.get_filled_amt() + deal_vol * deal_price
        self.p_order.avg_price = filled_amt / (self.p_order.filled_vol + deal_vol)
        self.p_order.filled_vol += deal_vol
        if self.p_order.filled_vol == self.p_order.input_vol:
            self.p_order.order_status = ParentOrderStatus.filled
            self.p_order.running_status = RunningStatus.finished
        else:
            self.p_order.order_status = ParentOrderStatus.partial


