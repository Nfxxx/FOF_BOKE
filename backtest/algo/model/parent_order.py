from pydantic import BaseModel
from typing import Optional
from enum import Enum
import algo.model.model as model


class ParentOrderStatus(Enum):
    unknown = 0  # 未知
    started = 1  # 开始
    partial = 2  # 部分成交
    filled = 3  # 全部成交
    canceled = 4  # 撤单
    error = 5  # 错误
    expired = 6  # 过期
    pending_cancel = 7  # 撤单中


class RunningStatus(Enum):
    inited = 1
    finished = 2


class ParentOrder(BaseModel):
    origin_id: Optional[str] = ""
    order_status: Optional[ParentOrderStatus] = ParentOrderStatus.unknown
    insert_time: Optional[int] = 0
    filled_vol: Optional[int] = 0
    input_price: Optional[float] = 0.0
    input_vol: Optional[int] = 0
    order_type: Optional[model.OrderType] = model.OrderType.limit
    order_action: Optional[model.OrderAction] = model.OrderAction.unknown
    broker: Optional[str] = ""
    latest_update_time: Optional[int] = 0
    remark: Optional[str] = ""
    symbol: Optional[str] = ""
    account_id: Optional[str] = ""
    strategy_id: Optional[str] = ""
    portfolio: Optional[str] = ""
    user: Optional[str] = ""
    avg_price: Optional[float] = 0.0
    algo: Optional[str] = ""
    group: Optional[str] = ""
    trader: Optional[str] = ""
    running_status: Optional[RunningStatus] = RunningStatus.inited
    params: Optional[str] = ""

    def is_buy(self):
        return self.order_action == model.OrderAction.buy_to_open or self.order_action == model.OrderAction.buy_to_close

    def get_direction(self):
        if self.is_buy():
            return 1
        else:
            return -1

    def get_side(self):
        if self.order_action in (model.OrderAction.buy_to_open, model.OrderAction.buy_to_close):
            return model.MarketSide.buy
        return model.MarketSide.sell

    def get_filled_amt(self):
        return self.filled_vol * self.avg_price

    def is_finished(self):
        return self.order_status in (
            ParentOrderStatus.canceled, ParentOrderStatus.error, ParentOrderStatus.filled, ParentOrderStatus.expired)

    def remain_vol(self):
        return self.input_vol - self.filled_vol

    def __str__(self):
        return (f"parent_order_id:{self.origin_id},symbol:{self.symbol},qty:{self.input_vol},"
                f"filled:{self.filled_vol},avg_price:{self.avg_price},status:{self.order_status}")
