import datetime

from pydantic import BaseModel
from typing import Optional
import algo.model.model as model
from enum import Enum


class SubOrderStatus(Enum):
    staged = 0
    new = 1
    partially_filled = 2
    pending_cancel = 3
    filled = 4
    cancelled = 5
    rejected = 6
    unknown = 7


class SubOrder(BaseModel):
    trader: Optional[str] = ""
    sub_order_id: Optional[str] = ""
    parent_id: Optional[str] = ""
    broker: Optional[str] = ""
    fund: Optional[str] = ""
    strategy: Optional[str] = ""
    account: Optional[str] = ""
    symbol: Optional[str] = ""
    side: Optional[model.MarketSide] = model.MarketSide.buy
    order_type: Optional[model.OrderType] = model.OrderType.limit
    price: Optional[float] = 0.0
    qty: Optional[int] = 0
    remark: Optional[str] = ""
    filled_vol: Optional[int] = 0
    exec_avg_price: Optional[float] = 0.0
    status: Optional[SubOrderStatus] = SubOrderStatus.unknown
    req_time: Optional[datetime.datetime] = datetime.datetime.now()

    def is_finished(self) -> Optional[bool]:
        return self.status in (SubOrderStatus.filled, SubOrderStatus.cancelled, SubOrderStatus.rejected)

    def filled_amt(self) -> Optional[float]:
        return self.filled_vol * self.exec_avg_price

    def unfilled_vol(self) -> Optional[float]:
        return self.qty - self.filled_vol

    def send_to_out(self):
        if self.sub_order_id == "":
            return False
        return True

    def is_buy(self):
        if self.side == model.MarketSide.buy:
            return True
        return False

    def __str__(self):
        return (f"sub_order_id:{self.sub_order_id},symbol:{self.symbol},qty:{self.qty},"
                f"filled:{self.filled_vol},price:{self.price},remark:{self.remark},status:{self.status}")
