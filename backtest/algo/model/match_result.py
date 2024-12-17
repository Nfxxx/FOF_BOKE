from typing import Optional, Dict
from pydantic import BaseModel


class DealInfo(BaseModel):
    price: Optional[float] = 0.0
    volume: Optional[int] = 0


class MatchResult(object):
    def __init__(self, sub_order_id):
        self.sub_order_id: Optional[str] = sub_order_id
        self.result_by_time: Dict[int, DealInfo] = {}
