from typing import Optional, Dict, List
from algo.model.sub_order import SubOrder
from logger import logger


class SliceManager(object):
    def __init__(self, parent_id):
        self.slice_map: Optional[Dict[str, SubOrder]] = {}
        self.unfilled_map: Optional[Dict[str, SubOrder]] = {}
        self.qty: Optional[int] = 0
        self.filled: Optional[int] = 0
        self.avg_price: Optional[float] = 0.0
        self.parent_id: Optional[str] = parent_id

    def get_sub_order(self, sub_order_id: Optional[str]) -> Optional[SubOrder]:
        return self.slice_map[sub_order_id]

    def get_unfilled_slices(self) -> List[SubOrder]:
        return self.unfilled_map.values()

    def get_unfilled_slices_by_px(self, px: Optional[float]) -> List[SubOrder]:
        return [v for k, v in self.unfilled_map.items() if v.price == px]

    def get_unfilled_slices_by_module(self, module_name: Optional[str]) -> List[SubOrder]:
        return [v for k, v in self.unfilled_map.items() if v.remark == module_name]

    def update_total_qty(self):
        self.qty = sum(v.qty for k, v in self.slice_map.items())
        self.filled = sum(v.filled_vol for k, v in self.slice_map.items())
        filled_amt = sum(v.filled_amt() for k, v in self.slice_map.items())
        self.avg_price = filled_amt/self.filled if self.filled != 0 else 0

    def update_sub_order(self, sub_order: Optional[SubOrder]):
        self.slice_map[sub_order.sub_order_id] = sub_order
        self.update_total_qty()
        if sub_order.is_finished():
            self.unfilled_map.pop(sub_order.sub_order_id)
        else:
            self.unfilled_map[sub_order.sub_order_id] = sub_order

