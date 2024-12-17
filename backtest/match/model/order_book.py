from match.model.order import Order
import pandas as pd
from algo.model.match_result import MatchResult


class OrderBook(object):
    def __init__(self, bids, asks):
        self.bids = bids
        self.asks = asks
        self.trans_list = []
        self.entrust_no = 0
        self.entrust_qty = 0
        self.current_time = 0

    @staticmethod
    def cancel_order(level_tree, order_no):
        return level_tree.remove_order_by_no(order_no)

    @staticmethod
    def add_order(level_tree, order):
        level_tree.insert_order(order)

    @staticmethod
    def match_order(level_tree, order_no, deal_qty, time):
        return level_tree.update_order(order_no, deal_qty, time)

    def update_record(self, order, instruction, deal_price, deal_vol):
        mb_map = {
            "ExecType": "F",
            "OrderNO": order.order_no,
            "OrdType": order.order_type,
            "Side": order.side,
            "Price": deal_price,
            "Qty": deal_vol,
            "Time": instruction.Time,
            "TradPhase": "C"
        }
        self.trans_list.append(mb_map)
        mb_map = {
            "ExecType": "F",
            "OrderNO": instruction.OrderNO,
            "OrdType": instruction.OrdType,
            "Side": instruction.Side,
            "Price": deal_price,
            "Qty": deal_vol,
            "Time": instruction.Time,
            "TradPhase": "C"
        }
        self.trans_list.append(mb_map)

    def save_file(self, path, date, symbol):
        pd.DataFrame(self.trans_list).to_csv(f"{path}/{date}/output/{symbol}-TransDetail-{date}.csv",
                                             index=None)

    def update_instruction(self, level_tree, instruction, is_bid):
        direction = 1 if is_bid else -1
        while True:
            if len(level_tree) < 1:  # level_tree 迭代过程中长度会减少
                return
            price = level_tree.get_best_lvl_px()
            if price * direction > instruction.Price * direction:
                return
            order_list = level_tree.price_tree[price]
            order = order_list.head_order  # order_list 迭代过程中会减少长度
            while True:
                if instruction.Qty <= 0:
                    return
                deal_vol = min(order.left_qty, instruction.Qty)
                deal_price = order.price
                self.match_order(level_tree, order.order_no, deal_vol, instruction.Time)
                self.update_record(order, instruction, deal_price, deal_vol)
                instruction.Qty -= deal_vol
                if order.next_order is None:
                    break
                order = order.next_order

    def get_match_result(self):
        trans_df = pd.DataFrame(self.trans_list)
        trans_df = trans_df[trans_df.OrderNO == self.entrust_no]
        match_result = {}
        for time, group in trans_df.groupby("Time"):
            filled_amt = (group["Price"]*group["Qty"]).sum()
            filled_vol = group["Qty"].sum()
            filled_price = filled_amt/filled_vol
            match_result[time] = (filled_price, filled_vol)
        return match_result

    def handle_instruction(self, instruction, entrust_no=0):
        if entrust_no != 0:
            self.entrust_no = entrust_no
        is_bid = instruction.Side == "B"
        near_tree = self.bids if is_bid else self.asks
        far_tree = self.asks if is_bid else self.bids
        near_px = near_tree.get_best_lvl_px()
        far_px = far_tree.get_best_lvl_px()
        self.current_time = instruction.Time
        if instruction.OrdType == "D":
            # assert near_tree.order_exists(instruction.OrderNO)
            if near_tree.order_exists(instruction.OrderNO):
                self.cancel_order(near_tree, instruction.OrderNO)
            return False, None
        if instruction.OrdType == "U":
            if instruction.Price == 0:
                instruction.Price = near_px
            assert instruction.Price == near_px
        if far_px is not None:  # 可能不存在对手盘
            direction = 1 if is_bid else -1
            if instruction.Price * direction >= far_px * direction:
                self.update_instruction(far_tree, instruction, is_bid)  # instruction 不用通过返回值传回
        if instruction.Qty > 0:
            order = Order(
                order_no=instruction.OrderNO,
                order_type=instruction.OrdType,
                price=instruction.Price,
                qty=instruction.Qty,
                side=instruction.Side,
                time=instruction.Time,
            )
            if instruction.Price not in near_tree.price_tree:
                near_tree.create_price(instruction.Price)
            order.set_list(near_tree.price_tree[instruction.Price])
            self.add_order(near_tree, order)
        if self.entrust_no != 0:
            if not near_tree.order_exists(self.entrust_no) and not far_tree.order_exists(self.entrust_no):
                match_result = self.get_match_result()
                return True, match_result
        return False, None
