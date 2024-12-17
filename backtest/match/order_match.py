from match.model.level_tree import LevelTree
import pandas as pd
from match.model.order import Order
from match.model.order_book import OrderBook
import copy


class OrderMatch(object):
    def __init__(self, path, date, symbol):
        self.path = path
        self.date = date
        self.symbol = symbol
        self.open_price = self.get_open_price()
        self.open_order_book = None
        self.continuous_instructions = None

    def get_instructions(self):
        instructions = pd.read_csv(f"{self.path}/{self.date}/instructions/{self.symbol}-Instruction-{self.date}.csv")
        instructions["EventID"] = instructions["EventID"] + 1000
        instructions["OrderNO"] = instructions["OrderNO"] + 1000
        return instructions

    @staticmethod
    def create_level_tree(order_book):
        bid_level_tree = LevelTree(is_bid=True)
        ask_level_tree = LevelTree(is_bid=False)
        for _, row in order_book.iterrows():
            order = Order(
                order_no=row.OrderNO,
                order_type=row.OrdType,
                price=row.Price,
                qty=row.Qty,
                side=row.Side,
                time=row.Time,
            )
            if row.Side == "B":
                if row.Price not in bid_level_tree.price_tree:
                    bid_level_tree.create_price(row.Price)
                order.set_list(bid_level_tree.price_tree[order.price])
                bid_level_tree.insert_order(order)
            else:
                if row.Price not in ask_level_tree.price_tree:
                    ask_level_tree.create_price(row.Price)
                order.set_list(ask_level_tree.price_tree[order.price])
                ask_level_tree.insert_order(order)
        return bid_level_tree, ask_level_tree

    def get_open_price(self):
        df_open = pd.read_csv(f"{self.path}/{self.date}/instructions/Open-{self.date}.csv")
        open_price_map = {row["symbol"]: row["open_price"] for _, row in df_open.iterrows()}
        return open_price_map[self.symbol]

    @staticmethod
    def remove_auc_matched(df_ins, match_vol):
        """
        去除竞合竞价撮合成功的委托
        :param df_ins:
        :param match_vol:
        :return:
        """
        df_ins["QtySum"] = df_ins.Qty.cumsum()
        df_ins["PreQtySum"] = df_ins["QtySum"].shift(1)
        df_ins["PreQtySum"].fillna(0, inplace=True)
        df_ins = df_ins[df_ins.QtySum > match_vol]
        df_ins["Qty"] = df_ins.apply(
            lambda x: x["Qty"] - (match_vol - x["PreQtySum"]) if x["PreQtySum"] < match_vol <= x["QtySum"] else x[
                "Qty"],
            axis=1).astype(int)
        del df_ins["QtySum"], df_ins["PreQtySum"]
        return df_ins

    def get_open_order_book(self, instructions):
        """
        基于委托指令和开盘价得到开盘order_book
        :param instructions:
        :return:
        """
        instructions = instructions[instructions.Time < 93000000]
        cancel_nos = set(instructions[instructions.OrdType.eq("D")].OrderNO)
        instructions = instructions[~instructions.OrderNO.isin(cancel_nos)]
        buy_ins = instructions[instructions.Side.eq("B")].sort_values(["Price", "EventID"], ascending=[False, True])
        sell_ins = instructions[instructions.Side.eq("S")].sort_values(["Price", "EventID"], ascending=[True, True])
        buy_vol = buy_ins[buy_ins.Price.ge(self.open_price)].Qty.sum()
        sell_vol = sell_ins[sell_ins.Price.le(self.open_price)].Qty.sum()
        match_vol = min(buy_vol, sell_vol)
        buy_ins = self.remove_auc_matched(buy_ins, match_vol)
        sell_ins = self.remove_auc_matched(sell_ins, match_vol)
        order_book = pd.concat([buy_ins, sell_ins])
        order_book["LeftQty"] = order_book["Qty"]
        order_book["ExecType"] = "A"
        order_book["EventID"] = order_book["OrderNO"]
        order_book["TradPhase"] = "A"
        return order_book.sort_values(by="EventID")

    def init_data(self):
        begin_time = 92500000
        end_time = 145700000
        instructions = self.get_instructions()
        bid_level_tree, ask_level_tree = self.create_level_tree(self.get_open_order_book(instructions))
        self.open_order_book = OrderBook(bid_level_tree, ask_level_tree)
        self.continuous_instructions = instructions[(instructions.Time > begin_time) & (instructions.Time < end_time)]

    def continuous_match(self, sub_order, insert_time):
        assert 93000000 <= insert_time < 145700000
        side = "B" if sub_order.is_buy() else "S"
        price = sub_order.price
        qty = sub_order.unfilled_vol()
        event_id = 0
        order_no = 0
        insert_success = False
        open_order_book = copy.deepcopy(self.open_order_book)
        for _, instruction in self.continuous_instructions.iterrows():
            if instruction.Time > insert_time and not insert_success:  # 插入自己的委托
                open_order_book.handle_instruction(pd.Series(
                    {"EventID": event_id + 1, "Time": insert_time, "OrderNO": order_no + 1, "OrdType": "L",
                     "Side": side,
                     "Price": price, "Qty": qty, "TradPhase": "C"}),
                    entrust_no=order_no + 1)
                insert_success = True
            match_success, match_result = open_order_book.handle_instruction(instruction)
            if match_success:
                return match_result
            event_id = instruction.EventID
            order_no = instruction.OrderNO
        return None
