from sortedcontainers import SortedDict

from match.model.order_list import OrderList


class LevelTree(object):
    def __init__(self, is_bid):
        self.price_tree = SortedDict()
        self.order_map = {}
        self.is_bid = is_bid
        self.volume = 0
        self.min_price = None
        self.max_price = None

    def __len__(self):
        return len(self.price_tree)

    def get_price(self, price):
        self.price_tree.get(price)

    def get_order(self, order_no):
        return self.order_map[order_no]

    def create_price(self, price):
        self.price_tree[price] = OrderList(price)
        if (self.max_price is None) or (price > self.max_price):
            self.max_price = price
        if (self.min_price is None) or (price < self.min_price):
            self.min_price = price

    def remove_price(self, price):
        self.price_tree.pop(price)
        len_tree = len(self.price_tree)
        if self.max_price == price:
            if len_tree >= 1:
                self.max_price = self.price_tree.peekitem(-1)[0]
            else:
                self.max_price = None
        if self.min_price == price:
            if len_tree >= 1:
                self.min_price = self.price_tree.peekitem(0)[0]
            else:
                self.min_price = None

    def price_exists(self, price):
        return price in self.price_tree

    def order_exists(self, order_no):
        return order_no in self.order_map

    def insert_order(self, order):
        self.price_tree[order.price].append_order(order)
        self.order_map[order.order_no] = order
        self.volume += order.left_qty

    def update_order(self, order_no, deal_qty, upt_time):
        order = self.order_map[order_no]
        if order.left_qty - deal_qty <= 0:
            return self.remove_order_by_no(order_no)
        else:
            order.update_qty(deal_qty, upt_time)
            self.volume -= deal_qty
            return order.order_type, order.price

    def remove_order_by_no(self, order_no):
        order = self.order_map[order_no]
        order_type, order_px = order.order_type, order.price
        self.volume -= order.left_qty
        order.order_list.remove_order(order)
        if len(order.order_list) == 0:
            self.remove_price(order.price)
        del self.order_map[order_no]
        return order_type, order_px

    def get_best_lvl_px(self):
        if self.is_bid:
            return self.max_price
        return self.min_price

    def get_lvl_px(self, n_lvl):
        len_tree = len(self.price_tree)
        if len_tree >= n_lvl:
            if self.is_bid:
                return self.price_tree.peekitem(len_tree - n_lvl)[0]
            else:
                return self.price_tree.peekitem(n_lvl - 1)[0]
        else:
            return None

    def get_lvl_volume(self, n_lvl):
        len_tree = len(self.price_tree)
        if len_tree >= n_lvl:
            if self.is_bid:
                return self.price_tree.peekitem(len_tree - n_lvl)[1].volume
            else:
                return self.price_tree.peekitem(n_lvl - 1)[1].volume
        else:
            return None

    def max(self):
        return self.max_price

    def min(self):
        return self.min_price
