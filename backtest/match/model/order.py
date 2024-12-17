
class Order(object):
    def __init__(self, order_no, order_type, price, qty, side, time):
        self.next_order = None
        self.prev_order = None

        self.order_no = order_no
        self.order_type = order_type
        self.price = price
        self.qty = qty
        self.left_qty = qty
        self.side = side
        self.create_time = time
        self.update_time = time

        self.order_list = None

    def update_qty(self, deal_qty, update_time):
        if self.order_list is not None:
            self.order_list.volume -= deal_qty
        self.left_qty -= deal_qty
        self.update_time = update_time

    def set_list(self, order_list):
        self.order_list = order_list

    def __str__(self):
        return "Order-{}:{}@{}".format(self.order_no, self.qty, self.price)
