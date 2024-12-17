class OrderList(object):
    def __init__(self, price):
        self.head_order = None
        self.tail_order = None
        self.price = price
        self.volume = 0
        self.length = 0
        self.last = None

    def __len__(self):
        return self.length

    def __iter__(self):
        self.last = self.head_order
        return self

    def __next__(self):
        if self.last is None:
            raise StopIteration
        else:
            return_val = self.last
            self.last = self.last.next_order
            return return_val

    def append_order(self, order):
        if len(self) == 0:
            order.next_order = None
            order.prev_order = None
            self.head_order = order
            self.tail_order = order
        else:
            order.prev_order = self.tail_order
            order.next_order = None
            self.tail_order.next_order = order
            self.tail_order = order
        self.length += 1
        self.volume += order.left_qty

    def remove_order(self, order):
        self.volume -= order.left_qty
        self.length -= 1
        if len(self) == 0:
            self.head_order = None
            self.tail_order = None
            return
        next_order = order.next_order
        prev_order = order.prev_order
        if next_order is not None and prev_order is not None:
            next_order.prev_order = prev_order
            prev_order.next_order = next_order
        elif next_order is not None:
            next_order.prev_order = None
            self.head_order = next_order
        elif prev_order is not None:
            prev_order.next_order = None
            self.tail_order = prev_order

    def get_first_order_no(self):
        return self.head_order.order_no if self.head_order is not None else 0

    def get_last_order_no(self):
        return self.tail_order.order_no if self.tail_order is not None else 0
