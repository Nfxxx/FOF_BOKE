from message_book.message_book_sz import get_message_book_sz
from message_book.message_book_sh import get_message_book_sh
from utils.tools import convert_message_book, assert_order_book


def get_open_order_book_from_mb(df_orders, df_trans, exchange):
    """
    从message_book得到开盘的order_book
    :param df_orders:
    :param df_trans:
    :param exchange:
    :return:
    """
    if exchange == "SH":
        df_mb = get_message_book_sh(df_orders, df_trans)
    else:
        df_mb = get_message_book_sz(df_orders, df_trans)
    df_mb_before_open = df_mb[df_mb.Time < 93000000]
    order_book = convert_message_book(df_mb_before_open)
    order_book["EventID"] = order_book["OrderNO"]
    assert_order_book(order_book)
    return order_book
