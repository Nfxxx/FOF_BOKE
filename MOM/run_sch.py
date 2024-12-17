import schedule
import time
import ReadMailToCsv
import marge_parentOrder
from chinese_calendar import is_holiday
from datetime import datetime
from utils import feishu_bot
import old_request


def old_scheduler():
    try:
        if is_holiday(datetime.now()) is False:
            a = old_request.old_request()

            print(a)
    except:
        feishu_bot.send_msg_to_feishu("持仓生成失败")


def read_stcok_DZD():
    try:

            ReadMailToCsv.read_last_stock()
            print('read_last_stock')
    except:
        pass


def read_csv():
    try:
        if is_holiday(datetime.now()) is False:
            ReadMailToCsv.read_csv()
            print('csv success')
    except:
            feishu_bot.send_msg_to_feishu("获取昨日PM持仓失败")


def out_parent():
    try:
        if is_holiday(datetime.now()) is False:
            a = marge_parentOrder.MargeParent().magre_Parent()
            print(a)
    except:
        feishu_bot.send_msg_to_feishu("母单创建失败")


# schedule.every().day.at('16:10').do(old_scheduler)
schedule.every().day.at('07:50').do(read_stcok_DZD)
schedule.every().day.at('08:45').do(read_csv)
# schedule.every().day.at('08:55').do(out_parent)
while True:
    schedule.run_pending()
    time.sleep(35)


