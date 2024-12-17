import imaplib
import email
from datetime import datetime
from utils import feishu_bot
def read_csv():
    imaplib.Commands["ID"] = "NONAUTH"
    mail = imaplib.IMAP4_SSL(host="imap.163.com", port=993)
    mail.login("mom_gz@163.com", "PTRUHBEILNJBDTXV")
    mail._simple_command("ID",'("name" "test" "version" "0.0.1")')
    mail.select()
    result, data = mail.search(None,'ALL')
    ids = data[0].split()
    for id in ids:
        result, data = mail.fetch(id, '(RFC822)')
        raw_email = data[0][1]
        email_message = email.message_from_bytes(raw_email)
        subject = email_message['Subject']
        sender = email.utils.parseaddr(email_message['From'])[1]
        email_date = email_message['Date']
        date = datetime.strptime(email_date[5:24], '%d %b %Y %H:%M:%S').strftime("%Y-%m-%d")
        edate =  datetime.strptime(email_date[5:24], '%d %b %Y %H:%M:%S').strftime("%Y-%m-%d")
        daily= datetime.now().strftime('%Y-%m-%d')
        if sender =='Xueerfank@126.com':
            try:
                if edate == daily:
                    if email_message.get_content_maintype() == 'multipart':
                        for part in email_message.walk():
                            if part.get_content_maintype() == 'multipart' or part.get('Content-Disposition') is None:
                                continue
                            filename = part.get_filename()
                            if filename:
                                with open(f'file/child/{filename}', 'wb') as f:
                                    f.write(part.get_payload(decode=True))
            except Exception as e:
                # feishu_bot.send_msg_to_feishu(f"{daily} 未找到Xueerfank的子单邮件")
                print(e)
        if sender =='zhangyangye@boke.com':
            try:
                if edate == daily:
                    if email_message.get_content_maintype() == 'multipart':
                        for part in email_message.walk():
                            if part.get_content_maintype() == 'multipart' or part.get('Content-Disposition') is None:
                                continue
                            filename = part.get_filename()
                            if filename:
                                with open(f'file/child/{filename}', 'wb') as f:
                                    f.write(part.get_payload(decode=True))
            except Exception as e:
                print(e)
        # if sender =='niefanxiang@bokesimu.com':
        #     try:
        #         if edate == daily:
        #             #print(f"识别标识：{fund_name}")
        #             if email_message.get_content_maintype() == 'multipart':
        #                 for part in email_message.walk():
        #                     if part.get_content_maintype() == 'multipart' or part.get('Content-Disposition') is None:
        #                         feishu_bot.send_msg_to_feishu(f"{daily} 未找到聂凡翔的子单邮件")
        #                         break
        #                     filename = part.get_filename()
        #                     if filename:
        #                         with open(f'file/child/{filename}', 'wb') as f:
        #                             f.write(part.get_payload(decode=True))
        #     except Exception as e:
        #         feishu_bot.send_msg_to_feishu(f"{daily} 未找到聂凡翔的子单邮件")
        #         print(e)




def read_last_stock():
    imaplib.Commands["ID"] = "NONAUTH"
    mail = imaplib.IMAP4_SSL(host="imaphz.qiye.163.com", port=993)
    mail.login("niefanxiang@bokesimu.com", "pcdTWZucpXesufAx")
    mail._simple_command("ID", '("name" "test" "version" "0.0.1")')
    mail.select()
    result, data = mail.search(None, 'ALL')
    ids = data[0].split()
    for id in ids:
        result, data = mail.fetch(id, '(RFC822)')
        raw_email = data[0][1]
        email_message = email.message_from_bytes(raw_email)
        subject = email_message['Subject']
        sender = email.utils.parseaddr(email_message['From'])[1]
        email_date = email_message['Date']
        date = datetime.strptime(email_date[5:24], '%d %b %Y %H:%M:%S').strftime("%Y-%m-%d")
        edate = datetime.strptime(email_date[5:24], '%d %b %Y %H:%M:%S').strftime("%Y-%m-%d")
        daily = datetime.now().strftime('%Y-%m-%d')
        if sender == 'htzqyyzx01@htsc.com':
            try:
                if edate == daily:
                    if email_message.get_content_maintype() == 'multipart':
                        for part in email_message.walk():
                            if part.get_content_maintype() == 'multipart' or part.get('Content-Disposition') is None:
                                continue
                            filename = part.get_filename()
                            print(filename)
                            if '=E9=80=9A=E8=B4=A6=E5=8D=95' in filename:
                                with open(f'file/对账单/{daily}_对账单.xlsx', 'wb') as f:
                                    f.write(part.get_payload(decode=True))
            except Exception as e:
                # feishu_bot.send_msg_to_feishu(f"{daily} 未找到Xueerfank的子单邮件")
                print(e)


if __name__ == '__main__':
    read_csv()