import json
import requests
# _url = 'https://open.feishu.cn/open-apis/bot/v2/hook/77b39eab-c7b1-4a8b-972b-971ab10dffad'
# s_url = 'https://open.feishu.cn/open-apis/bot/v2/hook/991f1e9a-3f85-45e4-9b4c-a40bddbff4ad'
# zy_url = 'https://open.feishu.cn/open-apis/bot/v2/hook/76c4342c-47ec-46c3-8994-b4bb7263a246'
all_data_url = 'https://open.feishu.cn/open-apis/bot/v2/hook/95497b0b-b5c6-48ee-a13b-fdef16db77f9'


def send_msg_to_feishu(msg, webhook=all_data_url):

    payload_message = {
        'msg_type': "text",
        'content':{
            'text':msg
        }
    }
    headers = { 'Content-Type':'application/json'}
    response = requests.request("POST", webhook, headers=headers, data=json.dumps(payload_message))
    return




def send_file_to_feishu(msg, webhook=all_data_url):

    payload_message = {
        'msg_type': "file",
        'content':{
            'text':msg
        }
    }
    headers = { 'Content-Type':'application/zip'}
    response = requests.request("POST", webhook, headers=headers, data=json.dumps(payload_message))
    return



def cron_error_info(process_ret, args, exec_time):
    try:
        if process_ret.returncode == 0:
            send_msg_to_feishu( f"{args[0].split('/')[-1]}  \t >>> DONE!exec time:{exec_time}")
        else:
            send_msg_to_feishu( f"{args[0].split('/')[-1]}  \t >>> ERROR! {process_ret.stderr.readlines()},  exec time:{exec_time}")
    except Exception as e:
        send_msg_to_feishu(
            f"{args[0].split('/')[-1]}  \t >>> ERROR! {process_ret},  exec time:{exec_time}")

def send_res_log_df(df_html):
    send_msg_to_feishu(df_html)


if __name__ == '__main__':
    send_msg_to_feishu("测试：原始数据已成功上传网盘")
