# -*- coding: utf-8 -*-
from ftplib import FTP
from datetime import datetime
import os


def ensure_directory_exists(directory):
    # 检查目录是否存在
    if not os.path.exists(directory):
        # 如果目录不存在，创建它
        os.makedirs(directory)
        print(f"目录已创建: {directory}")
    else:
        print(f"目录已存在: {directory}")


def download_file_from_ftp(ftp_host, ftp_user, ftp_password, local_file, cwd_name):
    try:
        ftp = FTP(ftp_host)
        ftp.login(user=ftp_user, passwd=ftp_password)
        # file_list=ftp.nlst()
        # print(file_list)
        # ftp.cwd('L2_option_ccfx')
        ftp.cwd(f'{cwd_name}')
        # 列出目录内容
        with open(local_file, 'wb') as local_file_handle:
            ftp.retrbinary(f'RETR {local_file}', local_file_handle.write)

    except Exception as e:
        print(f"下载{local_file} 文件时出错: {e}")

    finally:
        ftp.quit()


current_date = datetime.now()
formatted_date = current_date.strftime('%Y%m%d')
formatted_month = current_date.strftime('%Y%m')
print(formatted_date)

ftp_host = 'ftp.datayes.com'
ftp_user = 'shbksm'
ftp_password = 'lX3NCB'
###L2_optionccfx
path_file = f'/oss/xie_data/L2_optionccfx/{formatted_date}'
ensure_directory_exists(path_file)
cwd_name = 'L2_option_ccfx'
local_file = f'{path_file}/opt_ccfxl2_{formatted_date}.zip'
download_file_from_ftp(ftp_host, ftp_user, ftp_password, local_file, cwd_name)
###L2_futureccfx
path_file = f'/oss/xie_data/L2_futureccfx/{formatted_date}'
ensure_directory_exists(path_file)
cwd_name = 'L2_future_ccfx'
local_file = f'{path_file}/future_ccfxl2_{formatted_date}.zip'
download_file_from_ftp(ftp_host, ftp_user, ftp_password, local_file, cwd_name)
###option_price
path_file = f'/oss/xie_data/option_price/option_price{formatted_month}'
ensure_directory_exists(path_file)
cwd_name = 'L2_option_ccfx'
local_file = f'{path_file}/opt_ccfxl2_{formatted_date}.zip'
download_file_from_ftp(ftp_host, ftp_user, ftp_password, local_file, cwd_name)
