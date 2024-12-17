# -*- coding: utf-8 -*-
from dataapi_win36 import Client
import pandas as pd
import json
def read_json_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as json_file:
            data = json.load(json_file)
            return data
    except FileNotFoundError:
        print(f"文件未找到: {file_path}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON 解析错误: {e}")
        return None
    except Exception as e:
        print(f"读取文件时发生错误: {e}")
        return None
if __name__ == "__main__":
	date='20241016'
	api_urls = read_json_file('Url.json')
	find_table_name='HF转低频因子数据表1'
	# print(api_urls['A股精品因子数据'])
	try:
		client = Client()
		client.init('8d4665ca81fa5774a50ac3519a13081ee08a89e64b1f9a964ca093955ab72c1c')
		try:
			url1 = api_urls[find_table_name].replace('#DATE#',date)
		except:
			url1 = api_urls[find_table_name]
		print(url1)
		code, result = client.getData(url1)  # 调用getData函数获取数据，数据以字符串的形式返回
		if code == 200:
			# print(result.decode('utf-8', errors='replace'))  # url1须为json格式，才可使用utf-8编码
			if eval(result)['retCode'] == 1:
				pd_data = pd.DataFrame(eval(result)['data'])  # 将数据转化为DataFrame格式
				print(pd_data)
				pd_data.to_csv(f'{find_table_name}_{date}.csv',encoding='gbk')
			else:
				print(result.decode('utf-8', errors='replace'))
		else:
			print(code)
			print(result)

	except Exception as e:
		#traceback.print_exc()
		raise e