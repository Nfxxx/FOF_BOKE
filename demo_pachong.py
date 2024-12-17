import time

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import MOM.updateFofSql as updateFofSql
def fetch_webpage_content(url):
    # 发送 HTTP GET 请求
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    response = requests.get(url, headers=headers)
    # response = requests.get(url)
    # 确保请求成功
    if response.status_code == 200:
        # 解析 HTML 内容
        soup = BeautifulSoup(response.content, 'html.parser')
        # links=soup.find_all('div')
        a=str(soup)
        list=a.split('<tbody>')
        all_df_colmus=['Company','Fund','MarketValue']
        all_df=pd.DataFrame(columns=all_df_colmus)
        fu_name=0
        money=0
        for i in list:
            if '机构提示信息' in i:
                index_of_A = i.find("机构提示信息</td>")
                if index_of_A != -1:  # 确保"A"在字符串中存在
                    s = i[index_of_A:]
                # print(s)  # 输出: A后面的内容
            if '基金管理人全称(中文)' in i:
                match = re.search(r'<span id="complaint2">(.*?)</span>', i, re.DOTALL)

                if match:
                    fu_name = match.group(1)

                    # print(fu_name)
                else:
                    print("未找到基金名称")
                match = re.search(r'管理规模区间</td>\s*<td colspan="2">\s*(.*?)亿元', i, re.DOTALL)

                if match:
                    money = match.group(1)
                    if money =='20-50':
                        money='5-100亿'
                    elif  money =='100':
                        money='100亿'
                    elif money == '0-5':
                        money = '5亿以下'
                    elif money == '5-10':
                        money = '5-100亿'
                    elif money == '10-20':
                        money = '5-100亿'
                    elif money == '50-100':
                        money = '5-100亿'
                    # print(money)
                else:
                    print("未找到基金名称")
            if '私募基金信息披露备份系统投资者查询账号开立率' in i:
                index_of_A = i.find(r'私募基金信息披露备份系统投资者查询账号开立率</div><div id="investorOpenTip"></div></td>')
                if index_of_A != -1:  # 确保"A"在字符串中存在
                    s = i[index_of_A:]
                    match = re.search(r'<td>(.*?)%', s, re.DOTALL)

                    if match:
                        content_between = match.group(1)
                        check_open_ratio=content_between+'%'
                        # print(check_open_ratio)
                    else:
                        print("未找到私募基金信息披露备份系统投资者查询账号开立率")
            # print(i)
            if r'<td><a href="../fund/' in i:
                pattern_href = r'.html">(.*?)</a></td>'  # 提取<a>标签的href和文本
                matches_href = re.findall(pattern_href, i, re.DOTALL)
                # print(matches_href)
                df=pd.DataFrame(columns=all_df_colmus)
                df['Fund']=matches_href
                df['Company']=fu_name
                df['MarketValue']=money

                all_df=all_df._append(df,ignore_index=True)
        # print(soup.find('span'))
        # 或者你可以进一步查找特定的元素或数据
        # ...
        return all_df  # 或者返回你提取的数据
    else:
        print(f"Failed to fetch the webpage. Status code: {response.status_code}")
        return None


# 调用函数
# fetch_webpage_content('https://gs.amac.org.cn/amac-infodisc/res/pof/manager/101000004385.html')
# fetch_webpage_content('https://gs.amac.org.cn/amac-infodisc/res/pof/manager/1701230933104577.html')
Comp=updateFofSql.CompleteSql()
web=Comp.web_amac()
for values in web['web_name']:
    print(values)
    df=fetch_webpage_content(values)
    print(df)
    Comp.update_marketvalue_table(df)
    time.sleep(5)