# -*- coding: utf-8 -*-
"""
Created on Wed May 17 17:18:45 2023

@author: zhangqilin

根据fundinfo 和 traderec  按照相应格式写入excel

fundinfo 可以是从excel中读来，也可以是从数据库中读来

"""

import pandas as pd
import numpy as np
import re
import difflib

def fuzzy_match(Name, FundName,list=True,na=True):
    if list==True:
        for i in Name:
            q = min(len(i), len(FundName))
            similarity= difflib.SequenceMatcher(None, i[0:q], FundName[0:q]).ratio()
            # if FundName == '衍复希格斯三号':
                # print(Name)
            if similarity>0.9 and na==True:
                    return FundName
            elif similarity==1 and na==False:
                    return FundName
        else:
            return 0
    else:
        q = min(len(Name), len(FundName))
        similarity = difflib.SequenceMatcher(None, Name[0:q], FundName[0:q]).ratio()
        # if FundName == '衍复希格斯三号':
        # print(Name)
        if similarity > 0.9 and na == True:
            return FundName
        elif similarity == 1 and na == False:
            return FundName
        else:
            return 0



companylist = ['波克','赛韵','柏项','海南波克','BIG','轩克','世熠']

fundinfo = pd.read_excel('fundinfo2.xlsx',sheet_name = 'info')  # 每次有新的产品申购 需要更新至fundinfo2表格中

traderec0 = pd.read_excel('波克私募_投资动作及净值记录.xlsx',sheet_name = '交易动作记录')  # 这是从云文档“波克私募_投资动作及净值记录”导出的xlsx
# 这个表格会进行一定处理 会把里面的A/B份额统一成不带A B的基金业协会代码


fileName = '合计参考估值表-报表生成20240607.xlsx' # 这是上周的大表格 即 当周输出 = 上周大表格 + 交易动作记录



sd = pd.Timestamp('2024-06-11')  # 这是当周的首个交易日
ed = pd.Timestamp('2024-06-14')  # 这是当周的最后一个交易日
# 记得修改sd 和 ed  否则他会读之前的又写一遍


######  运行区

#净值表
netValueSheet = pd.read_excel(fileName, sheet_name='净值')
netValueSheet['日期'] = pd.to_datetime(netValueSheet['日期'])
netValueSheet = netValueSheet.set_index('日期')
netValueSheet.fillna(0, inplace=True)
#申赎表
sharesSheet = pd.read_excel(fileName, sheet_name='申赎份额')
sharesSheet = sharesSheet.iloc[:netValueSheet.shape[0]]
sharesSheet['日期'] = pd.to_datetime(sharesSheet['日期'])
sharesSheet = sharesSheet.set_index('日期')
sharesSheet.fillna(0, inplace=True)

#分红份额转投与报酬份额计提
dividendSheet = pd.read_excel(fileName, sheet_name='分红与计提报酬（份额）')
dividendSheet = dividendSheet.iloc[:netValueSheet.shape[0]]
dividendSheet['日期'] = pd.to_datetime(dividendSheet['日期'])
dividendSheet = dividendSheet.set_index('日期')
dividendSheet.fillna(0, inplace=True)

#分红现金与报酬金额计提
cashSheet = pd.read_excel(fileName, sheet_name='分红与计提报酬（现金）')
cashSheet = cashSheet.iloc[:netValueSheet.shape[0]]
cashSheet['日期'] = pd.to_datetime(cashSheet['日期'])
cashSheet = cashSheet.set_index('日期')
cashSheet.fillna(0, inplace=True)


#赎回到账的业绩报酬计提
RewardSheet =  pd.read_excel(fileName, sheet_name='赎回业绩报酬')
RewardSheet = RewardSheet.iloc[:netValueSheet.shape[0]]
RewardSheet['日期'] = pd.to_datetime(RewardSheet['日期'])
RewardSheet = RewardSheet.set_index('日期')
RewardSheet.fillna(0, inplace=True)

#赎回到款
BackMoneySheet =  pd.read_excel(fileName, sheet_name='赎回到款')
BackMoneySheet = BackMoneySheet.iloc[:netValueSheet.shape[0]]
BackMoneySheet['日期'] = pd.to_datetime(BackMoneySheet['日期'])
BackMoneySheet = BackMoneySheet.set_index('日期')
BackMoneySheet.fillna(0, inplace=True)


####################################


#波克私募_投资动作及净值记录 这个表的标的代码和fundnetvalue 不一样
traderec = traderec0[ (traderec0['申请日期']>=sd) & ( traderec0['申请日期']<=ed)] # 读取[sd,ed]之间日期的交易动作
## 更新交易动作
# 请勿修改for循环中的部分
# 这部分是fundinfo和交易动作记录匹配
for ii in range(traderec.shape[0]):
    tmp = traderec.iloc[ii]
    if tmp['标的代码'][-1] in ['A','B','C']:
        fundinfo['寻找'] = fundinfo['产品代码'] +  tmp['标的代码'][-1]

        fundname = fundinfo[fundinfo['寻找'] == tmp['标的代码']]['产品名称'].values[0]
    else:
        fundname = fundinfo[fundinfo['产品代码'] == tmp['标的代码']]['产品名称'].values[0]




    #fundname = fundinfo[fundinfo['寻找'] == tmp['标的代码']]['产品名称'].values[0]

    if tmp['投资主体']  in companylist:
        ivstn = tmp['投资主体']
    else:
        ivstn = fundinfo[fundinfo['产品代码'] == tmp['投资主体']]['产品名称'].values[0]
    lie = fundname + '（' +ivstn+ '）'

    if lie not in netValueSheet.columns:
        netValueSheet.loc[:,lie] = np.nan
        sharesSheet.loc[:,lie] = np.nan
        dividendSheet.loc[:,lie] = np.nan
        cashSheet.loc[:,lie] = np.nan
        RewardSheet.loc[:,lie] = np.nan
        BackMoneySheet.loc[:,lie] = np.nan





    if tmp['交易类别'] =='申购':

        sharesSheet.loc[tmp['申请日期'],lie] = tmp['确认份额']

    if tmp['交易类别'] == '赎回':
        sharesSheet.loc[tmp['申请日期'],lie] = -tmp['确认份额']
        # 赎回到款表默认按确认金额更新
        RewardSheet.loc[tmp['申请日期'],lie] = tmp['业绩报酬']
        BackMoneySheet.loc[tmp['申请日期'],lie] = -tmp['确认金额']


    if tmp['交易类别'] == '份额分红':
        dividendSheet.loc[tmp['申请日期'],lie] = tmp['确认份额']

    if tmp['交易类别'] == '现金分红':
        cashSheet.loc[tmp['申请日期'],lie] = tmp['确认金额']


    if tmp['交易类别'] == '业绩报酬-份额':
        dividendSheet.loc[tmp['申请日期'],lie] = -tmp['确认份额']


    if tmp['交易类别'] == '业绩报酬-现金':
        cashSheet.loc[tmp['申请日期'],lie] = -tmp['确认金额']

    if tmp['交易类别'] == '强制调增':
        dividendSheet.loc[tmp['申请日期'],lie] = tmp['确认份额']

    if tmp['交易类别'] == '强制调减':
        dividendSheet.loc[tmp['申请日期'],lie] = -tmp['确认份额']


## 从数据库更新 净值

###
import pandas as pd

import numpy as np

import pymysql
#连接数据库
conn=pymysql.connect(host = '172.18.103.142' # 连接名称，默认127.0.0.1 ## 172.18.100.166
,user = 'root' # 用户名
,passwd='boke123' # 密码
,port= 3306 # 端口，默认为3306
,db='funddata' # 数据库名称
,charset='utf8' # 字符编码
)
cur = conn.cursor() # 生成游标对象


sql1 = 'select * from funddata.fundnetvalue where Date>="'+str(sd)[:10]+'" and Date<="'+str(ed)[:10]+' "'
cur.execute(sql1) # 执行SQL语句
data = cur.fetchall() # 通过fetchall方法获得数据
Tnv = pd.DataFrame(data,columns = ['UID','Date','FundCode','FundName','NetValue','AccValue','Source','Comment'])
Tnv.sort_values(by = 'Date',inplace=True)
Tnv.index = pd.to_datetime(Tnv['Date'])
nvdata = Tnv.copy()

# 根据数据的净值更新表格 请勿修改for循环中的部分
for ii in range(nvdata.shape[0]):
    ttmp = nvdata.iloc[ii,:]
    if ttmp['FundCode'][-1] in ['A','B','C']:
        fundinfo['寻找'] = fundinfo['份额代码'] +  ttmp['FundCode'][-1]
        fundinfo['寻找_1'] = fundinfo['产品名称']
        try:
            fundname = fundinfo[fundinfo['寻找'] == ttmp['FundCode']]['产品名称'].values[0]
        except:
            try:
                fundname = fundinfo[fundinfo['寻找_1'] == ttmp['FundName']]['产品名称'].values[0]
            except:
                # print('ttmp:',  ttmp['FundName'])
                fundname_1 = fuzzy_match(fundinfo['寻找_1'], ttmp['FundName'])
                # print('fundname_1:', fundname_1)
                if fundname_1 != 0:
                    fundname = fundname_1
                else:
                    continue
    else:
        try:
            fundname = fundinfo[fundinfo['产品代码'] == ttmp['FundCode']]['产品名称'].values[0]
        except:
            try:
                fundname = fundinfo[fundinfo['寻找_1'] == ttmp['FundName']]['产品名称'].values[0]
            except:
                fundname_1=fuzzy_match(fundinfo['寻找_1'],ttmp['FundName'])
                # print('fundname_1:', fundname_1)
                if fundname_1 != 0:

                    fundname=fundname_1
                else:
                    continue

    hold = []

    for xx in netValueSheet.columns:

        pa = '（.+'
        try:
            tichu = re.findall(pa,xx)[0]

        except:
            tichu = ''
            pass
        rr = xx.replace(tichu,'')
        if fundname in rr:
            hold.append(xx)
        else:
            if fuzzy_match(fundname,rr,list=False,na=False) !=0:
                hold.append(xx)
    #     print('hold：',hold)
    # print(ttmp['Date'],hold,ttmp['NetValue'])
    netValueSheet.loc[pd.Timestamp(ttmp['Date']),hold] = ttmp['NetValue']


### 下面部分都是处理表格格式的
##  把零空出来
netValueSheet.replace(0,np.nan,inplace=True)
sharesSheet.replace(np.nan,0,inplace=True)

dividendSheet.replace(np.nan,0,inplace=True)
cashSheet.replace(np.nan,0,inplace=True)
RewardSheet.replace(np.nan,0,inplace=True)
BackMoneySheet.replace(np.nan,0,inplace=True)

## 砍断后面的
## 最后生成文件部分 打包成一个就好了
netValueSheet = netValueSheet.loc[:min(netValueSheet.index[-1],ed),:]
sharesSheet= sharesSheet.loc[:min(sharesSheet.index[-1],ed),:]
dividendSheet= dividendSheet.loc[:min(dividendSheet.index[-1],ed),:]
cashSheet= cashSheet.loc[:min(cashSheet.index[-1],ed),:]
RewardSheet= RewardSheet.loc[:min(RewardSheet.index[-1],ed),:]
BackMoneySheet= BackMoneySheet.loc[:min(BackMoneySheet.index[-1],ed),:]



checklist = [sharesSheet,dividendSheet,cashSheet,RewardSheet,BackMoneySheet]
for vv in range(len(checklist)):

    kongque = list(set(netValueSheet.index)-set(checklist[vv].index) )
    for kk in kongque:
        checklist[vv].loc[kk,:] = 0

sharesSheet =    checklist[0].copy()
sharesSheet.sort_index(inplace=True)
sharesSheet.columns = netValueSheet.columns

dividendSheet =    checklist[1].copy()
dividendSheet.sort_index(inplace=True)
dividendSheet.columns = netValueSheet.columns

cashSheet =    checklist[2].copy()
cashSheet.sort_index(inplace=True)
cashSheet.columns = netValueSheet.columns

RewardSheet =    checklist[3].copy()
RewardSheet.sort_index(inplace=True)
RewardSheet.columns = netValueSheet.columns


BackMoneySheet =    checklist[4].copy()
BackMoneySheet.sort_index(inplace=True)
BackMoneySheet.columns = netValueSheet.columns


 ##################################################################################################################

# 输出的这个是个根据云文档上的申赎记录而产生的类 大表格的东西，它把云文档上的申赎记录变成了跟大表格一样的格式。 如果有新申购的产品，那么在大表格中“净值”sheet要记得贴上。
fn = '合计参考估值表-报表生成'+ str(ed)[:10]+ '测试1.xlsx'
with pd.ExcelWriter(fn) as writer:
    netValueSheet.to_excel(writer,sheet_name = '净值')
    sharesSheet.to_excel(writer,sheet_name = '申赎份额')
    dividendSheet.to_excel(writer,sheet_name = '分红与计提报酬（份额）')
    cashSheet.to_excel(writer,sheet_name = '分红与计提报酬（现金）')
    RewardSheet.to_excel(writer,sheet_name = '赎回业绩报酬')
    BackMoneySheet.to_excel(writer,sheet_name = '赎回到款')













