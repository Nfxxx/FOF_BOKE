# -*- coding: utf-8 -*-
"""
Created on Thu May 19 13:55:29 2022

@author: zhangqilin
"""

import os
import pandas as pd
import time
import datetime
import re

import matplotlib.pyplot as plt
import numpy as np

from pylab import mpl  
    
mpl.rcParams['font.sans-serif'] = ['Microsoft YaHei'] # 指定默认字体：解决plot不能显示中文问题

mpl.rcParams['axes.unicode_minus'] = False # 解决保存图像是负号'-'显示为方块的问题


fn = 'C:\\Invest\\净值存储\\gtja_SAP智龙18号私募证券投资基金【赛 韵网络科技（上海）有限公司】TA 虚拟业绩报酬流水_20220519.xlsx'
fn = 'C:\\Invest\\净值存储\\cmschina_集合计划每日净值表.xls'
fn = 'C:\\Invest\\净值存储\\citics_【基金净值表现估算】 SQW758_希格 斯水手3号私募证券投资基金_2022-05 -18.xlsx'
fn = 'C:\\Invest\\净值存储\\ebscn_波克平衡多策略1号私募证券投资基金【均泰华山二号私募证券投资基金】20231108.xlsx'

folderpath ='C:\\Invest\\邮件读档\\' 
savf = 'C:\\Invest\\净值存储\\'


class ReadNV():

    def __init__(self,savf):

        self.savf = savf
        self.RecTiao = []
        self.Liushui=False
        self.M = []

        
     
    def ReadManyNV(self,FNlist,targetlist = []):
        for ii in FNlist:
            
             
            print('start reading '+ ii)
            try:
                self.ReadNetValue(ii)
            except:
                pass
                    
        
    def ReadNetValue(self,FN):
        if ('.xls' in FN) or ('.xlxs' in FN):
            if 'cmsc' in FN:   #招商
                self.ReadCMS2(FN,self.Liushui)
            if 'citi' in FN:    #中信
                self.ReadCITICS(FN,self.Liushui)
            if 'gtja' in FN:    #国君
                self.ReadGTJA(FN,self.Liushui)
            if 'guos'in FN:     #国信
                self.ReadGuosen(FN,self.Liushui)
            if 'ebsc'in FN:     #光大
               # print('GD')
                self.ReadEverB(FN,self.Liushui)
            if 'htsc' in FN:    #华泰
                self.ReadHT(FN,self.Liushui)
            if 'cnht' in FN:    #恒泰
                self.ReadHengTai(FN,self.Liushui)
            if 'cicc' in FN:    #中金
                self.ReadCicc(FN,self.Liushui)
            if 'gjdf' in FN:    #国金道富
                self.ReadGJDF(FN,self.Liushui)
            if 'csc' in FN:    #中信建投
                self.ReadZXJT(FN,self.Liushui)
            if 'gf' in FN:    #广发
                self.ReadGF(FN,self.Liushui)
            if 'xyzq' in FN:    #兴业
                self.ReadXY(FN,self.Liushui)

    
    
    def OutputRecTable(self):
        self.M = pd.DataFrame(self.RecTiao,columns = ['日期','产品代码','产品名称','单位净值','累计净值','来源文件'])
        #self.M.drop_duplicates(subset=['日期','产品代码','产品名称','单位净值','累计净值'])
        self.M.drop_duplicates(subset=['日期','产品代码','单位净值'])

    def ReadXY(self,fn,Liushui=False):
        D = pd.read_excel(fn,header=0)
        rq0 =str(int(D['基金净值日期'].values[0]))
        rq = rq0[:4]+'-'+rq0[4:6]+'-'+rq0[-2:]
        UnderlyingName = D['基金名称'].values[0]
        NetValue = round(float( D['单位净值'].values[0]),4)
        AccNetValue =round(float( D['累计净值'].values[0]),4)
        FundCode  = D['协会备案代码'].values[0]
        self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])
        
    
    def ReadGF(self,fn,Liushui=False):
        D = pd.read_excel(fn,header=1)
        
        titleline = D[D.iloc[:,0]=='净值日期'].index[0]
        dataline = titleline+1
        tmp = D.iloc[dataline:dataline+1,:]
        tmp.columns =  D.iloc[titleline,:]
        
        rq = tmp['净值日期'].values[0]
        UnderlyingName = tmp['产品名称'].values[0]
        NetValue = round(float( tmp['单位净值'].values[0]),4)
        AccNetValue =round(float( tmp['累计单位净值'].values[0]),4)
        FundCode  = tmp['协会备案编号'].values[0]
        self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])


     
    def ReadCicc(self,fn,Liushui=False):
        D = pd.read_excel(fn,header=1)
        for ii in range(D.shape[0]):
            rq = D['净值日期'].iloc[ii]
            UnderlyingName = D['产品名称'].iloc[ii]
            NetValue = round(float(D['单位净值'].iloc[ii]),4)
            AccNetValue = round(float(D['累计净值'].iloc[ii]),4)
            FundCode  = D['产品代码'].iloc[ii]
            self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])
    
    
    
    def ReadZXJT(self,fn,Liushui=False):
        D = pd.read_excel(fn)
        if '虚拟' not in fn:
            rq = D.iloc[1,0]
            UnderlyingName = D.loc[D[D.columns[0]]=='基金名称：',D.columns[1]].values[0]
            NetValue = float(D.loc[D[D.columns[0]]=='基金份额净值：',D.columns[1]].values[0])
            AccNetValue = float(D.loc[D[D.columns[0]]=='基金份额累计净值：',D.columns[1]].values[0])
            FundCode  = D.loc[D[D.columns[0]]=='基金代码：',D.columns[1]].values[0]
            self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])
            
    def ReadGJDF(self,fn,Liushui=False):
        D = pd.read_excel(fn)
        if D.shape[0]<5:
            for ii in range(D.shape[0]):
                rq = D['净值日期'].iloc[ii]
                UnderlyingName = D['基金名称'].iloc[ii]
                NetValue = round(float(D['单位净值'].iloc[ii]),4)
                AccNetValue = round(float(D['累计单位净值'].iloc[ii]),4)
                FundCode  = D['基金代码'].iloc[ii]
                self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])
        


        
    def ReadHengTai(self,fn,Liushui=False):   
        if '估值表' not in fn:
            D = pd.read_excel(fn,header=2)
            if D.shape[0]<10:
                for ii in range(D.shape[0]):
                    rqtmp = str(D['净值日期'].iloc[ii])
                    rq = rqtmp[:4]+'-'+rqtmp[4:6]+'-'+rqtmp[-2:]
                    #rq = D['净值日期'].iloc[ii]
                    UnderlyingName = D['基金名称'].iloc[ii]
                    NetValue = round(float(D['试算前单位净值'].iloc[ii]),4)
                    AccNetValue = round(float(D['试算前累计单位净值'].iloc[ii]),4)
                    FundCode  = D['基金代码'].iloc[ii]
                    self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])
        else:
             D = pd.read_excel(fn)
             pa = re.compile('_\w{3,8}_')
             p1= re.findall(pa,fn)
             FundCode = p1[0][1:-1]
             pa2 = re.compile('_.{10}估')
             p2 = re.findall(pa2,fn)
             rq =  p2[0][1:-1]
             pa3 = re.compile('__.{4,}金_')
             p3 = re.findall(pa3,D.iloc[0,0])
             UnderlyingName =  p3[0][3:-1]
             
             for ii in range(D.shape[0]):
                if '单位净值' in str(D.iloc[ii,0]):
                    NetValue = round(float(D.iloc[ii,1]),4)
                if '累计单位净值' in str(D.iloc[ii,0]):
                    AccNetValue = round(float(D.iloc[ii,1]),4)
             self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])
             
            
        
    

    def ReadHT(self,fn,Liushui=False):
        if '净值表' in fn:
            D = pd.read_excel(fn,header=0)
            
            rq = D['日期'].values[0]
            UnderlyingName = D['资产名称'].values[0]
            NetValue = round(float( D['资产份额净值(元)'].values[0]),4)
            AccNetValue =round(float( D['资产份额累计净值(元)'].values[0]),4)
            FundCode  = D['资产代码'].values[0]
            self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])

            
            
        
        
        if '虚拟' in fn:
            D = pd.read_excel(fn,header=1)
            for ii in range(D.shape[0]):
                rqtmp = str(D['净值日期'].iloc[ii])
                rq = rqtmp[:4]+'-'+rqtmp[4:6]+'-'+rqtmp[-2:]
                #rq = D['净值日期'].iloc[ii]
                UnderlyingName = D['基金名称'].iloc[ii]
                NetValue = round(float(D['单位净值'].iloc[ii]),4)
                AccNetValue = round(float(D['累计单位净值'].iloc[ii]),4)
                FundCode  = D['基金代码'].iloc[ii]
                self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])
        if '估值表' in fn:
            D = pd.read_excel(fn)
            pa1 = re.compile('^\w{3,7}_')
            pa2 = re.compile('_\w{1,}基金_')
            FundCode = re.findall(pa1,D.columns[0])[0][:-1]
            UnderlyingName = re.findall(pa2,D.columns[0])[0][1:-1]
            for ii in range(D.shape[0]):
                if '日期' in str(D.iloc[ii,0]):
                    rq = D.iloc[ii,0][-10:]
                if '单位净值' == str(D.iloc[ii,0]):  # 华泰估值表里会有 ‘累计单位净值’ ‘期初单位净值’这些东西
                    NetValue = round(float(D.iloc[ii,1]),4)
                if '累计单位净值' in str(D.iloc[ii,0]):
                    AccNetValue = round(float(D.iloc[ii,1]),4)
            self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])


        
        if '净值表' in fn:
            D = pd.read_excel(fn)
            for ii in range(D.shape[0]):                
                rq = D['日期'].iloc[ii]
                UnderlyingName = D['资产名称'].iloc[ii]
                NetValue = round(float(D['资产份额净值(元)'].iloc[ii]),4)
                AccNetValue = round(float(D['资产份额累计净值(元)'].iloc[ii]),4)
                FundCode  = D['资产代码'].iloc[ii]
                self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])

            
            
        


    
    def ReadEverB(self,fn,Liushui=False):
        D = pd.read_excel(fn)     
        if '【' in fn:
            print(fn)
            tmp = D.iloc[1,:]
            tmp.index =D.iloc[0,:].tolist()
            rq0 = tmp['净值日期']
            rq = rq0[:4]+'-'+rq0[4:6]+'-'+rq0[-2:]
            UnderlyingName = tmp['基金名称']
            NetValue = round(float(tmp['试算前单位净值']),4)
            AccNetValue = round(float(tmp['试算前累计单位净值']),4)
            FundCode  = tmp['基金代码']
            self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])
            
        
        
        if '净值表' in fn:
            tmp = D.iloc[1,:]
            rq = tmp[tmp.str.contains('-').fillna(False)].values[0][-10:]
            UnderlyingName = D.iloc[5,0]
            NetValue = round(float(D.iloc[5,-2]),4)
            AccNetValue = round(float(D.iloc[5,-1]),4)
            FundCode  = D.iloc[5,-3]
            self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])
    
    
    
    
    def ReadCITICS(self,fn,Liushui=False):
        D = pd.read_excel(fn)
        
        
        
        if '虚拟' not in fn:
            for ii in range(D.shape[0]):
                rq = D['日期'].iloc[ii]
                if len(rq)<4:
                    break
                UnderlyingName = D['资产名称'].iloc[ii]
                NetValue = round(float(D['资产份额净值(元)'].iloc[ii]),4)
                AccNetValue = round(float(D['资产份额累计净值(元)'].iloc[ii]),4)
                FundCode  = D['资产代码'].iloc[ii]
                if '(' in FundCode:
                    FundCode = FundCode[:FundCode.find('(')]

                self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])
                #InvestorName = D['客户姓名'].iloc[ii]
                #RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,InvestorName,fn])
        else:
            if '基金净值表现估算' in fn:
                       
                rq = D['日期'].values[0]
                UnderlyingName = D['资产名称'].values[0]
                NetValue = round(float( D['资产份额净值(元)'].values[0]),4)
                AccNetValue =round(float( D['资产份额累计净值(元)'].values[0]),4)
                FundCode  = D['协会备案代码'].values[0]
                self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])
                
                
            
            else:
                for ii in range(1):
                    rq = D['估值基准日'].iloc[ii]
                    if len(rq)<4:
                        break
                    UnderlyingName = D['产品名称'].iloc[ii]
                    NetValue = round(float(D['实际净值'].iloc[ii]),4)
                    AccNetValue = round(float(D['实际累计净值'].iloc[ii]),4)
                    FundCode  = D['产品代码'].iloc[ii]
                    self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])
            

        
    
    def ReadGTJA(self,fn,Liushui=False):
        D = pd.read_excel(fn)
        if '流水' not in fn:
            
            if D.shape[0]>1:
                hang = D.shape[0]-1
                for ii in range(hang):# 国军的表格最后一行是一个很大的说明
                    rq = D['净值日期'].iloc[ii]
                    UnderlyingName = D['基金名称'].iloc[ii]
                    NetValue = round(float(D['单位净值'].iloc[ii]),4)
                    AccNetValue = round(float(D['累计单位净值'].iloc[ii]),4)
                    FundCode  = D['基金代码'].iloc[ii]
                    self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])

                
                
                
            else:
                hang = D.shape[0]
                for ii in range(hang):# 国军的表格最后一行是一个很大的说明
                    rq = D['净值日期'].iloc[ii]
                    UnderlyingName = D['产品名称'].iloc[ii]
                    NetValue = round(float(D['单位净值'].iloc[ii]),4)
                    AccNetValue = round(float(D['累计单位净值'].iloc[ii]),4)
                    FundCode  = D['产品代码'].iloc[ii]
                    self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])
                
                
                #InvestorName = D['客户姓名'].iloc[ii]
                #RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,InvestorName,fn])
        #else:
        #    if self.Liushui:
        #        rq = D['净值日期'].iloc[ii]
#                UnderlyingName = D['基金名称'].iloc[ii]
#                NetValue = round(float(D['单位净值'].iloc[ii]),4)
#                AccNetValue = round(float(D['累计单位净值'].iloc[ii]),4)
#                FundCode  = D['基金代码'].iloc[ii]
#                self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])

                 
        
    def ReadOrient(self,fn):
        if '公告' in fn:
            D = pd.read_excel(fn)
            rq = str(D.iloc[1,0])[:10]
            UnderlyingName = D.iloc[5,2]
            NetValue = round(float(D.iloc[8,2]),4)  
            AccNetValue = round(float(D.iloc[9,2]),4)  
            FundCode = D.iloc[4,2]
            self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])
            
    
    
    
    def ReadEssence(self,fn,Liushui=False):
        D = pd.read_excel(fn)
        for ii in range(D.shape[0]):
            rq = D['日期'].iloc[ii]
            rq = str(rq)
            if '-' not in rq:
                rq = rq[:4]+'-'+rq[4:6]+'-'+rq[-2:]
            
            UnderlyingName = D['产品名称'].iloc[ii]
            FundCode =  D['产品代码'].iloc[ii]
            NetValue = round(float(D['单位净值'].iloc[ii]),4)
            AccNetValue = round(float(D['累计单位净值'].iloc[ii]),4)
            self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])
 
    
    
    def ReadGuosen(self,fn,Liushui=False):
        D = pd.read_excel(fn)
        if '虚拟' not in fn:
            for ii in range(D.shape[0]):
                rq = D['日期'].iloc[ii]
                UnderlyingName = D['产品名称'].iloc[ii]
                FundCode = D['产品代码'].iloc[ii]
                NetValue = round(float(D['单位净值'].iloc[ii]),4)
                AccNetValue = round(float(D['累计单位净值'].iloc[ii]),4)
                self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])
        else:
            for ii in range(D.shape[0]):
                rq = D['净值日期'].iloc[ii]
                UnderlyingName = D['产品名称'].iloc[ii]
                FundCode = D['产品代码'].iloc[ii]
                NetValue = round(float(D['计提前单位净值'].iloc[ii]),4)
                AccNetValue = round(float(D['累计单位净值'].iloc[ii]),4)
                self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])
            

    
    def ReadCMS2(self,fn,Liushui=False):
        if '计划每日' in fn:
            D = pd.read_excel(fn,header=2)
            pa2 = re.compile('20\d{2}')
            pa3 = re.compile('年\d{1,2}月')
            pa4 = re.compile('月\d{1,2}日')    
            for ii in range(D.shape[0]):
                rq1 = D['日期'].iloc[ii]
                print(rq1)
                rq = datetime.date(int(re.findall(pa2,rq1)[0]),int(re.findall(pa3,rq1)[0][1:-1]),int(re.findall(pa4,rq1)[0][1:-1])).isoformat()
                UnderlyingName = D['产品名称'].iloc[ii]
                FundCode = D['产品代码'].iloc[ii]
                NetValue = round(float(D['单位净值'].iloc[ii]),4)
                AccNetValue = round(float(D['累计单位净值'].iloc[ii]),4)
                self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])
        if '发送每日' in fn:
            D = pd.read_excel(fn)
            pa2 = re.compile('20\d{2}')
            pa3 = re.compile('年\d{1,2}月')
            pa4 = re.compile('月\d{1,2}日') 
            rq =D.iloc[1,0]
            rq = datetime.date(int(re.findall(pa2,rq)[0]),int(re.findall(pa3,rq)[0][1:-1]),int(re.findall(pa4,rq)[0][1:-1])).isoformat()
            for ii in range(2,D.shape[0]):
                if '基金代码' in D.iloc[ii,0]:
                    FundCode = D.iloc[ii,1]
                if '基金名称' in D.iloc[ii,0]:
                    UnderlyingName = D.iloc[ii,1]
                if '基金份额净值' in D.iloc[ii,0]:
                    NetValue = round(float(D.iloc[ii,1]),4)
                if '基金份额累计净值' in D.iloc[ii,0]:
                    AccNetValue = round(float(D.iloc[ii,1]),4)
            self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])

            
            
        if '虚拟计提' in fn:
            D = pd.read_excel(fn)

            for ii in range(D.shape[0]):
                rqtmp = str(D['业务日期'].iloc[ii])
                rq = rqtmp[:4]+'-'+rqtmp[4:6]+'-'+rqtmp[-2:]
                UnderlyingName = D['产品名称'].iloc[ii]
                FundCode = D['产品代码'].iloc[ii]
                NetValue = round(float(D['单位净值'].iloc[ii]),4)
                AccNetValue = round(float(D['累计单位净值'].iloc[ii]),4)
                self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])
        if '估值表' in fn:
            D = pd.read_excel(fn)
            pa1 = re.compile('^[0-9a-zA-Z]{3,}[\u4e00-\u9fa5]')
            pa2 = re.compile('\w{1,}基金')
            FundCode = re.findall(pa1,D.columns[0])[0][:-1]
            UnderlyingName = re.findall(pa2,D.columns[0])[0][len(FundCode):]
            for ii in range(D.shape[0]):
                if '日期' in str(D.iloc[ii,0]):
                    rq = D.iloc[ii,0][-10:]
                if '单位净值' == str(D.iloc[ii,0]):
                    NetValue = round(float(D.iloc[ii,1]),4)
                if '累计单位净值' in str(D.iloc[ii,0]):
                    AccNetValue = round(float(D.iloc[ii,1]),4)
            self.RecTiao.append([rq,FundCode,UnderlyingName,NetValue,AccNetValue,fn])

             
            
        
        sd = pd.Timestamp(sd) 
        ed = pd.Timestamp(ed) 

        ffn = '合计参考估值表-报表生成20230428 - 副本 (2).xlsx'
       
        D = pd.read_excel(ffn,sheet_name='净值',index_col=0)
        D2 = D.copy()
        
        rqlist = self.M['日期'].unique()
        rqlist.sort()
        for ii in rqlist:
           tmpi = pd.Timestamp(int(ii[:4]),int(ii[5:7]),int(ii[8:]))
           if tmpi not in D.index.tolist():
                dd = pd.DataFrame([],columns = D2.columns, index = [tmpi])
                D2 =D2.append(dd)
                
                tmp = D2[D2.index==tmpi]
                tmpdata =  self.M[self.M['日期']==ii][['日期','产品代码','单位净值']].drop_duplicates()
                codelist = tmpdata['产品代码'].drop_duplicates().tolist()
                for xx in codelist:
                    if xx in tmp.columns:
                        try:
                            D2.loc[[tmpi],[xx]] =    tmpdata[tmpdata['产品代码']==xx]['单位净值'].values
                        except:
                            print(str(tmpi)+str(xx))
                            pass
                        # 检查多个主体
                        for vv in range(1,8):
                            clname =  xx+'.'+str(vv)
                            if clname in tmp.columns:
                                D2.loc[[tmpi],[clname]] =    tmpdata[tmpdata['产品代码']==xx]['单位净值'].values
        D2.to_excel('nn.xlsx')         

    

    def IntoFormatTable(self,ffn):
        D = pd.read_excel(ffn,sheet_name='净值',index_col=0)
        D2 = D.copy()
        rqlist = self.M['日期'].unique()
        rqlist.sort()
        for ii in rqlist:
           tmpi = pd.Timestamp(int(ii[:4]),int(ii[5:7]),int(ii[8:]))
           if tmpi not in D.index.tolist():
                dd = pd.DataFrame([],columns = D2.columns, index = [tmpi])
                D2 =D2.append(dd)
                
                tmp = D2[D2.index==tmpi]
                tmpdata =  self.M[self.M['日期']==ii][['日期','产品代码','单位净值']].drop_duplicates()
                codelist = tmpdata['产品代码'].drop_duplicates().tolist()
                for xx in codelist:
                    if xx in tmp.columns:
                        try:
                            D2.loc[[tmpi],[xx]] =    tmpdata[tmpdata['产品代码']==xx]['单位净值'].values
                        except:
                            print(str(tmpi)+str(xx))
                            pass
                        # 检查多个主体
                        for vv in range(1,8):
                            clname =  xx+'.'+str(vv)
                            if clname in tmp.columns:
                                D2.loc[[tmpi],[clname]] =    tmpdata[tmpdata['产品代码']==xx]['单位净值'].values
        D2.to_excel('nn.xlsx')
            

def IntoFormatTable2(ffn,nvfn,sd,ed):
    sd = pd.Timestamp(sd)
    ed = pd.Timestamp(ed)
    D = pd.read_excel(ffn,sheet_name='净值',index_col=0,)
    nvdata0 = pd.read_excel(nvfn,dtype = {1:"datetime64"})
    nvdata = nvdata0[(nvdata0['日期']>=sd) &  (nvdata0['日期']<=ed)]
    
    for ii in range(nvdata.shape[0]):
        nvdata.iloc[ii,:]
        pass
       # 未完成 
        
    
    
    
    

                                      
        
def findAllFile0(base):    
    for root, ds, fs in os.walk(base):
        for f in fs:
            fullname = os.path.join(root, f)
            yield fullname

def findAllFile(base,targetlist=[]):
    irec = []
    
    if len(targetlist)==0:        
        for i in findAllFile0(base):
            irec.append(i)
    else:
        for i in findAllFile0(base):
            for vv in targetlist:
                if vv in i:
                    irec.append(i)    
    return irec
    


def PeriodRtn(RecFrame,stday,edday):
    RtnRec= []
    sttmp =RecFrame[RecFrame['日期']==stday]
    edtmp = RecFrame[RecFrame['日期']==edday]
    stlist = sttmp['产品代码'].tolist()
    edlist = edtmp['产品代码'].tolist()
    jiaoji = list(set(stlist) & set(edlist))
    for ii in jiaoji:
        FundName = edtmp[edtmp['产品代码']==ii]['产品名称'].values[0]
        FundName2 = FundName.replace('证券','')
        FundName2 = FundName2.replace('基金','')
        FundName2 = FundName2.replace('投资','')
        FundName2 = FundName2.replace('私募','')
        Rtn = round(edtmp[edtmp['产品代码']==ii]['累计净值'].values[0]/sttmp[sttmp['产品代码']==ii]['累计净值'].values[0]-1,4)
        RtnRec.append([FundName2,Rtn])
    Res = pd.DataFrame(RtnRec,columns=['FundName','PeriodRtn'])
    Res2 = Res.sort_values(by='PeriodRtn',ascending=False)
    fn ='PeriodRtn_'+stday+'_'+edday+'.xlsx'
    Res2.to_excel(fn)

    plt.rcdefaults()
    plt.rcParams['font.sans-serif']=['Microsoft YaHei'] #用来正常显示中文标签
    plt.rcParams['axes.unicode_minus'] = False #用来正常显示负号
    
    fig, ax = plt.subplots()
    people = Res2['FundName']
    y_pos = np.arange(len(people))
    performance = Res2['PeriodRtn']
    
    b = ax.barh(y_pos, performance, align='center')
    ax.set_yticks(y_pos)
    ax.set_yticklabels(people)
    ax.invert_yaxis()# labels read top-to-bottom
    
    for rect in b:
        w = rect.get_width()
        if w>=0:
            ax.text(w,rect.get_y()+rect.get_height()/2,str(round(w*100,4))+'%', ha='left',va='center',fontsize=4)
        else:
            ax.text(w,rect.get_y()+rect.get_height()/2,str(round(w*100,4))+'%', ha='right',va='center',fontsize=4)
            
    
    ax.set_yticklabels(people, fontsize=5)
    ax.set_xlabel('Rtn')
    ax.set_title(str(Res2.shape[0])+'_PeriodRtn_'+stday+'_'+edday)
    plt.grid()
    plt.show()
    fig.savefig('PeriodRtn_'+stday+'_'+edday+'.svg')


##################################################


a = ReadNV(savf)
#ff =  fn
#a.ReadNetValue(ff)


dirList=[]
folderpath = 'C:\\Invest\\净值存储\\'
savf = 'C:\\Invest\\净值存储\\'
#files=os.listdir(folderpath)
files =findAllFile(folderpath,targetlist = [])
#b = [folderpath+ii for ii in files]
b = files

a.ReadManyNV(b)
a.OutputRecTable()

#####################################
stday = '2024-01-01'
edday = '2025-01-12'

X = a.M    
    
X['UID'] = X['日期']+'_' + X['产品代码']

dd = X.drop_duplicates(subset=['UID'],keep='first')    
dd.index = dd['UID']
dd.drop(columns=['UID'],inplace=True)


#ddnew = dd[(dd['来源文件'].str.contains('ebsc'))]    #存某个托管的

ddnew = dd[(dd['日期']>=stday) & (dd['日期']<=edday)]   #  存一段时间的

ddnew.to_excel('clean_sss2.xlsx')
    
    
    
    
    