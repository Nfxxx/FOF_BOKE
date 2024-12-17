# -*- coding: utf-8 -*-
"""
Created on Thu May 19 12:38:33 2022

@author: zhangqilin
"""


import eml_parser
import base64
import os 
import re 
import pandas as pd
import random
import zipfile
folderpath ='C:\\Invest\\邮件读档\\' 
savepath = 'C:\\Invest\\净值存储\\'
#fn = '国泰君安证券资产托管发送：SAP智龙18号私募证券投资基金【赛韵网络科技（上海）有限公司】TA虚拟净值_2022-05-18.eml'
#fn = 'C:\\Invest\\邮件读档\\中邮永安景上源1号私募证券投资基金_赛韵网络科技（上海）有限公司_虚拟计提净值表_20220518.eml'


# 招商会发压缩包。。   

def RandomChar(n):
    fillcontent = '甲乙丙丁戊己庚辛壬癸子丑寅卯辰巳午未申酉戌亥'
    # 有些托管人的邮件附件文件名上既没有日期也没有产品名称信息 直接保存会导致文件名重复进行反复覆盖
    # 通过随机在其文件名中添加中文字符进行识别  因为某些文件在读取时会使用正则表达式 因此使用中文字符不影响原来的文件解析
    CharStr = ''
    for x in range(n):
        l = random.randint(0,len(fillcontent)-1)
        CharStr = CharStr + fillcontent[l]
    return CharStr
      


def OutputAttach(fn,outputpath):

    
    ziplist = []
    assert('eml' in fn[-3:],'Not Eml File')
    with open(fn,'rb') as f:
        a = f.read()
    pattern1 = re.compile('@(.+\.)+com')
    

    eml = eml_parser.eml_parser.decode_email_b(a,True,True)
    p1 =  re.findall(pattern1,eml['header']['from'])
    print(eml['header']['subject'])
    fntail =''
    
    if not 'ac_autoreport@cicc.com.cn' in eml['header']['from']:
        if eml.get('attachment'):
            for i in eml['attachment']:
                filename =  i['filename'].replace(' ','')
                if ('.rar' in filename or '.zip' in filename) and ('yywbfa@cmschina.com.cn' in eml['header']['from']) :
                    x =base64.b64decode(i['raw'])
                    if ('产品净值情况' in filename) or ('集合计划' in filename) or ('净值信息' in filename) :
                        fntail = RandomChar(8)
                    fname = outputpath+p1[0][:-1]+'_'+fntail+filename
                    fname.replace(' ','')
                    fname.replace(' ','')
                    fname.replace(' ','')
                    fname2 = fname.replace(' ','')
                    if not os.path.exists(fname2):
                        with open(fname2,'wb') as f:
                            f.write(x)
                        #print(fname)
                        ziplist.append(fname2)
                    else:
                        print('already exists_'+ fname2)
             
                if ('.xls' or 'xlsx') in filename:
                    x =base64.b64decode(i['raw'])
                    if ('产品净值情况' in filename) or ('集合计划' in filename) :
                        fntail = RandomChar(8)
                    fname = outputpath+p1[0][:-1]+'_'+fntail+filename
                    fname.replace(' ','')
                    fname.replace(' ','')
                    fname2 = fname.replace(' ','')
                    if not os.path.exists(fname2):
                        with open(fname2,'wb') as f:
                            f.write(x)
                        #print(fname)
                    else:
                        print('already exists_'+ fname2)
    else: # 处理中金这个不带附件的 生成一个表格
        T = pd.read_html(eml['body'][0]['content'])[0]
        fname = outputpath+'cicc_'+fntail+eml['header']['subject']+'.xls'
        fname.replace(' ','')
        fname.replace(' ','')
        fname.replace(' ','')
        fname2 = fname.replace(' ','')
        if not os.path.exists(fname2):
            T.to_excel(fname2)
        else:
            print('already exists_'+ fname2)
    return ziplist



def zipout(zipf):
    pa = re.compile('\\\\[a-z]{4,8}_')
    p1 = re.findall(pa,zipf)
    zf = zipfile.ZipFile(zipf,'r') 
    
    
    for fm in zf.namelist():
        zf.extract(fm,savepath+p1[0][1:-1]+'\\\\')
    zf.close()
                

    

    
#OutputAttach(folderpath+fn,folderpath)
    
import datetime
#t = os.path.getmtime(folderpath+files[2])
#datetime.datetime.fromtimestamp(t)

# 根据文件的创建时间 从什么时候开始读取文件  防止像之前那样一下读所有
startreadtime = '2023-12-23'
ProblemRec = []
dirList=[]
files=os.listdir(folderpath) #文件夹下所有目录的列表
zlist=[]
for f in files:
    if 'eml' in f[-3:]:
        if datetime.datetime.fromtimestamp(os.path.getmtime(folderpath+f))>=pd.Timestamp(startreadtime):
            
            try:
                z = OutputAttach(folderpath+f,savepath)
                if len(z)>0:
                    zlist.extend(z)
            except:
                print('Error!'+folderpath+f)
                ProblemRec.append(folderpath+f)
                pass
for x in zlist:
    try:
        zipout(x)
    except:
        pass
        


'''

### readzip  
    
def unzip_all_files(zip_file_path, extract_path ='C:\\Invest\\数据处理\\周报月报\\'):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        for file in zip_ref.namelist():
            if file.endswith('/'):
                continue
            print(file)
            zip_ref.extract(file, extract_path)    
    
fnlist=os.listdir('C:\\Invest\\净值存储\\')   
files =fnlist.copy()
zip_files = [f for f in files if f.endswith('.zip')]
for xf in zip_files:
    if 'zip' in xf:
        unzip_all_files('C:\\Invest\\净值存储\\'+xf)




zip_ref.extract(file, extract_path ='C:\\Invest\\数据处理\\周报月报\\')    






import os

path = os.getcwd() # 获取当前文件夹路径
files = os.listdir(path) # 获取当前文件夹下所有文件名

for file in files:
    if not os.path.isdir(file): # 判断是否是文件夹
        new_name = file.replace('old_string', 'new_string') # 将文件名中的old_string替换为new_string
        os.rename(file, new_name) # 重命名文件



'''



























    