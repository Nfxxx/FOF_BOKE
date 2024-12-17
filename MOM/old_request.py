
import pandas as pd
import warnings
import numpy as np
warnings.filterwarnings("ignore")
from datetime import datetime,timedelta
from utils import feishu_bot
def old_request():
    # lastDaiy = '20240126'
    # daily = '20240117'
    # run_daily = '20240118'
    yesterday = datetime.now() - timedelta(days=1)
    lastDaiy = yesterday.strftime('%Y%m%d')
    daily = datetime.now().strftime('%Y%m%d')
    run_daily=(datetime.now() + timedelta(days=1)).strftime('%Y%m%d')
    # print(daily,lastDaiy)
    hold = pd.read_excel(f'file/KF_parent_response/普通交易_持仓报表_{daily}_160000.xlsx')[['证券代码', '当前持仓']].copy()

    hold.rename(columns={'证券代码': 'secCode', '当前持仓': 'actNumber'}, inplace=True)
    hold = hold[hold['actNumber'] > 0]
    # print(hold[hold['secCode'] == 600715])
    # 当日所有PM 集合
    zyy = pd.read_csv(f'file/child/{daily}_500_zyy_request.csv')[
        ['secCode', 'targetNumber', 'market', 'sendTime', 'endTime']]
    zyy['logo'] = '500_zyy'
    zyy_old = zyy.copy()
    # print(zyy[zyy['secCode']== 600715])
    wxk = pd.read_excel(f'file/child/{daily}_500_wxk_request.xlsx').drop('Unnamed: 0', axis=1)[
        ['secCode', 'targetNumber', 'market', 'sendTime', 'endTime']]
    wxk['logo'] = '500_wxk'
    wxk_old = wxk.copy()
    # print(wxk[wxk['secCode'] == 2299])
    df = pd.DataFrame()
    df = df._append(zyy, ignore_index=True)
    df = df._append(wxk, ignore_index=True)
    marketCodeSide = pd.read_csv('file/eqt_1mbar/MarketCode2024.csv').drop(labels=['Unnamed: 0'], axis=1)
    # print(marketCodeSide)
    out_df = pd.DataFrame(columns=['secCode','logo', 'actNumber'])
    # print(len(hold['secCode'].unique()))
    # print(len(df['secCode'].unique()))
    lostName = set(hold['secCode'].unique()) - set(df['secCode'].unique())
    if len(lostName) > 0:
        # 昨日卖出失败的在这里添加回去
        print(lostName)
        magre_child = \
            pd.read_csv(f'file/self/{lastDaiy}_marge_Child.csv', encoding='gbk').drop(labels=['Unnamed: 0'], axis=1)[
                ['symbol', 'side', 'logo']]
        lost = magre_child[magre_child['symbol'].isin(lostName)].copy()
        lost.rename(columns={'symbol': 'secCode', }, inplace=True)
        lost_df = pd.merge(lost, hold[hold['secCode'].isin(lostName)], on='secCode', how='outer')
        lost_df['actNumber']=lost_df.apply(lambda x: x['actNumber']/len(lost_df[lost_df['secCode']==x['secCode']]),axis=1)
        lost_df = lost_df[['secCode', 'logo', 'actNumber']]
        # lost_df.loc[lost_df['secCode'] == 600715, 'logo'] = '500_zyy'
        # print(lost_df)
        not_find_df=lost_df['secCode'][lost_df.isna().any(axis=1)].tolist()
        # print(not_find_df)
        out_df = out_df._append(lost_df, ignore_index=True)
    # set_diff_df = pd.concat([hold, df]).drop_duplicates(keep=False)

    grouped = df.groupby(by=[df['secCode']])
    # 此处将全部子单合成初始大表，并将原始数据存储下来
    for name, group in grouped:

        secCode = group['secCode'].values[0]
        try:
            actNumber = hold['actNumber'][hold['secCode'] == secCode].values[0]
        except:
            actNumber =group['targetNumber'].values[0]
            # print(actNumber)
        group['actNumber'] = round(actNumber * group['targetNumber'] / (group['targetNumber'].sum()), 0).astype(int)
        # print(group)
        out_df = out_df._append(group[['secCode', 'logo', 'actNumber']], ignore_index=True)
    # out_df
    out_df.to_csv(f'file/self/{daily}_all_old.csv', encoding='gbk')
    zyy=out_df[out_df['logo']=='500_zyy']
    wxk=out_df[out_df['logo']=='500_wxk']
    df1 = pd.merge(zyy_old, zyy, on=['secCode'], how='outer')
    df1.rename(columns={'targetNumber': 'del_targetNumber'}, inplace=True)
    df1.rename(columns={'actNumber': 'old_targetNumber'}, inplace=True)
    df1.rename(columns={'logo_x': 'logo'}, inplace=True)
    df1['market']=df1.apply(lambda x: marketCodeSide['market'][marketCodeSide['symbol'] == x['secCode']].values[0], axis=1)
    df1['del_targetNumber'] = df1.apply(
        lambda x: ((x['del_targetNumber']) if (x['del_targetNumber'] > 10) else (x['old_targetNumber'])), axis=1)
    df1 = df1.fillna(method='ffill')
    df1.to_excel(f'file/child/500_zyy_request_old.xlsx')
    df1.to_excel(f'file/child/{run_daily}_500_zyy_request_old.xlsx')

    df2 = pd.merge(wxk_old, wxk, on=['secCode'], how='outer')

    df2.rename(columns={'targetNumber': 'del_targetNumber'}, inplace=True)
    df2.rename(columns={'actNumber': 'old_targetNumber'}, inplace=True)
    df2.rename(columns={'logo_y': 'logo'}, inplace=True)
    df2 = df2.copy()

    df2['del_targetNumber'] = df2.apply(
        lambda x: ((x['del_targetNumber']) if (x['del_targetNumber'] > 10) else (x['old_targetNumber'])), axis=1)
    df2['market'] = df2.apply(lambda x: marketCodeSide['market'][marketCodeSide['symbol'] == x['secCode']].values[0],
                              axis=1)
    df2 = df2.fillna(method='ffill')
    df2.to_excel(f'file/child/500_wxk_request_old.xlsx')
    df2.to_excel(f'file/child/{run_daily}_500_wxk_request_old.xlsx')
    feishu_bot.send_msg_to_feishu(f"{run_daily}持仓生成成功")
    return 0

if __name__ == '__main__':
    old_request()