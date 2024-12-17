import pandas as pd
from datetime import datetime
import os
from utils.cal import Close_price
import numpy as np
from utils import feishu_bot
Parent_col = ['strategy', 'clientName', 'orderType', 'symbol', 'orderQty', 'side', 'effectiveTime', 'expireTime',
              'limitAction', 'afterAction']


class MargeParent:
    def __init__(self):
        # self.address=addres
        self.Strategy = '卡方_TWAPPLUS'
        self.clientName = '666810086657'
        self.MoNiclientName = '520027603'
        self.orderType = ' '
        self.limitAction = '0'
        self.WeiTuo = '0'
        self.daily = datetime.now().strftime('%Y%m%d')
        # self.daily = '20240116'
        self.run_now = '7'

    def zyy_child(self):
        try:
            df = pd.read_csv(f'file/child/{self.daily}_500_zyy_request.csv')
            try:
                df_old_weight = pd.read_excel(f'file/child/{self.daily}_500_zyy_request_old.xlsx').drop('Unnamed: 0', axis=1)
            except:
                print('zyy_child未使用当日')
                df_old_weight = pd.read_excel(f'file/child/500_zyy_request_old.xlsx').drop('Unnamed: 0',
                                                                                                        axis=1)
            new_df = pd.merge(df, df_old_weight,
                              on=['secCode','market','sendTime','endTime'],
                              how='outer')
            new_df = new_df.fillna(0)
            # print(new_df)
            new_df['new_targetNumber'] = new_df.apply(lambda x: x['targetNumber'] - x['old_targetNumber'], axis=1)
            new_df = new_df[new_df['new_targetNumber'] != 0]
            new_df['new_direction'] = new_df.apply(lambda x: 2 if x['new_targetNumber'] < 0 else 1, axis=1)
            new_df['new_targetNumber'] = abs(new_df['new_targetNumber'])
            new_df.drop(['targetNumber'], axis=1, inplace=True)
            new_df.rename(columns={'new_targetNumber': 'targetNumber', 'new_direction': 'direction'}, inplace=True)
            # print(new_df)
            df = new_df
            # 将当日子单预设为当日预计仓位
            df['secCode'] = df['secCode'].apply(lambda x: "%06d" % int(x))

            df['Market'] = df.apply(lambda x: 1 if x['market'].upper() == 'SH' else 2, axis=1)
            df['sendTime'] = df['sendTime'].apply(lambda x: "%04d:00" % int(x)).apply(
                lambda x: str(x)[:2] + ':' + str(x)[2:])
            df['endTime'] = df['endTime'].apply(lambda x: "%04d:00" % int(x)).apply(lambda x: str(x)[:2] + ':' + str(x)[2:])
            df['logo'] = '500_zyy'
            df.rename(columns={'secCode': 'symbol', 'targetNumber': 'orderQty', 'direction': 'side'}, inplace=True)
            df['cal']= self.Strategy
            df['cal_action'] = '涨幅限制=;跌幅限制=;涨跌停是否继续交易=0;过期后是否继续交易=0;'

            return df[['symbol', 'orderQty', 'side', 'Market', 'sendTime', 'endTime', 'cal', 'logo', 'cal_action']]
        except:
            feishu_bot.send_msg_to_feishu(f"{self.daily} zyy_child 未找到")
            df=pd.DataFrame(columns=['symbol', 'orderQty', 'side', 'Market', 'sendTime', 'endTime', 'cal', 'logo', 'cal_action'])
            return df

    def wxk_child(self):
        try:
            try:
                df = pd.read_excel(f'file/child/{self.daily}_500_wxk_request.xlsx').drop('Unnamed: 0', axis=1)
            except:
                df = pd.read_csv(f'file/child/{self.daily}_500_wxk_request.csv').drop('Unnamed: 0', axis=1)
            try:
                df_old_weight = pd.read_excel(f'file/child/{self.daily}_500_wxk_request_old.xlsx').drop('Unnamed: 0', axis=1)
            except:
                print('wxk_child未使用当日')
                df_old_weight = pd.read_excel(f'file/child/500_wxk_request_old.xlsx').drop('Unnamed: 0', axis=1)

            new_df = pd.merge(df, df_old_weight,
                              on=['secCode','market','sendTime','endTime'],
                              how='outer')
            new_df = new_df.fillna(0)
            # 将当日子单预设为当日预计仓位
            new_df['new_targetNumber'] = new_df.apply(lambda x: x['targetNumber'] - x['old_targetNumber'], axis=1)
            new_df = new_df[new_df['new_targetNumber'] != 0]
            new_df['new_direction'] = new_df.apply(lambda x: 2 if x['new_targetNumber'] < 0 else 1, axis=1)
            new_df['new_targetNumber'] = abs(new_df['new_targetNumber'])
            new_df.drop(['direction','targetNumber'], axis=1, inplace=True)
            new_df.rename(columns={'new_targetNumber':'targetNumber','new_direction': 'direction'}, inplace=True)
            df = new_df
            df['secCode'] = df['secCode'].apply(lambda x: "%06d" % int(x))
            df['Market'] = df.apply(lambda x: 1 if x['market'].upper() == 'SH' else 2, axis=1)
            df['sendTime'] = df['sendTime'].apply(lambda x: "%04d:00" % int(x)).apply(
                lambda x: str(x)[:2] + ':' + str(x)[2:])
            df['endTime'] = df['endTime'].apply(lambda x: "%04d:00" % int(x)).apply(lambda x: str(x)[:2] + ':' + str(x)[2:])
            df['logo'] = '500_wxk'
            df.rename(columns={'secCode': 'symbol', 'targetNumber': 'orderQty', 'direction': 'side'}, inplace=True)
            df['cal'] = self.Strategy
            df['cal_action'] = '涨幅限制=;跌幅限制=;涨跌停是否继续交易=0;过期后是否继续交易=0;'
            return df[['symbol', 'orderQty', 'side', 'Market', 'sendTime', 'endTime', 'cal', 'logo', 'cal_action']]
        except:
            feishu_bot.send_msg_to_feishu(f"{self.daily} wxk_child 未找到")
            df=pd.DataFrame(columns=['symbol', 'orderQty', 'side', 'Market', 'sendTime', 'endTime', 'cal', 'logo', 'cal_action'])
            return df
    # 将所有的子单汇总成母单
    def proto_Parent(self):
        df = pd.DataFrame()
        zyy_child = self.zyy_child()
        wxk_child=self.wxk_child()
        df = df._append(zyy_child, ignore_index=True)
        df = df._append(wxk_child, ignore_index=True)
        df.to_csv(f'file/self/{self.daily}_marge_Child.csv',encoding='gbk')
        proto_df = pd.DataFrame(columns=['symbol', 'side', 'market', 'orderQty', 'sendTime', 'endTime', 'cal', 'cal_action'])
        out_df = pd.DataFrame(columns=['symbol', 'side', 'market', 'orderQty', 'sendTime', 'endTime', 'cal', 'cal_action'])
        grouped = df.groupby(by=[df['symbol'], df['side'], df['sendTime'], df['endTime'], df['cal'],df['cal_action']])
        # 此处将全部子单合成初始大表，并将原始数据存储下来
        for name, group in grouped:
            a = [group['symbol'].unique()[0], group['side'].unique()[0], group['Market'].unique()[0],
                 group['orderQty'].sum(), group['sendTime'].unique()[0], group['endTime'].unique()[0],
                 group['cal'].unique()[0],group['cal_action'].unique()[0]]
            out_df = out_df._append(pd.Series(a, index=out_df.columns), ignore_index=True)
        out_df.to_csv(f'file/self/{self.daily}_proto_Parent.csv',encoding='gbk')
        # 此处自成交互嘎
        grouped = out_df.groupby(by='symbol')
        for names, groups in grouped:
            if len(groups['side'].unique()) ==1:
                proto_df = proto_df._append(groups)
            else:
                order_type = groups['orderQty'][groups['side'] == 1].sum() - \
                             groups['orderQty'][groups['side'] == 2].sum()
                if order_type > 0:
                    a = [groups['symbol'].unique()[0], 1, groups['market'].unique()[0], order_type,
                         groups['sendTime'][groups['side'] == 1].values[0], groups['endTime'][groups['side'] == 1].values[0],
                         groups['cal'].unique()[0],groups['cal_action'].unique()[0]]
                    proto_df = proto_df._append(pd.Series(a, index=out_df.columns), ignore_index=True)
                elif order_type < 0:
                    a = [groups['symbol'].unique()[0], 2, groups['market'].unique()[0], abs(order_type),
                         groups['sendTime'][groups['side'] == 2].values[0],
                         groups['endTime'][groups['side'] == 2].values[0],
                         groups['cal'].unique()[0],group['cal_action'].unique()[0]]
                    proto_df = proto_df._append(pd.Series(a, index=out_df.columns), ignore_index=True)
        proto_df=proto_df[proto_df['orderQty']>99]
        return proto_df

    def magre_Parent(self):
        magre_child = self.proto_Parent()
        print(magre_child)
        df = pd.DataFrame()
        df['证券代码'] = magre_child['symbol']
        df['市场'] = magre_child['market']
        df['交易方向'] = magre_child['side']
        df['委托数量'] = magre_child['orderQty']
        df['算法开始时间'] = magre_child['sendTime']
        df['算法过期时间'] = magre_child['endTime']
        # df['算法类型'] = self.Strategy
        df['算法类型'] = magre_child.apply(lambda x: '卡方_TWAPPLUS' if x['cal'].upper() == 'TWAP' else '卡方_VWAPPLUS', axis=1 )
        df['限价'] = self.limitAction
        df['算法参数'] = magre_child['cal_action']
        df['备注'] = self.orderType
        df['创建后是否立即启动'] = self.run_now
        df['资产账号'] = self.clientName
        df['委托属性'] = self.WeiTuo
        # print(df['算法过期时间'])
        # print(len(df))
        df['委托数量'] = df.apply( lambda x:( (x['委托数量']) if (x['证券代码'][0:3] == '688' ) else int(str(int(x['委托数量']))[:-2]+'00')), axis=1)
        # print(df)
        df['委托数量'] = df.apply(
                    lambda x: 0 if x['证券代码'][0:3] == '688' and x['委托数量'] < 200 else x['委托数量'], axis=1)
        df=df[df['委托数量']>0]
        print(df)
        if os.path.exists(f'file/parent/{self.daily}'):
            pass
        else:
            os.mkdir(f'file/parent/{self.daily}')
        if os.path.exists(f'file/parent/{self.daily}/{self.daily}_trade_MOM_互噶.xlsx'):
            a=1
            df.to_excel(f'file/parent/{self.daily}/{self.daily}_trade_MOM_互噶_{a}.xlsx')
        else:
            df.to_excel(f'file/parent/{self.daily}/{self.daily}_trade_MOM_互噶.xlsx')
        feishu_bot.send_msg_to_feishu(f"{self.daily} 母单已生成")
        return df





def onlyPMparentOrder():
    a = MargeParent().magre_Parent()
    b = pd.read_excel('file/parent/20240116/20240116_trade_MOM_互噶.xlsx').drop(labels=['Unnamed: 0'],
                                                                                axis=1)
    b['证券代码'] = b['证券代码'].apply(lambda x: "%06d" % int(x))

    a = a[a['交易方向'] == 2]
    b = b[b['交易方向'] == 2]
    df3 = pd.merge(a, b, on='证券代码', how='outer')
    df3 = df3[df3['交易方向_x'] == 2]

    df3 = df3[['证券代码', '交易方向_x', '委托数量_x', '委托数量_y']]
    df3 = df3.fillna(0)
    df3 = df3[df3['委托数量_y'] > 0]
    df3['真实卖出'] = df3['委托数量_x'] - df3['委托数量_y']
    df3 = df3[df3['真实卖出'] > 0]
    print(df3)


if __name__ == '__main__':
    a = MargeParent().magre_Parent()
