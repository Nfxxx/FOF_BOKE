import smtplib
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import os
import pandas as pd
import warnings

warnings.filterwarnings("ignore")
import utils.cal as cal
from datetime import datetime

wxk_ini = ['IC2403', 18, '500_wxk', 'IC']
zyy_ini = ['IC2403', 18, '500_zyy', 'IC']
future_close = {'IC': 200}


class SendToPM:
    def __init__(self):
        self.mail_host = "smtp.163.com"  # 设置服务器
        self.mail_user = "mom_gz@163.com"  # 用户名
        self.mail_pass = "PTRUHBEILNJBDTXV"  # 口令是授权码，不是邮箱密码
        self.nowDaily = datetime.now().strftime('%Y%m%d')
        self.manualDay = '20240205'
        self.prevDay = '2024-02-06'
        self.daily = '20240206'
        self.statementDaily = '2024-02-07'
        self.proto_Parent = pd.DataFrame()
        self.future = pd.DataFrame()
        self.prevClose = pd.read_csv('file/eqt_1mbar/prevclose.csv')
        self.prevClose.rename(columns={'Unnamed: 0': 'daily'}, inplace=True)
        self.prevClose = self.prevClose.fillna(method='ffill')
        self.marketCodeSide = pd.read_csv('file/eqt_1mbar/MarketCode2024.csv').drop(labels=['Unnamed: 0'], axis=1)

        self.marketCodeSide['symbol'] = self.marketCodeSide['symbol'].apply(lambda x: "%06d" % int(x))
        self.dailyClose = cal.Close_price(daily=self.daily)
        # self.dailyClose = self.dailyClose.fillna(method='ffill')
        self.dailyOpen = cal.Open_price(daily=self.daily)
        # print(self.dailyClose['003816.SZ'].values[0])

    def time_trans(self, times):
        return datetime.strptime(times, '%Y/%m/%d').strftime("%Y%m%d")

    def find_future_file(self):
        folder_path = r'\\172.18.0.7\波克私募\资管文档\BKSM_MOM\future'
        all_file_contents = ''
        for files in os.listdir(folder_path):
            if self.daily in files:
                all_file_contents = files
            else:
                pass
        if all_file_contents == '':
            print(f'未找到期货端数据 请检查')
        folder = os.path.join(folder_path, all_file_contents)
        folder_path = os.path.join(folder, r'dump\PositionDetail.csv')
        close_path = os.path.join(folder, r'dump\MarketData.csv')
        # close_file = pd.read_csv(close_path, encoding='GB2312',
        #                          usecols=['SettlementPrice', 'PreClosePrice', 'LastPrice', 'InstrumentID'])
        # close_file = close_file[['SettlementPrice', 'OpenPrice', 'LastPrice', 'InstrumentID']]
        future_file = pd.read_csv(folder_path)
        future = pd.DataFrame(
            columns=['Name', 'Direction', 'Volume', 'closePrice', 'PositionProfitByTrade'])
        future_file = future_file[
            ['InstrumentID', 'Direction', 'OpenPrice', 'Volume']]
        grouped = future_file.groupby(by=[future_file['InstrumentID'], future_file['Direction']])
        # 此处将全部子单合成初始大表，并将原始数据存储下来
        for name, group in grouped:
            Name = group['InstrumentID'].unique()[0]
            Direction = group['Direction'].unique()[0]
            Volume = group['Volume'].sum()
            # open_price = close_file['OpenPrice'][close_file['InstrumentID'] == Name].values[0]
            # close_price = close_file['SettlementPrice'][close_file['InstrumentID'] == Name].values[0]
            Settlement_Price = 4392.8
            Prev_Sttl =  4425.2
            PositionProfitByTrade = round(Volume * (Prev_Sttl - Settlement_Price) * 200, 0)
            a = [Name, Direction, Volume, Settlement_Price, PositionProfitByTrade]
            future = future._append(pd.Series(a, index=future.columns), ignore_index=True)
        # print(future)
        self.future = future.copy()
        return future

    def stock_daily(self):
        # try:
        #     magre_child = \
        #         pd.read_csv(f'file/self/{self.daily}_marge_Child.csv', encoding='gbk').drop(labels=['Unnamed: 0'], axis=1)[
        #             ['symbol', 'orderQty', 'side', 'Market', 'logo']]
        # except:
        magre_child = \
            pd.read_csv(f'file/self/{self.daily}_marge_Child_clean.csv', encoding='gbk').drop(labels=['Unnamed: 0'],
                                                                                        axis=1)[
                ['symbol', 'orderQty', 'side', 'Market', 'logo']]
        magre_child['Market'] = magre_child.apply(lambda x: 1 if x['Market'].upper() == 'SH' else 2, axis=1)
        magre_child['symbol'] = magre_child['symbol'].apply(lambda x: "%06d" % int(x))
        # 此处需对互噶部分进行处理
        out_df = pd.DataFrame()
        grouped = magre_child.groupby(by='symbol')
        for names, groups in grouped:
            if len(groups['side'].unique()) == 2:
                groups['orderQty'] = groups['orderQty'] - groups['orderQty'].min()
                # print(groups)
                out_df = out_df._append(groups, ignore_index=True)
            else:
                out_df = out_df._append(groups, ignore_index=True)
        magre_child = out_df
        # print(out_df)
        magre_parent = magre_child[magre_child['orderQty'] >= 100]
        # print(len(magre_parent['symbol'].unique()))
        if self.proto_Parent.empty:
            self.proto_Parent = self.proto_merge_parent()
        else:
            pass
        # print(self.proto_Parent[self.proto_Parent['symbol'] == '688060'])
        print(len(self.proto_Parent['symbol'].unique()))
        print(len(magre_parent['symbol'].unique()))
        magre_parent = pd.merge(magre_parent, self.proto_Parent, how='left')
        magre_parent = magre_parent.fillna(' ')
        magre_parent = magre_parent[magre_parent['Time'] != ' ']
        if os.path.exists(f'file/self/{self.daily}'):
            pass
        else:
            os.mkdir(f'file/self/{self.daily}')
        # print(magre_parent[magre_parent['symbol'] == '000737'])
        magre_parent.to_csv(f'file/self/{self.daily}/{self.daily}_act_parent.csv')
        grouped = magre_parent.groupby(by=[magre_parent['symbol'], magre_parent['side']])
        # 此处将全部子单合成初始大表，并将原始数据存储下来
        ture_old = pd.DataFrame(
            columns=['symbol', 'side', 'orderQty', 'avgPrice', 'closePrice', 'prevClose', 'actMoney', 'logo',
                     'otherFee'])
        for name, group in grouped:
            symbol = group['symbol'].values[0]
            side = group['side'].values[0]
            avgPrice = group['avgPrice'].values[0]
            prevClose = group['prevClose'].values[0]
            closePrice = group['closePrice'].values[0]
            otherFee = group['otherFee'].values[0]
            actNumber = group['actNumber'].values[0]
            actMoney = group['actMoney'].values[0]
            if len(group) == 1:
                orderQty = actNumber
                selfActactMoney = actMoney
                logo = group['logo'].values[0]
                q = [symbol, side, orderQty, avgPrice, closePrice, prevClose, selfActactMoney, logo, otherFee]
                ture_old = ture_old._append(pd.Series(q, index=ture_old.columns), ignore_index=True)
            else:
                # print(group)
                group['qtyRatio'] = round(group['orderQty'] / group['orderQty'].sum(), 4)
                group['otherFee'] = otherFee * group['qtyRatio']
                group['actMoney'] = actMoney * group['qtyRatio']
                Filled_child = cal.average_cal(actNumber, group['orderQty'].values)
                group['orderQty'] = Filled_child
                a = group[['symbol', 'side', 'orderQty', 'avgPrice', 'closePrice', 'prevClose', 'actMoney', 'logo',
                           'otherFee']]
                ture_old = ture_old._append(a, ignore_index=True)
        if os.path.exists(f'file/self/{self.daily}'):
            pass
        else:
            os.mkdir(f'file/self/{self.daily}')
        # ture_old.to_csv(f'file/self/{self.daily}/{self.daily}_act_parent_after.csv')
        return ture_old

    def proto_merge_parent(self):

        try:
            # daily = datetime.now().strftime('%Y-%m-%d')
            df = pd.read_excel(f'file/对账单/{self.statementDaily}_对账单.xlsx', sheet_name='对账单', header=1)
            df = df.drop(df.index[-1])
        except:
            print('proto_merge_parent 没找到对账单')
            exit()
        # side = pd.read_csv('file/eqt_1mbar/MarketCode2024.csv').drop(labels=['Unnamed: 0'], axis=1)
        # side['symbol'] = side['symbol'].apply(lambda x: "%06d" % int(x))
        KF_Parent = df[
            ['业务标志', '发生日期', '证券代码', '成交股数', '成交价格', '成交金额', '发生金额', '手续费', '印花税',
             '过户费']]
        KF_Parent = KF_Parent[KF_Parent['业务标志'].isin(['证券买入', '证券卖出'])]
        KF_Parent['证券代码'] = KF_Parent['证券代码'].apply(lambda x: "%06d" % int(x))
        Parent_stock = KF_Parent.copy()
        stock = pd.DataFrame(
            columns=['Time', 'symbol', 'side', 'avgPrice', 'actNumber', 'actMoney', 'otherFee', 'closePrice',
                     'prevClose'])
        grouped = Parent_stock.groupby(
            by=[Parent_stock['业务标志'], Parent_stock['发生日期'], Parent_stock['证券代码']])
        # 此处将全部子单合成初始大表，并将原始数据存储下来
        for name, group in grouped:
            Time = group['发生日期'].unique()[0]
            symbol = group['证券代码'].unique()[0]
            sideCode = self.marketCodeSide['MarketCode'][self.marketCodeSide['symbol'] == symbol].values[0]
            market = 1 if group['业务标志'].unique()[0] == '证券买入' else 2
            avgPrice = round(group['成交价格'].mean(), 2)
            actNumber = group['成交股数'].sum()
            otherFee = group['手续费'].sum() + group['印花税'].sum() + group['过户费'].sum()
            closePrice = self.dailyClose[sideCode].values[0]
            actMoney = group['发生金额'].sum()
            prevClose = round(self.prevClose[sideCode][self.prevClose['daily'] == self.prevDay].values[0], 2)
            a = [Time, symbol, market, avgPrice, actNumber, actMoney, otherFee, closePrice, prevClose]
            stock = stock._append(pd.Series(a, index=stock.columns), ignore_index=True)
        return stock

    def update_cloes(self):
        try:
            df = pd.read_excel(f'file/对账单/{self.statementDaily}_对账单.xlsx', sheet_name='持仓清单', header=1)
            df = df.drop(df.index[-1])
        except:
            print('账户无持仓')
            return 0
        return df[['证券代码', '参考市价']]

    # 持仓收益计算
    def proto_holding(self):
        try:
            df = pd.read_excel(f'file/对账单/{self.statementDaily}_对账单.xlsx', sheet_name='持仓清单', header=1)
            df = df.drop(df.index[-1])
        except:
            print('账户无持仓')
            return 0
        act_hold = pd.read_excel(f'file/对账单/{self.statementDaily}_对账单.xlsx', sheet_name='对账单', header=1)
        act_hold = act_hold[['证券代码', '成交股数', '业务标志']][act_hold['业务标志'] == '证券买入']
        act_hold['证券代码'] = act_hold['证券代码'].apply(lambda x: "%06d" % int(x))
        act_hold['成交股数'] = act_hold.apply(
            lambda x: ((x['成交股数']) if x['业务标志'] == '证券买入' else (-x['成交股数'])), axis=1)
        # print(act_hold[act_hold['证券代码'] == '000737'])
        # 当日真实持仓部分 = 当日持仓清单 df - 当日对账单买入部分 act_hold
        KF_Holding = df[['证券代码', '股份余额', ]]
        KF_Holding['证券代码'] = KF_Holding['证券代码'].apply(lambda x: "%06d" % int(x))
        KF_Holding['成交股数'] = KF_Holding['证券代码'].apply(
            lambda x: 0 if len(act_hold['成交股数'][act_hold['证券代码'] == x].values) == 0 else (
                act_hold[act_hold['证券代码'] == x]['成交股数'].values[0]))
        KF_Holding = KF_Holding.fillna(0)
        KF_Holding['持仓股数'] = KF_Holding.apply(lambda x: min(x['股份余额'], x['成交股数']), axis=1)
        KF_Holding.rename(columns={'证券代码': 'symbol'}, inplace=True)
        side = pd.read_csv('file/eqt_1mbar/MarketCode2024.csv').drop(labels=['Unnamed: 0'], axis=1)
        side['symbol'] = side['symbol'].apply(lambda x: "%06d" % int(x))
        # print(KF_Holding)
        # print(side)
        df3 = pd.merge(KF_Holding, side, how='left')
        df3 = df3.fillna(0)
        df3 = df3[df3['MarketCode'] != 0]
        df3['prevClose'] = df3.apply(
            lambda x: self.prevClose[x['MarketCode']][self.prevClose['daily'] == self.prevDay].values[0], axis=1)

        df3['参考市价'] = df3.apply(
            lambda x: self.dailyClose[x['MarketCode']].values[0], axis=1)
        df3['prevClose'] = df3.apply(
            lambda x: x['参考市价'] if str(x['prevClose']) == 'nan' else x['prevClose'], axis=1)
        df3 = df3.drop(labels=['market'], axis=1)
        df3['参考市值'] = df3['参考市价'] * df3['股份余额']
        df3['持股收益'] = (df3['参考市价'] - df3['prevClose']) * df3['持仓股数']

        # print(df3[df3['symbol']=='003816'])
        df3.to_csv(f'file/self/{self.daily}/{self.daily}_holding.csv', encoding='gbk', index=False)
        return df3

    def child_stock_res(self, logo):
        # 轧差收益计算
        stock_self_deal = self.stock_self_deal()

        stock_self_deal = stock_self_deal[
            ['symbol', 'logo', 'side', 'selfOrderQty', 'closePrice', 'openPrice', 'prevClose', 'Pnl']]
        stock_self_deal['selfOrderQty'] = stock_self_deal.apply(
            lambda x: ((x['selfOrderQty']) if x['side'] == 1 else (-x['selfOrderQty'])),
            axis=1)
        # print(stock_self_deal[stock_self_deal['symbol'] == '603086'])
        stock_self_deal = stock_self_deal[stock_self_deal['logo'] == logo]
        stock_self_deal_daily_Profit = stock_self_deal['Pnl'].sum()
        # 当日买卖收益
        stock_daily = self.stock_daily()
        holding = self.proto_holding()
        # print(holding)
        close_update = self.update_cloes()
        close_update.rename(columns={'证券代码': 'symbol'}, inplace=True)
        close_update['symbol'] = close_update['symbol'].apply(lambda x: "%06d" % int(x))
        stock_daily = stock_daily[stock_daily['logo'] == logo]
        # print(stock_daily[stock_daily['side'] == 2 ])
        stock_daily['prevClose'] = stock_daily.apply(
            lambda x: x['closePrice'] if x['prevClose'] == ' ' else x['prevClose'], axis=1)
        # stock_daily['prevClose'].fillna(stock_daily['closePrice'], inplace=True)
        # print(stock_daily[stock_daily['symbol'] == '603958'])
        stock_daily['Pnl'] = stock_daily.apply(
            lambda x: (x['actMoney'] + x['orderQty'] * x['closePrice']) if x['side'] == 1 else (
                    x['actMoney'] - x['orderQty'] * x['prevClose']), axis=1)
        stock_daily['orderQty'] = stock_daily.apply(lambda x: ((x['orderQty']) if x['side'] == 1 else (-x['orderQty'])),
                                                    axis=1)
        # print(stock_daily)
        stock_daily_Profit = stock_daily['Pnl'].sum()
        # print(stock_daily_Profit)
        stock_daily.to_excel(f'换仓{logo}.xlsx')
        # 持仓收益总表
        child_out = pd.read_excel(f'file/child/{self.daily}/{self.daily}_{logo}_request_old.xlsx').drop(
            labels=['Unnamed: 0'],
            axis=1)
        # print(child_out)
        child_out['secCode'] = child_out['secCode'].apply(lambda x: "%06d" % int(x))
        child_out = child_out[['secCode', 'old_targetNumber', 'logo']]
        child_out.rename(columns={'secCode': 'symbol'}, inplace=True)
        grouped = child_out.groupby(by=[child_out['symbol']])
        out_df = pd.DataFrame(columns=['symbol', 'old_targetNumber', 'logo'])
        # 此处将全部子单合成初始大表，并将原始数据存储下来
        for name, group in grouped:
            secCode = group['symbol'].values[0]
            self_orderQty = stock_self_deal[stock_self_deal['symbol'] == secCode].values
            if len(self_orderQty) == 0:
                selfOrderQty = 0
            else:
                selfOrderQty = stock_self_deal['selfOrderQty'][stock_self_deal['symbol'] == secCode].values[0]
                if selfOrderQty > 0:
                    selfOrderQty = 0
                else:
                    selfOrderQty = selfOrderQty
            orderQty = stock_daily[stock_daily['symbol'] == secCode].values

            if len(orderQty) == 0:
                Qty = 0
            else:
                Qty = stock_daily['orderQty'][stock_daily['symbol'] == secCode].values[0]
                if Qty > 0:
                    Qty = 0
                else:
                    Qty = Qty
            old_targetNumber = group['old_targetNumber'].values[0] + Qty + selfOrderQty
            a = [secCode, old_targetNumber, logo]
            out_df = out_df._append(pd.Series(a, index=out_df.columns), ignore_index=True)
        # print(out_df)
        child_out = out_df
        # print(child_out[child_out['symbol'] == '688055'])
        df3 = pd.merge(child_out, holding, how='left')
        df3 = df3.fillna(0)
        df3 = df3[df3['MarketCode'] != 0]
        holding = df3[~df3['持仓股数'].isnull()]

        # holding['真实持仓数']=holding[['old_targetNumber','持仓股数']].min()
        holding = holding.copy()
        # print(holding)
        holding['old_targetNumber'] = abs(holding['old_targetNumber'])
        holding['真实持仓数'] = holding.apply(lambda x: min(x['old_targetNumber'], x['股份余额']), axis=1)
        # print(holding)
        holding['真实持股收益'] = holding['真实持仓数'] * (holding['参考市价'] - holding['prevClose'])
        holding['真实持股市值'] = round((holding['old_targetNumber']) * holding['参考市价'], 1)

        holding_Profit = holding['真实持股收益'].sum()
        # print(holding)

        # print(holding[holding['symbol'] == '603958'])
        holding.to_excel(f'持仓{logo}.xlsx')
        stock_self_deal.to_excel(f'轧差{logo}.xlsx')

        # 收益资金加总
        stock_Profit = holding_Profit + stock_daily_Profit + stock_self_deal_daily_Profit
        # stock_Profit = holding_Profit + stock_daily_Profit
        holding = holding[holding['old_targetNumber'] > 0]
        a = holding[['symbol', '真实持仓数', '股份余额', 'old_targetNumber']]
        b = stock_daily[['symbol', 'orderQty']][stock_daily['orderQty'] > 0]
        stock_self_deal['selfOrderQty'] = stock_self_deal.apply(
            lambda x: ((x['selfOrderQty']) if x['side'] == 1 else (-x['selfOrderQty'])),
            axis=1)
        c = stock_self_deal[['symbol', 'selfOrderQty']]
        # print(c)
        df3 = pd.merge(a, b, on='symbol', how='outer')
        df3 = pd.merge(df3, c, on='symbol', how='outer')
        df3 = df3.fillna(0)
        # print(df3)
        # df3['持仓总数'] = df3['old_targetNumber'] + df3['orderQty']
        df3['持仓总数'] = df3['old_targetNumber'] + df3['orderQty'] + df3['selfOrderQty']
        # print(df3[df3['持仓总数'] < 0])
        df3 = pd.merge(df3, close_update, how='left')
        df3.to_excel(f'CC_{logo}.xlsx')
        # stock_Margin = (( df3['old_targetNumber'] + df3['orderQty']+ abs(df3['selfOrderQty']/2)) * df3['参考市价']).sum()
        stock_Margin = ((df3['真实持仓数'] + df3['orderQty'] + abs(df3['selfOrderQty'] / 2)) * df3['参考市价']).sum()

        print([stock_Margin, stock_Profit])
        return [round(stock_Margin / 10000, 4), round(stock_Profit, 4)]

    def child_future_res(self, name, volume):
        if self.future.empty:
            self.future = self.find_future_file()
        else:
            pass
        future = self.future[self.future['Name'] == name].copy()
        # print(future)
        margin = (future['closePrice'] * 200) * volume

        Profit = future['PositionProfitByTrade'] * volume / future['Volume']
        return [round(margin.values[0] / 10000, 2), round(Profit.values[0], 2)]

    # 轧差收益计算
    def stock_self_deal(self):
        # 卖出PnL = 虚拟成交股数*（今开-昨收）-虚拟成交股数*今开*0.00067；
        # 买入PnL =  虚拟成交股数*（今收-今开）-虚拟成交股数*今开*0.00017
        # 使用prot_child表
        magre_child = \
            pd.read_csv(f'file/self/{self.daily}_marge_Child.csv', encoding='gbk').drop(labels=['Unnamed: 0'], axis=1)[
                ['symbol', 'orderQty', 'side', 'Market', 'logo']]
        magre_child['symbol'] = magre_child['symbol'].apply(lambda x: "%06d" % int(x))
        out_df = pd.DataFrame()
        grouped = magre_child.groupby(by='symbol')
        for names, groups in grouped:
            if len(groups['side'].unique()) == 2:
                groups['selfOrderQty'] = groups['orderQty'].min()
                groups['sideCode'] = \
                    self.marketCodeSide['MarketCode'][
                        self.marketCodeSide['symbol'] == groups['symbol'].values[0]].values[0]
                groups['closePrice'] = self.dailyClose[groups['sideCode'].values[0]].values[0]
                groups['openPrice'] = self.dailyOpen[groups['sideCode'].values[0]].values[0]
                groups['prevClose'] = groups.apply(
                    lambda x: self.prevClose[x['sideCode']][self.prevClose['daily'] == self.prevDay].values[0],
                    axis=1)
                groups['Pnl'] = groups.apply(
                    lambda x: (x['selfOrderQty'] * (x['closePrice'] - x['openPrice']) - x['selfOrderQty'] * x[
                        'openPrice'] * 0.00017) if
                    x['side'] == 1 else (
                            x['selfOrderQty'] * (x['openPrice'] - x['prevClose']) - x['selfOrderQty'] * x[
                        'openPrice'] * 0.00067),
                    axis=1)
                out_df = out_df._append(groups, ignore_index=True)
        #
        # print(out_df)
        return out_df

    def out_dataframe(self, logo, future_Margin, future_Profit, stock_Margin, stock_Profit, compare, compare_ratio,
                      last_future_Margin):
        Rit = stock_Profit / (last_future_Margin * 10000) * 100 - compare_ratio
        # print(Rit)

        df = pd.DataFrame(
            {'持有人': [logo], '期货总价值(万)': [-future_Margin], '期货当日盈亏': [future_Profit],
             '股票市值(万)': [stock_Margin],
             '股票今日盈亏': [stock_Profit], '对比指数': [compare], '对比指数涨跌幅': ["{}%".format(compare_ratio)],
             '超额': ["{}%".format(round(Rit, 2))]})
        return df

    def check_muanual(self):
        a = pd.read_excel(f'CC_500_zyy.xlsx')
        b = pd.read_excel(f'CC_500_wxk.xlsx')
        q = pd.merge(a, b, on='symbol', how='outer')
        # print(q[['symbol','持仓总数_y','持仓总数_x']])
        m = q[['symbol', '持仓总数_y', '持仓总数_x']].copy()
        m = m.fillna(0)
        m['持仓总数'] = m['持仓总数_y'] + m['持仓总数_x']
        m = m[['symbol', '持仓总数']]
        df = pd.read_excel(f'file/对账单/{self.statementDaily}_对账单.xlsx', sheet_name='持仓清单', header=1)
        df = df.drop(df.index[-1])

        df = df[['证券代码', '股份余额']]
        df.rename(columns={'证券代码': 'symbol', '股份余额': '股份余额_对账单'}, inplace=True)
        aaaa = pd.merge(df, m, on='symbol', how='outer')
        # print(aaaa)
        aaaa['filed'] = aaaa['持仓总数'] - aaaa['股份余额_对账单']
        aaaa = aaaa.fillna(0)
        manual = aaaa[aaaa['持仓总数'] == 0]
        print(manual)

    def money_check(self):
        stock_wxk = self.child_stock_res('500_wxk')[1]
        stock_zyy = self.child_stock_res('500_zyy')[1]
        self_cap=stock_wxk+stock_zyy
        Today_capita = pd.read_excel(f'file/对账单/{self.statementDaily}_对账单.xlsx', sheet_name='资金情况', header=6)[
            ['总资产']].values[0][0]
        repurchase = pd.read_excel(f'file/对账单/{self.statementDaily}_对账单.xlsx', sheet_name='对账单', header=1)
        try:
          repurchase=repurchase[repurchase['业务标志']=='拆出质押购回']['备注'].values[0].split(':')[1].split('实际')[0]
          # print(repurchase)
        except:
            repurchase=0
        # print(repurchase)
        last_capita = \
        pd.read_excel(f'file/对账单/{self.prevDay}_对账单.xlsx', sheet_name='资金情况', header=6)[['总资产']].values[0][
            0]

        ALL=float(Today_capita)-float(repurchase)-float(last_capita)
        print(self_cap-ALL)
        if self_cap-ALL>50:
            diff=(self_cap-ALL+10)/2
            stock_wxk=stock_wxk-diff
            stock_zyy=stock_zyy-diff
            # print(stock_wxk,stock_zyy)
            # print(stock_wxk+stock_zyy-(float(Today_capita)-float(repurchase)-float(last_capita)))
            return [stock_wxk,stock_zyy]
        else:
            print('无差')
            return [stock_wxk,stock_zyy]
        # print()
        # Trues_money=

    def wxk_response(self):

        future = self.child_future_res(wxk_ini[0], wxk_ini[1])
        compare_ratio = cal.cal_index()
        stock = self.child_stock_res('500_wxk')
        last =  1624.8592
        stock = [stock[0], self.money_check()[0]]
        # stock = [1624.8592, self.money_check()[0]]
        df = self.out_dataframe('500_wxk', future[0], future[1], stock[0], stock[1], wxk_ini[3], compare_ratio, last)
        print(df)
        df.to_csv(f'file/child/{self.daily}_500_wxk_response.csv', encoding='gbk', index=False)

    def zyy_response(self):
        # future = self.child_future_res(zyy_ini[0], zyy_ini[1])
        compare_ratio = cal.cal_index()
        stock = self.child_stock_res('500_zyy')
        last = 1633.1601
        stock = [stock[0],self.money_check()[1]]
        # stock = [1624.8592, self.money_check()[1]]
        df = self.out_dataframe('500_zyy', future[0], future[1], stock[0], stock[1], wxk_ini[3], compare_ratio, last)
        print(df)
        df.to_csv(f'file/child/{self.daily}_500_zyy_response.csv', encoding='gbk', index=False)

    def demo(self):
        df1 = pd.read_csv(f'file/child/{self.daily}_500_zyy_request.csv')
        # df1 = pd.read_excel(f'file/child/{self.daily}_500_wxk_request.xlsx').drop('Unnamed: 0', axis=1)
        df1['secCode'] = df1['secCode'].apply(lambda x: "%06d" % int(x))
        df1.rename(columns={'secCode': 'symbol'}, inplace=True, )
        df1['MarketCode'] = df1.apply(
            lambda x: self.marketCodeSide['MarketCode'][self.marketCodeSide['symbol'] == x['symbol']].values[0], axis=1)
        df1['closePrice'] = df1.apply(
            lambda x: self.dailyClose[x['MarketCode']].values[0], axis=1)
        df1['prevClose'] = df1.apply(
            lambda x: self.prevClose[x['MarketCode']][self.prevClose['daily'] == self.prevDay].values[0], axis=1)
        # print(df1)
        money = (df1['closePrice'] * df1['targetNumber']).sum()
        profit = ((df1['closePrice'] - df1['prevClose']) * df1['targetNumber']).sum()
        print(money, profit)
        # sideCode = self.marketCodeSide['MarketCode'][self.marketCodeSide['symbol'] == symbol].values[0]
        # df2=pd.merge(df1,)

    def other_Fee_cal(self):

        df = pd.read_excel(f'file/对账单/{self.statementDaily}_对账单.xlsx', sheet_name='对账单', header=1)
        df = df.drop(df.index[-1])
        KF_Parent = df[
            ['业务标志', '发生日期', '证券代码', '成交股数', '成交价格', '手续费','备注']]
        KF_Parent = KF_Parent[~KF_Parent['业务标志'].isin(['证券买入', '证券卖出','质押回购拆出','拆出质押购回'])]
        print(KF_Parent)


    def send_mail(self, receivers, xlsx_name):
        smtpObj = smtplib.SMTP_SSL(self.mail_host)  # 使用SSL连接邮箱服务器
        smtpObj.login(self.mail_user, self.mail_pass)  # 登录服务器
        message = MIMEMultipart()
        content = MIMEText(f'{self.daily}盘后汇总', 'plain', 'utf-8')  # 添加内容
        message.attach(content)
        message['From'] = self.mail_user  # 发件人
        message['To'] = receivers  # 收件人
        message['Subject'] = Header(f'{self.daily}盘后真实成交', 'utf-8')  # 主题
        # 添加Excel类型附件
        file_name = f'{self.daily}_{xlsx_name}_response.csv'  # 文件名
        file_path = os.path.join(f'file/child/{self.daily}_{xlsx_name}_response.csv')  # 文件路径
        xlsx = MIMEApplication(open(file_path, 'rb').read())  # 打开Excel,读取Excel文件
        xlsx["Content-Type"] = 'application/octet-stream'  # 设置内容类型
        xlsx.add_header('Content-Disposition', 'attachment', filename=file_name)  # 添加到header信息
        message.attach(xlsx)
        smtpObj.sendmail(self.mail_user, [receivers], message.as_string())  # 发送邮件

    def send_(self):
        zyy_mail = 'zhangyangye@boke.com'
        self.send_mail(zyy_mail, '500_zyy')

        wxk_mail = 'Xueerfank@126.com'
        self.send_mail(wxk_mail, '500_wxk')

        # test_mail = 'niefanxiang@bokesimu.com'
        # self.send_mail(test_mail, '500_wxk')
        print('发送完成')


if __name__ == '__main__':
    a = SendToPM()
    # a.other_Fee_cal()
    # a.demo()
    # a.wxk_response()
    # a.zyy_response()
    # a.check_muanual()
    mail=a.send_()
