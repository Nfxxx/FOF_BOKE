import pandas as pd
from sqlalchemy import create_engine

class CompleteSql:
  def __init__(self):
    host = '172.18.103.142'  # MySQL主机名
    user = 'root'  # MySQL用户名
    password = 'boke123'
    database = 'funddata'  # 数据库名称
    # 与MySQL建立连接
    BASE_CONN_URL = f"mysql+pymysql://{user}:{password}@{host}:3306/{database}"
    self.conn = create_engine(BASE_CONN_URL, connect_args={"charset": "utf8"})

  def link_table(self):
    # 查询语句
    query = "SELECT * FROM link_table;"
    # 将结果存入DataFrame
    df = pd.read_sql(query, self.conn)
    df = df[['date'
      , 'master_name'
      , 'master_code'
      , 'master_lv'
      , 'slave_name'
      , 'slave_code'
      , 'slave_lv'
      , 'asset'
      , 'profit_chg'
      , 'share'
      , 'total_share']]
    # print(df)
    return df

  def action_record_table(self):
    # 查询语句
    query = "SELECT * FROM action_record_table;"
    # 将结果存入DataFrame
    df = pd.read_sql(query, self.conn)
    df = df[['Name'
      , 'Master_name'
      , 'Slave_code_name'
      , 'Slave_name'
      , 'Trade_classes'
      , 'Application_date'
      , 'Confirm_Date'
      , 'Amount'
      , 'Share'
      , 'Value'
      , 'Fee'
      , 'Performance_reward'
      , 'Information_sources'
             ]]
    return df

  def fund_info_table(self):
    # 查询语句
    query = "SELECT * FROM fund_info_table;"
    # 将结果存入DataFrame
    df = pd.read_sql(query, self.conn)
    # print(df)
    return df

  def fundnetvalue(self):
    # 查询语句
    query = "SELECT * FROM fundnetvalue;"
    # 将结果存入DataFrame
    df = pd.read_sql(query, self.conn)
    df = df[['UID', 'Date', 'FundCode',
             'FundName',
             'NetValue',
             'AccValue',
             'Source',
             'Comment']]
    # print(df)
    return df

  def web_amac(self):
    # 查询语句
    query = "SELECT * FROM web_amac;"
    # 将结果存入DataFrame
    df = pd.read_sql(query, self.conn)
    # print(df)
    return df
  def market_table(self):
    # 查询语句
    query = "SELECT * FROM marketvalue_table;"
    # 将结果存入DataFrame
    df = pd.read_sql(query, self.conn)
    df = df[['Company','Fund','MarketValue']]
    # print(df)
    return df
  def update_marketvalue_table(self,df):
    a = CompleteSql()
    market_table = a.market_table()
    market_table_list = market_table['Fund'].values
    fundnetvalue_table_new = df[~df['Fund'].isin(market_table_list)]
    if len(fundnetvalue_table_new) != 0:
      fundnetvalue_table_new.to_sql('marketvalue_table', self.conn, if_exists='append', index=False)
    else:
      fundnetvalue_table_old = df[df['Fund'].isin(market_table_list)]
      fundnetvalue_table_old.to_sql('marketvalue_table', self.conn, if_exists='replace', index=False)
  def Sql_Close(self):
    self.conn.close()

def update_clean_sss2():
  host = '172.18.103.142'  # MySQL主机名
  user = 'root'  # MySQL用户名
  password = 'boke123'
  database = 'funddata'  # 数据库名称
  # 与MySQL建立连接
  BASE_CONN_URL = f"mysql+pymysql://{user}:{password}@{host}:3306/{database}"
  conn = create_engine(BASE_CONN_URL, connect_args={"charset": "utf8"})

  a = CompleteSql()

  fundnetvalue_table = a.fundnetvalue()
  fundnetvalue_table_list = fundnetvalue_table['UID'].values
  fundnetvalue_table_new = pd.read_excel('clean_sss2.xlsx')
  try:

    fundnetvalue_table_new=fundnetvalue_table_new.drop(labels=['Unnamed: 0'],axis=1)
  except:
    pass

  fundnetvalue_table_new.rename(


    columns={'日期': 'Date', '产品代码': 'FundCode', '产品名称': 'FundName', '单位净值': 'NetValue',
             '累计净值': 'AccValue', '来源文件': 'Source'
             },
    inplace=True)
  # print(fundnetvalue_table_new[fundnetvalue_table_new['Date']=='2024-01-31'])
  fundnetvalue_table_new = fundnetvalue_table_new[~fundnetvalue_table_new['UID'].isin(fundnetvalue_table_list)]
  # fundnetvalue_table_new = fundnetvalue_table_new.drop_duplicates(subset=['UID'],keep='first')
  # print(fundnetvalue_table_new)
  #
  if len(fundnetvalue_table_new) != 0:
    # fundnetvalue_table_new['UID']=fundnetvalue_table_new['UID'].str[:10]
    print(fundnetvalue_table_new)
    fundnetvalue_table_new['NetValue'] = fundnetvalue_table_new['NetValue'].astype(float)
    fundnetvalue_table_new.to_sql('fundnetvalue', conn, if_exists='append', index=False)
  else:
    print('fund_info_table 未发现新数据')

if __name__ == '__main__':
  update_clean_sss2()