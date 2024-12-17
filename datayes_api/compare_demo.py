import pandas as pd
api_table=pd.read_csv('424因子_20241016.csv')
api_table = api_table.drop(columns=['ticker', 'tradeDate'])
api_table.rename(columns={'secID': 'SECURITY_ID'}, inplace=True)
api_table = api_table.sort_index(axis=1)
sql_table=pd.read_csv('424因子表_10_16.csv')
sql_table= api_table.sort_index(axis=1)
a=api_table.columns
print(a)
b=sql_table.columns
print(b)
# 比较两个 DataFrame 的差异
diff = api_table.compare(sql_table)

print(diff)