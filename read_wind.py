from sqlalchemy import create_engine, text
import pandas as pd
import time
# 连接A数据库
engine_a = create_engine('mysql+pymysql://boke01:bk1234@172.18.103.161/wind')

# 获取A数据库的所有表名
with engine_a.connect() as conn:
    result = conn.execute(text("SHOW TABLES"))
    tables = [row[0] for row in result]

print(f"Tables to be copied: {tables}")



# 复制每个表的数据
for table in tables:
        with engine_a.connect() as conn:
            query = text(f"SELECT * FROM {table} where OPDATE>'2024-07-01'")
            df_a = pd.read_sql(query, engine_a)
            print(df_a)
            break


