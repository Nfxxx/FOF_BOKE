from sqlalchemy import create_engine, text
import pandas as pd
# 连接A数据库
engine_a = create_engine('mysql+pymysql://boke01:bk1234@172.18.103.161/wind')
# 连接B数据库
engine_b = create_engine('mysql+pymysql://root:boke123@172.18.103.142/wind_demo')
# engine_b = create_engine('mysql+pymysql://data_dev:Dulr7jsN0982gQ387h4M@rm-uf62t0x7ri1q459zm.mysql.rds.aliyuncs.com/wind')

# 获取A数据库的所有表名
with engine_a.connect() as conn:
    result = conn.execute(text("SHOW TABLES"))
    tables = [row[0] for row in result]

print(f"Tables to be copied: {tables}")

# 记录出错的表名
error_file = "error.txt"
# 记录成功的表名
successfully_file = "successfully.txt"

# 读取已经成功复制的表名
successfully_tables = set()
try:
    with open(successfully_file, "r") as f:
        successfully_tables = set(line.strip() for line in f)
except FileNotFoundError:
    pass

# 复制每个表的数据
for table in tables:
    if table in successfully_tables:
        print(f"Table {table} already copied, skipping...")
        continue
    try:
        with engine_a.connect() as conn:
            # 获取表的列信息
            columns_query = text(f"SHOW COLUMNS FROM {table}")
            columns_result = conn.execute(columns_query)
            columns_info = columns_result.fetchall()

            # 构建CREATE TABLE语句
            create_table_sql = f"CREATE TABLE {table} ("
            for column in columns_info:
                column_name = column[0]
                column_type = column[1]
                is_nullable = column[2]
                is_primary_key = column[3]
                create_table_sql += f"{column_name} {column_type} {'NOT NULL' if is_nullable == 'NO' else 'NULL'}, "
            create_table_sql = create_table_sql.rstrip(', ')
            create_table_sql += ")"

            # 获取表的数据
            query = text(f"SELECT * FROM {table}")
            df_a = pd.read_sql(query, engine_a)

        with engine_b.connect() as conn:
            # 删除B数据库中的同名表（如果存在）
            conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
            # 创建B数据库中的表结构
            conn.execute(text(create_table_sql))
            # 将DataFrame写入数据库
            df_a.to_sql(f'{table}', con=engine_b, if_exists='append', index=False)
        print(f"Table {table} copied successfully")
        # 记录成功的表名到successfully.txt文件中
        with open(successfully_file, "a") as f:
            f.write(f"{table}\n")
    except Exception as e:
        print(f"Error copying table {table}: {e}")
        # 记录出错的表名到error.txt文件中
        with open(error_file, "a") as f:
            f.write(f"Error copying table {table}: {e}\n")

print("数据复制完成")