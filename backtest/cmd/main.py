import os.path
from algo.model.parent_order import ParentOrder
import datetime
from algo.model.model import OrderType, OrderAction, StockBasicData, AlgoTradingParams
from algo.algo_order.algo_order import AlgoOrder
import pandas as pd
from sqlalchemy import create_engine
import algo.utils.tools as utils
import yaml
import warnings
from match.order_match import OrderMatch
warnings.filterwarnings("ignore")


def read_tick_data():
    path = r"C:\Users\niefanxiang\Desktop\flow-master\backtest\data"
    date = "20230601"
    symbol = "601818.SH"
    df_ticks = pd.read_csv(f"{path}/{date}/{symbol}-TickAB-{date}.csv")
    return df_ticks


def reade_algo_params():
    host = '172.18.103.142'  # MySQL主机名
    user = 'root'  # MySQL用户名
    password = 'boke123'
    database = 'oms'  # 数据库名称
    # 与MySQL建立连接
    BASE_CONN_URL = f"mysql+pymysql://{user}:{password}@{host}:3306/{database}"
    conn = create_engine(BASE_CONN_URL, connect_args={"charset": "utf8"})
    sql = f"select * from tb_stock_basic"
    df = pd.read_sql(sql, con=conn)
    stk_basic_data_map = {}
    for index, row in df.iterrows():
        row.symbol = utils.format_symbol(row.symbol)
        stk_basic_data = StockBasicData(**row)
        stk_basic_data_map[row.symbol] = stk_basic_data
    return stk_basic_data_map


def load_config():
    file_path = os.path.dirname(os.path.dirname(__file__))
    yaml_file = f"{file_path}/algo/etc/cornerstone_capstone_v1_config.yaml"
    with open(yaml_file, "r") as fp:
        config = yaml.load(fp, Loader=yaml.SafeLoader)
    algo_trad_params_map = {}
    for param in config["AlgoTradingParamsList"]:
        algo_trad_params = AlgoTradingParams()
        algo_trad_params.name = param["Name"]
        algo_trad_params.cluster_id = param["ClusterId"]
        # 把盘口的几档数据 合并成一层
        algo_trad_params.buffering_avg_lvl_num = param.get("BufferingAreaLevelNum", 0) # 把n层看做一层
        # vol_sum/pre_vol_sum 的对照百分比 盘口放量增速指标使用
        algo_trad_params.increased_pct = param.get("IncreasedPct", 0) # vol_sum增加多少
        algo_trad_params.max_slice_num = param["MaxSliceNum"] # slice1的数量上限
        algo_trad_params.max_slice1_num = param["MaxSlice1Num"] # 一个level slice的数量上限
        algo_trad_params.max_slice_num_in_lvl = param["MaxSliceNumInLvl"]
        algo_trad_params.max_lvl_num = param["MaxLvlNum"] # 最多观测几档
        algo_trad_params_map[param["ClusterId"]] = algo_trad_params
    return algo_trad_params_map


def main():
    stk_basic_data_map = reade_algo_params()
    algo_trad_params_map = load_config()
    symbol = "601818.SH"
    stk_basic_data = stk_basic_data_map[symbol]
    df_ticks = read_tick_data()
    df_ticks = df_ticks[df_ticks.Time >= 93000000]
    p_order = ParentOrder()
    p_order.origin_id = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    p_order.input_vol = 3700000
    p_order.order_type = OrderType.limit
    p_order.order_action = OrderAction.buy_to_open
    p_order.broker = "guangfa"
    p_order.symbol = "601818.SH"
    p_order.account_id = "test"
    p_order.strategy_id = "test"
    p_order.user = "zh"
    p_order.portfolio = "test"
    p_order.algo = "capstone_cornerstone"
    p_order.group = "reversal"
    p_order.trader = "zs"
    p_order.params = "{\"endtime\":\"145700000\",\"maxpov\":0.04," + \
                     "\"maxvol\":0.1,\"minvol\":0.04," + \
                     "\"catchupvol\":0.025,\"catchuptrigger\":0.02," + \
                     "\"openauctionrate\":0.1,\"pricebuffer\":0," + \
                     "\"post_1lot_magnifier\":1,\"sigma\":0}"
    req_params = utils.parse_algo_params(p_order.params)
    path = r"C:\Users\niefanxiang\Desktop\flow-master\backtest\data"
    date = "20230601"
    order_match = OrderMatch(path, date, p_order.symbol)
    order_match.init_data()
    algo_order = AlgoOrder(p_order, stk_basic_data, algo_trad_params_map[stk_basic_data.cluster], req_params,
                           order_match)
    for _, row in df_ticks.iterrows():
        tick = utils.covert_ser_to_tick(row)
        algo_order.on_tick_update(tick)
        if tick.time > req_params.end_time:
            break
        if algo_order.p_order.is_finished():
            break


if __name__ == "__main__":

    main()
