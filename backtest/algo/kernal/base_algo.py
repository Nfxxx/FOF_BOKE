from typing import Optional
from scipy.stats import norm
from algo.model.tick import Tick
from logger import logger


class BaseAlgo(object):
    @staticmethod
    def calc_x2(px: Optional[float], direction: Optional[int], arrival_px: Optional[float],
                volatility: Optional[float], min_vol: Optional[float], max_vol: Optional[float]) -> Optional[float]:
        slip = direction * (arrival_px - px) / arrival_px
        volatility = volatility / 64
        ret = min_vol + (max_vol - min_vol) * norm.cdf(-slip / volatility)
        return ret

    @staticmethod
    def calc_x(px: Optional[float], direction: Optional[int], arrival_px: Optional[float],
               volatility: Optional[float], min_vol: Optional[float], max_vol: Optional[float]) -> Optional[float]:
        #滑点 现价和开始标记价格的变化
        slip = direction * (arrival_px - px) / arrival_px
        #平滑一下
        volatility = volatility / 64
        # 概率密度函数
        ret = min_vol + (max_vol - min_vol) * norm.cdf(slip / volatility)
        return ret

    @staticmethod
    def is_limit(tick: Optional[Tick]):
        if tick.ask_price1 == 0 or tick.bid_price1 == 0:
            logger.info(f"symbol is limited")
            return True
        return False
