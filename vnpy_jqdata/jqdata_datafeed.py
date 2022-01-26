from datetime import timedelta
from typing import List, Optional
from pytz import timezone
import traceback

import pandas as pd
import jqdatasdk

from vnpy.trader.datafeed import BaseDatafeed
from vnpy.trader.setting import SETTINGS
from vnpy.trader.constant import Interval
from vnpy.trader.object import BarData, HistoryRequest


INTERVAL_VT2RQ = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "60m",
    Interval.DAILY: "1d",
}

CHINA_TZ = timezone("Asia/Shanghai")


class JqdataDatafeed(BaseDatafeed):
    """聚宽JQDatasdk数据服务接口"""

    def __init__(self):
        """"""
        self.username: str = SETTINGS["datafeed.username"]
        self.password: str = SETTINGS["datafeed.password"]

    def query_bar_history(self, req: HistoryRequest) -> Optional[List[BarData]]:
        """查询k线数据"""
        # 初始化API
        try:
            jqdatasdk.auth(self.username, self.password)
        except Exception:
            traceback.print_exc()
            return None

        # 查询数据
        tq_symbol = jqdatasdk.normalize_code(req.symbol)
        print(f'查询历史数据：{tq_symbol}, {req}')
        df = jqdatasdk.get_price(
            security=tq_symbol,
            frequency=INTERVAL_VT2RQ.get(req.interval),
            start_date=req.start,
            end_date=(req.end + timedelta(1)),
            panel=False
        )

        jqdatasdk.logout()
        # 解析数据
        bars: List[BarData] = []

        if df is not None:
            for tp in df.itertuples():
                # 天勤时间为与1970年北京时间相差的秒数，需要加上8小时差
                dt = pd.Timestamp(tp.Index).to_pydatetime()

                bar = BarData(
                    symbol=req.symbol,
                    exchange=req.exchange,
                    interval=req.interval,
                    datetime=CHINA_TZ.localize(dt),
                    open_price=tp.open,
                    high_price=tp.high,
                    low_price=tp.low,
                    close_price=tp.close,
                    volume=tp.volume,
                    open_interest=0,
                    gateway_name="JQ",
                )
                bars.append(bar)
        else:
            print(f'查询不到历史数据：{tq_symbol}')
        return bars
