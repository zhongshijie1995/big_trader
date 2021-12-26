import time
import typing

from sqlalchemy import create_engine
from tqdm import tqdm

import pandas as pd
import efinance as ef


class Collector(object):
    def __init__(self, engine_str: str = 'sqlite+pysqlite:///db.sqlite'):
        self.engine = create_engine(engine_str)

    @staticmethod
    def get_today_min_cut_by_stock_code(the_day: str, stock_code: str) -> pd.DataFrame:
        """
        获取一支股票当天的分钟状况

        :param the_day:
        :param stock_code:
        :return:
        """
        # 获取1分钟K线
        a = ef.stock.get_quote_history(stock_code, beg=the_day, end=the_day, klt=1)
        a = a.rename(columns={'日期': '时间'})
        # 获取1分钟交易单
        b = ef.stock.get_today_bill(stock_code)
        # 合并分钟状况
        c = pd.merge(a, b, on=['股票名称', '股票代码', '时间'])
        return c

    @staticmethod
    def get_real_time_summary() -> pd.DataFrame:
        """
        获取全市场实时概况

        :return:
        """
        return ef.stock.get_realtime_quotes()

    def get_today_min_cut_by_stock_list(self, today: str, stock_code_list: typing.List[str]) -> pd.DataFrame:
        """
        获取多支股票当天的分钟状况

        :param today:
        :param stock_code_list:
        :return:
        """
        # 遍历所有股票代码用于获取分钟状况
        result = None
        for stock_code in tqdm(stock_code_list):
            tmp = self.get_today_min_cut_by_stock_code(today, stock_code)
            if result is None:
                result = tmp
            else:
                result = result.append(tmp)
        return result

    def save_today_at_night(self) -> None:
        """
        夜间保存当日发生数据

        :return:
        """
        today = time.strftime('%Y%m%d', time.localtime())

        a = self.get_real_time_summary()
        a.to_sql('stock_summary', self.engine, index=False, if_exists='append')

        b = self.get_today_min_cut_by_stock_list(today, a['股票代码'])
        b.to_sql('stock_detail', self.engine, index=False, if_exists='append')
