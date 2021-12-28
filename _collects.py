import time
import typing
import loguru

import numpy as np
from sqlalchemy import create_engine
from tqdm import tqdm

import pandas as pd
import efinance as ef


class Collector(object):
    def __init__(self, engine_str: str = 'sqlite+pysqlite:///db.sqlite'):
        """
        数据采集器

        :param engine_str:
        """
        self.engine = create_engine(engine_str)
        self.numeric_col_list = [
            # 市场概况
            '涨跌幅', '最新价', '最高', '最低', '今开', '涨跌额', '换手率', '量比', '动态市盈率', '成交量', '成交额', '昨日收盘',
            '总市值', '流通市值', '行情ID',
            # 股票详情
            '开盘', '收盘', '振幅', '主力净流入', '小单净流入', '中单净流入', '大单净流入', '超大单净流入',
        ]
        self.datetime_col_list = [
            # 股票详情
            '时间',
            # 股票龙虎榜
            '上榜日期',
        ]

    def convert_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        获取转换数据类型后的数据框

        :param df:
        :return:
        """
        # 待转换列表
        tmp_num_col_list = []
        tmp_dt_col_list = []
        # 若存在与需转换列表中，则假如待转换列表
        for i in df.columns:
            if i in self.numeric_col_list:
                tmp_num_col_list.append(i)
            if i in self.datetime_col_list:
                tmp_dt_col_list.append(i)
        # 进行类型转换
        df[tmp_num_col_list] = df[tmp_num_col_list].apply(pd.to_numeric, errors='coerce')
        df[tmp_dt_col_list] = df[tmp_dt_col_list].apply(pd.to_datetime, errors='coerce')
        return df

    def get_today_min_cut_by_stock_code(self, the_day: str, stock_code: str) -> pd.DataFrame:
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
        # 类型转换
        result = self.convert_type(c)
        return result

    def get_real_time_summary(self) -> pd.DataFrame:
        """
        获取全市场实时概况

        :return:
        """
        # 获取实时市场概况
        result = ef.stock.get_realtime_quotes()
        # 增加当前时间列
        result['时间'] = np.datetime64(time.strftime('%Y-%m-%d %H:%M'))
        # 类型转换
        result = self.convert_type(result)
        return result

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

    def get_daily_billboard(self) -> pd.DataFrame:
        """
        获得当日股票龙虎榜

        :return:
        """
        result = ef.stock.get_daily_billboard()
        result = self.convert_type(result)
        return result

    def save_today_at_night(self) -> None:
        """
        夜间保存当日发生数据

        :return:
        """
        today = time.strftime('%Y%m%d', time.localtime())
        # 获取市场概况
        loguru.logger.info('获取市场概况')
        a = self.get_real_time_summary()
        a.to_sql('市场概况', self.engine, index=False, if_exists='append')
        # 获取股票详情
        loguru.logger.info('获取股票详情')
        b = self.get_today_min_cut_by_stock_list(today, a['股票代码'])
        b.to_sql('股票详情', self.engine, index=False, if_exists='append')
        # 获取股票龙虎榜
        loguru.logger.info('获取股票龙虎榜')
        c = self.get_daily_billboard()
        c.to_sql('股票龙虎榜', self.engine, index=False, if_exists='append')
        return None

    def select(self, sql: str) -> pd.DataFrame:
        """
        通过SQL进行数据库数据的获取

        :param sql:
        :return:
        """
        return pd.read_sql(sql, self.engine)


if __name__ == '__main__':
    collector = Collector()
    collector.save_today_at_night()
    print(collector.select('select count(*) from 市场概况'))
    print(collector.select('select count(*) from 股票详情'))
    print(collector.select('select count(*) from 股票龙虎榜'))
