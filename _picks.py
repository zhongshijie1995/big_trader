import datetime
import time

import _collects
import loguru


class Picker(object):
    def __init__(self, trade_min: int = 10, hold_day: int = 1):
        self.buy_time = '15:00'
        self.sell_time = '09:30'
        self.collector = _collects.Collector()
        self.date_list = self.get_date_list()
        self.date_list.sort()

    def get_date_list(self) -> list:
        sql = 'select distinct substr(时间, 1, 10) as 日期 from 市场概况'
        return self.collector.select(sql)['日期'].tolist()

    def get_stock_list_by_date(self, date_str: str) -> list:
        sql = 'select distinct 股票代码 from 市场概况 where substr(时间, 1, 10)=\'%s\'' % date_str
        return self.collector.select(sql)['股票代码'].tolist()

    def get_date_pair(self, hold_day: int) -> list:
        result = []
        for i in range(len(self.date_list)):
            if i + 1 > hold_day:
                result.append((self.date_list[i - hold_day], self.date_list[i]))
        return result

    def set_trade_time(self, trade_min: int) -> None:
        pass


if __name__ == '__main__':
    # picker = Picker()
    s = datetime.datetime.strptime('15:00', '%H:%M')
    print(datetime.timedelta(minutes=2))
