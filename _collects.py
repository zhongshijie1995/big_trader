import os
import time
import shutil

import pandas as pd
import efinance as ef


def get_today_min_cut_by_stock_code(today: str, stock_code: str) -> pd.DataFrame:
    """
    获取一支股票当天的分钟状况

    :param today:
    :param stock_code:
    :return:
    """
    # 获取1分钟K线
    a = ef.stock.get_quote_history(stock_code, beg=today, end=today, klt=1)
    a = a.rename(columns={'日期': '时间'})
    # 获取1分钟交易单
    b = ef.stock.get_today_bill(stock_code)
    # 合并分钟状况
    c = pd.merge(a, b, on=['股票名称', '股票代码', '时间'])
    return c


def get_today_min_cut_by_stock_list(today: str, stock_code_list: list[str]) -> pd.DataFrame:
    """
    获取多支股票当天的分钟状况

    :param today:
    :param stock_code_list:
    :return:
    """
    # 遍历所有股票代码用于获取分钟状况
    a = None
    for i, _a in enumerate(stock_code_list):
        tmp = get_today_min_cut_by_stock_code(today, _a)
        if a is None:
            a = tmp
        else:
            a = a.append(tmp)
        print(i, _a)
    return a


def get_real_time_summary() -> pd.DataFrame:
    """
    获取全市场实时概况

    :return:
    """
    return ef.stock.get_realtime_quotes()


def save_today_at_night(save_path: str = 'data') -> None:
    """
    夜间保存当日发生数据

    :return:
    """
    today = time.strftime('%Y%m%d', time.localtime())
    save_path = os.path.abspath(os.path.join(save_path, today))

    if os.path.exists(save_path):
        shutil.rmtree(save_path)
    os.makedirs(save_path)

    a = get_real_time_summary()
    a.to_csv(os.path.join(save_path, 'summary.csv'), index=False)

    b = get_today_min_cut_by_stock_list(today, a['股票代码'])
    b.to_csv(os.path.join(save_path, 'min_cuts.csv'), index=False)
