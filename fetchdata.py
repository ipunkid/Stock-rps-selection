#!/usr/bin/env python3

import baostock as bs
import pandas as pd
import os
import json
from datetime import datetime, timedelta

# 初始化baostock
bs.login()

# 缓存目录
cache_dir = "stock_cache"
os.makedirs(cache_dir, exist_ok=True)

def fetch_all_stock_codes():
    """获取所有A股股票代码"""
    rs = bs.query_stock_basic()
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    df = pd.DataFrame(data_list, columns=rs.fields)

    # 过滤上市状态为1（上市）且上市满1年时间的股票
    one_year_ago = datetime.now() - timedelta(days=365)
    df = df[(df['status'] == '1') & (df["type"] == '1') & (pd.to_datetime(df['ipoDate']) <= one_year_ago)]

    # Filter for A-shares in Shanghai and Shenzhen
    a_share_stocks = df[df['code'].str.startswith(('sh', 'sz'))]

    return a_share_stocks['code'].tolist()

def fetch_data(bs_code, start_date, end_date):
    """使用baostock获取股票数据"""
    rs = bs.query_history_k_data_plus(
        bs_code,
        "date,code,open,high,low,close,volume,amount",
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag="2"  # 前复权
    )

    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    df = pd.DataFrame(data_list, columns=rs.fields)
    return df

def save_cache(bs_code, df):
    """保存数据到缓存"""
    cache_file = os.path.join(cache_dir, f"{bs_code}.json")
    df.to_json(cache_file, orient="records", date_format="iso")

def load_cache(bs_code):
    """加载缓存文件"""
    cache_file_sh = os.path.join(cache_dir, f"{bs_code}.SH.json")
    cache_file_sz = os.path.join(cache_dir, f"{bs_code}.SZ.json")
    cache_file = cache_file_sh if os.path.exists(cache_file_sh) else cache_file_sz if os.path.exists(cache_file_sz) else None

    if cache_file and os.path.exists(cache_file):
        try:
            with open(cache_file, "r") as f:
                cached_data = json.load(f)
            df = pd.DataFrame(cached_data)
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            df.set_index("trade_date", inplace=True)
            df = df.sort_index()
            return df
        except (json.JSONDecodeError, ValueError):
            print(f"Error reading cache for {bs_code}.")
    return None

def fetch_and_cache_stock_data(bs_code, start_date, end_date):
    """获取并缓存股票数据"""
    df = fetch_data(bs_code, start_date, end_date)
    if not df.empty:
        df["trade_date"] = pd.to_datetime(df["date"])
        df.set_index("trade_date", inplace=True)
        df = df.sort_index()
        save_cache(bs_code, df)

def main():
    # 获取所有A股股票代码
    stock_list = fetch_all_stock_codes()
    end_date = datetime.today().strftime("%Y-%m-%d")
    start_date = (datetime.today() - timedelta(days=365 * 2)).strftime("%Y-%m-%d")

    for idx, bs_code in enumerate(stock_list):
        fetch_and_cache_stock_data(bs_code, start_date, end_date)

        # 显示进度
        progress = (idx + 1) / len(stock_list) * 100
        print(f"Progress: {progress:.2f}% ({idx + 1}/{len(stock_list)})", end="\r")

    print("\nData fetching completed.")

if __name__ == "__main__":
    main()

# 登出baostock
bs.logout()
