#!/usr/bin/env python3

import sys
import pandas as pd
import numpy as np
from typing import Dict, List
import os
import json
from datetime import datetime

# 缓存目录
CACHE_DIR = "stock_cache"
RPS_PERIODS = [50, 120, 250]

def load_all_stocks_data() -> Dict[str, pd.DataFrame]:
    all_stocks = {}
    for file in os.listdir(CACHE_DIR):
        if file.endswith('.json'):
            ts_code = file.split('.')[1]
            with open(os.path.join(CACHE_DIR, file), 'r') as f:
                data = json.load(f)
            df = pd.DataFrame(data)
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            all_stocks[ts_code] = df
    return all_stocks

def calculate_rps(all_stocks: Dict[str, pd.DataFrame], target_stock: str) -> Dict[str, float]:
    rps_data = {}
    target_df = all_stocks[target_stock]
    
    for period in RPS_PERIODS:
        # 计算所有股票的价格变化百分比
        all_changes = {}
        for ts_code, df in all_stocks.items():
            all_changes[ts_code] = df['close'].pct_change(periods=period).iloc[-1]
        
        # 将目标股票的变化百分比与所有股票比较
        target_change = all_changes[target_stock]
        if np.isnan(target_change):
            rps_data[f'rps{period}'] = np.nan
        else:
            better_count = sum(1 for change in all_changes.values() if change < target_change and not np.isnan(change))
            total_valid = sum(1 for change in all_changes.values() if not np.isnan(change))
            rps_data[f'rps{period}'] = (better_count / total_valid) * 100
    
    return rps_data

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 checkrps.py <6-digit stock code>")
        sys.exit(1)

    target_stock = sys.argv[1]
    if len(target_stock) != 6 or not target_stock.isdigit():
        print("Please provide a valid 6-digit stock code.")
        sys.exit(1)

    all_stocks = load_all_stocks_data()
    if target_stock not in all_stocks:
        print(f"Stock {target_stock} not found in cache.")
        sys.exit(1)

    rps_results = calculate_rps(all_stocks, target_stock)
    
    print(f"RPS data for {target_stock}:")
    for period in RPS_PERIODS:
        print(f"RPS{period}: {rps_results[f'rps{period}']:.2f}")

if __name__ == "__main__":
    main()