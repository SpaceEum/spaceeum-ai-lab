
import pandas as pd
import os
from tenkan_backtest_v2 import run_v2_strategy, OHLCV_DIR, TIMEFRAME_FOLDERS

# Load one file
ticker = "BTCUSDT"
file_path = os.path.join(OHLCV_DIR, TIMEFRAME_FOLDERS['15m'], f"{ticker}.csv")

print(f"Testing on {ticker}...")
df = pd.read_csv(file_path)
df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Run strategy
trades = run_v2_strategy(df, 0.0002, 5)

print(f"Total Trades: {len(trades)}")
if trades:
    print("First Trade Sample:")
    print(trades[0])
    
    df_t = pd.DataFrame(trades)
    print("\nMetrics:")
    print(f"Net Profit Sum: {df_t['net_profit'].sum()}")
    print(f"Win Rate: {len(df_t[df_t['net_profit'] > 0]) / len(df_t)}")
else:
    print("No trades found.")
