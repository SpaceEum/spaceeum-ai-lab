import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import sys
import os

# Add parent directory to path to import Heatmap_trade
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import Heatmap_trade

# --- Constants ---
TOP_N_TICKERS = 50
MAX_WORKERS = 10
LEVERAGE = 10.0

def get_all_usdt_pairs():
    """
    바이낸스 선물 거래소에서 거래량 상위 50개 USDT Perpetual Ticker를 가져옵니다.
    """
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    try:
        response = requests.get(url)
        data = response.json()

        # 1. Get valid symbols
        info_url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        info_resp = requests.get(info_url)
        info_data = info_resp.json()
        valid_symbols = {
            s['symbol']
            for s in info_data['symbols']
            if s['contractType'] == 'PERPETUAL' and s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING'
        }

        # 2. Get volume data
        usdt_tickers = [t for t in data if t['symbol'] in valid_symbols]

        # 3. Sort by quote volume (turnover)
        usdt_tickers.sort(key=lambda x: float(x['quoteVolume']), reverse=True)

        # 4. Top N
        top_50 = [t['symbol'] for t in usdt_tickers[:TOP_N_TICKERS]]
        return top_50
    except Exception as e:
        print(f"Error fetching USDT pairs: {e}")
        return []


def process_symbol(symbol, leverage=10.0):
    """Fetch data for a symbol and run backtest with specified leverage.
    Leverage is applied on isolated margin (conceptual, no explicit API call).
    """
    try:
        df = Heatmap_trade.fetch_binance_data(symbol)
        if df.empty:
            return None
        return Heatmap_trade.run_backtest(df, leverage=leverage)
    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        return None


def main():
    print("Fetching all USDT Perpetual Tickers...")
    symbols = get_all_usdt_pairs()
    print(f"Found {len(symbols)} tickers.")

    results = []
    print(f"Starting Backtest (Parallel) with leverage={LEVERAGE} (isolated)...")

    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_symbol = {executor.submit(process_symbol, symbol, LEVERAGE): symbol for symbol in symbols}
        completed = 0
        for future in concurrent.futures.as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            completed += 1
            print(f"[{completed}/{len(symbols)}] Finished {symbol}...", end='\r')
            try:
                res = future.result()
                if res:
                    res['symbol'] = symbol
                    results.append(res)
            except Exception as e:
                print(f"\nError processing {symbol}: {e}")

    print("\nBacktest Complete.")

    if not results:
        print("No results generated.")
        return

    results_df = pd.DataFrame(results)

    # Sort by profit percentage (higher is better)
    results_df = results_df.sort_values(by='profit_pct', ascending=False)

    # Save all results
    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    results_path = os.path.join(script_dir, f"{script_name}_results.csv")
    results_df.to_csv(results_path, index=False)
    print(f"All results saved to '{results_path}'")

    # Top 5 tickers
    top_5 = results_df.head(5)
    print("\n=== Top 5 Tickers (Profit %) ===")
    print(top_5[['symbol', 'profit_pct', 'total_profit', 'trade_count', 'win_rate']].to_string(index=False))

    # Save Top 5 summary
    output_path = os.path.join(script_dir, f"{script_name}_summary.txt")
    with open(output_path, "w") as f:
        f.write("=== Top 5 Tickers (Profit %) ===\n")
        f.write(top_5[['symbol', 'profit_pct', 'total_profit', 'trade_count', 'win_rate']].to_string(index=False))

if __name__ == "__main__":
    main()
