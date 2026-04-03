import pandas as pd
import os
import glob
from backtest_engine import run_backtest, DATA_DIR

def optimize_strategy():
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    print(f"Found {len(files)} files. Starting full optimization...")
    
    results = []
    
    for i, file in enumerate(files):
        res = run_backtest(file)
        if res:
            results.append(res)
            # Print progress every 10 files
            if (i + 1) % 10 == 0:
                print(f"Processed {i + 1}/{len(files)} files...")

    # Create DataFrame
    df = pd.DataFrame(results)
    
    # Filter for User Criteria
    # 1. Win Rate >= 50%
    # 2. Total Return > 0 (Positive)
    # 3. Trades > 10 (To ensure statistical significance, though user didn't specify, it's good practice)
    
    filtered_df = df[
        (df['Win Rate'] >= 50) & 
        (df['Total Return'] > 0)
    ].copy()
    
    filtered_df.sort_values('Total Return', ascending=False, inplace=True)
    
    print(f"\nOptimization Complete.")
    print(f"Total Tickers Tested: {len(df)}")
    print(f"Tickers Meeting Criteria (>50% WR, Positive Return): {len(filtered_df)}")
    
    print("\nTop 10 Candidates:")
    print(filtered_df.head(10))
    
    # Save
    filtered_df.to_csv("optimized_candidates.csv", index=False)
    df.to_csv("all_backtest_results.csv", index=False)

if __name__ == "__main__":
    optimize_strategy()
