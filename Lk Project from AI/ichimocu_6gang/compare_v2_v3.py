
import sqlite3
import pandas as pd
import os

DB_DIR = "g:/내 드라이브/LK Project/Lk Project from AI/ichimocu_6gang"
V3_DB = "tenkan_v3_result_20251217_061042.db"
V2_DB = "tenkan_v2_result_20251217_061101.db"

def get_summary(db_name):
    path = os.path.join(DB_DIR, db_name)
    if not os.path.exists(path):
        print(f"DB not found: {path}")
        return pd.DataFrame()
    
    conn = sqlite3.connect(path)
    try:
        df = pd.read_sql("SELECT * FROM ticker_summary", conn)
        return df
    except Exception as e:
        print(f"Error reading {db_name}: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

df2 = get_summary(V2_DB)
df3 = get_summary(V3_DB)

if df2.empty or df3.empty:
    print("One of the dataframes is empty.")
else:
    # Merge
    merged = pd.merge(df2, df3, on='ticker', suffixes=('_V2', '_V3'))
    
    # Select columns
    cols = ['ticker', 
            'win_rate_pct_V2', 'win_rate_pct_V3', 
            'mdd_pct_V2', 'mdd_pct_V3', 
            'total_net_profit_usd_V2', 'total_net_profit_usd_V3',
            'total_trades_V2', 'total_trades_V3']
            
    output = merged[cols].to_string(index=False)
    print(output)
    with open("comparison_result.txt", "w") as f:
        f.write(output)
