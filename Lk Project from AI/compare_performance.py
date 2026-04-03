import sqlite3
import pandas as pd
import os
import re
from datetime import datetime

# Paths
LDW_DIR = os.path.join(os.path.dirname(__file__), '1.00 LDW', 'LDW02_1st')
MAIN_BOT_DIR = os.path.join(os.path.dirname(__file__), 'Binance_Futures_Bot')
LDW_DB = os.path.join(LDW_DIR, 'futures_trades.db')
MAIN_LOG = os.path.join(MAIN_BOT_DIR, 'trading_bot.log')

def get_ldw_stats():
    if not os.path.exists(LDW_DB):
        return {"Bot": "LDW02_1st", "Status": "DB Not Found"}
    
    try:
        conn = sqlite3.connect(LDW_DB)
        df = pd.read_sql_query("SELECT * FROM trades", conn)
        conn.close()
        
        if df.empty:
            return {"Bot": "LDW02_1st", "Trades": 0, "Win Rate": "0%", "Total PnL": "0%"}
            
        closed_trades = df[df['status'] == 'closed']
        total_trades = len(closed_trades)
        if total_trades == 0:
            return {"Bot": "LDW02_1st", "Trades": 0, "Win Rate": "0%", "Total PnL": "0%"}
            
        wins = closed_trades[closed_trades['pnl'] > 0]
        win_rate = (len(wins) / total_trades) * 100
        total_pnl = closed_trades['pnl'].sum()
        
        return {
            "Bot": "LDW02_1st",
            "Trades": total_trades,
            "Win Rate": f"{win_rate:.2f}%",
            "Total PnL": f"{total_pnl:.2f}%"
        }
    except Exception as e:
        return {"Bot": "LDW02_1st", "Status": f"Error: {e}"}

def get_main_bot_stats_list():
    import glob
    
    # 1. Scan for all trades*.db files
    db_pattern = os.path.join(MAIN_BOT_DIR, 'trades*.db')
    db_files = glob.glob(db_pattern)
    
    stats_list = []
    
    # 2. Process DBs
    for db_path in db_files:
        version_name = os.path.basename(db_path).replace('trades', '').replace('.db', '')
        if version_name == "" or version_name == "_":
            bot_name = "Binance_Bot (Main)"
        else:
            bot_name = f"Binance_Bot {version_name.strip('_')}"
            
        try:
            conn = sqlite3.connect(db_path)
            try:
                df = pd.read_sql_query("SELECT * FROM trades", conn)
            except:
                df = pd.DataFrame() # Handle empty DB structure
            conn.close()
            
            if not df.empty:
                entries = len(df[df['status'].str.contains('ENTRY')])
                sl_orders = len(df[df['status'].str.contains('SL')])
                tp_orders = len(df[df['status'].str.contains('TP')])
                
                stats_list.append({
                    "Bot": bot_name,
                    "Source": "DB",
                    "Entries": entries,
                    "SL Orders": sl_orders,
                    "TP Orders": tp_orders,
                    "Note": os.path.basename(db_path)
                })
            else:
                stats_list.append({
                    "Bot": bot_name,
                    "Source": "DB",
                    "Entries": 0,
                    "Note": "Empty DB"
                })
        except Exception as e:
           stats_list.append({"Bot": bot_name, "Source": "DB", "Note": f"Error: {e}"})

    # 3. Fallback/Check Logs (Only for main bot if no DB found, or just add as extra info)
    # If stats_list is empty, maybe try logs
    if not stats_list and os.path.exists(MAIN_LOG):
        try:
            with open(MAIN_LOG, 'r', encoding='utf-8', errors='ignore') as f:
                logs = f.readlines()
            entries = sum(1 for line in logs if "Order Placed for" in line)
            errors = sum(1 for line in logs if "ERROR" in line)
            stats_list.append({
                "Bot": "Binance_Bot (Log)",
                "Source": "Log",
                "Entries": entries,
                "Errors": errors,
                "Note": "Legacy Logs"
            })
        except:
            pass
            
    return stats_list

def main():
    print("=== Bot Performance Comparison ===")
    
    ldw_stats = get_ldw_stats()
    main_stats_list = get_main_bot_stats_list()
    
    print("\n--- LDW02_1st ---")
    print(pd.DataFrame([ldw_stats]).to_string(index=False))
    
    print("\n\n--- Binance_Futures_Bot Versions ---")
    if main_stats_list:
        print(pd.DataFrame(main_stats_list).to_string(index=False))
    else:
        print("No data found.")
    
    print("\n==================================")

if __name__ == "__main__":
    main()
