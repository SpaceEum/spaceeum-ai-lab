
import sqlite3
import pandas as pd
import os

def load_summary(db_path):
    if not os.path.exists(db_path):
        print(f"File not found: {db_path}")
        return None
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql("SELECT * FROM strategy_summary", conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Error loading {db_path}: {e}")
        return None

def compare_dbs():
    file_old = "tenkan_v3_result_20251217_072448.db"
    file_new = "tenkan_v3_result_20251217_130421.db"
    
    print(f"--- 비교 대상 ---")
    print(f"파일 A (Old): {file_old}")
    print(f"파일 B (New): {file_new}")
    
    df1 = load_summary(file_old)
    df2 = load_summary(file_new)
    
    if df1 is None or df2 is None:
        print("파일을 로드할 수 없어 비교를 중단합니다.")
        return

    # 전략 타입(strategy_type)을 기준으로 병합
    merged = pd.merge(df1, df2, on='strategy_type', suffixes=('_Old', '_New'), how='outer')
    
    print("\n--- 전략별 성과 비교 ---")
    # 보기 좋게 컬럼 순서 정렬
    cols = ['strategy_type', 'total_trades_Old', 'total_trades_New', 'win_rate_Old', 'win_rate_New']
    
    # 추가 비교 컬럼
    if 'avg_profit_usd_Old' in merged.columns:
        cols.extend(['avg_profit_usd_Old', 'avg_profit_usd_New'])
        
    print(merged[cols].to_string(index=False))

if __name__ == "__main__":
    compare_dbs()
