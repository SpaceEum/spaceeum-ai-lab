
import sqlite3
import pandas as pd
import glob
import os

def load_summary(db_path):
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql("SELECT * FROM strategy_summary", conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Error loading {db_path}: {e}")
        return None

def compare_dbs(file1, file2):
    print(f"--- 비교 대상 ---")
    print(f"파일 A: {os.path.basename(file1)}")
    print(f"파일 B: {os.path.basename(file2)}")
    
    df1 = load_summary(file1)
    df2 = load_summary(file2)
    
    if df1 is None or df2 is None:
        return

    # 전략 타입(strategy_type)을 기준으로 병합
    merged = pd.merge(df1, df2, on='strategy_type', suffixes=('_A', '_B'), how='outer')
    
    print("\n--- 전략별 성과 비교 ---")
    # 보기 좋게 컬럼 순서 정렬
    cols = ['strategy_type', 'total_trades_A', 'total_trades_B', 'win_rate_A', 'win_rate_B']
    
    # exclude_avg_pnl_pct 컬럼이 있으면 추가
    if 'exclude_avg_pnl_pct_A' in merged.columns:
        cols.extend(['exclude_avg_pnl_pct_A', 'exclude_avg_pnl_pct_B'])
        
    print(merged[cols].to_string(index=False))

if __name__ == "__main__":
    # 1. 파일 목록 찾기
    files = sorted(glob.glob("tenkan_v1_result_*.db"))
    
    if len(files) < 2:
        print("비교할 DB 파일이 2개 이상 필요합니다.")
        print(f"발견된 파일: {files}")
    else:
        # 사용자가 요청한 파일명을 우선 찾음
        target_a = "tenkan_v1_result_20251217_120555.db"
        target_b = "tenkan_v1_result_20251217_121517.db" # 120603 대신 최신파일 사용 추정
        
        # 실제 존재하는지 확인
        file_a = target_a if target_a in files else files[-2] # 없으면 뒤에서 2번째
        file_b = target_b if target_b in files else files[-1] # 없으면 마지막
        
        compare_dbs(file_a, file_b)
