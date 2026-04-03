
import sqlite3
import pandas as pd
import os

def analyze_results():
    db_path = "tenkan_v3_result_20251217_143117.db"
    
    if not os.path.exists(db_path):
        print(f"File not found: {db_path}")
        # 파일이 없을 경우 가장 최근 db 파일을 찾아서 사용 시도
        files = [f for f in os.listdir('.') if f.startswith('tenkan_v3_result_') and f.endswith('.db')]
        if files:
            files.sort(reverse=True)
            db_path = files[0]
            print(f"대신 가장 최근 파일({db_path})을 분석합니다.")
        else:
            return

    try:
        conn = sqlite3.connect(db_path)
        # ticker_summary 테이블 로드
        df = pd.read_sql("SELECT * FROM ticker_summary", conn)
        conn.close()
        
        print(f"분석 대상 파일: {db_path}")
        print(f"총 분석 티커 수: {len(df)}")
        
        # 종합 점수 계산 (Score = WinRate * PnL_Ratio * (100 - MDD) / 100)
        # PnL Ratio나 MDD가 없는 경우를 대비해 예외처리
        if 'pnl_ratio' not in df.columns: df['pnl_ratio'] = 0
        if 'mdd_pct' not in df.columns: df['mdd_pct'] = 0
        if 'win_rate_pct' not in df.columns: df['win_rate_pct'] = 0
        
        # 최소 거래 횟수 5회 이상 필터링
        df_filtered = df[df['total_trades'] >= 5].copy()
        
        # 점수 계산
        df_filtered['score'] = df_filtered['win_rate_pct'] * df_filtered['pnl_ratio'] * (100 - df_filtered['mdd_pct']) / 100
        
        # 정렬 (점수 내림차순)
        df_top20 = df_filtered.sort_values(by='score', ascending=False).head(20)
        
        print("\n=== BEST 20 종목 (승률/손익비/MDD 종합) ===")
        print(df_top20[['ticker', 'score', 'win_rate_pct', 'pnl_ratio', 'mdd_pct', 'total_net_profit_usd']].to_string(index=False))
        
    except Exception as e:
        print(f"Error validating {db_path}: {e}")

if __name__ == "__main__":
    analyze_results()
