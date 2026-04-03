import sqlite3
import pandas as pd
import glob
import os

# 현재 폴더에서 가장 최근의 tenkan_v2_result DB 파일 찾기
db_files = glob.glob('tenkan_v2_result_*.db')
if not db_files:
    print("DB 파일을 찾을 수 없습니다.")
    exit()

latest_db = max(db_files, key=os.path.getctime)
print(f"가장 최근 DB 파일: {latest_db}")

conn = sqlite3.connect(latest_db)

# 1. 요약 테이블 변환
try:
    df_summary = pd.read_sql_query("SELECT * FROM strategy_summary", conn)
    csv_summary = latest_db.replace('.db', '_summary.csv')
    df_summary.to_csv(csv_summary, index=False, encoding='utf-8-sig') # 엑셀 호환 인코딩
    print(f"변환 완료: {csv_summary}")
except Exception as e:
    print(f"요약 테이블 변환 실패: {e}")

# 2. 상세 내역 테이블 변환
try:
    df_trades = pd.read_sql_query("SELECT * FROM trades_detail", conn)
    csv_detail = latest_db.replace('.db', '_details.csv')
    df_trades.to_csv(csv_detail, index=False, encoding='utf-8-sig')
    print(f"변환 완료: {csv_detail}")
except Exception as e:
    print(f"상세 내역 테이블 변환 실패: {e}")

conn.close()
