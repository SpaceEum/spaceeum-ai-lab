import sqlite3
import pandas as pd
from datetime import datetime

# --- 설정 ---
INPUT_DB = "final_15m_20251002_015716.db" # 원본 DB 파일명

# --- 함수 정의 ---
def redata():
    """
    final DB에서 total_trades 20 이상인 데이터를 trade DB로 추출합니다.
    """
    try:
        # 1. DB 연결
        conn = sqlite3.connect(INPUT_DB)
        query = "SELECT * FROM analyzed_results WHERE total_trades >= 20 AND win_rate_percent > 50.1"
        df = pd.read_sql_query(query, conn)
        conn.close()

        # 2. 필터링된 데이터 확인
        if df.empty:
            print("조건에 맞는 데이터가 없습니다.")
            return

        # 3. 새로운 DB에 저장
        output_db = f"trade_{datetime.now().strftime('%Y%m%d')}.db"
        conn = sqlite3.connect(output_db)
        df.to_sql('filtered_trades', conn, if_exists='replace', index=False)

        print(f"trade 횟수가 20 이상인 데이터가 '{output_db}'로 성공적으로 저장되었습니다.")
    except Exception as e:
        print(f"오류 발생: {e}")
if __name__ == "__main__":
    redata()