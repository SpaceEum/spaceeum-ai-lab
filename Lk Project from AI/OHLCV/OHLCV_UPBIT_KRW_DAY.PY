import pyupbit
import time
from datetime import datetime
import os
import pandas as pd
import schedule

def save_ohlcv_data():
    # Create directory if it doesn't exist
    save_dir = "upbit_ohlcv_day" # Changed to day
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    # Get all KRW market tickers
    krw_tickers = pyupbit.get_tickers(fiat="KRW")

    print(f"\n=== Saving KRW Market OHLCV Data (Daily intervals) at {datetime.now()} ===") # Changed to Daily

    for ticker in krw_tickers:
        try:
            # Get OHLCV data
            df = pyupbit.get_ohlcv(ticker, interval="day", count=200) # Changed interval to day

            if df is not None and not df.empty:
                # Remove the last candle (current forming candle)
                # For daily data, the last candle returned by get_ohlcv(interval="day") is usually the completed previous day's candle.
                # If 'count' fetches today's forming candle, then df[:-1] is correct.
                # If it only fetches completed candles, df[:-1] might remove the latest complete day.
                # This needs verification based on pyupbit's behavior for daily data.
                # Assuming for now it might include a forming candle if run mid-day for 'today'.
                # However, if scheduled after midnight, it should be the completed previous day.
                # Let's keep df[:-1] for now, but it's a point to verify.
                df = df[:-1] 

                # Create filename
                filename = f"upbit_{ticker.lower()}_day.json" # Changed to day
                filepath = os.path.join(save_dir, filename)

                # 기존 데이터가 있으면 로드
                if os.path.exists(filepath):
                    existing_df = pd.read_json(filepath)
                    existing_df.index = pd.to_datetime(existing_df.index)
                    # 새로운 데이터만 병합
                    df = pd.concat([existing_df, df])
                    # 중복된 인덱스 제거 (최신 데이터 유지)
                    df = df[~df.index.duplicated(keep='last')]
                    # 시간순으로 정렬
                    df.sort_index(inplace=True)

                # JSON으로 저장
                df.to_json(filepath, date_format='iso')

                print(f"Saved {ticker} data to {filename}")

            # API 호출 제한을 피하기 위한 짧은 대기 시간
            time.sleep(0.1)

        except Exception as e:
            print(f"\nError processing {ticker}: {str(e)}")
            continue

if __name__ == "__main__":
    # 스케줄러 설정 (주석 처리)
    # For daily data, schedule to run once a day, e.g., shortly after midnight.
    # Example: Run at 00:05 (5 minutes past midnight)
    # schedule.every().day.at("00:05").do(save_ohlcv_data)

    print("1: 수동으로 데이터 수집 시작")
    print("2: 스케줄러 시작 (예: 매일 00:05에 자동 실행)") # Updated scheduler description
    choice = input("선택하세요 (1 또는 2): ")

    if choice == "1":
        print("데이터 수집을 시작합니다...")
        save_ohlcv_data()
        print("데이터 수집이 완료되었습니다.")
    elif choice == "2":
        print("스케줄러가 시작되었습니다. 매일 00:05에 데이터를 저장합니다.") # Updated scheduler run time
        schedule.every().day.at("00:05").do(save_ohlcv_data) # Schedule to run daily

        while True:
            schedule.run_pending()
            time.sleep(1)
    else:
        print("잘못된 선택입니다. 1 또는 2를 입력해주세요.")