import pyupbit
import time
from datetime import datetime
import os
import pandas as pd
import schedule

def save_ohlcv_data():
    # Create directory if it doesn't exist
    save_dir = "upbit_ohlcv_min15"
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    # Get all KRW market tickers
    krw_tickers = pyupbit.get_tickers(fiat="KRW")

    print(f"\n=== Saving KRW Market OHLCV Data (15min intervals) at {datetime.now()} ===")

    for ticker in krw_tickers:
        try:
            # Get OHLCV data
            df = pyupbit.get_ohlcv(ticker, interval="minute15", count=200)

            if df is not None and not df.empty:
                # Remove the last candle (current forming candle)
                df = df[:-1] 

                # Create filename
                filename = f"upbit_{ticker.lower()}_min15.json"
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
    # schedule.every().hour.at(":01").do(save_ohlcv_data)
    # schedule.every().hour.at(":16").do(save_ohlcv_data)
    # schedule.every().hour.at(":31").do(save_ohlcv_data)
    # schedule.every().hour.at(":46").do(save_ohlcv_data)

    print("1: 수동으로 데이터 수집 시작")
    print("2: 스케줄러 시작 (매시간 01분, 16분, 31분, 46분)")
    choice = input("선택하세요 (1 또는 2): ")

    if choice == "1":
        print("데이터 수집을 시작합니다...")
        save_ohlcv_data()
        print("데이터 수집이 완료되었습니다.")
    elif choice == "2":
        print("스케줄러가 시작되었습니다. 매시간 01분, 16분, 31분, 46분에 데이터를 저장합니다.")
        schedule.every().hour.at(":01").do(save_ohlcv_data)
        schedule.every().hour.at(":16").do(save_ohlcv_data)
        schedule.every().hour.at(":31").do(save_ohlcv_data)
        schedule.every().hour.at(":46").do(save_ohlcv_data)
        while True:
            schedule.run_pending()
            time.sleep(1)