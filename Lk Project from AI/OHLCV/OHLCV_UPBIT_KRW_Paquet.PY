import pyupbit
import time
from datetime import datetime
import os
import pandas as pd
import schedule
import pandas_ta as ta

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
                # --- 데이터 타입 명시적 변환 (FutureWarning 해결) ---
                # 모든 컬럼을 float64로 변환하여 계산 시 타입 불일치 문제를 방지합니다.
                df = df.astype('float64')

                # --- 보조지표 계산 추가 ---
                # 추세 지표
                try:
                    # 모든 지표를 한 번에 계산 (strategy 사용)
                    # 이렇게 하면 개별 지표에서 오류가 발생해도 전체가 멈추지 않고,
                    # 계산 가능한 지표들은 모두 추가됩니다.
                    custom_strategy = ta.Strategy(
                        name="All Indicators",
                        description="SMA, EMA, MACD, ADX, PSAR, Ichimoku, RSI, Stoch, CCI, ROC, WILLR, BBands, ATR, KC, OBV, MFI, VWAP, CHOP",
                        ta=[
                            {"kind": "sma", "length": 5}, {"kind": "sma", "length": 10}, {"kind": "sma", "length": 20}, {"kind": "sma", "length": 60}, {"kind": "sma", "length": 120}, {"kind": "sma", "length": 240},
                            {"kind": "ema", "length": 5}, {"kind": "ema", "length": 10}, {"kind": "ema", "length": 20}, {"kind": "ema", "length": 60}, {"kind": "ema", "length": 120}, {"kind": "ema", "length": 240},
                            {"kind": "macd"},
                            {"kind": "adx"},
                            {"kind": "psar"},
                            {"kind": "ichimoku"},
                            {"kind": "rsi"},
                            {"kind": "stoch"},
                            {"kind": "cci"},
                            {"kind": "roc"},
                            {"kind": "willr"},
                            {"kind": "bbands"},
                            {"kind": "atr"},
                            {"kind": "kc"},
                            {"kind": "obv"},
                            {"kind": "mfi"},
                            {"kind": "vwap"},
                            {"kind": "chop"},
                        ]
                    )
                    df.ta.strategy(custom_strategy)
                except Exception as indicator_e:
                    print(f"    - Indicator calculation error for {ticker}: {indicator_e}")
                # --------------------------

                # 일봉(`interval="day"`) 데이터는 일반적으로 완성된 캔들을 반환하므로 마지막 행을 제거하지 않습니다.
                # 마지막 행을 제거하면 가장 최신 데이터를 잃게 됩니다.
                if df.empty:
                    continue

                # Create filename
                filename = f"upbit_{ticker.lower()}_day.parquet" # Changed to day and parquet
                filepath = os.path.join(save_dir, filename)

                # 기존 데이터가 있으면 로드
                if os.path.exists(filepath):
                    existing_df = pd.read_parquet(filepath)
                    # 새로운 데이터만 병합
                    df = pd.concat([existing_df, df])
                    # 중복된 인덱스 제거 (최신 데이터 유지)
                    df = df[~df.index.duplicated(keep='last')]

                # 시간순으로 정렬
                df.sort_index(inplace=True)
                # Parquet으로 저장
                df.to_parquet(filepath)

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