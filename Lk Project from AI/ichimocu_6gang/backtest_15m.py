# /path/to/your/project/backtest.py

import os
import glob
import pandas as pd
import pandas_ta as ta
import sqlite3
from datetime import datetime
import numpy as np
import time
import sys
import ccxt
import schedule

# --- ✨ 추가: 백테스트 실행 상태 플래그 ---
IS_BACKTEST_RUNNING = False

# --- 설정 ---
# 데이터 최상위 폴더
OHLCV_DIR = "D:/project/ohlcv"

# 타임프레임별 데이터 폴더 이름
TIMEFRAME_FOLDERS = {
    '15m': "Binance_Fureres_USDT_15Minute_ohlcv",
    # '30m': "Binance_Fureres_USDT_30Minute_ohlcv",
    # '1h': "Binance_Fureres_USDT_1Hour_ohlcv",
    # '4h': "Binance_Fureres_USDT_4Hour_ohlcv",
    #'1d': "Binance_Fureres_USDT_1Day_ohlcv"
}

# 지표 설정
ICHIMOKU_PERIODS = [9, 26, 52]
SMA_PERIOD = 60
BOLLINGER_PERIOD = 10

# --- ✨ 추가: 레버리지 설정 (live_trader.py와 동기화) ---
LEVERAGE = 10

# 거래 수수료
FEE_RATE = 0.0004

# --- 티커 필터링 설정 ---
TOP_N_FOR_FILTER = 400 # 각 조건(거래량, 거래대금, 미결제약정)별로 상위 N개 티커를 선정

# --- 종합 점수 가중치 설정 ---
WEIGHTS = {
    'pnl': 0.1,      
    'win_rate': 0.5, 
    'avg_pnl': 0.4   
}

# --- 백테스팅 개별 전략 실행 로직 ---
def run_backtest(df, fee_rate, leverage, strategy='V1_GoldenCross', trailing_stop_pct=None, stop_loss_pct=None, take_profit_pct=None):
    """
    'stratege.md' 기반 일목균형표 매매 전략 백테스트
    - 매수 조건 (전략별 상이):
        - V1_GoldenCross: 골든 크로스 & 정배열
        - V2_KijunBreakout: 기준선 돌파 (26일 신고가) & 정배열
        - V3_400PointRise: 전환선+기준선 동시 돌파 (9일+26일 신고가) & 정배열
    - 공통 매수 조건 (AND):
        1. 주가 > 구름대 (선행스팬1, 선행스팬2)
        2. 후행스팬 조건: 현재 주가 > 26봉 전 주가 (상승 추세 확인)
        3. 전환선 상승 중
        4. 주가 > 전환선 (기본 원칙)
    - 매도 조건 (OR):
        1. 데드 크로스 (전환선 < 기준선) 를 기본으로 하되, 외부에서 손절/익절/TS 조건이 주어지면 함께 사용
        2. 리스크 관리 조건 (트레일링 스탑, 손절, 익절)
        3. 강제 청산
    """
    # 지표 컬럼명 정의
    tenkan_col = f'ITS_{ICHIMOKU_PERIODS[0]}'
    kijun_col = f'IKS_{ICHIMOKU_PERIODS[1]}'
    senkou_a_col = f'ISA_{ICHIMOKU_PERIODS[0]}'
    senkou_b_col = f'ISB_{ICHIMOKU_PERIODS[1]}'
    
    # 후행스팬 비교를 위해 26봉 전 주가 컬럼 추가
    df['close_shifted_26'] = df['close'].shift(ICHIMOKU_PERIODS[1])
    
    # 신규 전략을 위한 이동평균 고점 계산
    df['tenkan_period_high'] = df['high'].rolling(ICHIMOKU_PERIODS[0]).max()
    df['kijun_period_high'] = df['high'].rolling(ICHIMOKU_PERIODS[1]).max()


    in_position = False
    highest_price_since_entry = 0
    trades = []

    # 지표 계산으로 생성된 NaN만 제거하도록 명시적으로 컬럼 지정
    dropna_cols = [tenkan_col, kijun_col, senkou_a_col, senkou_b_col, 'close_shifted_26', 'tenkan_period_high', 'kijun_period_high']
    df_cleaned = df.dropna(subset=dropna_cols).copy()

    for i in range(1, len(df_cleaned)):
        prev_row = df_cleaned.iloc[i-1]
        current_row = df_cleaned.iloc[i]

        # --- 1. 포지션 보유 시, 종료 조건 확인 ---
        if in_position:
            entry_price = trades[-1]['entry_price']
            highest_price_since_entry = max(highest_price_since_entry, current_row['high'])
            
            exit_triggered = False
            exit_price = 0
            is_liquidated = False

            # 0. 강제 청산 (격리 마진 시뮬레이션)
            liquidation_price = entry_price * (1 - (1 / leverage))
            if current_row['low'] <= liquidation_price:
                exit_triggered = True
                exit_price = liquidation_price
                is_liquidated = True

            # 1. 데드 크로스 (지표 기반 종료)
            if not exit_triggered:
                is_dead_cross = prev_row[tenkan_col] >= prev_row[kijun_col] and current_row[tenkan_col] < current_row[kijun_col]
                if is_dead_cross:
                    exit_triggered = True
                    exit_price = current_row['close']

            # 2. 고정 손절 (Stop-Loss)
            if not exit_triggered and stop_loss_pct is not None:
                sl_price = entry_price * (1 - stop_loss_pct)
                if current_row['low'] <= sl_price:
                    exit_triggered = True
                    exit_price = sl_price

            # 3. 고정 익절 (Take-Profit)
            if not exit_triggered and take_profit_pct is not None:
                tp_price = entry_price * (1 + take_profit_pct)
                if current_row['high'] >= tp_price:
                    exit_triggered = True
                    exit_price = tp_price
            
            # 4. 트레일링 스탑
            if not exit_triggered and trailing_stop_pct is not None:
                ts_price = highest_price_since_entry * (1 - trailing_stop_pct)
                if current_row['low'] <= ts_price:
                    exit_triggered = True
                    exit_price = ts_price

            if exit_triggered:
                in_position = False
                pnl = 0
                if is_liquidated:
                    pnl = -1.0
                else:
                    pnl = ((exit_price / entry_price) - 1) * leverage - (leverage * FEE_RATE) - (leverage * (exit_price / entry_price) * FEE_RATE)
                trades[-1].update({
                    'exit_date': current_row.name,
                    'exit_price': exit_price,
                    'pnl': pnl
                })

        # --- 2. 포지션 미보유 시, 진입 조건 확인 ---
        if not in_position:
            # 공통 매수 조건
            price_above_cloud = current_row['close'] > current_row[senkou_a_col] and current_row['close'] > current_row[senkou_b_col]
            chikou_above_price = current_row['close'] > current_row['close_shifted_26']
            tenkan_rising = current_row[tenkan_col] > prev_row[tenkan_col]
            price_above_tenkan = current_row['close'] > current_row[tenkan_col]

            entry_signal = False
            
            # 공통 조건 우선 체크
            common_conditions_met = price_above_cloud and chikou_above_price and tenkan_rising and price_above_tenkan

            if common_conditions_met:
                if strategy == 'V1_GoldenCross':
                    is_golden_cross = prev_row[tenkan_col] <= prev_row[kijun_col] and current_row[tenkan_col] > current_row[kijun_col]
                    if is_golden_cross:
                        entry_signal = True
                
                elif strategy == 'V2_KijunBreakout':
                    # 26일 신고가 갱신 (돌파갭)
                    is_kijun_breakout = current_row['kijun_period_high'] > prev_row['kijun_period_high']
                    if is_kijun_breakout:
                        entry_signal = True

                elif strategy == 'V3_400PointRise':
                    # 9일, 26일 동시 신고가 갱신 (400점 상승)
                    is_tenkan_breakout = current_row['tenkan_period_high'] > prev_row['tenkan_period_high']
                    is_kijun_breakout = current_row['kijun_period_high'] > prev_row['kijun_period_high']
                    if is_tenkan_breakout and is_kijun_breakout:
                        entry_signal = True

            if entry_signal:
                in_position = True
                entry_price = current_row['close']
                highest_price_since_entry = entry_price

                volume_ratio = np.nan
                if 'volume_ma20' in current_row and pd.notna(current_row['volume_ma20']) and current_row['volume_ma20'] > 0:
                    volume_ratio = current_row['volume'] / current_row['volume_ma20']

                trading_intensity = np.nan
                if 'trading_intensity' in current_row and pd.notna(current_row['trading_intensity']):
                    trading_intensity = current_row['trading_intensity']

                trades.append({
                    'entry_date': current_row.name, 'entry_price': entry_price,
                    'exit_date': None, 'exit_price': None, 'pnl': None,
                    'volume_ratio': volume_ratio, 'trading_intensity': trading_intensity
                })

    empty_result = {
        '매수 기준선': strategy, '매도 기준선': 'N/A', '총 거래 횟수': 0,
        '수익률(%)': 0, '승률(%)': 0, '평균 손익(%)': 0, '손익비': 0, '평균 진입 거래량 비율': 0,
        '평균 진입 체결강도': 0, '손실거래 평균 거래량비율': 0, '손실거래 평균 체결강도': 0,
        '트레일링 스탑(%)': round((trailing_stop_pct or 0) * 100, 3),
        '손절(%)': round((stop_loss_pct or 0) * 100, 3),
        '익절(%)': round((take_profit_pct or 0) * 100, 3)
    }

    if not trades or trades[-1]['exit_price'] is None:
        if trades and trades[-1]['exit_price'] is None:
            trades.pop()
        if not trades:
            return empty_result

    trade_df = pd.DataFrame(trades)
    total_trades = len(trade_df)
    winning_trades = trade_df[trade_df['pnl'] > 0]
    losing_trades = trade_df[trade_df['pnl'] <= 0]

    win_rate = (len(winning_trades) / total_trades) * 100 if total_trades > 0 else 0
    total_pnl = (trade_df['pnl'] + 1).prod() - 1
    avg_pnl = trade_df['pnl'].mean() * 100

    avg_profit = winning_trades['pnl'].mean()
    avg_loss = abs(losing_trades['pnl'].mean())
    profit_loss_ratio = avg_profit / avg_loss if avg_loss > 0 else np.inf

    avg_win_volume_ratio = winning_trades['volume_ratio'].mean()
    avg_win_trading_intensity = winning_trades['trading_intensity'].mean()
    avg_loss_volume_ratio = losing_trades['volume_ratio'].mean()
    avg_loss_trading_intensity = losing_trades['trading_intensity'].mean()

    return {
        '매수 기준선': strategy, '매도 기준선': 'N/A',
        '총 거래 횟수': total_trades, '수익률(%)': round(total_pnl * 100, 3),
        '승률(%)': round(win_rate, 3), '평균 손익(%)': round(avg_pnl, 3),
        '손익비': round(profit_loss_ratio, 3),
        '평균 진입 거래량 비율': round(avg_win_volume_ratio, 3) if pd.notna(avg_win_volume_ratio) else 0,
        '평균 진입 체결강도': round(avg_win_trading_intensity, 3) if pd.notna(avg_win_trading_intensity) else 0,
        '손실거래 평균 거래량비율': round(avg_loss_volume_ratio, 3) if pd.notna(avg_loss_volume_ratio) else 0,
        '손실거래 평균 체결강도': round(avg_loss_trading_intensity, 3) if pd.notna(avg_loss_trading_intensity) else 0,
        '트레일링 스탑(%)': round((trailing_stop_pct or 0) * 100, 3),
        '손절(%)': round((stop_loss_pct or 0) * 100, 3),
        '익절(%)': round((take_profit_pct or 0) * 100, 3)
    }


# --- ✨ 추가: 증분 백테스팅을 위한 헬퍼 함수 ---
def get_latest_final_db(timeframe):
    """가장 최근에 생성된 final DB 파일을 찾습니다."""
    db_files = glob.glob(f"final_{timeframe}_*.db")
    if not db_files:
        return None, None
    latest_db = max(db_files, key=os.path.getctime)
    return latest_db, pd.to_datetime(os.path.getmtime(latest_db), unit='s')

# --- 타임프레임별 전체 백테스트 및 분석 로직 ---
def run_full_backtest_for_timeframe(timeframe):
    """
    주어진 타임프레임에 대해 전체 백테스트 및 분석을 수행하고 최종 DB를 생성합니다.
    """
    data_dir = os.path.join(OHLCV_DIR, TIMEFRAME_FOLDERS.get(timeframe, ""))
    db_file = f"final_15m_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    
    print(f"\n{'='*25} [ {timeframe} ] 타임프레임 백테스트 시작 {'='*25}")
    
    if not os.path.exists(data_dir):
        print(f"오류: [ {timeframe} ] 데이터 폴더를 찾을 수 없습니다 - {data_dir}")
        return

    all_results = []
    all_csv_files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]
    
    backtest_target_files = []
    final_tickers_list = []

    # --- 티커 필터링 ---
    print(f"\n- [{timeframe}] 5가지 조건으로 백테스트 대상 티커를 필터링합니다...")
    try:
        # --- 1단계: 시장성 필터링 (거래량, 거래대금, 미결제약정) ---
        print(f"\n- 1/3: 로컬 데이터 및 API로 시장성 상위 티커를 필터링합니다...")
        
        # 이 try 블록은 필터링 전체 과정을 감쌉니다.
        
        ticker_stats = []
        one_month_ago = datetime.now() - pd.Timedelta(days=30)
        total_files_for_stats = len(all_csv_files)

        for i, filename in enumerate(all_csv_files):
            if (i + 1) % 20 == 0 or i == 0 or (i + 1) == total_files_for_stats:
                print(f"  - 로컬 데이터 통계 계산 진행: {i+1}/{total_files_for_stats} ({filename.split('.')[0]})")
            
            try:
                file_path = os.path.join(data_dir, filename)
                # ✨ 수정: 파일이 비어있거나 손상된 경우를 대비해 engine='python' 및 예외 처리 강화
                if os.path.getsize(file_path) == 0:
                    print(f"  - [경고] {filename} 파일이 비어있어 건너뜁니다.")
                    continue
                df_stats = pd.read_csv(file_path, header=0, engine='python')
                df_stats.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                df_stats['timestamp'] = pd.to_datetime(df_stats['timestamp'])
                
                df_monthly = df_stats[df_stats['timestamp'] >= one_month_ago].copy()
                if df_monthly.empty:
                    continue

                total_volume = df_monthly['volume'].sum()
                df_monthly['turnover'] = df_monthly['volume'] * df_monthly['close']
                total_turnover = df_monthly['turnover'].sum()

                ticker_stats.append({
                    'ticker': filename.split('.')[0],
                    'monthly_volume': total_volume,
                    'monthly_turnover': total_turnover
                })
            except Exception as e_file:
                print(f"  - {filename} 통계 계산 중 오류: {e_file}")
                continue
        
        if not ticker_stats:
            raise ValueError("통계를 계산할 유의미한 티커가 없습니다.")

        stats_df = pd.DataFrame(ticker_stats)

        # 조건 1: 1개월 거래량
        top_by_volume = set(stats_df.nlargest(TOP_N_FOR_FILTER, 'monthly_volume')['ticker'])
        
        # 조건 2: 1개월 거래대금
        top_by_turnover = set(stats_df.nlargest(TOP_N_FOR_FILTER, 'monthly_turnover')['ticker'])

        # 조건 3: 7일 기준 미결제약정
        exchange = ccxt.binance({'options': {'defaultType': 'future'}})
        oi_stats = []
        since_timestamp = exchange.milliseconds() - 7 * 24 * 60 * 60 * 1000 # 7일 전
        
        # 거래량/거래대금 상위 티커들의 합집합을 대상으로만 API를 조회하여 효율성 증대
        tickers_to_check_oi = sorted(list(top_by_volume.union(top_by_turnover)))
        total_tickers_for_oi = len(tickers_to_check_oi)

        for i, ticker in enumerate(tickers_to_check_oi):
            if (i + 1) % 10 == 0 or i == 0 or (i + 1) == total_tickers_for_oi:
                print(f"    - 미결제약정 API 조회 진행: {i+1}/{total_tickers_for_oi} ({ticker})")
            
            try:
                oi_history = exchange.fetch_open_interest_history(ticker, timeframe, since=since_timestamp, limit=500)
                if not oi_history:
                    continue
                
                interests = [item['openInterestValue'] for item in oi_history]
                avg_oi = sum(interests) / len(interests)
                oi_stats.append({'ticker': ticker, 'avg_open_interest_7d': avg_oi})
                
                time.sleep(exchange.rateLimit / 1000) # API 속도 제한 준수
            except Exception as e_oi:
                # 지원하지 않는 티커 등 오류는 조용히 건너뜀
                # API 조회가 실패하는 티커들이 있을 수 있으므로 경고만 출력하고 계속 진행합니다.
                print(f"      - [경고] {ticker} 미결제약정 조회 중 오류: {e_oi}")
                continue
        
        oi_df = None
        if not oi_stats:
            print(f"  - [경고] 미결제약정 데이터를 가져올 수 있는 티커가 없습니다. 미결제약정 필터를 건너뛰고 2가지 조건으로만 필터링합니다.")
            final_tickers_set = top_by_volume.intersection(top_by_turnover)
        else:
            oi_df = pd.DataFrame(oi_stats)
            top_by_open_interest = set(oi_df.nlargest(TOP_N_FOR_FILTER, 'avg_open_interest_7d')['ticker'])

            # --- 3가지 조건에 모두 만족하는 티커 찾기 (교집합) ---
            final_tickers_set = top_by_volume.intersection(top_by_turnover).intersection(top_by_open_interest)
        
        if not final_tickers_set:
            raise ValueError("설정된 필터링 조건을 모두 만족하는 티커가 없습니다.")

        initial_filtered_list = sorted(list(final_tickers_set))
        print(f"\n- 2/3: 시장성 조건(상위 {TOP_N_FOR_FILTER}개) 만족 티커: {len(initial_filtered_list)}개")

        # --- 2단계: 기술적 분석 필터링 (현재 적용된 조건 없음) ---
        print(f"\n- 3/3: 기술적 분석 필터링을 진행합니다 (현재 적용된 조건 없음)...")
        tech_filtered_list = []
        for i, ticker in enumerate(initial_filtered_list):
            if (i + 1) % 20 == 0 or i == 0 or (i + 1) == len(initial_filtered_list):
                print(f"    - 기술적 분석 필터링 진행: {i+1}/{len(initial_filtered_list)} ({ticker})")
            try:
                file_path = os.path.join(data_dir, f"{ticker}.csv")
                if os.path.getsize(file_path) == 0:
                    print(f"      - [경고] {ticker}.csv 파일이 비어있어 건너뜁니다.")
                    continue
                df_filter = pd.read_csv(file_path, header=0, engine='python')
                df_filter.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                
                # 데이터가 충분한지 확인 (SMA 기간)
                if len(df_filter) < SMA_PERIOD + 1:
                    continue
                
                # 기술적 분석 조건이 제거되었으므로, 데이터 유효성만 통과하면 추가
                tech_filtered_list.append(ticker)

            except Exception as e_tech:
                print(f"      - [경고] {ticker} 추가 필터링 중 오류: {e_tech}")
                continue
        
        final_tickers_list = tech_filtered_list

    except Exception as e:
        import traceback
        print(f"오류: [{timeframe}] 티커 필터링 중 심각한 오류 발생: {e}")
        traceback.print_exc()
        print("오류로 인해 티커 필터링에 실패했습니다. 백테스트를 중단합니다.")
        return
    # --- 필터링 로직 종료 ---

    if not final_tickers_list:
        print(f"\n- [{timeframe}] 최종적으로 백테스트를 진행할 티커가 없습니다. 종료합니다.")
        return
    
    backtest_target_files = [f"{ticker}.csv" for ticker in final_tickers_list]
    print(f"\n- [{timeframe}] 모든 필터링 완료. 최종 선정된 {len(final_tickers_list)}개 티커로 백테스트를 시작합니다.")

    # --- 추가: 최종 필터링된 티커 목록 DB 저장 ---
    if oi_df is not None:
        save_df = pd.merge(stats_df, oi_df, on='ticker', how='inner')
    else:
        save_df = stats_df.copy()
    save_df = save_df[save_df['ticker'].isin(final_tickers_list)].copy()

    # ✨ 수정: live_trade_log.db와 충돌하지 않도록 파일명을 명확하게 변경
    db_filename = f"Top_200_15m_{datetime.now().strftime('%Y%m%d')}.db"
    conn = sqlite3.connect(db_filename)
    save_df.to_sql('top_tickers', conn, if_exists='replace', index=False)
    conn.close()
    print(f"- [{timeframe}] 최종 필터링된 {len(save_df)}개 티커를 '{db_filename}'에 저장했습니다.")

    # --- ✨ 추가: 증분 백테스팅 로직 ---
    old_results_df = None
    tickers_to_backtest = final_tickers_list
    
    latest_db_path, latest_db_date = get_latest_final_db(timeframe.split('_')[0]) # '15m'
    if latest_db_path:
        print(f"\n- [{timeframe}] 증분 백테스팅을 위해 이전 결과 파일을 로드합니다: {latest_db_path} (생성일: {latest_db_date})")
        try:
            conn_old = sqlite3.connect(latest_db_path)
            # 이전 DB의 모든 결과를 로드
            old_results_df = pd.read_sql_query("SELECT * FROM analyzed_results", conn_old)
            conn_old.close()

            old_tickers = set(old_results_df['ticker'].unique())
            new_tickers = set(final_tickers_list)

            tickers_to_run_fresh = list(new_tickers - old_tickers)
            tickers_to_copy = list(new_tickers.intersection(old_tickers))
            
            print(f"  - 신규 백테스트 대상 ({len(tickers_to_run_fresh)}개): {tickers_to_run_fresh[:5]}...")
            print(f"  - 기존 결과 복사 대상 ({len(tickers_to_copy)}개): {tickers_to_copy[:5]}...")

            tickers_to_backtest = tickers_to_run_fresh # 실제 백테스트는 신규 티커에 대해서만 수행
            # 기존 결과에서 복사할 데이터만 필터링
            all_results.append(old_results_df[old_results_df['ticker'].isin(tickers_to_copy)])

        except Exception as e_inc:
            print(f"  - [경고] 이전 결과 파일 처리 중 오류 발생: {e_inc}. 전체 백테스트를 진행합니다.")
            all_results = [] # 초기화

    total_tickers_to_test = len(tickers_to_backtest)
    for i, ticker in enumerate(tickers_to_backtest):
        filename = f"{ticker}.csv"
        file_path = os.path.join(data_dir, filename)
        
        if (i + 1) % 5 == 0 or i == 0 or (i + 1) == total_tickers_to_test:
            print(f"- [{timeframe}] 백테스트 진행: {i+1}/{total_tickers_to_test} ({ticker})")

        try:
            # ✨ 수정: CSV 파일에 헤더가 있을 수 있으므로 header=0으로 읽음
            df = pd.read_csv(file_path, header=0)
            df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
        except Exception as e:
            print(f"- {filename} 파일 처리 중 오류: {e}")
            continue

        # Calculate and append indicators to the DataFrame
        df.ta.ichimoku(tenkan=ICHIMOKU_PERIODS[0], kijun=ICHIMOKU_PERIODS[1], senkou=ICHIMOKU_PERIODS[2], append=True)
        df.ta.sma(length=SMA_PERIOD, append=True)
        df.ta.bbands(length=BOLLINGER_PERIOD, append=True)
        
        # 20일 거래량 이동평균 계산 추가
        df['volume_ma20'] = df['volume'].rolling(window=20).mean()

        # 체결강도 계산 추가 (근사치)
        ti_period = 20
        df['buy_volume'] = np.where(df['close'] > df['open'], df['volume'], 0)
        df['sell_volume'] = np.where(df['close'] < df['open'], df['volume'], 0)
        df['rolling_buy_vol'] = df['buy_volume'].rolling(window=ti_period).sum()
        df['rolling_sell_vol'] = df['sell_volume'].rolling(window=ti_period).sum()
        df['trading_intensity'] = (df['rolling_buy_vol'] / (df['rolling_sell_vol'] + 1e-9)) * 100

        # --- 'stratege.md' 기반 전략 실행으로 수정 ---
        ticker_results = []
        strategies_to_test = ['V1_GoldenCross', 'V2_KijunBreakout', 'V3_400PointRise']
        trailing_stop_percentages_to_test = [None, 0.02, 0.03, 0.04, 0.05, 0.07, 0.10]

        sl_tp_combinations = []
        stop_loss_levels = [0.01, 0.02, 0.03, 0.04]
        profit_loss_ratios = np.arange(2.0, 5.5, 0.5) # 2.0, 2.5, ..., 5.0
        for sl in stop_loss_levels:
            for ratio in profit_loss_ratios:
                sl_tp_combinations.append({'sl': sl, 'tp': sl * ratio})

        for strategy_name in strategies_to_test:
            # --- 1. 'stratege.md' 전략 + 트레일링 스탑 ---
            for ts_pct in trailing_stop_percentages_to_test:
                result = run_backtest(df.copy(), fee_rate=FEE_RATE, leverage=LEVERAGE, strategy=strategy_name, trailing_stop_pct=ts_pct)
                if ts_pct is None:
                    result['매도 기준선'] = '데드크로스'
                else:
                    result['매도 기준선'] = f"데드크로스 or TS {ts_pct*100:.1f}%"
                ticker_results.append(result)

            # --- 2. 'stratege.md' 전략 + 고정 손익비 ---
            for sl_tp in sl_tp_combinations:
                result = run_backtest(df.copy(), fee_rate=FEE_RATE, leverage=LEVERAGE, strategy=strategy_name, stop_loss_pct=sl_tp['sl'], take_profit_pct=sl_tp['tp'])
                result['매도 기준선'] = f"데드크로스 or SL/TP {sl_tp['sl']*100:.1f}%:{sl_tp['tp']*100:.1f}%"
                ticker_results.append(result)


        if not ticker_results:
            continue

        results_df = pd.DataFrame(ticker_results).sort_values(by='수익률(%)', ascending=False)
        
        db_df = results_df.copy()
        db_df['ticker'] = ticker
        db_df.rename(columns={
            '매수 기준선': 'entry_strategy', '매도 기준선': 'exit_strategy',
            '총 거래 횟수': 'total_trades', '수익률(%)': 'pnl_percent',
            '승률(%)': 'win_rate_percent', '평균 손익(%)': 'avg_pnl_percent',
            '손익비': 'profit_loss_ratio',
            '평균 진입 거래량 비율': 'avg_entry_volume_ratio',
            '평균 진입 체결강도': 'avg_entry_intensity',
            '손실거래 평균 거래량비율': 'avg_loss_entry_volume_ratio',
            '손실거래 평균 체결강도': 'avg_loss_entry_intensity',
            '트레일링 스탑(%)': 'trailing_stop_pct',
            '손절(%)': 'stop_loss_pct',
            '익절(%)': 'take_profit_pct'
        }, inplace=True)
        
        db_df = db_df[db_df['pnl_percent'] > 100]
        all_results.append(db_df)

    if not all_results:
        print(f"\n[ {timeframe} ] 분석할 유의미한 백테스트 결과가 없습니다. (수익률 100% 초과)")
        return

    print(f"- [{timeframe}] 모든 티커 처리 완료.")

    final_df = pd.concat(all_results, ignore_index=True)
    initial_count = len(final_df)
    final_df = final_df[final_df['total_trades'] >= 20].copy()
    trades_filtered_count = len(final_df)
    final_df = final_df[final_df['win_rate_percent'] >= 50].copy()
    final_count = len(final_df)

    print(f"\n[ {timeframe} ] 최종 분석 필터링: {initial_count}개 -> {trades_filtered_count}개 (거래수) -> {final_count}개 (승률)")

    if final_df.empty:
        print(f"\n[ {timeframe} ] 분석할 데이터가 없습니다. (설정된 필터 기준 충족 결과 없음)")
        return

    def normalize(series):
        return (series - series.min()) / (series.max() - series.min()) if series.max() > series.min() else 0.5

    if len(final_df) > 1:
        final_df['pnl_norm'] = normalize(final_df['pnl_percent'])
        final_df['win_rate_norm'] = normalize(final_df['win_rate_percent'])
        final_df['avg_pnl_norm'] = normalize(final_df['avg_pnl_percent'])
        final_df['composite_score'] = (
            final_df['pnl_norm'] * WEIGHTS['pnl'] +
            final_df['win_rate_norm'] * WEIGHTS['win_rate'] +
            final_df['avg_pnl_norm'] * WEIGHTS['avg_pnl']
        )
    else:
        final_df['composite_score'] = 1.0

    df_sorted = final_df.sort_values(by='composite_score', ascending=False)

    try:
        conn = sqlite3.connect(db_file)
        df_sorted.to_sql('analyzed_results', conn, if_exists='replace', index=False)
        conn.close()
        print(f"\n[ {timeframe} ] 분석 결과를 '{db_file}' 파일에 성공적으로 저장했습니다.")
    except Exception as e:
        print(f"\n[ {timeframe} ] 분석 결과를 DB에 저장하는 중 오류 발생: {e}")
        return

    best_result = df_sorted.iloc[0]
    print(f"--- [ {timeframe} ] 종합 점수 최적 전략 ---")
    print(f"종목: {best_result['ticker']}, 매수: {best_result['entry_strategy']}, 매도: {best_result['exit_strategy']}, TS: {best_result['trailing_stop_pct']:.3f}%, 수익률: {best_result['pnl_percent']:.3f}%")
    print('='*85)


def run_all_backtests():
    """설정된 모든 타임프레임에 대해 백테스트를 순차적으로 실행합니다."""
    global IS_BACKTEST_RUNNING
    if IS_BACKTEST_RUNNING:
        print(f"[{datetime.now()}] 경고: 이미 다른 백테스트가 실행 중입니다. 이번 스케줄은 건너뜁니다.")
        return

    IS_BACKTEST_RUNNING = True
    try:
        print(f"[{datetime.now()}] 전체 타임프레임 백테스트 작업을 시작합니다.")
        execution_order = list(TIMEFRAME_FOLDERS.keys())
        for timeframe in execution_order:
            run_full_backtest_for_timeframe(timeframe)
        print(f"\n[{datetime.now()}] 모든 타임프레임에 대한 백테스트가 완료되었습니다.")
    finally:
        # 작업이 성공하든 실패하든 항상 플래그를 리셋
        IS_BACKTEST_RUNNING = False

# --- 메인 실행 로직 ---
def main():
    """
    사용자 선택에 따라 수동 또는 자동(스케줄) 모드로 백테스트를 실행합니다.
    """
    while True:
        print("\n--- 백테스트 실행 모드를 선택하세요 ---")
        print("1: 수동 실행 (즉시 15분 타임프레임 백테스트 시작)")
        print("2: 자동 스케줄 실행 (매주 월요일 08:01에 모든 타임프레임 백테스트 실행)")
        choice = input("선택 (1 또는 2): ")

        if choice == '1':
            # --- 수동 전체 테스트 모드 ---
            run_all_backtests()
            break
        elif choice == '2':
            # --- 자동 스케줄 모드 ---
            print("\n--- 시작 시 백테스트 1회 즉시 실행 ---")
            run_all_backtests()
            print("------------------------------------")
            
            print("\n자동 스케줄 모드가 시작되었습니다. (다음 실행: 매주 월요일 08:01)")
            print("\n스케줄러가 실행 중입니다. (Ctrl+C로 종료)")
            schedule.every().monday.at("08:01").do(run_all_backtests)

            print("\n--- 스케줄 목록 ---")
            try:
                print(schedule.get_jobs()[0])
            except IndexError:
                print("스케줄이 설정되지 않았습니다.")
            finally:
                print("------------------")

            while True:
                schedule.run_pending()
                time.sleep(1)
        else:
            print("잘못된 입력입니다. 1 또는 2를 입력해주세요.")

if __name__ == "__main__":
    main()
