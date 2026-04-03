# /path/to/your/project/backtest.py

import os
import pandas as pd
import pandas_ta as ta
import sqlite3
from datetime import datetime
import numpy as np
import time
import glob
import sys
import ccxt
import schedule

# --- 설정 ---
# 데이터 최상위 폴더
OHLCV_DIR = "D:/project/ohlcv"

# 타임프레임별 데이터 폴더 이름
TIMEFRAME_FOLDERS = {
    '15m': "Binance_Fureres_USDT_15Minute_ohlcv",
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
TOP_N_FOR_FILTER = 300 # 각 조건(거래량, 거래대금, 미결제약정)별로 상위 N개 티커를 선정

# --- 종합 점수 가중치 설정 ---
WEIGHTS = {
    'pnl': 0.1,      
    'win_rate': 0.5, 
    'avg_pnl': 0.4   
}

# --- ✨ 추가: 스케줄 중복 실행 방지를 위한 잠금 변수 ---
is_backtest_running = False

# --- 백테스팅 개별 전략 실행 로직 ---
def run_backtest(df, entry_line_col, fee_rate, leverage, exit_line_col=None, trailing_stop_pct=None, stop_loss_pct=None, take_profit_pct=None):
    """
    주어진 데이터프레임과 다양한 종료 조건(지표, 트레일링 스탑, 고정 손익비)에 따라 백테스트를 실행합니다.
    레버리지와 격리 마진(강제 청산)을 시뮬레이션합니다.
    """
    chikou_period = ICHIMOKU_PERIODS[1]
    df['shifted_entry_line'] = df[entry_line_col].shift(chikou_period)
    # ✨ 수정: 매도 기준선도 후행스팬을 적용하도록 변경
    # exit_line_col이 None이 아닐 때만 실행
    if exit_line_col and exit_line_col in df.columns:
        df['shifted_exit_line'] = df[exit_line_col].shift(chikou_period) 

    position_size = 0.0  # 0.0: 포지션 없음, 1.0: 전체 포지션 보유
    highest_price_since_entry = 0
    trades = []
    entry_price = 0
    max_deviation = []
    sma_values = []

    tp1_triggered, tp2_triggered = False, False
    
    # ✨ 추가: 진입 신호가 유효한 남은 캔들 수를 추적하는 변수
    price_signal_active_for_candles = 0
    
    dropna_cols = [entry_line_col, 'shifted_entry_line']
    if exit_line_col and 'shifted_exit_line' in df.columns:
        dropna_cols.append('shifted_exit_line')
    df_cleaned = df.dropna(subset=dropna_cols).copy()

    for i in range(1, len(df_cleaned)):
        prev_row = df_cleaned.iloc[i-1]
        current_row = df_cleaned.iloc[i]

        # --- 1. 포지션 보유 시, 종료 조건 확인 (분할 익절 및 손절) ---
        if position_size > 0:
            highest_price_since_entry = max(highest_price_since_entry, current_row['high'])

            # 현재 캔들의 SMA 값과 고가를 기록 (매도 조건 발동 시 계산에 사용)
            sma_val = current_row[f'SMA_{SMA_PERIOD}']
            candle_high = current_row['high']

            sma_values.append((candle_high, sma_val))

            # 0. 강제 청산 (격리 마진 시뮬레이션)
            liquidation_price = entry_price * (1 - (1 / leverage))
            if current_row['low'] <= liquidation_price:
                entry_trade = trades.pop()
                pnl = -1.0 # 강제 청산 시 원금 100% 손실



                trades.append({'entry_date': entry_trade['entry_date'], 'entry_price': entry_price, 'exit_date': current_row.name, 'exit_price': liquidation_price, 'pnl': pnl, 'size': position_size, 'volume_ratio': entry_trade.get('volume_ratio'), 'trading_intensity': entry_trade.get('trading_intensity')})
                position_size = 0
                continue
            # ✨ 수정: live_trader.py와 동일하게 손절, 분할 익절, 트레일링 스탑 로직 복원
            
            # 1. 손절 (9기간 신저가)
            lowest_price_in_9_periods = df_cleaned['low'].iloc[i-9:i].min()
            if current_row['close'] < lowest_price_in_9_periods:
                entry_trade = trades.pop()
                exit_price = current_row['close']
                pnl = ((exit_price / entry_price) - 1) * leverage - (leverage * fee_rate) - (leverage * (exit_price / entry_price) * fee_rate)
                trades.append({'entry_date': entry_trade['entry_date'], 'entry_price': entry_price, 'exit_date': current_row.name, 'exit_price': exit_price, 'pnl': pnl, 'size': position_size, 'volume_ratio': entry_trade.get('volume_ratio'), 'trading_intensity': entry_trade.get('trading_intensity')})
                position_size = 0
                continue

            # 3. 트레일링 스탑
            if trailing_stop_pct is not None:
                ts_price = highest_price_since_entry * (1 - trailing_stop_pct)
                if current_row['close'] < ts_price:
                    entry_trade = trades.pop()
                    exit_price = current_row['close']
                    pnl = ((exit_price / entry_price) - 1) * leverage - (leverage * fee_rate) - (leverage * (exit_price / entry_price) * fee_rate)
                    trades.append({'entry_date': entry_trade['entry_date'], 'entry_price': entry_price, 'exit_date': current_row.name, 'exit_price': exit_price, 'pnl': pnl, 'size': position_size, 'volume_ratio': entry_trade.get('volume_ratio'), 'trading_intensity': entry_trade.get('trading_intensity')})
                    position_size = 0
                    continue

            # 4. 지표 신호
            if exit_line_col is not None:
                if (prev_row['close'] >= prev_row['shifted_exit_line'] and current_row['close'] < current_row['shifted_exit_line']):
                    entry_trade = trades.pop()
                    exit_price = current_row['close']
                    pnl = ((exit_price / entry_price) - 1) * leverage - (leverage * fee_rate) - (leverage * (exit_price / entry_price) * fee_rate)
                    trades.append({'entry_date': entry_trade['entry_date'], 'entry_price': entry_price, 'exit_date': current_row.name, 'exit_price': exit_price, 'pnl': pnl, 'size': position_size, 'volume_ratio': entry_trade.get('volume_ratio'), 'trading_intensity': entry_trade.get('trading_intensity')})
                    position_size = 0
                    continue

        # --- 2. 포지션 미보유 시, 진입 조건 확인 ---
        if position_size == 0:
            # --- ✨ 수정: 새로운 진입 로직 적용 ---
            if i < (chikou_period + 5): continue # 후행스팬 계산을 위한 최소 데이터 확보

            # 1. 새로운 상향 돌파 신호 확인
            is_new_cross_up = prev_row['close'] > prev_row['shifted_entry_line'] and df_cleaned.iloc[i-2]['close'] <= df_cleaned.iloc[i-2]['shifted_entry_line']
            if is_new_cross_up:
                # 상향 돌파 시, 5개 캔들 동안 신호를 유효하게 설정
                price_signal_active_for_candles = 5

            # 2. 신호가 유효한 기간인지 확인
            if price_signal_active_for_candles > 0:
                price_signal_active_for_candles -= 1 # 카운터 감소

                # 2-1. 신호 무효화 조건: 후행스팬이 선 아래로 다시 내려간 경우
                if current_row['close'] < current_row['shifted_entry_line']:
                    price_signal_active_for_candles = 0 # 신호 무효화
                    continue

                # ✨ 수정: 필터링 기준을 '완성된 캔들'(-2)로 변경 (live_trader와 일치)
                filter_check_candle = df_cleaned.iloc[i-2]

                # --- 필터 조건들 ---
                # 1. 거래량 필터 (live_trader와 동일하게 avg_win_volume_ratio 사용)
                #    이 값은 백테스트 결과에서 나오므로, 첫 백테스트에서는 기본값 1.0을 사용
                avg_win_volume_ratio = 1.0 # 백테스트 실행 시에는 이 값을 알 수 없으므로 1.0으로 가정
                required_volume = filter_check_candle['volume_ma20'] * avg_win_volume_ratio
                is_volume_ok = filter_check_candle['volume'] > required_volume if 'volume_ma20' in filter_check_candle and pd.notna(filter_check_candle['volume_ma20']) else True

                # 2. 체결강도 필터
                is_intensity_ok = filter_check_candle['trading_intensity'] > filter_check_candle['trading_intensity_ma10'] if 'trading_intensity_ma10' in filter_check_candle and pd.notna(filter_check_candle['trading_intensity_ma10']) else True

                # 3. MACD 필터 (0 이상)
                is_macd_ok = filter_check_candle['MACD_12_26_9'] >= 0 if 'MACD_12_26_9' in filter_check_candle and pd.notna(filter_check_candle['MACD_12_26_9']) else True

                # 4. 모멘텀 필터 (60봉 전보다 가격이 높은가)
                is_momentum_ok = filter_check_candle['close'] > df_cleaned.iloc[i - 2 - SMA_PERIOD]['close'] if i >= (SMA_PERIOD + 2) else True # 필터링 캔들 기준

                # 5. 윗꼬리 및 급등 필터 (live_trader와 동일하게 추가)
                is_long_wick = False
                body_length = abs(filter_check_candle['close'] - filter_check_candle['open'])
                if body_length > 0:
                    if filter_check_candle['close'] > filter_check_candle['open']: # 양봉
                        upper_wick = filter_check_candle['high'] - filter_check_candle['close']
                        if upper_wick >= body_length * 1.3: is_long_wick = True
                    else: # 음봉
                        upper_wick = filter_check_candle['high'] - filter_check_candle['open']
                        if upper_wick >= body_length * 1.3: is_long_wick = True
                
                is_sudden_pump = False
                prev_body_length = abs(df_cleaned.iloc[i-3]['close'] - df_cleaned.iloc[i-3]['open'])
                if prev_body_length > 0 and body_length >= prev_body_length * 3:
                    is_sudden_pump = True

                # --- ✨ 추가: 60 SMA 및 60일 신고가 필터 ---
                # 6. 현재 가격이 60 SMA 위에 있는지 확인
                is_above_sma = filter_check_candle['close'] > filter_check_candle[f'SMA_{SMA_PERIOD}'] if f'SMA_{SMA_PERIOD}' in filter_check_candle else True

                # 7. 60일(trading days) 신고가인지 확인 (15분봉 기준 60일 = 60 * 24 * 4 = 5760봉)
                high_period = 60 * 24 * 4
                is_60d_high = filter_check_candle['close'] >= df_cleaned['high'].iloc[i-2-high_period:i-2].max() if i > (high_period + 2) else True

                # 모든 필터 조건을 통과해야 진입
                if not (is_volume_ok and is_intensity_ok and is_macd_ok and is_momentum_ok and not is_long_wick and not is_sudden_pump and is_above_sma and is_60d_high):
                    continue

                position_size = 1.0 # 전체 포지션 진입
                entry_price = current_row['close']
                highest_price_since_entry = entry_price
                tp1_triggered, tp2_triggered = False, False # 익절 플래그 초기화

                deviation = (current_row['high'] - current_row[f'SMA_{SMA_PERIOD}']) / current_row[f'SMA_{SMA_PERIOD}']
                deviation_percent = deviation * 100

                max_deviation.append(deviation_percent)



                # 진입 시점의 거래량 비율과 체결강도 기록
                volume_ratio = (current_row['volume'] / current_row['volume_ma20']) if 'volume_ma20' in current_row and pd.notna(current_row['volume_ma20']) and current_row['volume_ma20'] > 0 else np.nan
                trading_intensity = current_row['trading_intensity'] if 'trading_intensity' in current_row and pd.notna(current_row['trading_intensity']) else np.nan

                trades.append({
                    'entry_date': current_row.name,
                    'volume_ratio': volume_ratio, 'trading_intensity': trading_intensity
                })

    empty_result = {
        '매수 기준선': entry_line_col, '매도 기준선': exit_line_col or 'N/A', '총 거래 횟수': 0, '수익률(%)': 0, '승률(%)': 0, '평균 손익(%)': 0, '손익비': 0, '평균 진입 거래량 비율': 0, '평균 진입 체결강도': 0, '손실거래 평균 거래량비율': 0, '손실거래 평균 체결강도': 0, '트레일링 스탑(%)': round((trailing_stop_pct or 0) * 100, 3), '손절(%)': round((stop_loss_pct or 0) * 100, 3), '익절(%)': round((take_profit_pct or 0) * 100, 3), '평균 최대 가격 괴리(%)': 0
    } 

    # 1. 미체결 진입 제거
    if trades and 'exit_price' not in trades[-1]:
        trades.pop()

    if not trades:
        return empty_result
    
    trade_df = pd.DataFrame(trades)

    # 2. 최대 가격 deviation 계산 및 상위/하위 10% 제외
    avg_max_deviation = 0
    if sma_values:
        deviations = [((high - sma) / sma) * 100 for high, sma in sma_values]
        # Trim deviation
        if deviations:
            trim_size = int(len(deviations) * 0.1)
            trimmed_deviations = sorted(deviations)[trim_size:-trim_size] if len(deviations) > 2 * trim_size else deviations
            if trimmed_deviations:
                avg_max_deviation = np.mean(trimmed_deviations)

    # 미체결된 진입 기록 제거
    if trades and 'exit_price' not in trades[-1]:
        trades.pop()

    if not trades:
        return empty_result

    trade_df = pd.DataFrame(trades)
    # 분할매도를 하나의 거래로 그룹화하여 통계 계산
    trade_df['trade_group'] = (trade_df['entry_date'] != trade_df['entry_date'].shift()).cumsum()
    total_trades = trade_df['trade_group'].nunique()

    # 가중 평균 PNL 계산
    trade_df['weighted_pnl'] = trade_df['pnl'] * trade_df['size']
    final_pnl_per_trade = trade_df.groupby('trade_group')['weighted_pnl'].sum()

    total_pnl = (final_pnl_per_trade + 1).prod() - 1
    winning_trades = final_pnl_per_trade[final_pnl_per_trade > 0]
    # ✨ 수정: losing_trades 변수 정의
    losing_trades = trade_df[trade_df['pnl'] <= 0]
    win_rate = len(winning_trades) / total_trades * 100 if total_trades > 0 else 0
    avg_pnl = final_pnl_per_trade.mean() * 100

    avg_profit = trade_df[trade_df['pnl'] > 0]['pnl'].mean() # winning_trades가 재할당되므로 trade_df에서 직접 계산
    avg_loss = abs(losing_trades['pnl'].mean())
    profit_loss_ratio = avg_profit / avg_loss if avg_loss > 0 else np.inf

    # 수익이 난 거래(winning_trades)와 손실이 난 거래(losing_trades) 각각의 평균을 계산
    avg_win_volume_ratio = trade_df[trade_df['pnl'] > 0]['volume_ratio'].mean()
    avg_win_trading_intensity = trade_df[trade_df['pnl'] > 0]['trading_intensity'].mean()
    avg_loss_volume_ratio = losing_trades['volume_ratio'].mean()
    avg_loss_trading_intensity = losing_trades['trading_intensity'].mean()

    return {
        '매수 기준선': entry_line_col, '매도 기준선': exit_line_col, # 이름은 외부에서 덮어쓰기됨
        '총 거래 횟수': total_trades, '수익률(%)': round(total_pnl * 100, 3),
        '평균 최대 가격 괴리(%)': round(avg_max_deviation,3),
        '승률(%)': round(win_rate, 3), '평균 손익(%)': round(avg_pnl, 3),
        '손익비': round(profit_loss_ratio, 3),
        '평균 진입 거래량 비율': round(avg_win_volume_ratio, 3) if pd.notna(avg_win_volume_ratio) else 0, # 기존 이름 유지 (수익 거래 기준)
        '평균 진입 체결강도': round(avg_win_trading_intensity, 3) if pd.notna(avg_win_trading_intensity) else 0, # 기존 이름 유지 (수익 거래 기준)
        '손실거래 평균 거래량비율': round(avg_loss_volume_ratio, 3) if pd.notna(avg_loss_volume_ratio) else 0,
        '손실거래 평균 체결강도': round(avg_loss_trading_intensity, 3) if pd.notna(avg_loss_trading_intensity) else 0,
        '트레일링 스탑(%)': round((trailing_stop_pct or 0) * 100, 3),
        '손절(%)': round((stop_loss_pct or 0) * 100, 3),
        '익절(%)': round((take_profit_pct or 0) * 100, 3)
    }

def run_ticker_filtering_process():
    """
    우량 티커를 필터링하고 'Filtered_Tickers_{date}.db' 파일을 생성하는 전체 프로세스입니다.
    """
    timeframe = '15m' # 필터링은 15m 기준으로만 실행
    data_dir = os.path.join(OHLCV_DIR, TIMEFRAME_FOLDERS.get(timeframe, ""))
    all_csv_files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]

    print(f"\n- [{timeframe}] 3가지 조건으로 우량 티커를 필터링하여 DB를 생성/업데이트합니다...")
    try:
        ticker_stats = []
        one_month_ago = datetime.now() - pd.Timedelta(days=30)
        total_files_for_stats = len(all_csv_files)

        for i, filename in enumerate(all_csv_files):
            if (i + 1) % 20 == 0 or i == 0 or (i + 1) == total_files_for_stats:
                print(f"  - 1/2: 로컬 데이터 통계 계산 진행: {i+1}/{total_files_for_stats} ({filename.split('.')[0]})")
            
            try:
                file_path = os.path.join(data_dir, filename)
                df_stats = pd.read_csv(file_path, header=0)
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

        top_by_volume = set(stats_df.nlargest(TOP_N_FOR_FILTER, 'monthly_volume')['ticker'])
        print(f"  - [조건 1] 1개월 거래량 상위 {TOP_N_FOR_FILTER}개 티커 선정 완료. (총 {len(top_by_volume)}개)")
        
        top_by_turnover = set(stats_df.nlargest(TOP_N_FOR_FILTER, 'monthly_turnover')['ticker'])
        print(f"  - [조건 2] 1개월 거래대금 상위 {TOP_N_FOR_FILTER}개 티커 선정 완료. (총 {len(top_by_turnover)}개)")

        print(f"\n  - 2/2: API를 통해 최근 7일간의 평균 미결제약정 상위 티커를 필터링합니다...")
        exchange = ccxt.binance({'options': {'defaultType': 'future'}})
        oi_stats = []
        since_timestamp = exchange.milliseconds() - 7 * 24 * 60 * 60 * 1000
        
        tickers_to_check_oi = sorted(list(top_by_volume.union(top_by_turnover)))
        total_tickers_for_oi = len(tickers_to_check_oi)

        for i, ticker in enumerate(tickers_to_check_oi):
            if (i + 1) % 10 == 0 or i == 0 or (i + 1) == total_tickers_for_oi:
                print(f"    - 미결제약정 조회 진행: {i+1}/{total_tickers_for_oi} ({ticker})")
            
            try:
                oi_history = exchange.fetch_open_interest_history(ticker, timeframe, since=since_timestamp, limit=500)
                if not oi_history: continue
                interests = [item['openInterestValue'] for item in oi_history]
                avg_oi = sum(interests) / len(interests)
                oi_stats.append({'ticker': ticker, 'avg_open_interest_7d': avg_oi})
                time.sleep(exchange.rateLimit / 1000)
            except Exception as e_oi:
                print(f"      - [경고] {ticker} 미결제약정 조회 중 오류: {e_oi}")
                continue
        
        oi_df = None
        if not oi_stats:
            print(f"  - [경고] 미결제약정 데이터를 가져올 수 있는 티커가 없습니다. 미결제약정 필터를 건너뛰고 2가지 조건으로만 필터링합니다.")
            final_tickers_set = top_by_volume.intersection(top_by_turnover)
        # ✨ 수정: OI 데이터가 너무 적으면(10개 미만) OI 필터링을 건너뜁니다.
        elif len(oi_stats) < 10:
            print(f"  - [경고] 미결제약정 데이터가 너무 적습니다({len(oi_stats)}개). 미결제약정 필터를 건너뛰고 2가지 조건으로만 필터링합니다.")
            final_tickers_set = top_by_volume.intersection(top_by_turnover)
            oi_df = pd.DataFrame(oi_stats) # DB 저장을 위해 df는 생성
        else:
            oi_df = pd.DataFrame(oi_stats)
            top_by_open_interest = set(oi_df.nlargest(TOP_N_FOR_FILTER, 'avg_open_interest_7d')['ticker'])
            print(f"  - [조건 3] 7일 평균 미결제약정 상위 {TOP_N_FOR_FILTER}개 티커 선정 완료. (총 {len(top_by_open_interest)}개)")
            final_tickers_set = top_by_volume.intersection(top_by_turnover).intersection(top_by_open_interest)
        
        if not final_tickers_set: raise ValueError("설정된 필터링 조건을 모두 만족하는 티커가 없습니다.")

        final_tickers_list = sorted(list(final_tickers_set))
        filter_condition_count = "3가지" if oi_df is not None else "2가지"
        print(f"\n- 필터링 완료. 총 {len(final_tickers_list)}개의 티커가 {filter_condition_count} 조건을 모두 만족합니다.")

        if oi_df is not None: save_df = pd.merge(stats_df, oi_df, on='ticker', how='inner')
        else: save_df = stats_df.copy()
        save_df = save_df[save_df['ticker'].isin(final_tickers_list)].copy()
        db_filename = f"top_200_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        conn = sqlite3.connect(db_filename)
        save_df.to_sql('top_tickers', conn, if_exists='replace', index=False)
        conn.close()
        print(f"- [{timeframe}] 최종 필터링된 {len(save_df)}개 티커를 '{db_filename}'에 저장했습니다.")
    except Exception as e:
        import traceback
        print(f"오류: [{timeframe}] 티커 필터링 중 심각한 오류 발생: {e}")
        traceback.print_exc()
        print("오류로 인해 티커 필터링에 실패했습니다. 백테스트를 중단합니다.")

# --- 타임프레임별 전체 백테스트 및 분석 로직 ---
def run_full_backtest_for_timeframe(timeframe):
    """
    주어진 타임프레임에 대해 전체 백테스트 및 분석을 수행하고 최종 DB를 생성합니다.
    """
    data_dir = os.path.join(OHLCV_DIR, TIMEFRAME_FOLDERS.get(timeframe, ""))
    db_file = f"final_{timeframe}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    
    print(f"\n{'='*25} [ {timeframe} ] 타임프레임 백테스트 시작 {'='*25}")
    
    if not os.path.exists(data_dir):
        print(f"오류: [ {timeframe} ] 데이터 폴더를 찾을 수 없습니다 - {data_dir}")
        return

    all_results = []

    # --- ✨ 수정: 증분 백테스트 로직 ---
    print(f"\n- [{timeframe}] 증분 백테스트를 위한 티커 목록을 준비합니다...")
    
    # 1. 최신 Ticker DB 파일 2개 찾기
    ticker_db_files = sorted(glob.glob("top_200_*.db"), key=os.path.getctime, reverse=True)
    
    new_tickers_set = set()
    old_tickers_set = set()

    if ticker_db_files:
        print(f"  - 최신 티커 DB: {ticker_db_files[0]}")
        conn = sqlite3.connect(ticker_db_files[0])
        new_tickers_set = set(pd.read_sql_query("SELECT ticker FROM top_tickers", conn)['ticker'])
        conn.close()
    else:
        print("  - 경고: 필터링된 티커 DB 파일(top_200_*.db)을 찾을 수 없습니다. 티커 필터링을 먼저 실행합니다.")
        # 티커 필터링 로직을 여기에 직접 호출하거나, 별도 스크립트로 분리하는 것을 고려할 수 있습니다.
        # 현재는 필터링을 건너뛰고 모든 CSV를 대상으로 하는 것으로 대체합니다.
        all_csv_files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]
        new_tickers_set = {f.split('.')[0] for f in all_csv_files}

    if len(ticker_db_files) > 1:
        print(f"  - 이전 티커 DB: {ticker_db_files[1]}")
        conn = sqlite3.connect(ticker_db_files[1])
        old_tickers_set = set(pd.read_sql_query("SELECT ticker FROM top_tickers", conn)['ticker'])
        conn.close()

    tickers_to_backtest = sorted(list(new_tickers_set - old_tickers_set))
    tickers_to_copy = new_tickers_set & old_tickers_set

    print(f"  - 신규 백테스트 대상: {len(tickers_to_backtest)}개")

    # 2. 기존 결과 상속
    latest_final_db = sorted(glob.glob(f"final_{timeframe}_*.db"), key=os.path.getctime, reverse=True)
    # ✨ 수정: 상속할 DB가 없으면, 상속 대상 티커도 신규 백테스트 대상으로 전환
    if not latest_final_db and tickers_to_copy:
        print(f"  - 경고: 상속할 final DB가 없습니다. 상속 대상 {len(tickers_to_copy)}개를 신규 백테스트 대상으로 전환합니다.")
        tickers_to_backtest = sorted(list(new_tickers_set)) # 모든 티커를 백테스트
        tickers_to_copy = set() # 상속 대상 초기화

    print(f"  - 최종 신규 백테스트 대상: {len(tickers_to_backtest)}개")
    print(f"  - 최종 기존 결과 상속 대상: {len(tickers_to_copy)}개")
    if tickers_to_copy:
        if latest_final_db:
            print(f"\n- [{timeframe}] '{latest_final_db[0]}'에서 기존 {len(tickers_to_copy)}개 티커 결과를 상속합니다.")
            conn = sqlite3.connect(latest_final_db[0])
            old_results_df = pd.read_sql_query("SELECT * FROM analyzed_results", conn)
            conn.close()
            copied_results = old_results_df[old_results_df['ticker'].isin(tickers_to_copy)]
            # ✨ 수정: 실제로 상속된 결과가 있을 때만 리스트에 추가
            if not copied_results.empty:
                all_results.append(copied_results)
                print(f"  - {len(copied_results)}개 행 상속 완료.")

    backtest_target_files = [f"{ticker}.csv" for ticker in tickers_to_backtest]
    # --- 증분 백테스트 로직 종료 ---

    total_tickers_to_test = len(backtest_target_files)
    if total_tickers_to_test > 0:
        print(f"\n- [{timeframe}] 신규 {total_tickers_to_test}개 티커에 대한 백테스트를 시작합니다.")
    else:
        print(f"\n- [{timeframe}] 신규 백테스트 대상 티커가 없습니다.")

    for i, filename in enumerate(backtest_target_files):
        ticker = filename.split('.')[0]
        file_path = os.path.join(data_dir, filename)
        
        if (i + 1) % 5 == 0 or i == 0 or (i + 1) == total_tickers_to_test:
            print(f"- [{timeframe}] 백테스트 진행: {i+1}/{total_tickers_to_test} ({ticker})")

        try:
            df = pd.read_csv(file_path, header=0)
            df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)

            # ✨ 수정: 최근 2년치 데이터만 사용하도록 필터링
            two_years_ago = datetime.now() - pd.Timedelta(days=2*365)
            df = df[df.index >= two_years_ago]
            if len(df) < 200: # 데이터가 너무 적으면 건너뛰기
                print(f"  - [경고] {ticker}: 2년치 데이터가 200개 미만입니다. (현재 데이터 수: {len(df)})")
                print(f"  - [경고] {ticker}: 2년치 데이터가 200개 미만이라 건너뜁니다.")
                continue
        except Exception as e:
            print(f"- {filename} 파일 처리 중 오류: {e}")
            continue

        df.ta.ichimoku(tenkan=ICHIMOKU_PERIODS[0], kijun=ICHIMOKU_PERIODS[1], senkou=ICHIMOKU_PERIODS[2], append=True)
        df.ta.sma(length=SMA_PERIOD, append=True)
        df.ta.bbands(length=BOLLINGER_PERIOD, append=True)
        # ✨ 개선: live_trader와 동일하게 MACD 지표 추가
        df.ta.macd(append=True)
        df['volume_ma20'] = df['volume'].rolling(window=20).mean()
        ti_period = 20
        df['buy_volume'] = np.where(df['close'] > df['open'], df['volume'], 0)
        df['sell_volume'] = np.where(df['close'] < df['open'], df['volume'], 0)
        df['rolling_buy_vol'] = df['buy_volume'].rolling(window=ti_period).sum()
        df['rolling_sell_vol'] = df['sell_volume'].rolling(window=ti_period).sum()
        df['trading_intensity'] = (df['rolling_buy_vol'] / (df['rolling_sell_vol'] + 1e-9)) * 100
        df['trading_intensity_ma10'] = df['trading_intensity'].rolling(window=10).mean()

        # ✨ 수정: 진입/매도 전략을 60 SMA로 변경
        exit_lines_to_test = {
            '60일 SMA': f'SMA_{SMA_PERIOD}'
        }
        entry_lines_to_test = exit_lines_to_test.copy()
        
        ticker_results = []
        # ✨ 수정: 트레일링 스탑 테스트 로직 복원
        trailing_stop_percentages_to_test = [None, 0.02, 0.03, 0.04, 0.05, 0.07, 0.10]

        for entry_name, entry_col in entry_lines_to_test.items():
            if entry_col not in df.columns: continue
            for exit_name, exit_col in exit_lines_to_test.items():
                if exit_col not in df.columns: continue
                # 분할 익절 + 트레일링 스탑 조합 테스트
                for ts_pct in trailing_stop_percentages_to_test:
                    result = run_backtest(df.copy(), entry_col, fee_rate=FEE_RATE, leverage=LEVERAGE, exit_line_col=exit_col, trailing_stop_pct=ts_pct)
                    result['매수 기준선'] = entry_name
                    result['매도 기준선'] = exit_name
                    ticker_results.append(result)

        if not ticker_results: continue

        results_df = pd.DataFrame(ticker_results).sort_values(by='수익률(%)', ascending=False)
        db_df = results_df.copy()
        db_df['ticker'] = ticker
        db_df.rename(columns={
            '매수 기준선': 'entry_strategy', '매도 기준선': 'exit_strategy', '총 거래 횟수': 'total_trades',
            '수익률(%)': 'pnl_percent', '승률(%)': 'win_rate_percent', '평균 손익(%)': 'avg_pnl_percent',
            '손익비': 'profit_loss_ratio', '평균 진입 거래량 비율': 'avg_entry_volume_ratio',
            '평균 진입 체결강도': 'avg_entry_intensity', '손실거래 평균 거래량비율': 'avg_loss_entry_volume_ratio',
            '손실거래 평균 체결강도': 'avg_loss_entry_intensity', '트레일링 스탑(%)': 'trailing_stop_pct',
            '손절(%)': 'stop_loss_pct', '익절(%)': 'take_profit_pct'
        }, inplace=True)
        # ✨ 수정: 수익률 100% 필터를 제거하여 모든 결과를 일단 저장하도록 변경
        if not db_df.empty:
            all_results.append(db_df)

    if not all_results:
        print(f"\n[ {timeframe} ] 분석할 유의미한 백테스트 결과가 없습니다. (수익률 100% 초과 또는 신규 티커 없음)")
        return

    print(f"- [{timeframe}] 모든 티커 처리 완료.")
    final_df = pd.concat(all_results, ignore_index=True)
    initial_count = len(final_df)

    if final_df.empty:
        print(f"\n[ {timeframe} ] 분석할 데이터가 없습니다. (설정된 필터 기준 충족 결과 없음)")
        return

    def normalize(series):

        return (series - series.min()) / (series.max() - series.min()) if series.max() > series.min() else 0.5

    if len(final_df) > 1:
        final_df['pnl_norm'] = normalize(final_df['pnl_percent'])
        final_df['win_rate_norm'] = normalize(final_df['win_rate_percent'])
        final_df['avg_pnl_norm'] = normalize(final_df['avg_pnl_percent'])
        final_df['composite_score'] = (final_df['pnl_norm'] * WEIGHTS['pnl'] + final_df['win_rate_norm'] * WEIGHTS['win_rate'] + final_df['avg_pnl_norm'] * WEIGHTS['avg_pnl'])
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
    print(f"[{datetime.now()}] 전체 타임프레임 백테스트 작업을 시작합니다.")
    execution_order = list(TIMEFRAME_FOLDERS.keys())
    for timeframe in execution_order:
        run_full_backtest_for_timeframe(timeframe)
    print(f"\n[{datetime.now()}] 모든 타임프레임에 대한 백테스트가 완료되었습니다.")

# --- 메인 실행 로직 ---
def main():
    """
    사용자 선택에 따라 수동 또는 자동(스케줄) 모드로 백테스트를 실행합니다.
    """
    while True:
        print("\n--- 백테스트 실행 모드를 선택하세요 ---")
        print("1: 수동 실행 (즉시 모든 타임프레임 백테스트 시작)")
        print("2: 자동 스케줄 실행 (매일 12:05에 모든 타임프레임 백테스트 실행)")
        choice = input("선택 (1 또는 2): ")

        if choice == '1':
            # --- 수동 전체 테스트 모드 ---
            run_ticker_filtering_process() # 증분 백테스트를 위해 티커 목록 DB를 먼저 생성/업데이트
            run_all_backtests()
            break
        elif choice == '2':
            # --- 자동 스케줄 모드 ---
            global is_backtest_running

            def scheduled_job():
                global is_backtest_running
                if is_backtest_running:
                    print(f"[{datetime.now()}] 경고: 이전 백테스트가 아직 실행 중이므로 스케줄된 작업을 건너뜁니다.")
                    return
                
                is_backtest_running = True
                print(f"\n--- 스케줄된 작업 시작 ({datetime.now()}) ---")
                try:
                    run_ticker_filtering_process()
                    run_all_backtests()
                finally:
                    is_backtest_running = False
                    print(f"--- 스케줄된 작업 종료 ({datetime.now()}) ---")

            # 1. 즉시 1회 실행
            print("\n--- 자동 모드 시작: 백테스트를 1회 즉시 실행합니다. ---")
            scheduled_job()
            print("\n--- 즉시 실행 완료. 이제 스케줄 대기 모드로 전환합니다. ---")

            # 2. 스케줄러 설정 및 대기
            print("스케줄러가 실행 중입니다. (매일 12:05 실행, Ctrl+C로 종료)")
            schedule.every().day.at("12:05").do(scheduled_job)

            print("\n--- 스케줄 목록 ---")
            print(schedule.get_jobs()[0])
            print("------------------")

            while True:
                schedule.run_pending()
                time.sleep(1)
        else:
            print("잘못된 입력입니다. 1 또는 2를 입력해주세요.")

if __name__ == "__main__":
    main()
