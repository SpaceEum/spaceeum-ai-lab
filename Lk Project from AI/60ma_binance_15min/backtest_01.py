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
import logging

# --- ✨ 추가: 백테스트 실행 상태 플래그 ---
IS_BACKTEST_RUNNING = False

# --- 설정 ---
# 데이터 최상위 폴더
OHLCV_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "OHLCV")

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

# --- strategy.md 기반 설정 ---
LEVERAGE = 10
STRATEGY_MD_CONFIG = {
    'leverage': 10,
    'initial_capital': 10000.0,
    'asset_allocation_pct': 0.05,
    'stop_loss_pct': 0.03,
    'trailing_stop_activation_pct': 0.03,
    'trailing_stop_callback_pct': 0.03,
    'ma_period': 60,
    'ichi_tenkan': 9,
    'ichi_kijun': 26,
    'ichi_senkou': 52,
}

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# --- strategy.md 기반 종목 선정 로직 ---
def get_top_tickers_by_volume(exchange, limit=500):
    """바이낸스 USDT 선물 시장에서 24시간 거래량 기준 상위 티커 목록을 가져옵니다."""
    try:
        all_tickers = exchange.fetch_tickers()
        
        # --- ✨ 수정: 심볼 이름에 'USDT'가 포함된 티커만 필터링하여 안정성 확보 ---
        usdt_swap_tickers = [
            ticker_data for ticker_data in all_tickers.values()
            if 'USDT' in ticker_data.get('symbol', '') and not ticker_data.get('symbol', '').endswith('BUSD')
        ]
        logging.info(f"전체 {len(all_tickers)}개 티커 중 {len(usdt_swap_tickers)}개의 USDT 무기한 계약 티커를 필터링했습니다.")

        # 'quoteVolume' (거래대금)을 기준으로 모든 티커를 내림차순으로 정렬합니다.
        sorted_tickers_data = sorted(
            usdt_swap_tickers,
            key=lambda t: t.get('quoteVolume', 0),
            reverse=True
        )
        
        # --- ✨ 요청사항: 정렬된 상위 티커 목록을 터미널에 표시 ---
        logging.info(f"--- 거래대금(USDT) 기준 상위 {limit}개 티커 목록 ---")
        for i, ticker_data in enumerate(sorted_tickers_data[:limit]):
            symbol = ticker_data.get('symbol', 'N/A')
            quote_volume = ticker_data.get('quoteVolume', 0)
            logging.info(f"{i+1:3d}. {symbol:<15} | 24h Quote Volume: {quote_volume:,.2f} USDT")
        
        top_tickers = [ticker['symbol'] for ticker in sorted_tickers_data[:limit]]
        logging.info(f"거래량 상위 {len(top_tickers)}개 티커 로드 완료.")
        return top_tickers
    except Exception as e:
        logging.error(f"티커 목록을 가져오는 중 오류 발생: {e}")
        return []

def check_reverse_array_condition(df, periods=5):
    """데이터프레임에 대해 역배열 조건이 N기간 연속 충족되는지 확인합니다."""
    if len(df) < 80:
        return False

    # 일목균형표 및 MA 지표 계산
    ichimoku_df = ta.ichimoku(high=df['high'], low=df['low'], close=df['close'], tenkan=9, kijun=26, senkou=52)
    
    if isinstance(ichimoku_df, tuple):
        # pandas-ta v0.3.14+는 튜플(ichimoku, span)을 반환할 수 있음
        # 올바른 순서로 병합: ITS, IKS, ICS, ISA, ISB
        ichimoku_df = pd.concat([ichimoku_df[0], ichimoku_df[1]], axis=1)
        # ICS_26이 중간에 위치하므로 순서 재정렬
        if 'ICS_26' in ichimoku_df.columns:
             cols = [c for c in ichimoku_df.columns if c != 'ICS_26'] + ['ICS_26']
             ichimoku_df = ichimoku_df[cols]

    if ichimoku_df is None or ichimoku_df.empty:
        return False
    
    # 컬럼 이름 표준화
    expected_cols = ['ITS_9', 'IKS_26', 'ISA_9_26_52', 'ISB_26_52', 'ICS_26']
    if len(ichimoku_df.columns) >= 5:
        ichimoku_df = ichimoku_df.iloc[:, :5] # 필요한 5개 컬럼만 선택
        ichimoku_df.columns = expected_cols
    else:
        return False # 필요한 컬럼이 부족한 경우

    sma_s = ta.sma(close=df['close'], length=60)
    if sma_s is None:
        return False

    df = df.join(ichimoku_df)
    df['SMA_60'] = sma_s

    df.dropna(inplace=True)
    if len(df) < periods:
        return False

    # 후행스팬 조건: 현재 종가 < 26기간 전 종가
    # pandas-ta의 ICS_26은 이미 26기간 뒤로 shift된 값이므로, 비교 대상은 현재 종가
    # 하지만, check_reverse_array_condition은 미래 데이터를 볼 수 없으므로,
    # '현재 종가가 26기간 전 종가보다 낮다'는 조건을 직접 구현
    df['close_shifted_26'] = df['close'].shift(26)

    df['reverse_condition'] = (
        (df['close'] < df['ISA_9_26_52']) &
        (df['ISA_9_26_52'] < df['ISB_26_52']) &
        (df['close'] < df['SMA_60']) &
        (df['ITS_9'] < df['IKS_26']) &
        (df['IKS_26'] < df['ISA_9_26_52']) &
        (df['close'] < df['close_shifted_26'])
    )

    if len(df) >= periods and df['reverse_condition'].tail(periods).all():
        return True
    return False


# --- 백테스팅 개별 전략 실행 로직 ---
def run_strategy_md_backtest(df, config):
    """strategy.md에 명시된 60기간 신고가 돌파 전략 백테스트"""
    
    # 지표 계산
    df['SMA_60'] = ta.sma(df['close'], length=config['ma_period'])
    df['Highest_60'] = df['close'].rolling(window=config['ma_period']).max()
    ichimoku_df = ta.ichimoku(df['high'], df['low'], df['close'], 
                              tenkan=config['ichi_tenkan'], 
                              kijun=config['ichi_kijun'], 
                              senkou=config['ichi_senkou'])
    if isinstance(ichimoku_df, tuple):
        ichimoku_df = pd.concat([ichimoku_df[0], ichimoku_df[1]], axis=1)
    
    # 컬럼 이름 표준화
    ichimoku_df = ichimoku_df.iloc[:, [3]] # Senkou Span B만 필요
    ichimoku_df.columns = ['senkou_span_b']
    df = df.join(ichimoku_df)

    df['close_shifted_60'] = df['close'].shift(config['ma_period'])
    df['sma_shifted_26'] = df['SMA_60'].shift(config['ichi_kijun'])
    
    df.dropna(inplace=True)

    in_position = False
    trades = []
    highest_price_since_entry = 0
    trailing_stop_active = False

    # --- ✨ 개선: 자금 관리 로직 추가 ---
    portfolio_value = config['initial_capital']
    initial_capital = config['initial_capital']
    trade_value = initial_capital * config['asset_allocation_pct'] # 고정 금액 진입

    for i in range(1, len(df)):
        prev_row = df.iloc[i-1]
        current_row = df.iloc[i]

        # --- 1. 포지션 보유 시, 종료 조건 확인 ---
        if in_position:
            entry_price = trades[-1]['entry_price']
            highest_price_since_entry = max(highest_price_since_entry, current_row['high'])
            
            exit_triggered = False
            exit_price = 0
            exit_reason = ""

            # --- ✨ 개선: 강제 청산(Liquidation) 조건 우선 확인 ---
            # 10배 레버리지 격리 마진 시, 약 -10% 하락 시 청산 발생
            liquidation_price = entry_price * (1 - (1 / config['leverage']))
            if current_row['low'] <= liquidation_price:
                exit_triggered = True
                exit_price = liquidation_price
                exit_reason = "Liquidation"

            # 트레일링 스탑 활성화 조건
            if not exit_triggered and not trailing_stop_active and current_row['high'] >= entry_price * (1 + config['trailing_stop_activation_pct']):
                trailing_stop_active = True

            # 청산 조건 1: 선행스팬 2 하향 돌파
            if not exit_triggered and prev_row['close'] >= prev_row['senkou_span_b'] and current_row['close'] < current_row['senkou_span_b']:
                exit_triggered = True
                exit_price = current_row['close']
                exit_reason = "Senkou Span B Cross"

            # 청산 조건 2: 손절
            stop_loss_price = entry_price * (1 - config['stop_loss_pct'])
            if not exit_triggered and current_row['low'] <= stop_loss_price:
                exit_triggered = True
                exit_price = stop_loss_price
                exit_reason = "Stop Loss"

            # 청산 조건 3: 트레일링 스탑
            if not exit_triggered and trailing_stop_active:
                ts_price = highest_price_since_entry * (1 - config['trailing_stop_callback_pct'])
                if current_row['low'] <= ts_price:
                    exit_triggered = True
                    exit_price = ts_price
                    exit_reason = "Trailing Stop"

            if exit_triggered:
                in_position = False
                pnl = 0
                # 강제 청산 시 해당 거래에 투입된 자금의 100%를 잃습니다.
                if exit_reason == "Liquidation":
                    # 해당 거래의 손익률(pnl)은 -1.0 (-100%)이 됩니다.
                    pnl = -1.0 
                else:
                    pnl_before_fee = ((exit_price / entry_price) - 1) * config['leverage']
                    fee = (FEE_RATE * config['leverage']) + (FEE_RATE * config['leverage'] * (exit_price / entry_price))
                    pnl = pnl_before_fee - fee
                
                # 포트폴리오 가치 업데이트
                pnl_amount = trade_value * pnl
                portfolio_value += pnl_amount
                
                trades[-1].update({
                    'exit_date': current_row.name,
                    'exit_price': exit_price,
                    'pnl': pnl, # 순수익률 저장
                    'exit_reason': exit_reason
                })

        # --- 2. 포지션 미보유 시, 진입 조건 확인 ---
        if not in_position:
            entry_conditions = [
                current_row['close'] > current_row['SMA_60'],
                current_row['close'] == current_row['Highest_60'],
                current_row['SMA_60'] > prev_row['SMA_60'],
                current_row['close'] > current_row['close_shifted_60'],
                current_row['close'] > current_row['sma_shifted_26']
            ]

            if all(entry_conditions):
                in_position = True
                entry_price = current_row['close']
                highest_price_since_entry = entry_price
                trailing_stop_active = False
                trades.append({
                    'entry_date': current_row.name, 'entry_price': entry_price,
                    'exit_date': None, 'exit_price': None, 'pnl': None, 'exit_reason': None
                })

    # 결과 계산
    if not trades:
        return None

    trade_df = pd.DataFrame(trades)
    # 아직 청산되지 않은 거래 제거
    trade_df.dropna(subset=['exit_price'], inplace=True)
    if trade_df.empty:
        return None

    total_trades = len(trade_df)
    winning_trades = trade_df[trade_df['pnl'] > 0]
    losing_trades = trade_df[trade_df['pnl'] <= 0]

    win_rate = (len(winning_trades) / total_trades) * 100 if total_trades > 0 else 0
    total_pnl_pct = (portfolio_value / initial_capital - 1) * 100

    avg_win = winning_trades['pnl'].mean() if not winning_trades.empty else 0
    avg_loss = abs(losing_trades['pnl'].mean()) if not losing_trades.empty else 0
    profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else np.inf

    stop_loss_count = len(trade_df[trade_df['exit_reason'] == 'Stop Loss'])
    trailing_stop_count = len(trade_df[trade_df['exit_reason'] == 'Trailing Stop'])
    # 익절은 트레일링 스탑 또는 전략 청산(Senkou) 중 수익이 난 경우
    take_profit_count = len(winning_trades)

    # --- ✨ 요청사항: Senkou Span B Cross로 인한 손실 횟수 추가 ---
    senkou_loss_count = len(trade_df[(trade_df['exit_reason'] == 'Senkou Span B Cross') & (trade_df['pnl'] <= 0)])

    # --- ✨ 요청사항: 10% 이상 수익을 낸 거래 횟수 추가 ---
    # pnl은 레버리지가 적용된 수익률이므로 0.1 (10%)과 비교합니다.
    strategic_take_profit_count = len(trade_df[trade_df['pnl'] >= 0.1])

    return {
        'return_pct': total_pnl_pct,
        'profit_loss_ratio': profit_loss_ratio,
        'win_rate': win_rate,
        'strategic_take_profit_count': strategic_take_profit_count,
        'total_trades': total_trades,
        'stop_loss_count': stop_loss_count,
        'take_profit_count': take_profit_count,
        'trailing_stop_count': trailing_stop_count,
        'senkou_loss_count': senkou_loss_count
    }


def run_backtest_original(df, fee_rate, leverage, strategy='V1_GoldenCross', trailing_stop_pct=None, stop_loss_pct=None, take_profit_pct=None):
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

    logging.info(f"\n{'='*25} [ {timeframe} ] 타임프레임 백테스트 시작 {'='*25}")
    
    if not os.path.exists(data_dir):
        logging.error(f"오류: [ {timeframe} ] 데이터 폴더를 찾을 수 없습니다 - {data_dir}")
        return

    all_results = []
    
    backtest_target_files = []
    final_tickers_list = []

    # --- 티커 필터링 ---
    logging.info(f"\n- [{timeframe}] 'strategy.md' 조건으로 백테스트 대상 티커를 필터링합니다...")
    try:
        # 1. 거래량 상위 500개 티커를 가져옵니다.
        exchange = ccxt.binanceusdm({'enableRateLimit': True})
        top_tickers = get_top_tickers_by_volume(exchange, limit=500)
        
        # 2. 역배열 조건 확인을 생략하고 500개 티커 전체를 백테스트 대상으로 설정합니다.
        final_tickers_list = top_tickers
        logging.info("역배열 조건 필터링을 생략하고, 거래량 상위 500개 티커 전체를 백테스트 대상으로 설정합니다.")

    except Exception as e:
        import traceback
        logging.error(f"오류: [{timeframe}] 티커 필터링 중 심각한 오류 발생: {e}")
        traceback.print_exc()
        logging.error("오류로 인해 티커 필터링에 실패했습니다. 백테스트를 중단합니다.")
        return
    # --- 필터링 로직 종료 ---

    if not final_tickers_list:
        logging.info(f"\n- [{timeframe}] 최종적으로 백테스트를 진행할 티커가 없습니다. 종료합니다.")
        return
    
    logging.info(f"\n- [{timeframe}] 모든 필터링 완료. 최종 선정된 {len(final_tickers_list)}개 티커에 대한 백테스트를 시작합니다.")

    # 결과 DB 파일 생성 및 테이블 초기화 (백테스트 시작 전 한 번만)
    result_db_file = f"result_{timeframe}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    conn = sqlite3.connect(result_db_file)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS backtest_results (
            ticker TEXT,
            return_pct REAL,
            profit_loss_ratio REAL,
            win_rate REAL,
            stop_loss_count INTEGER,
            take_profit_count INTEGER,
            trailing_stop_count INTEGER,
            strategic_take_profit_count INTEGER,
            senkou_loss_count INTEGER,
            best_stop_loss_pct REAL
        )
    ''')
    conn.commit()
    logging.info(f"\n[ {timeframe} ] 백테스트 결과가 '{result_db_file}' 파일에 실시간으로 저장됩니다.")

    stop_loss_range = [i / 100.0 for i in range(1, 11)] # 0.01 to 0.10
    total_tickers_to_test = len(final_tickers_list)
    for i, ticker in enumerate(final_tickers_list):
        filename = f"{ticker.split(':')[0].replace('/', '')}.csv"
        file_path = os.path.join(data_dir, filename)
        
        if (i + 1) % 5 == 0 or i == 0 or (i + 1) == total_tickers_to_test:
            logging.info(f"- [{timeframe}] 백테스트 진행: {i+1}/{total_tickers_to_test} ({ticker})")

        try:
            df = pd.read_csv(file_path, header=0)
            df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
        except Exception as e:
            logging.error(f"- {filename} 파일 처리 중 오류: {e}")
            continue

        # --- ✨ 개선: 다양한 손절매 비율로 백테스트 실행 ---
        ticker_results = []
        for sl_pct in stop_loss_range:
            temp_config = STRATEGY_MD_CONFIG.copy()
            temp_config['stop_loss_pct'] = sl_pct
            
            logging.debug(f"- [{timeframe}] {ticker} 백테스트 중 (SL: {sl_pct*100:.0f}%)")
            result = run_strategy_md_backtest(df.copy(), temp_config)

            if result is not None:
                result['stop_loss_pct_used'] = sl_pct
                ticker_results.append(result)

        if not ticker_results:
            logging.info(f"- [{timeframe}] {ticker}: 모든 손절매 비율에서 유의미한 결과 없음. 건너뜁니다.")
            continue

        # 가장 높은 수익률을 기록한 결과 찾기
        best_result = max(ticker_results, key=lambda x: x['return_pct'])
        best_result['ticker'] = ticker
        best_stop_loss_pct = best_result['stop_loss_pct_used']
        
        # 개별 티커 결과 DB에 저장
        try:
            cursor.execute('''
                INSERT INTO backtest_results (ticker, return_pct, profit_loss_ratio, win_rate, stop_loss_count, take_profit_count, trailing_stop_count, strategic_take_profit_count, senkou_loss_count, best_stop_loss_pct)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                best_result['ticker'],
                best_result['return_pct'],
                best_result['profit_loss_ratio'],
                best_result['win_rate'],
                best_result['stop_loss_count'],
                best_result['take_profit_count'],
                best_result['trailing_stop_count'],
                best_result['strategic_take_profit_count'],
                best_result['senkou_loss_count'],
                best_stop_loss_pct * 100 # 퍼센트로 저장
            ))
            conn.commit()
            logging.info(f"- [{timeframe}] {ticker} 최적 결과 저장 완료 (SL: {best_stop_loss_pct*100:.0f}%, 수익률: {best_result['return_pct']:.2f}%)")
        except Exception as e:
            logging.error(f"- [{timeframe}] {ticker} 결과 DB 저장 중 오류: {e}")
            conn.rollback() # 오류 발생 시 롤백

    conn.close() # 모든 티커 처리 후 DB 연결 종료
    logging.info(f"\n- [{timeframe}] 모든 티커 처리 완료. 최종 결과는 '{result_db_file}'에서 확인 가능합니다.")

    # 저장된 DB에서 결과 로드하여 최고 수익률 종목 출력
    try:
        conn_read = sqlite3.connect(result_db_file)
        df_sorted = pd.read_sql_query("SELECT * FROM backtest_results ORDER BY return_pct DESC", conn_read)
        conn_read.close()
    except Exception as e:
        logging.error(f"\n[ {timeframe} ] 저장된 결과 DB 로드 중 오류 발생: {e}")
        return

    if not df_sorted.empty:
        best_result = df_sorted.iloc[0]
        logging.info(f"--- [ {timeframe} ] 최고 수익률 전략 ---")
        logging.info(f"종목: {best_result['ticker']}, 수익률: {best_result['return_pct']:.2f}%, 승률: {best_result['win_rate']:.2f}%")
        logging.info('='*85)
    else:
        logging.warning(f"\n[ {timeframe} ] 저장된 백테스트 결과가 없습니다.")


def run_all_backtests():
    """설정된 모든 타임프레임에 대해 백테스트를 순차적으로 실행합니다."""
    global IS_BACKTEST_RUNNING
    if IS_BACKTEST_RUNNING:
        logging.warning(f"[{datetime.now()}] 경고: 이미 다른 백테스트가 실행 중입니다. 이번 스케줄은 건너뜁니다.")
        return

    IS_BACKTEST_RUNNING = True
    try:
        logging.info(f"[{datetime.now()}] 전체 타임프레임 백테스트 작업을 시작합니다.")
        execution_order = list(TIMEFRAME_FOLDERS.keys())
        for timeframe in execution_order:
            run_full_backtest_for_timeframe(timeframe)
        logging.info(f"\n[{datetime.now()}] 모든 타임프레임에 대한 백테스트가 완료되었습니다.")
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
