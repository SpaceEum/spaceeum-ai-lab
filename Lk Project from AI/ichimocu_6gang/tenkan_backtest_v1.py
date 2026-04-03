# tenkan_backtest_v1.py
# 'stratege.md'의 II. 전환선 매매 전략 구현 (v1)

import os
import glob
import pandas as pd
import pandas_ta as ta
import sqlite3
from datetime import datetime
import numpy as np
import time
import ccxt
import schedule

# --- 설정 ---
OHLCV_DIR = "G:/내 드라이브/LK Project/Lk Project from AI/OHLCV"
TIMEFRAME_FOLDERS = {'15m': "Binance_Fureres_USDT_15Minute_ohlcv"}
TENKAN_PERIOD = 9
ALBAN_PERIOD = 10
SMA_PERIOD = 60 # 보조지표
LEVERAGE = 10
FEE_RATE = 0.0004
IS_BACKTEST_RUNNING = False

# --- 데이터 로드 및 필터링 (기존 로직 활용) ---
def load_and_filter_tickers(timeframe='15m', top_n=200):
    """
    기본적인 시장성 데이터로 티커를 필터링하여 반환합니다.
    (backtest_15m.py의 로직을 단순화하여 적용)
    """
    data_dir = os.path.join(OHLCV_DIR, TIMEFRAME_FOLDERS.get(timeframe, ""))
    if not os.path.exists(data_dir):
        print(f"데이터 폴더 없음: {data_dir}")
        return []

    all_files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]
    
    # Fast Mode for Verification
    if top_n <= 10:
        print(f"[{timeframe}] 고속 검증 모드: 주요 5개 종목만 테스트합니다.")
        return ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT']
    
    ticker_stats = []
    
    # 최근 30일 기준 거래대금 상위 필터링
    one_month_ago = datetime.now() - pd.Timedelta(days=30)
    
    print(f"[{timeframe}] 티커 필터링 시작 ({len(all_files)}개 파일)...")
    
    for filename in all_files:
        try:
            file_path = os.path.join(data_dir, filename)
            if os.path.getsize(file_path) == 0: continue
            
            # 헤더 읽기 최적화 (전체 로드 X)
            df_header = pd.read_csv(file_path, nrows=1, header=0) # 컬럼 확인용
            
            # 전체 로드 (통계 계산용) - 실제로는 부분 로드가 효율적이나 여기선 단순화
            df = pd.read_csv(file_path, header=0, engine='python')
            df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            df_recent = df[df['timestamp'] >= one_month_ago]
            if df_recent.empty: continue
            
            turnover = (df_recent['volume'] * df_recent['close']).sum()
            ticker_stats.append({'ticker': filename.split('.')[0], 'turnover': turnover})
            
        except Exception:
            continue
            
    if not ticker_stats: return []
    
    # 거래대금 상위 N개 선정
    df_stats = pd.DataFrame(ticker_stats)
    top_tickers = df_stats.nlargest(top_n, 'turnover')['ticker'].tolist()
    print(f"필터링 완료: {len(top_tickers)}개 티커 선정.")
    return top_tickers

# --- 핵심 전략 로직 ---
def run_tenkan_strategy(df, fee_rate, leverage, trailing_stop_pct=None, stop_loss_pct=None, take_profit_pct=None):
    """
    전환선 4가지 조건에 기반한 매매 시뮬레이션
    """
    # 1. 지표 계산
    # 전환선 (9): (9일 고가 + 9일 저가) / 2
    high_9 = df['high'].rolling(window=TENKAN_PERIOD).max()
    low_9 = df['low'].rolling(window=TENKAN_PERIOD).min()
    df['tenkan'] = (high_9 + low_9) / 2
    
    # 기준선 (26) - 보조용 (데드크로스 청산 등)
    high_26 = df['high'].rolling(window=26).max()
    low_26 = df['low'].rolling(window=26).min()
    df['kijun'] = (high_26 + low_26) / 2
    
    # 9기간 고가/저가 (이전 값 비교용)
    df['period_high_9'] = high_9
    df['period_low_9'] = low_9
    
    # 10기간 고가 (알반갭 확인용)
    df['period_high_10'] = df['high'].rolling(window=ALBAN_PERIOD).max()

    # 구름대 계산 (선행스팬 1, 2) - 26일 앞에 그려지므로, 현재 시점의 구름은 26일 전 데이터로 계산
    high_52 = df['high'].rolling(window=52).max()
    low_52 = df['low'].rolling(window=52).min()
    
    # 선행스팬 1: (전환선 + 기준선) / 2
    base_span_a = (df['tenkan'] + df['kijun']) / 2
    # 선행스팬 2: (52일 고가 + 52일 저가) / 2
    base_span_b = (high_52 + low_52) / 2
    
    # 현재 시점(i)에 유효한 구름대 값 (26일 전 계산값)
    df['span_a'] = base_span_a.shift(26)
    df['span_b'] = base_span_b.shift(26)
    
    # 데이터 정리
    df.dropna(inplace=True)
    
    trades = []
    in_position = False
    entry_price = 0
    highest_price_since_entry = 0
    entry_type = "" # '200%', 'Alban', '100%'

    for i in range(1, len(df)):
        curr = df.iloc[i]
        prev = df.iloc[i-1]
        
        # --- 청산 로직 ---
        if in_position:
            highest_price_since_entry = max(highest_price_since_entry, curr['high'])
            exit_triggered = False
            exit_price = 0
            exit_reason = ""
            
            # 1. 강제 청산 (Liquidation)
            liquidation_price = entry_price * (1 - (1 / leverage))
            if curr['low'] <= liquidation_price:
                exit_triggered = True; exit_price = liquidation_price; exit_reason = "Liquidation"

            # 2. 손절 (Stop Loss)
            if not exit_triggered and stop_loss_pct:
                sl_price = entry_price * (1 - stop_loss_pct)
                if curr['low'] <= sl_price:
                    exit_triggered = True; exit_price = sl_price; exit_reason = "StopLoss"

            # 3. 익절 (Take Profit)
            if not exit_triggered and take_profit_pct:
                tp_price = entry_price * (1 + take_profit_pct)
                if curr['high'] >= tp_price:
                    exit_triggered = True; exit_price = tp_price; exit_reason = "TakeProfit"

            # 4. 트레일링 스탑
            if not exit_triggered and trailing_stop_pct:
                ts_price = highest_price_since_entry * (1 - trailing_stop_pct)
                if curr['low'] <= ts_price:
                    exit_triggered = True; exit_price = ts_price; exit_reason = "TrailingStop"
                    
            # 5. 데드크로스 (전환선 < 기준선) - 기본 청산 전략
            if not exit_triggered:
                dead_cross = (prev['tenkan'] >= prev['kijun']) and (curr['tenkan'] < curr['kijun'])
                if dead_cross:
                    exit_triggered = True; exit_price = curr['close']; exit_reason = "DeadCross"

            if exit_triggered:
                pnl = ((exit_price / entry_price) - 1) * leverage - (leverage * FEE_RATE * 2) # 진입/청산 수수료 약식 적용
                trades.append({
                    'ticker': curr.name, # DataFrame index is timestamp usually, handled outside
                    'entry_price': entry_price,
                    'exit_price': exit_price,
                    'pnl': pnl,
                    'entry_type': entry_type,
                    'exit_reason': exit_reason
                })
                in_position = False
                continue

        # --- 진입 로직 ---
        if not in_position:
            # 기본 원칙: 주가는 전환선 위에 있어야 함
            if not (curr['close'] > curr['tenkan']):
                continue
                
            # 상태 변수 계산
            is_tenkan_rising = curr['tenkan'] > prev['tenkan']
            if not is_tenkan_rising:
                continue # 전환선이 상승하지 않으면 매수 안함
            
            high_9_rising = curr['period_high_9'] > prev['period_high_9']
            low_9_rising = curr['period_low_9'] > prev['period_low_9']
            low_9_falling = curr['period_low_9'] < prev['period_low_9']
            
            # 알반갭 확인: 현재 종가가 이전 9개 캔들의 최고가보다 커야 함
            alban_gap = curr['close'] > prev['period_high_9']

            detected_type = None
            
            # 4. 속임수 상승 체크 (최우선 필터)
            if low_9_falling:
                # 매수 금지
                continue 

            # 5. 음운(Negative Cloud) 필터링
            # 구름이 음운(Span A < Span B)일 때는 선행스팬 1(Span A)이 반드시 상승해야 함
            is_negative_cloud = curr['span_a'] < curr['span_b']
            pass_cloud_filter = True
            if is_negative_cloud:
                if not (curr['span_a'] > prev['span_a']): pass_cloud_filter = False
            

            # 진입 신호 판별
                # 신고가 조건 (User Request)
                # 1. 10기간 신고가 (Current Close > Max High of Prev 9) -> 200%, 100% 상승용
                is_10_period_high = curr['close'] > prev['period_high_9']

                # 2. 11기간 신고가 (Current Close > Max High of Prev 10) -> 알반갭용
                is_11_period_high = curr['close'] > prev['period_high_10']

                # 200% 상승 조건 정밀화 (User Request)
                # ... (High/Low logic remains) ...
                # 고가 갱신: 새 캔들이 고가를 만듦 (Current High > Prev High)
                cond_high_200 = curr['high'] > prev['high']
                
                # 저가 갱신: ...
                try:
                    low_dropped = df['low'].iloc[i-9]
                    low_next_tail = df['low'].iloc[i-8]
                    cond_low_200 = low_dropped < low_next_tail
                except IndexError:
                    cond_low_200 = False

                is_200_rise = high_9_rising and low_9_rising and cond_high_200 and cond_low_200 and is_10_period_high
                
                # 알반갭: 11기간 신고가
                # high_9_rising 조건은 없어도 됨? 전략 표에는 "10일 신고가 갱신하며 상승"이라 되어있음.
                # 보통 알반갭은 갭상승이므로 is_11_period_high 자체가 강력함.
                is_alban_gap = alban_gap and is_11_period_high 
                
                # 100% 상승: 고가만 상승 + 10기간 신고가
                is_100_rise = high_9_rising and not low_9_rising and is_10_period_high

                if is_200_rise:
                    detected_type = '200%_Rise' 
                elif is_alban_gap:
                    detected_type = 'Alban_Gap'
                elif is_100_rise:
                    detected_type = '100%_Rise'
            
            if detected_type:
                in_position = True
                entry_price = curr['close']
                highest_price_since_entry = entry_price
                entry_type = detected_type
                
    return trades

# --- 결과 분석 및 저장 ---
def process_results(all_trades, db_filename):
    if not all_trades:
        print("거래 기록이 없습니다.")
        return

    df_trades = pd.DataFrame(all_trades)
    
    # 전략 타입별 성과 분석
    summary = []
    for type_name in df_trades['entry_type'].unique():
        subset = df_trades[df_trades['entry_type'] == type_name]
        total = len(subset)
        wins = len(subset[subset['pnl'] > 0])
        avg_pnl = subset['pnl'].mean() * 100
        win_rate = (wins / total * 100) if total > 0 else 0
        
        summary.append({
            'strategy_type': type_name,
            'total_trades': total,
            'win_rate': round(win_rate, 2),
            'exclude_avg_pnl_pct': round(avg_pnl, 2)
        })
        
    print("\n=== 전략별 성과 요약 ===")
    print(pd.DataFrame(summary))
    
    # 2. 티커별 성과 분석
    ticker_summary = []
    for ticker in df_trades['ticker'].unique():
        subset = df_trades[df_trades['ticker'] == ticker]
        
        # 기본 통계
        total = len(subset)
        wins = len(subset[subset['pnl'] > 0])
        total_profit = subset['pnl'].sum()
        win_rate = (wins / total * 100) if total > 0 else 0.0
        
        # 손익비 (Profit Factor) - V1용 추가
        avg_win = subset[subset['pnl'] > 0]['pnl'].mean()
        avg_loss = abs(subset[subset['pnl'] <= 0]['pnl'].mean())
        if pd.isna(avg_win): avg_win = 0
        if pd.isna(avg_loss): avg_loss = 0
        pnl_ratio = (avg_win / avg_loss) if avg_loss > 0 else 0.0
        
        # MDD (최대 낙폭) - V1용 약식 계산 (누적 PnL 기준)
        # 초기 자본 1000불 가정하고 PnL 누적 -> Balance 흐름 생성
        balances = [1000.0]
        curr_bal = 1000.0
        for p in subset['pnl']:
            curr_bal += p
            balances.append(curr_bal)
            
        peak = 1000.0
        max_dd = 0.0
        for b in balances:
            if b > peak: peak = b
            dd = (peak - b) / peak
            if dd > max_dd: max_dd = dd
        mdd_pct = max_dd * 100

        ticker_summary.append({
            'ticker': ticker,
            'total_trades': total,
            'win_rate_pct': round(win_rate, 2),
            'pnl_ratio': round(pnl_ratio, 2),
            'mdd_pct': round(mdd_pct, 2),
            'total_net_profit_usd': round(total_profit, 2)
        })
    
    df_ticker_stats = pd.DataFrame(ticker_summary)
    if not df_ticker_stats.empty:
        # 1. 순수익 기준 정렬
        df_profit_rank = df_ticker_stats.sort_values(by='total_net_profit_usd', ascending=False)
        print("\n=== TOP 20 종목 (순수익 기준) ===")
        print(df_profit_rank[['ticker', 'win_rate_pct', 'pnl_ratio', 'mdd_pct', 'total_net_profit_usd']].head(20))
        
        # 2. 종합 추천 점수 계산 (User Request)
        # Score = WinRate * PnL_Ratio * (100 - MDD) / 100
        # 최소 거래 횟수 필터 (5회 이상)
        df_rec = df_ticker_stats[df_ticker_stats['total_trades'] >= 5].copy()
        
        if not df_rec.empty:
            df_rec['score'] = df_rec['win_rate_pct'] * df_rec['pnl_ratio'] * (100 - df_rec['mdd_pct']) / 100
            df_rec = df_rec.sort_values(by='score', ascending=False)
            
            print("\n=== BEST 10 종목 추천 (승률, 손익비, MDD 종합) ===")
            print(df_rec[['ticker', 'score', 'win_rate_pct', 'pnl_ratio', 'mdd_pct', 'total_net_profit_usd']].head(10))

    # DB 저장
    conn = sqlite3.connect(db_filename)
    df_trades.to_sql('trades_detail', conn, if_exists='replace', index=False)
    pd.DataFrame(summary).to_sql('strategy_summary', conn, if_exists='replace', index=False)
    if not df_ticker_stats.empty:
        df_ticker_stats.to_sql('ticker_summary', conn, if_exists='replace', index=False)
    conn.close()
    print(f"\n상세 거래 내역 및 요약을 '{db_filename}'에 저장했습니다.")

# --- 메인 실행 ---
def run_v1_backtest():
    print("=== 전환선 전략 v1 백테스트 시작 ===")
    
    tickers = load_and_filter_tickers('15m', top_n=1000) # 전체 종목 테스트
    if not tickers:
        print("티커를 찾을 수 없습니다.")
        return

    all_trades = []
    
    # 파라미터 (고정값으로 테스트)
    TS = 0.05 # 트레일링 스탑 5%
    SL = 0.03 # 손절 3%
    TP = None # 익절 없음 (추세 추종)
    
    print(f"\n백테스트 실행 중... (TS={TS}, SL={SL})")
    for idx, ticker in enumerate(tickers):
        filepath = os.path.join(OHLCV_DIR, TIMEFRAME_FOLDERS['15m'], f"{ticker}.csv")
        try:
            df = pd.read_csv(filepath, header=0, engine='python')
            df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # 기간 필터링: 최근 6개월 (V3와 동일 조건)
            one_year_ago = datetime.now() - pd.Timedelta(days=180)
            df = df[df['timestamp'] >= one_year_ago].copy()
            if df.empty: continue
            
            trades = run_tenkan_strategy(df, FEE_RATE, LEVERAGE, trailing_stop_pct=TS, stop_loss_pct=SL, take_profit_pct=TP)
            
            for t in trades:
                t['ticker'] = ticker # 티커 정보 추가
                
            all_trades.extend(trades)
            
            if (idx + 1) % 10 == 0:
                print(f"{idx+1}/{len(tickers)} 완료...")
                
        except Exception as e:
            print(f"{ticker} 처리 중 에러: {e}")
            continue

    filename = f"tenkan_v1_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    process_results(all_trades, filename)

if __name__ == "__main__":
    run_v1_backtest()
