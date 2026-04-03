# tenkan_backtest_v3.py
# 'stratege.md'의 II. 전환선 매매 전략 + IV. 선행스팬 1 매매 전략 (v2) + VI. 후행스팬 (v3)

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
import sys

# --- 설정 ---
OHLCV_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "OHLCV")
TIMEFRAME_FOLDERS = {'15m': "Binance_Fureres_USDT_15Minute_ohlcv"}
TENKAN_PERIOD = 9
ALBAN_PERIOD = 10
SMA_PERIOD = 60 # 보조지표
LEVERAGE = 5 # 레버리지 5배
FEE_RATE = 0.0002 # 지정가(Maker) 수수료 기준 (0.02%)
IS_BACKTEST_RUNNING = False

# --- 데이터 로드 및 필터링 (기존 로직 활용) ---
def load_and_filter_tickers(timeframe='15m', top_n=200):
    data_dir = os.path.join(OHLCV_DIR, TIMEFRAME_FOLDERS.get(timeframe, ""))
    if not os.path.exists(data_dir):
        print(f"데이터 폴더 없음: {data_dir}")
        return []


    all_files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]
    ticker_stats = []
    
    # 6개월 전 날짜 계산
    one_year_ago = datetime.now() - pd.Timedelta(days=180)
    print(f"[{timeframe}] 티커 필터링 시작 (기준상태: 최근 6개월, {len(all_files)}개 파일)...")
    
    for filename in all_files:
        try:
            file_path = os.path.join(data_dir, filename)
            if os.path.getsize(file_path) == 0: continue
            
            # 전체 로드
            df = pd.read_csv(file_path, header=0, engine='python')
            df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # 최근 1년 데이터만 필터링 (화면 표시는 6개월이라도 넉넉히 1년으로 읽고 아래서 처리)
            # 여기서는 최근 데이터 유무 확인이 우선
            last_timestamp = df['timestamp'].iloc[-1]
            if last_timestamp < (datetime.now() - pd.Timedelta(days=7)):
                continue # 데이터가 1주일 이상 됨 (상폐/업데이트 안됨)

            df_recent = df[df['timestamp'] >= one_year_ago]
            if df_recent.empty: continue
            
            turnover = (df_recent['volume'] * df_recent['close']).sum()
            ticker_stats.append({'ticker': filename.split('.')[0], 'turnover': turnover})
            
        except Exception:
            continue
            
    if not ticker_stats: return []
    df_stats = pd.DataFrame(ticker_stats)
    top_tickers = df_stats.nlargest(top_n, 'turnover')['ticker'].tolist()
    print(f"필터링 완료: {len(top_tickers)}개 티커 선정.")
    return top_tickers

# --- 핵심 전략 로직 ---
def run_v3_strategy(df, fee_rate, leverage):
    """
    V1(전환선) + V2(선행스팬 1 지지/저항) + V3(후행스팬) 매매 시뮬레이션
    """
    if df.empty: return [] # 데이터 없으면 리턴

    # 1. 지표 계산
    # 전환선 (9)
    high_9 = df['high'].rolling(window=TENKAN_PERIOD).max()
    low_9 = df['low'].rolling(window=TENKAN_PERIOD).min()
    df['tenkan'] = (high_9 + low_9) / 2
    
    # 기준선 (26)
    high_26 = df['high'].rolling(window=26).max()
    low_26 = df['low'].rolling(window=26).min()
    df['kijun'] = (high_26 + low_26) / 2
    
    # 9기간 고가/저가 (V1용)
    df['period_high_9'] = high_9
    df['period_low_9'] = low_9
    
    # 10기간 고가 (V1 알반갭용)
    df['period_high_10'] = df['high'].rolling(window=ALBAN_PERIOD).max()

    # V2: 구름대 계산
    high_52 = df['high'].rolling(window=52).max()
    low_52 = df['low'].rolling(window=52).min()
    
    # base_span_a: 현재(i) 계산된 선행스팬 1 값 (i+26 에 그려짐)
    base_span_a = (df['tenkan'] + df['kijun']) / 2
    # base_span_b: 현재(i) 계산된 선행스팬 2 값 (i+26 에 그려짐)
    base_span_b = (high_52 + low_52) / 2
    
    # df['span_a']: 현재 시점(i)에 유효한 구름대 값 (26일 전 계산값)
    df['span_a'] = base_span_a.shift(26)
    df['span_b'] = base_span_b.shift(26)
    
    # --- V2 핵심 로직: 앞 구름(Front Span 1)의 Min/Max 계산 ---
    # base_span_a[i]는 i+26 시점의 구름값입니다.
    # 그러므로 i 시점에서 바라본 i ~ i+26 구간의 앞 구름 값들은 base_span_a[i-26] 부터 base_span_a[i] 까지입니다.
    # 즉, base_span_a의 최근 27개 값(0~26 shift)의 Min/Max가 곧 앞 구름의 Min/Max입니다.
    df['front_span1_min'] = base_span_a.rolling(window=27).min()
    df['front_span1_max'] = base_span_a.rolling(window=27).max()

    # 데이터 정리
    df.dropna(inplace=True)
    
    trades = []
    
    # 자금 관리 변수
    balance = 1000.0 # 초기 자본금 $1000
    trade_allocation = 0 # 이번 트레이드에 배정된 증거금 (Balance의 10%)
    
    # 포지션 상태 변수
    current_qty = 0.0 # 보유 수량 (Coin)
    avg_entry_price = 0.0
    highest_price_since_entry = 0.0
    entry_log = [] # 진입 이력
    
    # 진입 단계 추적 (0:없음, 1:관망, 2:진입)
    entry_stage = 0

    for i in range(1, len(df)):
        # --- V3 후행스팬 유효성 체크를 위한 인덱스 확인 ---
        if i < 26: continue # 후행스팬(i-26) 비교를 위해 최소 26개 이상 필요

        curr = df.iloc[i]
        prev = df.iloc[i-1]
        
        # V3: 후행스팬 (Chikou Span) 로직
        # "현재 종가(curr['close'])"가 "26일 전 종가(df.iloc[i-26]['close'])" 보다 높아야 함
        chikou_prev_price = df.iloc[i-26]['close']
        is_chikou_valid = curr['close'] > chikou_prev_price

        # --- 청산 로직 (진입 완료된 상태만 체크) ---
        if entry_stage == 2:
            highest_price_since_entry = max(highest_price_since_entry, curr['high'])
            exit_triggered = False; exit_price = 0; exit_reason = ""
            
            # 1. 강제 청산
            liquidation = avg_entry_price * (1 - (1 / leverage))
            if curr['low'] <= liquidation:
                exit_triggered = True; exit_price = liquidation; exit_reason = "Liquidation"

            # 2. V2 이탈 청산 (앞 구름 지지/저항 이탈)
            if not exit_triggered:
                # Case A: 고점 돌파 진입 상태 -> 고점(Max) 이탈 시 청산
                if prev['close'] >= prev['front_span1_max'] and curr['close'] < curr['front_span1_max']:
                    exit_triggered = True; exit_price = curr['close']; exit_reason = "Span1_High_Breakdown"
            
            if exit_triggered:
                # 수익률 계산 (레버리지 포함)
                pnl = ((exit_price / avg_entry_price) - 1) * leverage - (leverage * FEE_RATE * 2)
                
                # 자금 관리 업데이트
                gross_offer = current_qty * exit_price
                gross_pnl_val = (exit_price - avg_entry_price) * current_qty * leverage # PnL Value
                fee_val = (exit_price * current_qty * leverage) * FEE_RATE
                net_pnl_val = gross_pnl_val - fee_val
                
                balance += net_pnl_val
                
                trades.append({
                    'ticker': curr.name if hasattr(curr, 'name') else 'N/A', 
                    'entry_price': avg_entry_price, 
                    'exit_price': exit_price,
                    'net_profit': net_pnl_val,
                    'balance': balance,
                    'entry_type': "+".join(entry_log), 
                    'exit_reason': exit_reason,
                    'position_size': 1.0
                })
                
                # 포지션 초기화
                entry_stage = 0
                current_qty = 0
                avg_entry_price = 0
                highest_price_since_entry = 0
                entry_log = []
                continue

        # --- 진입 로직 ---
        # 1. V1 시그널
        v1_signal = None
        if (curr['close'] > curr['tenkan']):
            is_tenkan_rising = curr['tenkan'] > prev['tenkan']
            if is_tenkan_rising:
                high_9_rising = curr['period_high_9'] > prev['period_high_9']
                low_9_rising = curr['period_low_9'] > prev['period_low_9']
                low_9_falling = curr['period_low_9'] < prev['period_low_9']
                # 알반갭: 현재 종가가 이전 9개 캔들의 최고가보다 커야 함
                # prev['period_high_9']는 i-1 시점 기준 9개 캔들(i-9 ~ i-1)의 최고가
                alban_gap = curr['close'] > prev['period_high_9']

                if not low_9_falling:
                    # 음운 필터 (V1, V2, V3 공통)
                    is_negative_cloud = curr['span_a'] < curr['span_b']
                    pass_cloud_filter = True
                    if is_negative_cloud:
                        if not (curr['span_a'] > prev['span_a']): pass_cloud_filter = False
                    
                    if pass_cloud_filter:
                        # 신고가 조건 (User Request)
                        # 1. 10기간 신고가 (Current Close > Max High of Prev 9) -> 200%, 100% 상승용
                        is_10_period_high = curr['close'] > prev['period_high_9']

                        # 2. 11기간 신고가 (Current Close > Max High of Prev 10) -> 알반갭용
                        is_11_period_high = curr['close'] > prev['period_high_10']

                        # 200% 상승 조건 정밀화 (User Request)
                        # 고가 갱신: 새 캔들이 고가를 만듦 (Current High > Prev High)
                        cond_high_200 = curr['high'] > prev['high']
                        
                        # 저가 갱신: 10기간 전 캔들(없어지는 캔들)이 최저가였어서 사라짐
                        # i-9 (10번째 전) < i-8 (9번째 전)
                        try:
                            low_dropped = df['low'].iloc[i-9]
                            low_next_tail = df['low'].iloc[i-8]
                            cond_low_200 = low_dropped < low_next_tail
                        except IndexError:
                            cond_low_200 = False

                        is_200_rise = high_9_rising and low_9_rising and cond_high_200 and cond_low_200 and is_10_period_high
                        
                        # 알반갭: 11기간 신고가
                        is_alban_gap = alban_gap and is_11_period_high 
                        
                        # 100% 상승: 고가만 상승 + 10기간 신고가
                        is_100_rise = high_9_rising and not low_9_rising and is_10_period_high

                        if is_200_rise: v1_signal = '200%_Rise'
                        elif is_alban_gap: v1_signal = 'Alban_Gap'
                        elif is_100_rise: v1_signal = '100%_Rise'

        # 2. V2 돌파 시그널 감지
        v2_low_breakout = (prev['close'] <= prev['front_span1_min'] and curr['close'] > curr['front_span1_min'])
        v2_high_breakout = (prev['close'] <= prev['front_span1_max'] and curr['close'] > curr['front_span1_max'])

        # --- 포지션 진입 실행 (0% -> 100% 전략) With V3 Chikou Filter ---
        
        # Case A: 셋업 감지 (저점 돌파) -> 매수 안 함, 관망(Setup) 상태로 전환
        # **V3 추가 조건**: 후행스팬 유효 (is_chikou_valid)
        if entry_stage == 0:
            if v1_signal and v2_low_breakout:
                if is_chikou_valid:
                    entry_stage = 1 # 관망 상태 진입
                    entry_log.append(f"{v1_signal}&LowBreak&Chikou(Wait)")

        # Case B: 관망 중 결정 (진입 or 취소)
        elif entry_stage == 1:
            # 1. 진입 조건: 고점 돌파 시 100% 진입, **단 후행스팬도 여전히 좋아야 함**
            if v2_high_breakout and is_chikou_valid:
                # 진입 수량 및 자금 계산
                trade_allocation = balance * 0.10 # 자본의 10% 투입
                trade_value = trade_allocation * leverage
                current_qty = trade_value / curr['close']
                
                # 진입 수수료 차감
                entry_fee = trade_value * FEE_RATE
                balance -= entry_fee
                
                avg_entry_price = curr['close']
                entry_stage = 2 # 진입 완료 상태
                entry_log.append("HighBreak(Entry)")
                highest_price_since_entry = curr['close']
            
            # 2. 취소 조건: 저점 다시 이탈 시 셋업 무효화 (매수 없이 종료)
            # 또는 후행스팬이 꺾였을 때도 취소할지 고민필요 -> 일단 저점 이탈만 취소 조건으로 유지
            elif (prev['close'] >= prev['front_span1_min'] and curr['close'] < curr['front_span1_min']):
                entry_stage = 0 # 셋업 취소
                entry_log = [] # 로그 초기화
                
    return trades

# --- 결과 분석 및 저장 ---
def process_results(all_trades, db_filename):
    if not all_trades:
        print("거래 기록이 없습니다.")
        return
    df_trades = pd.DataFrame(all_trades)
    
    # 1. 전략 타입별 분석
    summary = []
    for type_name in df_trades['entry_type'].unique():
        subset = df_trades[df_trades['entry_type'] == type_name]
        total = len(subset)
        wins = len(subset[subset['net_profit'] > 0])
        total_profit = subset['net_profit'].sum()
        avg_profit = subset['net_profit'].mean()
        win_rate = (wins / total * 100) if total > 0 else 0
        
        summary.append({
            'strategy_type': type_name, 
            'total_trades': total,
            'win_rate': round(win_rate, 2), 
            'total_net_profit_usd': round(total_profit, 2),
            'avg_profit_usd': round(avg_profit, 2)
        })
    
    print("\n=== 전략별 성과 요약 (USD) ===")
    print(pd.DataFrame(summary))
    
    # 2. 티커별 성과 분석
    ticker_summary = []
    for ticker in df_trades['ticker'].unique():
        subset = df_trades[df_trades['ticker'] == ticker]
        
        # 기본 통계
        total = len(subset)
        wins = len(subset[subset['net_profit'] > 0])
        total_profit = subset['net_profit'].sum()
        win_rate = (wins / total * 100) if total > 0 else 0.0
        
        # 손익비 (Profit Factor)
        avg_win = subset[subset['net_profit'] > 0]['net_profit'].mean()
        avg_loss = abs(subset[subset['net_profit'] <= 0]['net_profit'].mean())
        if pd.isna(avg_win): avg_win = 0
        if pd.isna(avg_loss): avg_loss = 0
        pnl_ratio = (avg_win / avg_loss) if avg_loss > 0 else 0.0
        
        # MDD (최대 낙폭)
        # 잔고 변화: 초기 1000 -> 거래1(1010) -> 거래2(990) ...
        balances = [1000.0] + subset['balance'].tolist()
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
        # 1. 순수익 기준 정렬 (기존)
        df_profit_rank = df_ticker_stats.sort_values(by='total_net_profit_usd', ascending=False)
        print("\n=== TOP 20 종목 (순수익 기준) ===")
        print(df_profit_rank[['ticker', 'win_rate_pct', 'pnl_ratio', 'mdd_pct', 'total_net_profit_usd']].head(20))
        
        # 2. 종합 추천 점수 계산 (User Request: WinRate High, PnL High, MDD Low)
        # Score = WinRate * PnL_Ratio * (100 - MDD) / 100
        # 최소 거래 횟수 필터 (5회 이상)
        df_rec = df_ticker_stats[df_ticker_stats['total_trades'] >= 5].copy()
        
        if not df_rec.empty:
            df_rec['score'] = df_rec['win_rate_pct'] * df_rec['pnl_ratio'] * (100 - df_rec['mdd_pct']) / 100
            df_rec = df_rec.sort_values(by='score', ascending=False)
            
            print("\n=== BEST 10 종목 추천 (승률, 손익비, MDD 종합) ===")
            print(df_rec[['ticker', 'score', 'win_rate_pct', 'pnl_ratio', 'mdd_pct', 'total_net_profit_usd']].head(10))
     
    conn = sqlite3.connect(db_filename)
    df_trades.to_sql('trades_detail', conn, if_exists='replace', index=False)
    pd.DataFrame(summary).to_sql('strategy_summary', conn, if_exists='replace', index=False)
    if not df_ticker_stats.empty:
        df_ticker_stats.to_sql('ticker_summary', conn, if_exists='replace', index=False)
    conn.close()
    print(f"\n상세 거래 내역, 전략 요약, 종목별 성과를 '{db_filename}'에 저장했습니다.")

# --- 메인 실행 ---
def run_v3_backtest():
    print("=== 데이터 업데이트 확인 및 실행 (15m) ===")
    try:
        # OHLCV 수집기 경로 추가 (OHLCV 폴더 자체를 path에 추가해야 함)
        if OHLCV_DIR not in sys.path:
            sys.path.append(OHLCV_DIR)
        import collect_ohlcv
        
        # 15분봉 업데이트 실행
        print("최신 데이터 수집 중... (시간이 소요될 수 있습니다)")
        collect_ohlcv.setup_logging()
        collect_ohlcv.run_15m_update()
        print("\n데이터 업데이트 완료.")
    except Exception as e:
        print(f"데이터 업데이트 실패 (건너뜀): {e}")

    print("=== 전환선(V1) + 선행스팬1(V2) + 후행스팬(V3) 전략 백테스트 시작 ===")
    tickers = load_and_filter_tickers('15m', top_n=1000)
    if not tickers: print("티커를 찾을 수 없습니다."); return

    all_trades = []
    # 익절/손절 삭제 (추세 추종 청산만 사용)
    # 6개월 전 날짜 계산
    one_year_ago = datetime.now() - pd.Timedelta(days=180)
    
    print(f"\n백테스트 실행 중... (기간: 최근 6개월, 청산: 추세이탈)")
    for idx, ticker in enumerate(tickers):
        filepath = os.path.join(OHLCV_DIR, TIMEFRAME_FOLDERS['15m'], f"{ticker}.csv")
        try:
            df = pd.read_csv(filepath, header=0, engine='python')
            df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # 메인 실행에서도 필터링 적용
            df_recent = df[df['timestamp'] >= one_year_ago].copy()
            if df_recent.empty: continue
            
            trades = run_v3_strategy(df_recent, FEE_RATE, LEVERAGE)
            for t in trades: t['ticker'] = ticker
            all_trades.extend(trades)
            if (idx + 1) % 10 == 0: print(f"{idx+1}/{len(tickers)} 완료...")
        except Exception as e:
            print(f"{ticker} 처리 중 에러: {e}"); continue

    filename = f"tenkan_v3_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    process_results(all_trades, filename)

if __name__ == "__main__":
    run_v3_backtest()
