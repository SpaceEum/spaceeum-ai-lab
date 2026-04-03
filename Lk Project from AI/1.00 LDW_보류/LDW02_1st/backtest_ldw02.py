import pandas as pd
import numpy as np
import os
import glob
from concurrent.futures import ProcessPoolExecutor
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

# 데이터 경로 (절대 경로 사용)
DATA_PATH = r"g:\내 드라이브\LK Project\Lk Project from AI\OHLCV\Binance_Fureres_USDT_1Hour_ohlcv"

def calculate_indicators(df):
    # SMA 20
    df['SMA_20'] = df['close'].rolling(window=20).mean()
    
    # 일목균형표
    high_9 = df['high'].rolling(window=9).max()
    low_9 = df['low'].rolling(window=9).min()
    df['tenkan_sen'] = (high_9 + low_9) / 2 # 전환선

    high_26 = df['high'].rolling(window=26).max()
    low_26 = df['low'].rolling(window=26).min()
    df['kijun_sen'] = (high_26 + low_26) / 2 # 기준선

    # 선행스팬 (미래로 shift 되어야 맞지만, 현재 시점 기준 비교를 위해 shift 하지 않고 현재 값으로 계산 후 로직에서 처리하거나,
    # 여기서는 autotrade.py 로직에 맞춰 shift(26)을 하되, 비교 시점 유의)
    # autotrade.py에서는:
    # senkou_span_a = ((tenkan + kijun)/2).shift(26)
    # 로직: current_price > senkou_span_a (오늘의 주가와 26일 앞에 그려진 구름대 비교) -> 
    # 실제로는 '현재 캔들' 위치에 있는 구름대 값은 26일 전 데이터로 계산되어 26일 미래로 밀린 값임.
    # Pandas shift(26)을 하면 현재 행에 26일 전 데이터가 옴 -> 이게 '현재 시점에 그려진 선행스팬'이 아님.
    # 선행스팬은 "현재 계산된 값을 26일 뒤에 그리는 것"임.
    # 따라서 "현재 주가"와 비교해야 할 "구름대"는 26일 전에 계산되어 지금 시점으로 온 값임.
    # 즉, 26일 전의 (전환+기준)/2 값임.
    # df['senkou_span_a'] = ((df['tenkan_sen'] + df['kijun_sen']) / 2).shift(26) -> 이게 맞음 (시간축 정렬시)
    
    df['senkou_span_a'] = ((df['tenkan_sen'] + df['kijun_sen']) / 2).shift(26)
    
    high_52 = df['high'].rolling(window=52).max()
    low_52 = df['low'].rolling(window=52).min()
    df['senkou_span_b'] = ((high_52 + low_52) / 2).shift(26)
    
    return df

def run_backtest_for_leverage(df, leverage):
    # 백테스트 초기값
    initial_balance = 1000
    balance = initial_balance
    position = 0 # 0: None, 1: Long, -1: Short
    entry_price = 0
    entry_amt = 0 # 보유 수량 (Coin개수)
    
    trades = []
    equity_curve = [initial_balance]
    
    # 수수료 (바이낸스 선물 Taker 약 0.04~0.05%)
    fee_rate = 0.0004 
    
    # 전략 설정: 10% 비중 사용
    stake_ratio = 0.1 
    
    # TP/SL 기준 (Account ROE 기준)
    tp_roe = 0.06 # 6%
    sl_roe = -0.03 # -3%
    
    # itertuples 사용
    for row in df.itertuples():
        current_price = row.close
        
        # 구름대
        cloud_top = max(row.senkou_span_a, row.senkou_span_b)
        cloud_bottom = min(row.senkou_span_a, row.senkou_span_b)
        
        # 신호 조건
        long_signal = (row.close > row.SMA_20) and (row.close > cloud_top) and (row.tenkan_sen > row.kijun_sen)
        short_signal = (row.close < row.SMA_20) and (row.close < cloud_bottom) and (row.tenkan_sen < row.kijun_sen)
        
        # 포지션 관리
        if position == 0:
            # 진입 가능 여부: 잔고가 있어야 함
            if balance <= 0: break

            # 진입 크기: 잔고의 10% * 레버리지
            margin = balance * stake_ratio
            trade_value = margin * leverage
            
            # 최소 주문 금액 대략 체크 (예: 5불 이상)
            if trade_value < 5: continue

            if long_signal:
                position = 1
                entry_price = current_price
                entry_amt = trade_value / entry_price
                
                # 수수료 차감 (거래대금 기준)
                fee = trade_value * fee_rate
                balance -= fee
                
            elif short_signal:
                position = -1
                entry_price = current_price
                entry_amt = trade_value / entry_price
                
                fee = trade_value * fee_rate
                balance -= fee
                
        elif position == 1: # Long
            # 1. PnL (ROE) 체크
            # 가격 변동률
            raw_pnl_pct = (current_price - entry_price) / entry_price
            # 레버리지 적용 ROE
            roe = raw_pnl_pct * leverage
            
            exit_reason = None
            if roe >= tp_roe: exit_reason = 'TP'
            elif roe <= sl_roe: exit_reason = 'SL'
            elif short_signal: exit_reason = 'Signal' # 반대신호
            
            if exit_reason:
                # 청산
                # 롱 수익금 = (현재가 - 진입가) * 수량
                pnl_value = (current_price - entry_price) * entry_amt
                
                # 수수료 (나갈 때도 전체 포지션 가치 기준)
                exit_value = entry_amt * current_price
                fee = exit_value * fee_rate
                
                balance += pnl_value - fee
                
                trades.append({'res': 'win' if pnl_value > 0 else 'loss', 'pnl_roe': roe, 'pnl_val': pnl_value, 'reason': exit_reason})
                position = 0
                entry_amt = 0
                
        elif position == -1: # Short
            raw_pnl_pct = (entry_price - current_price) / entry_price
            roe = raw_pnl_pct * leverage
            
            exit_reason = None
            if roe >= tp_roe: exit_reason = 'TP'
            elif roe <= sl_roe: exit_reason = 'SL'
            elif long_signal: exit_reason = 'Signal'
            
            if exit_reason:
                # 숏 수익금 = (진입가 - 현재가) * 수량
                pnl_value = (entry_price - current_price) * entry_amt
                
                exit_value = entry_amt * current_price
                fee = exit_value * fee_rate
                
                balance += pnl_value - fee
                
                trades.append({'res': 'win' if pnl_value > 0 else 'loss', 'pnl_roe': roe, 'pnl_val': pnl_value, 'reason': exit_reason})
                position = 0
                entry_amt = 0

        # Equity Curve 업데이트
        # 포지션 보유 중일 때의 미실현 손익은 equity_curve에 반영하지 않고, 실현 손익 기준 Balance만 반영 (보수적) or 반영 (Drawdown 계산용)
        # 보통 MDD는 Equity 기준이므로 미실현 손익 반영이 맞음.
        temp_balance = balance
        if position == 1:
            unrealized_pnl = (current_price - entry_price) * entry_amt
            temp_balance += unrealized_pnl
        elif position == -1:
            unrealized_pnl = (entry_price - current_price) * entry_amt
            temp_balance += unrealized_pnl
            
        equity_curve.append(temp_balance)

    # 결과 통계
    if not trades:
        return None
        
    final_balance = balance
    total_return = (final_balance - initial_balance) / initial_balance * 100
    
    wins = [t for t in trades if t['res'] == 'win']
    losses = [t for t in trades if t['res'] == 'loss']
    win_rate = len(wins) / len(trades) * 100 if trades else 0
    
    # MDD
    s = pd.Series(equity_curve)
    cummax = s.cummax()
    dd = (s - cummax) / cummax
    mdd = dd.min() * 100
    
    # Profit Factor (Gross Win / Gross Loss)
    gross_win = sum([t['pnl_val'] for t in wins])
    gross_loss = sum([abs(t['pnl_val']) for t in losses])
    pf = gross_win / gross_loss if gross_loss > 0 else 999
    
    return {
        'Leverage': leverage,
        'Final Balance': final_balance,
        'Return (%)': total_return,
        'Win Rate (%)': win_rate,
        'MDD (%)': mdd,
        'Profit Factor': pf,
        'Total Trades': len(trades)
    }

def process_file_multi_lev(file_path):
    try:
        ticker = os.path.basename(file_path).replace('.csv', '')
        
        # 로드
        df = pd.read_csv(file_path)
        df.columns = [col.lower() for col in df.columns]
        
        required = ['open', 'high', 'low', 'close']
        if not all(c in df.columns for c in required): return []
        
        df = calculate_indicators(df)
        df.dropna(inplace=True)
        if len(df) < 100: return []
        
        results = []
        leverages = [1, 5, 10, 20]
        
        for lev in leverages:
            res = run_backtest_for_leverage(df, lev)
            if res:
                res['Ticker'] = ticker
                # 점수 계산 (승률 * PF / MDD penalty)
                # MDD가 0이면 1로 처리. 
                # 사용자 요청: "적정한 레버리지 추천" -> Return과 Stability의 조화
                # Score = Return / (|MDD| * 0.5 + 1) ? 
                # 기존 Score 로직 유지하되, 전체 비교를 위해
                risk_adj_return = res['Return (%)'] / (abs(res['MDD (%)']) + 1)
                res['Score'] = risk_adj_return
                results.append(res)
                
        return results

    except Exception as e:
        return []

def main():
    files = glob.glob(os.path.join(DATA_PATH, "*.csv"))
    print(f"총 {len(files)}개 파일로 멀티 레버리지(1, 5, 10, 20x) 백테스트 시작...")
    
    all_results = []
    with ProcessPoolExecutor() as executor:
        for res_list in executor.map(process_file_multi_lev, files):
            if res_list:
                all_results.extend(res_list)
                
    if not all_results:
        print("결과 없음.")
        return
        
    df = pd.DataFrame(all_results)
    
    # 1. 레버리지별 평균 성과 (적정 레버리지 추천용)
    print("\n=== 레버리지별 평균 성과 ===")
    summary = df.groupby('Leverage')[['Return (%)', 'MDD (%)', 'Win Rate (%)', 'Profit Factor']].mean()
    print(summary)
    
    best_lev = summary['Return (%)'].idxmax()
    print(f"\n>> 수익률 기준 추천 레버리지: {best_lev}x")
    
    # 2. 전체 랭킹 (레버리지 포함 Top 10)
    # 거래 횟수 필터 (너무 적으면 신뢰도 낮음)
    df_filtered = df[df['Total Trades'] >= 20]
    
    print("\n=== 전체 Top 10 (모든 레버리지 포함) ===")
    top10 = df_filtered.sort_values(by='Score', ascending=False).head(10)
    print(top10[['Ticker', 'Leverage', 'Return (%)', 'MDD (%)', 'Win Rate (%)', 'Profit Factor', 'Score']].to_string(index=False))
    
    # 파일 저장
    df.to_csv("all_leverage_results.csv", index=False)
    top10.to_csv("top10_leverage.csv", index=False)


if __name__ == "__main__":
    main()
