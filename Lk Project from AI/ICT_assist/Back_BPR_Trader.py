import ccxt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# indicator 폴더에 있는 FVG.py의 함수들을 가져옵니다.
from indicator.FVG import find_bullish_fvg, find_bearish_fvg

def get_bpr_zones(df: pd.DataFrame):
    """
    데이터프레임에서 Up_BPR와 Down_BPR 존을 찾습니다.
    - Up_BPR: 하락 FVG -> 상승 FVG 순서로 겹칠 때 (매수 존)
    - Down_BPR: 상승 FVG -> 하락 FVG 순서로 겹칠 때 (매도 존)
    """
    bullish_fvgs = find_bullish_fvg(df)
    bearish_fvgs = find_bearish_fvg(df)
    bpr_zones = []

    for bull_fvg in bullish_fvgs:
        for bear_fvg in bearish_fvgs:
            # 겹치는지 확인
            if bull_fvg['bottom'] < bear_fvg['top'] and bull_fvg['top'] > bear_fvg['bottom']:
                bpr_bottom = max(bull_fvg['bottom'], bear_fvg['bottom'])
                bpr_top = min(bull_fvg['top'], bear_fvg['top'])

                if bpr_top <= bpr_bottom:
                    continue

                # FVG 발생 순서에 따라 BPR 타입 결정
                if bear_fvg['time'] < bull_fvg['time']:
                    # Up_BPR (하락 FVG -> 상승 FVG)
                    start_time = bear_fvg['time'] - pd.Timedelta(hours=1)
                    end_time = bull_fvg['time'] + pd.Timedelta(hours=1)
                    zone_slice = df.loc[start_time:end_time]
                    stop_loss = zone_slice['low'].min()
                    bpr_type = 'Up_BPR'
                elif bull_fvg['time'] < bear_fvg['time']:
                    # Down_BPR (상승 FVG -> 하락 FVG)
                    start_time = bull_fvg['time'] - pd.Timedelta(hours=1)
                    end_time = bear_fvg['time'] + pd.Timedelta(hours=1)
                    zone_slice = df.loc[start_time:end_time]
                    stop_loss = zone_slice['high'].max()
                    bpr_type = 'Down_BPR'
                else:
                    continue

                bpr_zones.append({
                    'type': bpr_type,
                    'start_time': end_time, # BPR이 완성된 시간
                    'bottom': bpr_bottom,
                    'top': bpr_top,
                    'stop_loss': stop_loss
                })
    return bpr_zones

def run_backtest(df: pd.DataFrame, bpr_zones: list):
    """
    BPR 전략 백테스트를 실행합니다.
    """
    trades = []
    position = None
    
    # BPR 존을 발생 시간 순으로 정렬
    bpr_zones.sort(key=lambda x: x['start_time'])
    zone_idx = 0

    for i in range(len(df)):
        current_candle = df.iloc[i]
        current_time = current_candle.name
        
        # 현재 포지션이 있으면, SL/TP 확인
        if position:
            # Stop Loss 확인
            if ((position['type'] == 'long' and current_candle['low'] <= position['stop_loss']) or
                (position['type'] == 'short' and current_candle['high'] >= position['stop_loss'])):
                position['exit_price'] = position['stop_loss']
                position['exit_time'] = current_time
                position['pnl'] = (position['exit_price'] - position['entry_price']) if position['type'] == 'long' else (position['entry_price'] - position['exit_price'])
                trades.append(position)
                position = None
            # Take Profit 확인
            elif ((position['type'] == 'long' and current_candle['high'] >= position['take_profit']) or
                  (position['type'] == 'short' and current_candle['low'] <= position['take_profit'])):
                position['exit_price'] = position['take_profit']
                position['exit_time'] = current_time
                position['pnl'] = (position['exit_price'] - position['entry_price']) if position['type'] == 'long' else (position['entry_price'] - position['exit_price'])
                trades.append(position)
                position = None

        # 새로운 포지션 진입 확인 (현재 포지션이 없을 때만)
        if not position:
            # 현재 시간 이후에 생성된 BPR 존들을 순회
            for zone in bpr_zones:
                if current_time > zone['start_time']:
                    # Up_BPR (Long 진입)
                    if zone['type'] == 'Up_BPR' and current_candle['low'] <= zone['top'] and current_candle['high'] >= zone['bottom']:
                        entry_price = min(current_candle['open'], zone['top']) # 캔들이 존에 닿는 가격으로 진입
                        stop_loss = zone['stop_loss']
                        risk = entry_price - stop_loss
                        if risk <= 0: continue
                        take_profit = entry_price + (risk * 2) # 1:2 손익비
                        
                        position = {
                            'type': 'long', 'entry_time': current_time, 'entry_price': entry_price,
                            'stop_loss': stop_loss, 'take_profit': take_profit, 'bpr_zone': zone
                        }
                        # 하나의 존에 한번만 진입하기 위해 사용된 존은 제거
                        bpr_zones.remove(zone)
                        break # 다음 캔들로

                    # Down_BPR (Short 진입)
                    elif zone['type'] == 'Down_BPR' and current_candle['low'] <= zone['top'] and current_candle['high'] >= zone['bottom']:
                        entry_price = max(current_candle['open'], zone['bottom']) # 캔들이 존에 닿는 가격으로 진입
                        stop_loss = zone['stop_loss']
                        risk = stop_loss - entry_price
                        if risk <= 0: continue
                        take_profit = entry_price - (risk * 2) # 1:2 손익비

                        position = {
                            'type': 'short', 'entry_time': current_time, 'entry_price': entry_price,
                            'stop_loss': stop_loss, 'take_profit': take_profit, 'bpr_zone': zone
                        }
                        bpr_zones.remove(zone)
                        break # 다음 캔들로

    return trades

def print_trade_results(trades, df: pd.DataFrame):
    """
    백테스트 결과를 출력합니다.
    """
    if not trades:
        print("거래가 발생하지 않았습니다.")
        return

    cumulative_pnl = 0
    wins = 0
    losses = 0
    pnl_history = []
    trade_exit_times = []
    
    print("\n--- 거래 내역 ---")
    for trade in trades:
        pnl = trade['pnl']
        cumulative_pnl += pnl
        pnl_history.append(cumulative_pnl)
        trade_exit_times.append(trade['exit_time'])

        if pnl > 0:
            wins += 1
            result = "익절"
        else:
            losses += 1
            result = "손절"
        print(f"[{trade['type'].upper()}] 진입: {trade['entry_time']} ({trade['entry_price']:.2f}) |"
              f" 청산: {trade['exit_time']} ({trade['exit_price']:.2f}) |"
              f" PNL: {pnl:.2f} | 결과: {result}")

    print("\n--- 백테스트 결과 요약 ---")
    total_trades = len(trades)
    win_rate = (wins / total_trades) * 100 if total_trades > 0 else 0
    
    print(f"총 거래 횟수: {total_trades}")
    print(f"승리: {wins}, 패배: {losses}")
    print(f"승률: {win_rate:.2f}%")
    print(f"총 손익 (PNL): {cumulative_pnl:.2f}")

if __name__ == '__main__':
    # 1. 데이터 가져오기
    binance = ccxt.binanceusdm()
    ticker = 'BTC/USDT'
    timeframe = '1h'
    
    # 1년치 데이터 (365일 * 24시간)
    limit = 365 * 24 
    
    print(f"{ticker} {timeframe} 데이터를 바이낸스에서 가져옵니다...")
    ohlcv = binance.fetch_ohlcv(ticker, timeframe, limit=limit)
    
    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    
    print(f"데이터 로드 완료: {df.index[0]} ~ {df.index[-1]} ({len(df)}개 캔들)")

    # 2. BPR 존 찾기
    print("\nBPR 존을 분석 중입니다...")
    bpr_zones = get_bpr_zones(df)
    print(f"총 {len(bpr_zones)}개의 BPR 존을 찾았습니다.")

    # 3. 백테스트 실행
    print("\n백테스트를 실행합니다...")
    trades = run_backtest(df.copy(), bpr_zones)

    # 4. 결과 출력
    print_trade_results(trades, df)