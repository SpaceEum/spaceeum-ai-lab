import requests
import pandas as pd
import numpy as np
import pandas_ta as ta
from datetime import datetime, timedelta

def fetch_binance_data(symbol, interval='1h', limit=365):
    """
    바이낸스에서 특정 심볼의 과거 K-line 데이터를 가져옵니다.
    기본값으로 지난 1년간의 1시간봉 데이터를 가져옵니다.
    API 제약으로 인해 여러 번 호출하여 데이터를 병합합니다.
    """
    url = "https://fapi.binance.com/fapi/v1/klines"
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=limit)

    all_data = []
    current_start_time = int(start_time.timestamp() * 1000)
    end_time_ms = int(end_time.timestamp() * 1000)
    api_limit = 1500

    try:
        while current_start_time < end_time_ms:
            params = {
                'symbol': symbol,
                'interval': interval,
                'startTime': current_start_time,
                'endTime': end_time_ms,
                'limit': api_limit
            }
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            if not data:
                break

            all_data.extend(data)
            # 다음 요청의 시작 시간을 마지막으로 받은 데이터의 시간으로 설정
            last_timestamp = data[-1][0]
            current_start_time = last_timestamp + 1

            # API 요청 속도 제한을 피하기 위한 짧은 대기
            import time
            time.sleep(0.1)

        if not all_data:
            print(f"Warning: No data returned for {symbol} in the given period.")
            return pd.DataFrame()

        df = pd.DataFrame(all_data, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
        ])

        # 데이터 타입 변환
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col])

        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)

        return df[['open', 'high', 'low', 'close', 'volume']]

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {symbol}: {e}")
        return pd.DataFrame()

def calculate_heatmap_signals(df):
    """
    Heatmap_trade_strategy.txt에 명시된 전략에 따라 신호를 생성합니다.
    1. 추세 확인: 60 EMA
    2. 진입 신호: Parabolic SAR
    3. 신뢰도 확인: 히트맵 볼륨 (거래량 강도)
    """
    if df.empty:
        return df

    # 1. 60 EMA 계산
    df.ta.ema(length=60, append=True)
    
    # 2. Parabolic SAR 계산
    df.ta.psar(append=True)
    
    # 3. 히트맵 볼륨 계산 (거래량 이동평균 대비 배율)
    # 전략 설명의 Extra High: 3, High: 1.5, Medium: 0.5 값을 기준으로 설정
    volume_ma = df['volume'].rolling(window=20).mean()
    df['volume_ratio'] = df['volume'] / volume_ma
    
    # 신호 생성
    df['signal'] = 0
    
    # 파라볼릭 SAR의 매수/매도 신호 확인 (PSARl: long signal, PSARs: short signal)
    long_signals = (df['close'] > df['EMA_60']) & (df['PSARl_0.02_0.2'].notna()) & (df['volume_ratio'] >= 1.5)
    short_signals = (df['close'] < df['EMA_60']) & (df['PSARs_0.02_0.2'].notna()) & (df['volume_ratio'] >= 1.5)

    df.loc[long_signals, 'signal'] = 1
    df.loc[short_signals, 'signal'] = -1

    # 포지션 유지: 이전 신호가 현재 신호와 같으면 유지
    df['signal'] = df['signal'].replace(0, np.nan).ffill().fillna(0)

    return df

def run_backtest(df, initial_capital=10000, leverage=1.0):
    """
    생성된 신호를 기반으로 백테스트를 실행합니다.
    """
    if df.empty or len(df) < 2:
        return None

    df_signal = calculate_heatmap_signals(df.copy())

    capital = initial_capital
    position = 0
    entry_price = 0
    stop_loss_price = 0  # 손절 가격을 저장할 변수 추가
    trades = []
    win_count = 0
    trade_count = 0
    peak_capital = initial_capital
    max_drawdown = 0

    for i in range(1, len(df_signal)):
        signal = df_signal['signal'].iloc[i]
        prev_signal = df_signal['signal'].iloc[i-1]
        current_price = df_signal['close'].iloc[i]
        
        # 포지션 종료 조건 1: 손절가 도달
        is_stop_loss_triggered = (position == 1 and current_price < stop_loss_price) or \
                                 (position == -1 and current_price > stop_loss_price)

        # 포지션 종료 조건 2: 신호가 반대로 변경됨
        is_signal_reversed = (position != 0 and signal == -position)

        # 손절 또는 신호 반전 시 포지션 종료
        if is_stop_loss_triggered or is_signal_reversed:
            pnl = (current_price - entry_price) * position
            profit_pct = (pnl / entry_price) * leverage
            capital += capital * profit_pct
            
            trades.append({
                'exit_time': df_signal.index[i],
                'pnl': pnl * leverage, # Simplified PnL for win rate
            })
            
            if pnl > 0:
                win_count += 1
            
            position = 0
            entry_price = 0
            stop_loss_price = 0
        
        # 자본금 최고점 갱신 및 최대 낙폭 계산
        peak_capital = max(peak_capital, capital)
        drawdown = (peak_capital - capital) / peak_capital
        max_drawdown = max(max_drawdown, drawdown)

        # 신규 포지션 진입: 포지션이 없는데 신호가 발생했거나, 신호가 반전되었을 때
        if (position == 0 and signal != 0) or is_signal_reversed:
            position = signal
            entry_price = current_price
            
            # 손절가 설정: "직전 저점"을 지난 14일의 최저/최고가로 정의
            lookback_period = 14
            if signal == 1: # 매수 포지션
                stop_loss_price = df_signal['low'].iloc[i-lookback_period:i].min()
            elif signal == -1: # 매도 포지션
                stop_loss_price = df_signal['high'].iloc[i-lookback_period:i].max()
            trade_count += 1

    # 최종 자본 계산 (만약 포지션이 열려있다면)
    if position != 0:
        current_price = df_signal['close'].iloc[-1]
        pnl = (current_price - entry_price) * position
        profit_pct = (pnl / entry_price) * leverage
        capital += capital * profit_pct

    final_capital = capital
    total_profit = final_capital - initial_capital
    profit_pct = (total_profit / initial_capital) * 100
    win_rate = (win_count / trade_count * 100) if trade_count > 0 else 0
    mdd_pct = max_drawdown * 100

    return {
        'final_capital': final_capital,
        'total_profit': total_profit,
        'profit_pct': profit_pct,
        'trade_count': trade_count,
        'win_rate': win_rate,
        'mdd_pct': mdd_pct,
    }

if __name__ == '__main__':
    # 개별 파일 테스트용 코드
    symbol_to_test = 'BTCUSDT'
    leverage_to_test = 10.0
    
    print(f"--- Testing Heatmap_trade.py for {symbol_to_test} with {leverage_to_test}x leverage ---")
    
    # 1. 데이터 가져오기
    print(f"Fetching data for {symbol_to_test}...")
    btc_df = fetch_binance_data(symbol_to_test)
    
    if not btc_df.empty:
        print(f"Data fetched successfully. Shape: {btc_df.shape}")
        
        # 2. 백테스트 실행
        print("Running backtest...")
        backtest_result = run_backtest(btc_df, leverage=leverage_to_test)
        
        if backtest_result:
            print("\n--- Backtest Result ---")
            print(f"Final Capital: ${backtest_result['final_capital']:,.2f}")
            print(f"Total Profit: ${backtest_result['total_profit']:,.2f}")
            print(f"Profit Percentage: {backtest_result['profit_pct']:.2f}%")
            print(f"Total Trades: {backtest_result['trade_count']}")
            print(f"Win Rate: {backtest_result['win_rate']:.2f}%")
            print(f"Max Drawdown: {backtest_result['mdd_pct']:.2f}%")
            print("-----------------------")
        else:
            print("Backtest failed to produce results.")
    else:
        print("Could not fetch data, aborting test.")