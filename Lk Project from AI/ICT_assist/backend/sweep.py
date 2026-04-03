import ccxt.async_support as ccxt
import pandas as pd
import asyncio
from datetime import datetime

# Configuration
TIMEFRAMES = ['1h', '4h']
LOOKBACK_PERIODS = [60, 120, 300]
TOP_N_TICKERS = 500

async def get_top_tickers(exchange, limit=TOP_N_TICKERS):
    try:
        # 1. Get all tickers to identify USDT pairs
        tickers = await exchange.fetch_tickers()
        usdt_symbols = []
        for symbol, ticker in tickers.items():
            if (symbol.endswith(':USDT') or symbol.endswith('/USDT')):
                usdt_symbols.append(symbol)
        
        print(f"Found {len(usdt_symbols)} USDT pairs. Calculating 30-day volume...")

        # 2. Fetch 30-day volume for each pair
        # We need a separate function to fetch volume to use with asyncio.gather
        async def fetch_30d_volume(symbol):
            try:
                # Fetch daily candles, limit 30
                ohlcv = await exchange.fetch_ohlcv(symbol, '1d', limit=30)
                if not ohlcv:
                    return (symbol, 0)
                
                # Calculate approx quote volume: sum(base_volume * close_price)
                # ohlcv structure: [timestamp, open, high, low, close, volume]
                total_volume = sum(candle[5] * candle[4] for candle in ohlcv)
                return (symbol, total_volume)
            except Exception:
                return (symbol, 0)

        # Use semaphore to limit concurrency
        sem = asyncio.Semaphore(50)
        async def bounded_fetch_volume(symbol):
            async with sem:
                return await fetch_30d_volume(symbol)

        tasks = [bounded_fetch_volume(sym) for sym in usdt_symbols]
        volume_data = await asyncio.gather(*tasks)

    # 3. Sort by calculated volume
        sorted_data = sorted(volume_data, key=lambda x: x[1], reverse=True)
        
        # 4. Return top N symbols with volume
        # Return a dict {ticker: volume} for easy lookup
        top_tickers_map = {item[0]: item[1] for item in sorted_data[:limit]}
        print(f"Selected top {len(top_tickers_map)} tickers by 30-day volume.")
        return top_tickers_map

    except Exception as e:
        print(f"Error fetching top tickers: {e}")
        return {}

async def fetch_ohlcv(exchange, symbol, timeframe, limit):
    try:
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df
    except Exception as e:
        # print(f"Error fetching {symbol} {timeframe}: {e}")
        return None

def check_sweeps(df, symbol, timeframe, volume=0):
    sweeps = []
    if df is None or len(df) < max(LOOKBACK_PERIODS) + 2:
        return sweeps

    # Use the last COMPLETED candle (iloc[-2])
    # iloc[-1] is the current forming candle (which we ignore per user request)
    target_candle = df.iloc[-2]
    
    # We compare against the N candles BEFORE the target candle
    # Range: [target_index - period : target_index]
    # target_index is -2. So we look at slice [-(period+2) : -2]
    
    # Iterate in DESCENDING order (120 -> 60 -> 30)
    # If we find a sweep for a longer period, we don't need to report shorter periods
    # because the longer period implies the shorter one (subset).
    sorted_periods = sorted(LOOKBACK_PERIODS, reverse=True)
    
    found_high = False
    found_low = False
    
    for period in sorted_periods:
        start_idx = -(period + 2)
        end_idx = -2
        
        prev_candles = df.iloc[start_idx:end_idx]
        
        if len(prev_candles) < period:
            continue
            
        max_high = prev_candles['high'].max()
        min_low = prev_candles['low'].min()
        
        # Check High Sweep
        if not found_high and target_candle['high'] > max_high:
            sweeps.append({
                'ticker': symbol,
                'timeframe': timeframe,
                'time': target_candle['timestamp'].isoformat(),
                'type': 'High Sweep',
                'period': period,
                'price': target_candle['high'],
                'details': f"Sweep {period} High",
                'ticker_volume': volume
            })
            found_high = True # Stop checking shorter periods for High Sweep
            
        # Check Low Sweep
        if not found_low and target_candle['low'] < min_low:
            sweeps.append({
                'ticker': symbol,
                'timeframe': timeframe,
                'time': target_candle['timestamp'].isoformat(),
                'type': 'Low Sweep',
                'period': period,
                'price': target_candle['low'],
                'details': f"Sweep {period} Low",
                'ticker_volume': volume
            })
            found_low = True # Stop checking shorter periods for Low Sweep
            
    return sweeps

async def get_all_sweeps():
    exchange = ccxt.binanceusdm()
    all_sweeps = []
    
    try:
        # 1. Fetch Top Tickers
        tickers_map = await get_top_tickers(exchange)
        tickers = list(tickers_map.keys())
        print(f"Analyzing top {len(tickers)} tickers...")
        
        # 2. Create Tasks
        tasks = []
        limit = 350 # Enough for max lookback (300) + buffer
        
        # Semaphore to limit concurrent requests (avoid rate limits)
        sem = asyncio.Semaphore(50) 

        async def bounded_fetch(ticker, tf, vol):
            async with sem:
                return await fetch_and_analyze(exchange, ticker, tf, limit, vol)

        for ticker in tickers:
            volume = tickers_map.get(ticker, 0)
            for tf in TIMEFRAMES:
                tasks.append(bounded_fetch(ticker, tf, volume))
        
        # 3. Execute
        results = await asyncio.gather(*tasks)
        
        for res in results:
            if res:
                all_sweeps.extend(res)
                
    finally:
        await exchange.close()
        
    return all_sweeps

async def fetch_and_analyze(exchange, symbol, timeframe, limit, volume):
    df = await fetch_ohlcv(exchange, symbol, timeframe, limit)
    return check_sweeps(df, symbol, timeframe, volume)

if __name__ == "__main__":
    # Test run
    loop = asyncio.get_event_loop()
    start_time = datetime.now()
    sweeps = loop.run_until_complete(get_all_sweeps())
    end_time = datetime.now()
    print(f"Found {len(sweeps)} sweeps in {end_time - start_time}")
    # for s in sweeps[:5]:
    #     print(s)
