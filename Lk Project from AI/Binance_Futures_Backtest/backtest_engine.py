import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime

# ==========================================
# Configuration
# ==========================================
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "OHLCV", "Binance_Fureres_USDT_1Hour_ohlcv")
INITIAL_BALANCE = 10000  # USDT
LEVERAGES = [1, 5, 10, 20]  # Standard backtest without leverage first to verify logic, or simulate 1x
RISK_PER_TRADE = 0.10  # 10% of equity per trade (as per request)
STOP_LOSS_MULTIPLIER = 2.0  # ATR multiplier
TAKE_PROFIT_MULTIPLIER = 4.0  # ATR multiplier (1:2 Risk:Reward)
FEE_RATE = 0.0004  # Binance Futures Taker Fee (0.04%)

# Strategy Parameters
EMA_PERIOD = 200
RSI_PERIOD = 14
ADX_PERIOD = 14
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
ATR_PERIOD = 14
VOL_OSC_SHORT = 5
VOL_OSC_LONG = 10
DISPARITY_PERIODS = [5, 10, 20, 60]

# ==========================================
# Indicator Functions
# ==========================================
def calculate_indicators(df):
    df = df.copy()
    
    # 1. EMA 200
    df['EMA_200'] = df['Close'].ewm(span=EMA_PERIOD, adjust=False).mean()
    
    # 2. Ichimoku Cloud
    # Conversion Line (Tenkan-sen): (9-period high + 9-period low) / 2
    nine_period_high = df['High'].rolling(window=9).max()
    nine_period_low = df['Low'].rolling(window=9).min()
    df['Tenkan_sen'] = (nine_period_high + nine_period_low) / 2
    
    # Base Line (Kijun-sen): (26-period high + 26-period low) / 2
    twenty_six_period_high = df['High'].rolling(window=26).max()
    twenty_six_period_low = df['Low'].rolling(window=26).min()
    df['Kijun_sen'] = (twenty_six_period_high + twenty_six_period_low) / 2
    
    # Leading Span A (Senkou Span A): (Conversion Line + Base Line) / 2
    df['Senkou_Span_A'] = ((df['Tenkan_sen'] + df['Kijun_sen']) / 2).shift(26)
    
    # Leading Span B (Senkou Span B): (52-period high + 52-period low) / 2
    fifty_two_period_high = df['High'].rolling(window=52).max()
    fifty_two_period_low = df['Low'].rolling(window=52).min()
    df['Senkou_Span_B'] = ((fifty_two_period_high + fifty_two_period_low) / 2).shift(26)
    
    # 3. ADX
    # True Range
    df['H-L'] = df['High'] - df['Low']
    df['H-PC'] = abs(df['High'] - df['Close'].shift(1))
    df['L-PC'] = abs(df['Low'] - df['Close'].shift(1))
    df['TR'] = df[['H-L', 'H-PC', 'L-PC']].max(axis=1)
    
    # DM
    df['UpMove'] = df['High'] - df['High'].shift(1)
    df['DownMove'] = df['Low'].shift(1) - df['Low']
    df['PlusDM'] = np.where((df['UpMove'] > df['DownMove']) & (df['UpMove'] > 0), df['UpMove'], 0)
    df['MinusDM'] = np.where((df['DownMove'] > df['UpMove']) & (df['DownMove'] > 0), df['DownMove'], 0)
    
    # Smoothed
    df['TR14'] = df['TR'].rolling(window=ADX_PERIOD).sum() # Simplified smoothing for backtest speed
    df['PlusDI14'] = 100 * (df['PlusDM'].rolling(window=ADX_PERIOD).sum() / df['TR14'])
    df['MinusDI14'] = 100 * (df['MinusDM'].rolling(window=ADX_PERIOD).sum() / df['TR14'])
    df['DX'] = 100 * abs(df['PlusDI14'] - df['MinusDI14']) / (df['PlusDI14'] + df['MinusDI14'])
    df['ADX'] = df['DX'].rolling(window=ADX_PERIOD).mean()
    
    # 4. MACD
    exp1 = df['Close'].ewm(span=MACD_FAST, adjust=False).mean()
    exp2 = df['Close'].ewm(span=MACD_SLOW, adjust=False).mean()
    df['MACD'] = exp1 - exp2
    df['Signal_Line'] = df['MACD'].ewm(span=MACD_SIGNAL, adjust=False).mean()
    
    # 5. Volume Oscillator
    # (Short MA - Long MA) / Long MA * 100
    vol_short = df['Volume'].rolling(window=VOL_OSC_SHORT).mean()
    vol_long = df['Volume'].rolling(window=VOL_OSC_LONG).mean()
    df['Vol_Osc'] = ((vol_short - vol_long) / vol_long) * 100
    
    # 6. ATR
    df['ATR'] = df['TR'].rolling(window=ATR_PERIOD).mean()
    
    # 7. Price Disparity (Jong-I-Gyeok)
    # Compare current close with close n periods ago
    for p in DISPARITY_PERIODS:
        df[f'Close_Lag_{p}'] = df['Close'].shift(p)
        
    return df

# ==========================================
# Backtest Engine
# ==========================================
import sqlite3

# ... existing code ...

# ==========================================
# Backtest Engine
# ==========================================
def run_backtest_for_leverage(df, leverage):
    # This runs the strategy logic on a prepared DF for a specific leverage
    balance = INITIAL_BALANCE
    position = None # None, 'LONG', 'SHORT'
    entry_price = 0.0
    sl_price = 0.0
    tp_price = 0.0
    trade_log = []
    
    # Optimize: Convert columns to numpy arrays for fast indexing
    # Prices
    opens = df['Open'].values
    highs = df['High'].values
    lows = df['Low'].values
    closes = df['Close'].values
    timestamps = df['Timestamp'].values
    
    # Indicators
    ema_200 = df['EMA_200'].values
    senkou_a = df['Senkou_Span_A'].values
    senkou_b = df['Senkou_Span_B'].values
    adx = df['ADX'].values
    atr = df['ATR'].values
    vol_osc = df['Vol_Osc'].values
    
    # MACD components for crossover check
    macd = df['MACD'].values
    signal_line = df['Signal_Line'].values
    
    # Pre-calculate boolean conditions where possible to save time in loop
    # Disparity columns
    disparity_cols = [f'Close_Lag_{p}' for p in DISPARITY_PERIODS]
    disparity_values = [df[col].values for col in disparity_cols]
    
    # Loop
    # Start at 1 because we check i-1
    for i in range(1, len(df)):
        # Calculate conditions on the fly using array access
        
        # Check Exit Conditions first
        if position == 'LONG':
            # Stop Loss
            if lows[i] <= sl_price:
                exit_price = sl_price # Assume filled at SL
                raw_pnl_pct = (exit_price - entry_price) / entry_price 
                leveraged_pnl_pct = raw_pnl_pct * leverage
                
                margin_used = balance * RISK_PER_TRADE
                pnl_amount = margin_used * leveraged_pnl_pct
                
                balance += pnl_amount
                trade_log.append({'Type': 'Sell (SL)', 'Price': exit_price, 'PnL': leveraged_pnl_pct*100, 'Balance': balance, 'Win': False})
                position = None
            # Take Profit
            elif highs[i] >= tp_price:
                exit_price = tp_price
                raw_pnl_pct = (exit_price - entry_price) / entry_price
                leveraged_pnl_pct = raw_pnl_pct * leverage
                
                margin_used = balance * RISK_PER_TRADE
                pnl_amount = margin_used * leveraged_pnl_pct
                
                balance += pnl_amount
                trade_log.append({'Type': 'Sell (TP)', 'Price': exit_price, 'PnL': leveraged_pnl_pct*100, 'Balance': balance, 'Win': True})
                position = None
                
        elif position == 'SHORT':
            # Stop Loss
            if highs[i] >= sl_price:
                exit_price = sl_price
                raw_pnl_pct = (entry_price - exit_price) / entry_price
                leveraged_pnl_pct = raw_pnl_pct * leverage
                
                margin_used = balance * RISK_PER_TRADE
                pnl_amount = margin_used * leveraged_pnl_pct
                
                balance += pnl_amount
                trade_log.append({'Type': 'Cover (SL)', 'Price': exit_price, 'PnL': leveraged_pnl_pct*100, 'Balance': balance, 'Win': False})
                position = None
            # Take Profit
            elif lows[i] <= tp_price:
                exit_price = tp_price
                raw_pnl_pct = (entry_price - exit_price) / entry_price
                leveraged_pnl_pct = raw_pnl_pct * leverage
                
                margin_used = balance * RISK_PER_TRADE
                pnl_amount = margin_used * leveraged_pnl_pct
                
                balance += pnl_amount
                trade_log.append({'Type': 'Cover (TP)', 'Price': exit_price, 'PnL': leveraged_pnl_pct*100, 'Balance': balance, 'Win': True})
                position = None

        # Check Entry Conditions (if no position)
        if position is None:
            if adx[i] < 25:
                continue
            if vol_osc[i] <= 0:
                continue
            
            curr_close = closes[i]
            curr_ema = ema_200[i]
            curr_span_a = senkou_a[i]
            curr_span_b = senkou_b[i]
            
            is_above_ema = curr_close > curr_ema
            is_above_cloud = curr_close > max(curr_span_a, curr_span_b)
            is_macd_gold = macd[i-1] < signal_line[i-1] and macd[i] > signal_line[i]
            
            is_disparity_up = True
            for d_vals in disparity_values:
                if curr_close <= d_vals[i]:
                    is_disparity_up = False
                    break
            
            if is_above_ema and is_above_cloud and is_macd_gold and is_disparity_up:
                position = 'LONG'
                entry_price = curr_close
                trade_atr = atr[i]
                sl_price = entry_price - (trade_atr * STOP_LOSS_MULTIPLIER)
                tp_price = entry_price + (trade_atr * TAKE_PROFIT_MULTIPLIER)
                trade_log.append({'Type': 'Buy', 'Price': entry_price, 'Time': timestamps[i]})
                continue 
                
            is_below_ema = curr_close < curr_ema
            is_below_cloud = curr_close < min(curr_span_a, curr_span_b)
            is_macd_dead = macd[i-1] > signal_line[i-1] and macd[i] < signal_line[i]
            
            is_disparity_down = True
            for d_vals in disparity_values:
                if curr_close >= d_vals[i]:
                    is_disparity_down = False
                    break
            
            if is_below_ema and is_below_cloud and is_macd_dead and is_disparity_down:
                position = 'SHORT'
                entry_price = curr_close
                trade_atr = atr[i]
                sl_price = entry_price + (trade_atr * STOP_LOSS_MULTIPLIER)
                tp_price = entry_price - (trade_atr * TAKE_PROFIT_MULTIPLIER)
                trade_log.append({'Type': 'Short', 'Price': entry_price, 'Time': timestamps[i]})

    # Summary
    total_trades = len([t for t in trade_log if 'PnL' in t])
    wins = len([t for t in trade_log if 'Win' in t and t['Win'] is True])
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
    total_return = (balance - INITIAL_BALANCE) / INITIAL_BALANCE * 100
    
    # Calculate Risk/Reward Ratio (approximate from params)
    rr_ratio = TAKE_PROFIT_MULTIPLIER / STOP_LOSS_MULTIPLIER

    return {
        'Leverage': leverage,
        'Total Return': total_return,
        'Win Rate': win_rate,
        'Profit/Loss Ratio': rr_ratio, # Fixed based on strategy params
        'Trades': total_trades,
        'Final Balance': balance,
        'Stop Loss': STOP_LOSS_MULTIPLIER,
        'Take Profit': TAKE_PROFIT_MULTIPLIER,
        'Trailing Stop': 0 # Not implemented in this simple engine
    }

# ==========================================
# Configuration
# ==========================================
# Use absolute path or relative to script to ensure it works when called from elsewhere
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, 'OHLCV', 'Binance_Fureres_USDT_1Hour_ohlcv')

INITIAL_BALANCE = 1000  # USDT
LEVERAGES = [5] # Default to 5x as requested for optimization
RISK_PER_TRADE = 0.10
STOP_LOSS_MULTIPLIER = 2.0  
TAKE_PROFIT_MULTIPLIER = 4.0  
FEE_RATE = 0.0004 

def run_backtest_file(file_path, target_leverage=5):
    try:
        df = pd.read_csv(file_path)
        df.columns = [col.strip().capitalize() for col in df.columns] 
        if 'Open time' in df.columns:
            df.rename(columns={'Open time': 'Timestamp'}, inplace=True)
        
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        df.sort_values('Timestamp', inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        # FILTER: Last 2 Years Only
        if len(df) > 0:
            last_date = df['Timestamp'].iloc[-1]
            start_date = last_date - pd.Timedelta(days=730) 
            df = df[df['Timestamp'] >= start_date]
            df.reset_index(drop=True, inplace=True)
            
        if len(df) < 200: 
            return None
        
        df = calculate_indicators(df)
        df.dropna(inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        res = run_backtest_for_leverage(df, target_leverage)
        res['Ticker'] = os.path.basename(file_path).replace('.csv', '')
        return res
            
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

def save_to_db(results):
    if not results:
        return
        
    db_filename = f"result_{datetime.now().strftime('%Y-%m-%d_%H%M')}.db"
    db_path = os.path.join(os.path.dirname(__file__), db_filename)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS backtest_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Ticker TEXT,
            Return REAL,
            Profit_Loss_Ratio REAL,
            Win_Rate REAL,
            Stop_Loss REAL,
            Take_Profit REAL,
            Trailing_Stop REAL,
            Trades INTEGER,
            Final_Balance REAL
        )
    ''')
    
    for res in results:
        cursor.execute('''
            INSERT INTO backtest_results (Ticker, Return, Profit_Loss_Ratio, Win_Rate, Stop_Loss, Take_Profit, Trailing_Stop, Trades, Final_Balance)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            res['Ticker'],
            res['Total Return'],
            res['Profit/Loss Ratio'],
            res['Win Rate'],
            res['Stop Loss'],
            res['Take Profit'],
            res['Trailing Stop'],
            res['Trades'],
            res['Final Balance']
        ))
        
    conn.commit()
    conn.close()
    print(f"Saved {len(results)} results to {db_path}")

def run_analysis(leverage=5, top_n=5):
    print(f"Starting analysis with Leverage {leverage}x...")
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    if not files:
        print(f"No data files found in {DATA_DIR}")
        return []

    all_results = []
    
    for i, file in enumerate(files):
        res = run_backtest_file(file, target_leverage=leverage)
        if res and res['Trades'] >= 10: # Minimum 10 trades to be considered
            all_results.append(res)
            
        if (i+1) % 10 == 0:
            print(f"Processed {i+1}/{len(files)} files...", end='\r')
            
    print(f"\nProcessed {len(files)} files. Found {len(all_results)} valid candidates.")
    
    # Sort by Total Return
    all_results.sort(key=lambda x: x['Total Return'], reverse=True)
    
    # Save to DB
    save_to_db(all_results)
    
    # Select Top N
    # Ticker from filename is likely 'ZRXUSDT'. We need 'ZRX/USDT:USDT'
    # Or if formatting is 'ZRX', then 'ZRX/USDT:USDT'.
    # Safest is to remove 'USDT' from end if present, then rebuild.
    top_tickers = []
    for res in all_results[:top_n]:
        raw_ticker = res['Ticker'] # e.g. "ZRXUSDT"
        if raw_ticker.endswith('USDT'):
            base = raw_ticker[:-4]
        else:
            base = raw_ticker
            
        formatted = f"{base}/USDT:USDT"
        top_tickers.append(formatted)
    
    print(f"Top {top_n} Tickers: {top_tickers}")
    return top_tickers

if __name__ == "__main__":
    run_analysis()
