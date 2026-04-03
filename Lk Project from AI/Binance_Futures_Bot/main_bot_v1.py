import json
import subprocess
import time
import os
import logging
import sqlite3
from datetime import datetime, timedelta

import ccxt
import pandas as pd
import numpy as np
from dotenv import load_dotenv

# ==========================================
# Configuration & Setup
# ==========================================
# Load .env from the same directory as this script
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

API_KEY = os.getenv("BINANCE_API_KEY")
SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

# Trading Settings (Load from .env with Fallbacks)
TIMEFRAME = os.getenv("TIMEFRAME", '1h')
LEVERAGE = int(os.getenv("LEVERAGE", 5))
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", 0.10))  # 10% of equity default
STOP_LOSS_MULTIPLIER = float(os.getenv("STOP_LOSS_MULTIPLIER", 3.0))
TAKE_PROFIT_MULTIPLIER = float(os.getenv("TAKE_PROFIT_MULTIPLIER", 6.0))

# Dynamic Filename generation for Version Control
# If script is named 'main_bot_v2.py', logs will be 'trading_bot_v2.log' and DB 'trades_v2.db'
script_name = os.path.splitext(os.path.basename(__file__))[0]
version_suffix = script_name.replace('main_bot', '') # e.g. "_v2" or ""

LOG_FILE = f'trading_bot{version_suffix}.log'
DB_FILE = os.path.join(os.path.dirname(__file__), f'trades{version_suffix}.db')

def retry_db_action(action_func, max_retries=5, delay=2.0):
    for attempt in range(max_retries):
        try:
            return action_func()
        except sqlite3.OperationalError as e:
            if "locked" in str(e) or "unable to open database file" in str(e):
                logger.warning(f"DB busy/locked, retrying ({attempt+1}/{max_retries})...")
                time.sleep(delay)
            else:
                raise e
        except Exception as e:
            logger.error(f"DB Action failed: {e}")
            raise e
    raise Exception("Max retries exceeded for DB action")

def init_db():
    def _init():
        db_path = os.path.abspath(DB_FILE)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                symbol TEXT,
                side TEXT,
                type TEXT,
                price REAL,
                amount REAL,
                status TEXT
            )
        ''')
        conn.commit()
        conn.close()

    try:
        retry_db_action(_init)
    except Exception as e:
        logger.error(f"Failed to init DB after retries: {e}")

def log_trade_to_db(trade_data):
    def _log():
        db_path = os.path.abspath(DB_FILE)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO trades (timestamp, symbol, side, type, price, amount, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            trade_data['timestamp'],
            trade_data['symbol'],
            trade_data['side'],
            trade_data['type'],
            trade_data['price'],
            trade_data['amount'],
            trade_data['status']
        ))
        conn.commit()
        conn.close()

    try:
        retry_db_action(_log)
    except Exception as e:
        logger.error(f"Failed to log trade to DB: {e}")

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

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), LOG_FILE)),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# Target Tickers Management
TARGET_TICKERS_FILE = os.path.join(os.path.dirname(__file__), 'target_tickers.json')
WEEKLY_UPDATE_SCRIPT = os.path.join(os.path.dirname(__file__), 'weekly_update.py')

def load_target_tickers():
    try:
        if os.path.exists(TARGET_TICKERS_FILE):
            with open(TARGET_TICKERS_FILE, 'r') as f:
                tickers = json.load(f)
                return tickers
    except Exception as e:
        logger.error(f"Error loading target tickers: {e}")
    
    # Fallback to default if load fails
    return ['ZRX/USDT:USDT', 'WLD/USDT:USDT', 'DOGE/USDT:USDT', 'HIFI/USDT:USDT', '1000CAT/USDT:USDT']

TARGET_TICKERS = load_target_tickers()

# Initialize Exchange
exchange = ccxt.binance({
    'apiKey': API_KEY,
    'secret': SECRET_KEY,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'future',
    }
})

# ==========================================
# Indicator Logic (Same as Backtest)
# ==========================================
def calculate_indicators(df):
    df = df.copy()
    # 1. EMA 200
    df['EMA_200'] = df['Close'].ewm(span=EMA_PERIOD, adjust=False).mean()
    
    # 2. Ichimoku
    nine_period_high = df['High'].rolling(window=9).max()
    nine_period_low = df['Low'].rolling(window=9).min()
    df['Tenkan_sen'] = (nine_period_high + nine_period_low) / 2
    
    twenty_six_period_high = df['High'].rolling(window=26).max()
    twenty_six_period_low = df['Low'].rolling(window=26).min()
    df['Kijun_sen'] = (twenty_six_period_high + twenty_six_period_low) / 2
    
    df['Senkou_Span_A'] = ((df['Tenkan_sen'] + df['Kijun_sen']) / 2).shift(26)
    
    fifty_two_period_high = df['High'].rolling(window=52).max()
    fifty_two_period_low = df['Low'].rolling(window=52).min()
    df['Senkou_Span_B'] = ((fifty_two_period_high + fifty_two_period_low) / 2).shift(26)
    
    # 3. ADX
    df['H-L'] = df['High'] - df['Low']
    df['H-PC'] = abs(df['High'] - df['Close'].shift(1))
    df['L-PC'] = abs(df['Low'] - df['Close'].shift(1))
    df['TR'] = df[['H-L', 'H-PC', 'L-PC']].max(axis=1)
    
    df['UpMove'] = df['High'] - df['High'].shift(1)
    df['DownMove'] = df['Low'].shift(1) - df['Low']
    df['PlusDM'] = np.where((df['UpMove'] > df['DownMove']) & (df['UpMove'] > 0), df['UpMove'], 0)
    df['MinusDM'] = np.where((df['DownMove'] > df['UpMove']) & (df['DownMove'] > 0), df['DownMove'], 0)
    
    df['TR14'] = df['TR'].rolling(window=ADX_PERIOD).sum()
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
    vol_short = df['Volume'].rolling(window=VOL_OSC_SHORT).mean()
    vol_long = df['Volume'].rolling(window=VOL_OSC_LONG).mean()
    df['Vol_Osc'] = ((vol_short - vol_long) / vol_long) * 100
    
    # 6. ATR
    df['ATR'] = df['TR'].rolling(window=ATR_PERIOD).mean()
    
    # 7. Price Disparity
    for p in DISPARITY_PERIODS:
        df[f'Close_Lag_{p}'] = df['Close'].shift(p)
        
    return df

# ==========================================
# Trading Logic
# ==========================================
def fetch_ohlcv(symbol):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=300)
        df = pd.DataFrame(ohlcv, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms')
        return df
    except Exception as e:
        logger.error(f"Error fetching data for {symbol}: {e}")
        return None

def check_signal(df):
    if len(df) < 200:
        return None
        
    curr = df.iloc[-2]  # Use completed candle
    prev = df.iloc[-3]  # Use previous completed candle
    
    # Common Conditions
    is_adx_strong = curr['ADX'] >= 25
    is_vol_active = curr['Vol_Osc'] > 0
    
    # LONG
    is_above_ema = curr['Close'] > curr['EMA_200']
    is_above_cloud = curr['Close'] > max(curr['Senkou_Span_A'], curr['Senkou_Span_B'])
    is_macd_gold = prev['MACD'] < prev['Signal_Line'] and curr['MACD'] > curr['Signal_Line']
    is_disparity_up = all(curr['Close'] > curr[f'Close_Lag_{p}'] for p in DISPARITY_PERIODS)
    
    if is_above_ema and is_above_cloud and is_adx_strong and is_macd_gold and is_vol_active and is_disparity_up:
        return 'LONG'
        
    # SHORT
    is_below_ema = curr['Close'] < curr['EMA_200']
    is_below_cloud = curr['Close'] < min(curr['Senkou_Span_A'], curr['Senkou_Span_B'])
    is_macd_dead = prev['MACD'] > prev['Signal_Line'] and curr['MACD'] < curr['Signal_Line']
    is_disparity_down = all(curr['Close'] < curr[f'Close_Lag_{p}'] for p in DISPARITY_PERIODS)
    
    if is_below_ema and is_below_cloud and is_adx_strong and is_macd_dead and is_vol_active and is_disparity_down:
        return 'SHORT'
        
    return None

def execute_trade(symbol, signal, atr):
    try:
        balance = exchange.fetch_balance()['USDT']['free']
        current_price = exchange.fetch_ticker(symbol)['last']
        
        # Position Size (10% of Equity)
        amount_usdt = balance * RISK_PER_TRADE
        amount = amount_usdt / current_price
        
        # Set Leverage
        exchange.set_leverage(LEVERAGE, symbol)
        
        if signal == 'LONG':
            sl_price = current_price - (atr * STOP_LOSS_MULTIPLIER)
            tp_price = current_price + (atr * TAKE_PROFIT_MULTIPLIER)
            
            # Place Entry
            order = exchange.create_market_buy_order(symbol, amount)
            logger.info(f"LONG Order Placed for {symbol} at {current_price}")
            log_trade_to_db({
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'symbol': symbol,
                'side': 'buy',
                'type': 'market',
                'price': current_price,
                'amount': amount,
                'status': 'ENTRY_LONG'
            })
            
            # Place SL/TP
            # Use closePosition=True for robustness (closes entire position, no amount needed)
            common_params = {
                'closePosition': True,
                'workingType': 'MARK_PRICE', # Important for Stop Price triggering
                'timeInForce': 'GTC'         # Good Till Cancel
            }
            
            try:
                sl_params = common_params.copy()
                sl_params['stopPrice'] = sl_price
                exchange.create_order(symbol, 'STOP_MARKET', 'sell', None, params=sl_params)
                logger.info(f"SL Placed for {symbol} at {sl_price}")
                log_trade_to_db({
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'symbol': symbol,
                    'side': 'sell',
                    'type': 'stop_market',
                    'price': sl_price,
                    'amount': 0,
                    'status': 'SL_LONG'
                })
            except Exception as e:
                logger.error(f"Failed to place SL for {symbol}: {e}")

            try:
                tp_params = common_params.copy()
                tp_params['stopPrice'] = tp_price
                exchange.create_order(symbol, 'TAKE_PROFIT_MARKET', 'sell', None, params=tp_params)
                logger.info(f"TP Placed for {symbol} at {tp_price}")
                log_trade_to_db({
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'symbol': symbol,
                    'side': 'sell',
                    'type': 'take_profit',
                    'price': tp_price,
                    'amount': 0,
                    'status': 'TP_LONG'
                })
            except Exception as e:
                logger.error(f"Failed to place TP for {symbol}: {e}")
            
        elif signal == 'SHORT':
            sl_price = current_price + (atr * STOP_LOSS_MULTIPLIER)
            tp_price = current_price - (atr * TAKE_PROFIT_MULTIPLIER)
            
            order = exchange.create_market_sell_order(symbol, amount)
            logger.info(f"SHORT Order Placed for {symbol} at {current_price}")
            log_trade_to_db({
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'symbol': symbol,
                'side': 'sell',
                'type': 'market',
                'price': current_price,
                'amount': amount,
                'status': 'ENTRY_SHORT'
            })
            
            common_params = {
                'closePosition': True,
                'workingType': 'MARK_PRICE',
                'timeInForce': 'GTC'
            }
            
            try:
                sl_params = common_params.copy()
                sl_params['stopPrice'] = sl_price
                exchange.create_order(symbol, 'STOP_MARKET', 'buy', None, params=sl_params)
                logger.info(f"SL Placed for {symbol} at {sl_price}")
                log_trade_to_db({
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'symbol': symbol,
                    'side': 'buy',
                    'type': 'stop_market',
                    'price': sl_price,
                    'amount': 0,
                    'status': 'SL_SHORT'
                })
            except Exception as e:
                logger.error(f"Failed to place SL for {symbol}: {e}")

            try:
                tp_params = common_params.copy()
                tp_params['stopPrice'] = tp_price
                exchange.create_order(symbol, 'TAKE_PROFIT_MARKET', 'buy', None, params=tp_params)
                logger.info(f"TP Placed for {symbol} at {tp_price}")
                log_trade_to_db({
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'symbol': symbol,
                    'side': 'buy',
                    'type': 'take_profit',
                    'price': tp_price,
                    'amount': 0,
                    'status': 'TP_SHORT'
                })
            except Exception as e:
                logger.error(f"Failed to place TP for {symbol}: {e}")
            
    except Exception as e:
        logger.error(f"Error executing trade for {symbol}: {e}")

def get_next_weekly_update_time():
    now = datetime.now()
    # Monday is 0 (weekday)
    days_ahead = 0 - now.weekday()
    if days_ahead <= 0: # Target day already happened this week
        days_ahead += 7
        
    next_monday = now + timedelta(days=days_ahead)
    target_time = next_monday.replace(hour=1, minute=30, second=0, microsecond=0)
    
    # If today is Monday and it's before 01:30, the calculation above puts it to next Monday (-0 + 7).
    # We need to correct if today is Monday but before 01:30.
    if now.weekday() == 0 and now.hour < 1 and now.minute < 30:
        target_time = now.replace(hour=1, minute=30, second=0, microsecond=0)
        
    # If calculate timestamp is in the past (e.g. it's Monday 02:00), ensure it's next week
    if target_time <= now:
        target_time += timedelta(days=7)
        
    return target_time

def run_bot():
    logger.info("Bot Started...")
    init_db() # Initialize DB on start
    last_traded_candles = {} # Track last traded candle timestamp for each symbol
    
    # Reload tickers at start
    global TARGET_TICKERS
    TARGET_TICKERS = load_target_tickers()
    logger.info(f"Loaded Target Tickers: {TARGET_TICKERS}")

    while True:
        now = datetime.now()
        
        # 1. Calculate Next Hourly Check (XX:00:02)
        next_trade_time = now.replace(minute=0, second=2, microsecond=0)
        if next_trade_time <= now:
            next_trade_time += timedelta(hours=1)
            
        # 2. Calculate Next Weekly Update (Monday 01:30:00)
        next_update_time = get_next_weekly_update_time()
        
        # 3. Determine which event is first
        if next_update_time < next_trade_time:
            # Sleep until Update time
            sleep_seconds = (next_update_time - now).total_seconds()
            logger.info(f"Waiting {sleep_seconds:.2f}s for Weekly Update at {next_update_time}")
            time.sleep(sleep_seconds)
            
            # Run Update
            logger.info("Starting Weekly Update (Subprocess)...")
            try:
                subprocess.Popen(["python", WEEKLY_UPDATE_SCRIPT])
                # We don't wait() here to avoid blocking, but we sleep a bit to let it start
                # Actually, if we just continue loop, we will sleep until next_trade_time
            except Exception as e:
                logger.error(f"Failed to start weekly update: {e}")
                
            # After triggering update, we loop again. 
            # The next iteration will find next_update_time is now 7 days away, so it will target next_trade_time.
            continue
            
        else:
            # Sleep until Trade time
            sleep_seconds = (next_trade_time - now).total_seconds()
            logger.info(f"Waiting {sleep_seconds:.2f}s for Ticker Check at {next_trade_time}")
            time.sleep(sleep_seconds)

        # --- EXECUTE HOURLY CHECK ---
        logger.info("Starting hourly ticker check...")
        
        # Reload tickers before checking (in case update finished recently)
        TARGET_TICKERS = load_target_tickers()
        
        trade_executed = False # Track if any trade occurred

        for symbol in TARGET_TICKERS:
            try:
                # Check if we already have a position
                positions = exchange.fetch_positions([symbol])
                has_position = False
                for p in positions:
                    if float(p['contracts']) > 0:
                        has_position = True
                        break
                
                if has_position:
                    continue # Skip if already in trade
                
                df = fetch_ohlcv(symbol)
                if df is not None:
                    df = calculate_indicators(df)
                    signal = check_signal(df)
                    
                    if signal:
                        candle_time = df.iloc[-2]['Timestamp']
                        
                        # Prevent re-entry on same candle
                        if symbol in last_traded_candles and last_traded_candles[symbol] == candle_time:
                            continue

                        logger.info(f"Signal Found for {symbol}: {signal} at {candle_time}")
                        atr = df.iloc[-2]['ATR'] # Use ATR from confirmed candle
                        execute_trade(symbol, signal, atr)
                        last_traded_candles[symbol] = candle_time
                        trade_executed = True # Signal found and processed
                        
            except Exception as e:
                logger.error(f"Error in loop for {symbol}: {e}")

        if not trade_executed:
            logger.info("No trading signals found for this hour.")

if __name__ == "__main__":
    run_bot()
