import ccxt
import pandas as pd
import time
import schedule
import logging
from datetime import datetime
import os
from dotenv import load_dotenv
import sqlite3
import json

# .env 파일 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("trading_bot.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("BinanceBot")

# 설정 변수
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")
# SYMBOL = os.getenv("SYMBOL", "XRP/USDT") # 단일 심볼 대신 JSON 파일 사용
TIMEFRAME = os.getenv("TIMEFRAME", "1h")
LEVERAGE = int(os.getenv("LEVERAGE", 1))
STAKE_AMOUNT = float(os.getenv("STAKE_AMOUNT", 0.5)) # 잔고의 50% 사용 (다중 티커 시 개별 티커당 비중 조절 필요)

# 다중 티커 로드
script_dir = os.path.dirname(os.path.abspath(__file__))
json_path = os.path.join(script_dir, "target_tickers.json")

try:
    with open(json_path, "r") as f:
        TARGET_TICKERS = json.load(f)
    logger.info(f"타겟 티커 로드 완료: {TARGET_TICKERS}")
except Exception as e:
    logger.error(f"target_tickers.json 로드 실패: {e}")
    TARGET_TICKERS = []

# 바이낸스 연결 초기화
try:
    binance = ccxt.binance({
        'apiKey': BINANCE_API_KEY,
        'secret': BINANCE_SECRET_KEY,
        'enableRateLimit': True,
        'options': {
            'defaultType': 'future' # 선물 거래 설정
        }
    })
except Exception as e:
    logger.error(f"바이낸스 연결 설정 중 오류 발생: {e}")
    exit()

# DB 초기화
def init_db():
    conn = sqlite3.connect('futures_trades.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trades
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  timestamp TEXT,
                  symbol TEXT,
                  side TEXT,
                  type TEXT,
                  price REAL,
                  amount REAL,
                  cost REAL,
                  reason TEXT,
                  pnl REAL,
                  status TEXT)''')
    conn.commit()
    return conn

# 거래 기록 저장
def log_trade(conn, trade_data):
    try:
        c = conn.cursor()
        c.execute("""INSERT INTO trades 
                     (timestamp, symbol, side, type, price, amount, cost, reason, pnl, status)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                  (trade_data['timestamp'], trade_data['symbol'], trade_data['side'], 
                   trade_data['type'], trade_data['price'], trade_data['amount'], 
                   trade_data['cost'], trade_data['reason'], trade_data['pnl'], trade_data['status']))
        conn.commit()
    except Exception as e:
        logger.error(f"DB 저장 중 오류: {e}")

# 보조지표 계산 함수
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

    df['senkou_span_a'] = ((df['tenkan_sen'] + df['kijun_sen']) / 2).shift(26) # 선행스팬 A
    
    high_52 = df['high'].rolling(window=52).max()
    low_52 = df['low'].rolling(window=52).min()
    df['senkou_span_b'] = ((high_52 + low_52) / 2).shift(26) # 선행스팬 B
    
    return df

# 현재 포지션 조회
def get_position(symbol):
    try:
        positions = binance.fetch_positions([symbol])
        for position in positions:
            if position['symbol'] == symbol:
                return position
        return None
    except Exception as e:
        logger.error(f"포지션 조회 실패 ({symbol}): {e}")
        return None

# 레버리지 설정
def set_leverage(symbol, leverage):
    try:
        binance.set_leverage(leverage, symbol)
        # logger.info(f"레버리지 설정 완료: {symbol} -> {leverage}x")
    except Exception as e:
        pass
        # logger.error(f"레버리지 설정 실패 ({symbol}): {e}") 

# 시장가 주문 실행
def execute_order(symbol, side, amount, params={}):
    try:
        order = binance.create_market_order(symbol, side, amount, params)
        logger.info(f"주문 실행 성공: {side} {amount} {symbol}")
        return order
    except Exception as e:
        logger.error(f"주문 실행 실패 ({symbol} {side}): {e}")
        return None

# 개별 티커 트레이딩 로직
def process_ticker(symbol, conn):
    try:
        # 레버리지 설정 (매번 확인하지 않고 필요시 에러 처리하거나 초기 1회만 하도록 최적화 가능)
        set_leverage(symbol, LEVERAGE)

        # 1. 시세 데이터 가져오기
        ohlcv = binance.fetch_ohlcv(symbol, TIMEFRAME, limit=100)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # 지표 계산
        df = calculate_indicators(df)
        
        curr = df.iloc[-1] # 현재 봉
        prev = df.iloc[-2] # 직전 확정 봉
        
        current_price = curr['close']
        
        # 2. 포지션 확인
        position = get_position(symbol)
        in_position = False
        position_amt = 0.0
        entry_price = 0.0
        side_pos = 'none'
        
        if position and float(position['contracts']) > 0:
            in_position = True
            position_amt = float(position['contracts'])
            entry_price = float(position['entryPrice'])
            side_pos = position['side'] # 'long' or 'short'
            logger.info(f"[{symbol}] 포지션 보유중: {side_pos.upper()}, 수량: {position_amt}, 진입가: {entry_price}, 현재가: {current_price}")
        else:
            logger.info(f"[{symbol}] 대기 중... 현재가: {current_price}")

        # 3. 매매 전략 판단 (Trend Following)
        cloud_top = max(prev['senkou_span_a'], prev['senkou_span_b'])
        cloud_bottom = min(prev['senkou_span_a'], prev['senkou_span_b'])
        
        # 롱 조건
        long_condition = (current_price > prev['SMA_20']) and \
                         (current_price > cloud_top) and \
                         (prev['tenkan_sen'] > prev['kijun_sen'])
        
        # 숏 조건
        short_condition = (current_price < prev['SMA_20']) and \
                          (current_price < cloud_bottom) and \
                          (prev['tenkan_sen'] < prev['kijun_sen'])

        # 4. 포지션 진입/청산 로직
        if in_position:
            # 청산 로직 (손절/익절 포함)
            if side_pos == 'long':
                pnl_pct = (current_price - entry_price) / entry_price * 100 * LEVERAGE
                if pnl_pct >= 6.0:
                    logger.info(f"[{symbol}] 롱 익절 조건 도달 (PNL: {pnl_pct:.2f}%)")
                    order = execute_order(symbol, 'sell', position_amt, {'reduceOnly': True})
                    if order: log_trade(conn, {'timestamp': datetime.now().isoformat(), 'symbol': symbol, 'side': 'sell', 'type': 'market', 'price': order['price'], 'amount': order['amount'], 'cost': order['cost'], 'reason': 'Takeprofit', 'pnl': pnl_pct, 'status': 'closed'})
                elif pnl_pct <= -3.0:
                    logger.info(f"[{symbol}] 롱 손절 조건 도달 (PNL: {pnl_pct:.2f}%)")
                    order = execute_order(symbol, 'sell', position_amt, {'reduceOnly': True})
                    if order: log_trade(conn, {'timestamp': datetime.now().isoformat(), 'symbol': symbol, 'side': 'sell', 'type': 'market', 'price': order['price'], 'amount': order['amount'], 'cost': order['cost'], 'reason': 'Stoploss', 'pnl': pnl_pct, 'status': 'closed'})
                elif short_condition:
                    logger.info(f"[{symbol}] 롱 포지션 종료 (반대 숏 신호 발생)")
                    order = execute_order(symbol, 'sell', position_amt, {'reduceOnly': True})
                    if order: log_trade(conn, {'timestamp': datetime.now().isoformat(), 'symbol': symbol, 'side': 'sell', 'type': 'market', 'price': order['price'], 'amount': order['amount'], 'cost': order['cost'], 'reason': 'Trend Reversal', 'pnl': pnl_pct, 'status': 'closed'})

            elif side_pos == 'short':
                pnl_pct = (entry_price - current_price) / entry_price * 100 * LEVERAGE
                if pnl_pct >= 6.0:
                    logger.info(f"[{symbol}] 숏 익절 조건 도달 (PNL: {pnl_pct:.2f}%)")
                    order = execute_order(symbol, 'buy', position_amt, {'reduceOnly': True})
                    if order: log_trade(conn, {'timestamp': datetime.now().isoformat(), 'symbol': symbol, 'side': 'buy', 'type': 'market', 'price': order['price'], 'amount': order['amount'], 'cost': order['cost'], 'reason': 'Takeprofit', 'pnl': pnl_pct, 'status': 'closed'})
                elif pnl_pct <= -3.0:
                    logger.info(f"[{symbol}] 숏 손절 조건 도달 (PNL: {pnl_pct:.2f}%)")
                    order = execute_order(symbol, 'buy', position_amt, {'reduceOnly': True})
                    if order: log_trade(conn, {'timestamp': datetime.now().isoformat(), 'symbol': symbol, 'side': 'buy', 'type': 'market', 'price': order['price'], 'amount': order['amount'], 'cost': order['cost'], 'reason': 'Stoploss', 'pnl': pnl_pct, 'status': 'closed'})
                elif long_condition:
                    logger.info(f"[{symbol}] 숏 포지션 종료 (반대 롱 신호 발생)")
                    order = execute_order(symbol, 'buy', position_amt, {'reduceOnly': True})
                    if order: log_trade(conn, {'timestamp': datetime.now().isoformat(), 'symbol': symbol, 'side': 'buy', 'type': 'market', 'price': order['price'], 'amount': order['amount'], 'cost': order['cost'], 'reason': 'Trend Reversal', 'pnl': pnl_pct, 'status': 'closed'})

        else:
            # 신규 진입 로직
            # 잔고 조회 (전체 계좌)
            balance = binance.fetch_balance()
            usdt_balance = float(balance['USDT']['free'])
            
            # 사용자 요청: 주문 가능 잔고의 10% 사용
            target_stake = 0.1
            amount_to_invest = (usdt_balance * target_stake) * LEVERAGE
            
            if amount_to_invest > 6: # 최소 주문 가능 금액 위일 때
                quantity = amount_to_invest / current_price
                
                if long_condition:
                    logger.info(f"[{symbol}] 롱 진입 조건 만족! 진입 실행.")
                    order = execute_order(symbol, 'buy', quantity)
                    if order: log_trade(conn, {'timestamp': datetime.now().isoformat(), 'symbol': symbol, 'side': 'buy', 'type': 'market', 'price': order['price'], 'amount': order['amount'], 'cost': order['cost'], 'reason': 'Long Entry', 'pnl': 0, 'status': 'open'})
                
                elif short_condition:
                    logger.info(f"[{symbol}] 숏 진입 조건 만족! 진입 실행.")
                    order = execute_order(symbol, 'sell', quantity)
                    if order: log_trade(conn, {'timestamp': datetime.now().isoformat(), 'symbol': symbol, 'side': 'sell', 'type': 'market', 'price': order['price'], 'amount': order['amount'], 'cost': order['cost'], 'reason': 'Short Entry', 'pnl': 0, 'status': 'open'})
            # else:
            #     logger.debug(f"[{symbol}] 잔고 부족 또는 할당량 미달로 진입 불가")

    except Exception as e:
        logger.error(f"[{symbol}] 트레이딩 처리 중 오류: {e}")

# 메인 루프
def run_trading_logic():
    logger.info("--- 전체 티커 트레이딩 로직 시작 ---")
    conn = init_db()
    
    for symbol in TARGET_TICKERS:
        process_ticker(symbol, conn)
        time.sleep(1) # API 요청 제한 고려 (티커 간 1초 대기)
        
    conn.close()
    logger.info("--- 사이클 종료 ---")

if __name__ == "__main__":
    if not TARGET_TICKERS:
        logger.error("거래할 티커가 없습니다. target_tickers.json을 확인하세요.")
        exit()
        
    # 초기 실행
    run_trading_logic()
    
    # 스케줄링 (1분마다 체크)
    schedule.every(1).minutes.do(run_trading_logic)
    
    logger.info("자동매매 봇 (Multi-Ticker) 시작됨. (Ctrl+C로 종료)")
    
    while True:
        schedule.run_pending()
        time.sleep(1)