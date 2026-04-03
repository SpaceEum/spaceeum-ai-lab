# /path/to/your/project/live_trader.py

import os
import glob
import sqlite3
import pandas as pd
import json
import pandas_ta as ta
import ccxt
import csv
import sys
import numpy as np
import time
from datetime import datetime
import schedule
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# --- ⚠️ 중요: API 키 설정 ---
API_KEY = os.environ.get("BINANCE_API_KEY")
SECRET_KEY = os.environ.get("BINANCE_SECRET_KEY")

# --- 거래 설정 ---
LEVERAGE = 10
INVESTMENT_RATIO = 0.05  # 전체 자산의 5%를 각 거래에 사용 (여러 전략 동시 실행을 위해 비중 조절)
FEE_RATE = 0.0004 # 거래 수수료 (PNL 계산 시 사용)

# --- 지표 설정 (backtest.py와 동일하게 유지) ---
ICHIMOKU_PERIODS = [9, 26, 52]
SMA_PERIOD = 60
BOLLINGER_PERIOD = 10
CHIKOU_PERIOD = ICHIMOKU_PERIODS[1] # 후행스팬 기간

# --- 전역 상태 변수 ---
# 각 타임프레임별로 로드된 전략(DataFrame)을 저장
STRATEGIES = {}
# 거래소 객체
EXCHANGE = None
# 이 스크립트가 직접 연 포지션만 관리. { 'ticker': {'amount': float, 'entry_price': float} }
MY_OPEN_POSITIONS = {} # { 'ticker': {'amount': float, 'entry_price': float, 'highest_price': float, 'trailing_stop_pct': float, 'stop_loss_pct': float} }
TRADE_LOG_DB = "trade_PNL.db" # 거래 기록 DB 파일명
POSITIONS_FILE = "open_positions.json" # 보유 포지션 저장 파일
CSV_LOG_FILE = "result_trade.csv" # CSV 거래 기록 파일명

# --- ✨ 추가: UI/상태 표시용 전역 변수 ---
sync_counter = 0

# --- 로거 클래스 ---
class Logger:
    """터미널 출력을 파일과 콘솔에 동시에 기록하는 클래스"""
    def __init__(self, filename="default.log"):
        self.terminal = sys.stdout
        self.log_file = open(filename, "a", encoding='utf-8')

    def write(self, message):
        self.terminal.write(message)
        self.log_file.write(message)

    def flush(self):
        self.terminal.flush()
        self.log_file.flush()

    def __del__(self):
        self.log_file.close()

# --- 헬퍼 함수 ---
def get_latest_final_db(timeframe):
    """특정 타임프레임의 가장 최근 final DB 파일을 찾습니다."""
    # ✨ 수정: 'trade_*.db' 파일을 찾도록 변경
    db_files = glob.glob(f"trade_*.db")
    if not db_files:
        return None
    return max(db_files, key=os.path.getctime)

def initialize_exchange():
    """ccxt를 사용하여 바이낸스 선물 거래소에 연결합니다."""
    global EXCHANGE
    if not API_KEY or not SECRET_KEY:
        print("🛑 오류: .env 파일에 BINANCE_API_KEY와 BINANCE_SECRET_KEY가 설정되지 않았습니다.")
        return False
    try:
        EXCHANGE = ccxt.binance({
            'apiKey': API_KEY, 'secret': SECRET_KEY, 'enableRateLimit': True,
            'options': {'defaultType': 'future'}
        })
        EXCHANGE.load_markets()

        print("✅ 바이낸스 선물 거래소에 성공적으로 연결되었습니다.")
        return True
    except Exception as e:
        print(f"거래소 연결 중 오류 발생: {e}")
        return False

def load_all_strategies():
    """모든 타임프레임에 대한 최적 전략 목록을 로드합니다."""
    print("\n--- 모든 타임프레임의 최적 전략 로드 시작 ---")
    # ✨ 수정: 15분봉 전략을 사용하도록 변경
    timeframes = ['15m']
    for tf in timeframes:
        db_file = get_latest_final_db(tf)
        if not db_file:
            print(f"- [ {tf} ] 경고: 해당 타임프레임의 final DB 파일을 찾을 수 없습니다.")
            continue
        try:
            conn = sqlite3.connect(db_file)
            # ✨ 수정: 'filtered_trades' 테이블에서 데이터를 읽도록 변경
            df = pd.read_sql_query("SELECT * FROM filtered_trades", conn)
            conn.close()
            if not df.empty:
                # --- ✨ 필터 추가: 승률이 52% 초과인 전략만 선택 ---
                original_count = len(df)
                df_filtered = df[df['win_rate_percent'] > 52].copy()
                filtered_count = len(df_filtered)

                if not df_filtered.empty:
                    STRATEGIES[tf] = df_filtered
                    print(f"- [ {tf} ] 성공: '{db_file}'에서 {original_count}개 로드 후, 승률 52% 초과 {filtered_count}개 필터링 완료.")
                else:
                    print(f"- [ {tf} ] 경고: '{db_file}'에서 승률 52% 초과 전략을 찾지 못했습니다. (원본 {original_count}개)")
            else:
                print(f"- [ {tf} ] 경고: '{db_file}'에 분석된 전략이 없습니다.")
        except Exception as e:
            print(f"- [ {tf} ] 오류: DB '{db_file}' 로드 중 오류 발생: {e}")
    print("--- 전략 로드 완료 ---")

# --- 거래 기록 DB 관련 함수 ---
def initialize_trade_log_db():
    """거래 기록용 데이터베이스와 테이블을 초기화합니다."""
    conn = sqlite3.connect(TRADE_LOG_DB)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME NOT NULL,
            ticker TEXT NOT NULL,
            side TEXT NOT NULL,
            amount REAL NOT NULL,
            price REAL NOT NULL,
            reason TEXT NOT NULL,
            usdt_balance REAL,
            trade_pnl_percent REAL,
            cumulative_pnl_percent REAL
        )
    ''')
    conn.commit()
    conn.close()
    print("✅ 거래 기록 DB가 준비되었습니다.")

def save_positions_to_file():
    """현재 보유 포지션 정보를 JSON 파일에 저장합니다."""
    try:
        with open(POSITIONS_FILE, 'w') as f:
            json.dump(MY_OPEN_POSITIONS, f, indent=4)
    except Exception as e:
        print(f"🛑 포지션 파일 저장 중 오류: {e}")

def load_positions_from_file():
    """JSON 파일에서 보유 포지션 정보를 불러옵니다."""
    global MY_OPEN_POSITIONS
    try:
        if os.path.exists(POSITIONS_FILE):
            with open(POSITIONS_FILE, 'r') as f:
                MY_OPEN_POSITIONS = json.load(f)
                print(f"\n✅ 파일에서 {len(MY_OPEN_POSITIONS)}개의 포지션을 불러왔습니다: {list(MY_OPEN_POSITIONS.keys())}")
    except Exception as e:
        print(f"🛑 포지션 파일 로드 중 오류: {e}")
        MY_OPEN_POSITIONS = {}

def log_to_csv(data):
    """거래 내역 딕셔너리를 CSV 파일에 추가합니다."""
    try:
        file_exists = os.path.exists(CSV_LOG_FILE)
        # CSV 파일 헤더 순서 정의
        fieldnames = [
            '시간', '티커', '구분', '거래가격', '거래수량', 
            '거래금액(USDT)', '이번거래수익률(%)', '총수익률(%)', '사유'
        ]
        with open(CSV_LOG_FILE, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader() # 파일이 없으면 헤더 작성
            
            # fieldnames에 없는 키는 제외하고 기록
            filtered_data = {k: v for k, v in data.items() if k in fieldnames}
            writer.writerow(filtered_data)
    except Exception as e:
        print(f"🛑 CSV 파일 기록 중 오류: {e}")

def get_last_cumulative_pnl():
    """데이터베이스에서 마지막 누적 수익률을 가져옵니다."""
    try:
        # ✨ 수정: DB 파일이 존재하지 않으면, 파일을 생성하지 않고 즉시 0.0을 반환합니다.
        # 이것이 DB가 비정상적으로 생성되는 것을 막는 핵심입니다.
        if not os.path.exists(TRADE_LOG_DB):
            return 0.0

        conn = sqlite3.connect(TRADE_LOG_DB)
        # ✨ 수정: NULL이 아닌 마지막 누적 수익률을 가져오도록 쿼리 변경
        last_trade = pd.read_sql_query("SELECT cumulative_pnl_percent FROM trades WHERE cumulative_pnl_percent IS NOT NULL ORDER BY id DESC LIMIT 1", conn)
        conn.close()
        if not last_trade.empty and pd.notna(last_trade['cumulative_pnl_percent'].iloc[0]):
            return last_trade['cumulative_pnl_percent'].iloc[0] / 100.0 # 비율로 반환
        return 0.0 # 거래가 없으면 0.0 반환
    except Exception as e:
        print(f"  - 마지막 누적 수익률 조회 중 오류: {e}")
        return 0.0

def log_trade(ticker, side, amount, price, reason):
    """거래 내역을 데이터베이스에 기록합니다."""
    global EXCHANGE, MY_OPEN_POSITIONS
    try:
        # ✨ 수정: DB에 기록하기 전에 'trades' 테이블이 존재하는지 확인하고, 없으면 다시 초기화합니다.
        # 이 방어적인 로직이 테이블이 없는 문제를 최종적으로 해결합니다.
        conn_check = sqlite3.connect(TRADE_LOG_DB)
        cursor_check = conn_check.cursor()
        cursor_check.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trades';")
        if not cursor_check.fetchone():
            print(f"🛑 경고: 'trades' 테이블이 '{TRADE_LOG_DB}'에 존재하지 않습니다. 다시 초기화 시도...")
            conn_check.close() # 기존 연결 닫기
            initialize_trade_log_db() # 테이블 강제 생성
        else:
            conn_check.close() # 확인 후 연결 닫기

        balance = EXCHANGE.fetch_balance()
        usdt_balance = balance['total']['USDT']
        trade_pnl_percent, cumulative_pnl_percent = None, None

        if side == 'sell':
            position_data = MY_OPEN_POSITIONS.get(ticker)
            # ✨ 수정: position_data가 있고, price가 0보다 클 때만 수익률 계산
            # price가 0이면 강제청산/수동청산 등 외부 요인으로 간주하여 수익률 계산 안함
            if position_data and 'entry_price' in position_data and price > 0:
                entry_price = position_data['entry_price']
                # ✨ 수정: 레버리지와 수수료를 정확히 반영하여 PNL 계산 (backtest.py와 동기화)
                trade_pnl = ((price / entry_price) - 1) * LEVERAGE - \
                            (LEVERAGE * FEE_RATE) - \
                            (LEVERAGE * (price / entry_price) * FEE_RATE)

                trade_pnl_percent = trade_pnl * 100
                last_cumulative_pnl_ratio = get_last_cumulative_pnl()
                new_cumulative_pnl_ratio = (1 + last_cumulative_pnl_ratio) * (1 + trade_pnl) - 1
                cumulative_pnl_percent = new_cumulative_pnl_ratio * 100

        conn = sqlite3.connect(TRADE_LOG_DB)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO trades (timestamp, ticker, side, amount, price, reason, usdt_balance, trade_pnl_percent, cumulative_pnl_percent) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",(datetime.now(), ticker, side, amount, price, reason, usdt_balance, trade_pnl_percent, cumulative_pnl_percent))
        conn.commit()
        conn.close()
        print(f"💾 [ {ticker} ] 거래 기록 완료: {side.upper()} {amount:.4f} @ {price:.4f} | 이유: {reason} | 잔고: {usdt_balance:.2f} USDT")
        
        # ✨ 추가: 거래 기록 후에는 항상 포지션 파일 상태를 업데이트
        save_positions_to_file()

        # --- ✨ 추가: CSV 파일에 거래 내역 기록 ---
        csv_data = {
            '시간': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            '티커': ticker,
            '구분': side,
            '거래가격': f"{price:.8f}".rstrip('0').rstrip('.'),
            '거래수량': amount,
            '거래금액(USDT)': round(price * amount, 4) if price > 0 else 0,
            '이번거래수익률(%)': round(trade_pnl_percent, 4) if trade_pnl_percent is not None else '',
            '총수익률(%)': round(cumulative_pnl_percent, 4) if cumulative_pnl_percent is not None else '',
            '사유': reason
        }
        log_to_csv(csv_data)

    except Exception as e:
        print(f"🛑 거래 기록 중 심각한 오류 발생: {e}")

def place_limit_order_with_retry(exchange, ticker, side, amount, initial_price, params={}):
    # --- ✨ 추가: 주문 수량 정밀도 조정 ---
    try:
        # 주문 수량을 거래소의 정밀도에 맞게 포맷팅
        formatted_amount = exchange.amount_to_precision(ticker, amount)
        print(f"   - 정밀도 조정: 수량({amount:.8f} -> {formatted_amount})")
    except Exception as e:
        print(f"  - 🛑 [ {ticker} ] 주문 수량 정밀도 조정 중 오류: {e}. 주문을 진행할 수 없습니다.")
        return None

    # 정밀도 조정 후 주문 수량이 0이 되는 경우 방지
    if float(formatted_amount) <= 0:
        print(f"  - 🛑 [ {ticker} ] 정밀도 조정 후 주문 수량이 0 이하가 되어 주문을 진행할 수 없습니다. (원본: {amount})")
        return None

    MAX_RETRIES = 5
    RETRY_DELAY_SECONDS = 5
    order = None
    for i in range(MAX_RETRIES):
        try:
            # 재시도 시 가격을 다시 가져와서 포맷팅
            price_to_use = initial_price if i == 0 else exchange.fetch_ticker(ticker)['last']
            formatted_price = exchange.price_to_precision(ticker, price_to_use)

            print(f"--- [시도 {i+1}/{MAX_RETRIES}] 지정가 {'매수' if side == 'buy' else '매도'} 주문 --- (params: {params})")
            order = exchange.create_limit_buy_order(ticker, formatted_amount, formatted_price, params) if side == 'buy' else exchange.create_limit_sell_order(ticker, formatted_amount, formatted_price, params)
            print(f"   - 주문 ID: {order['id']}. {RETRY_DELAY_SECONDS}초 후 체결 여부 확인...")
            time.sleep(RETRY_DELAY_SECONDS)
            fetched_order = exchange.fetch_order(order['id'], ticker)
            if fetched_order['status'] == 'closed':
                print(f"✅ 주문이 성공적으로 체결되었습니다! (체결가: {fetched_order['average']})")
                return fetched_order
            else:
                print(f"   - 주문 미체결 (상태: {fetched_order['status']}). 취소 후 재시도...")
                exchange.cancel_order(order['id'], ticker)
        except Exception as e:
            # ✨ 추가: ReduceOnly 주문 거부 오류 처리 (포지션이 이미 청산된 경우)
            if 'ReduceOnly Order is rejected' in str(e):
                print(f"   - 경고: ReduceOnly 주문이 거부되었습니다. 포지션이 이미 청산된 것으로 보입니다.")
                return {'status': 'already_closed'}
            print(f"   - 지정가 주문 처리 중 오류: {e}")
            if order and 'id' in order:
                try: 
                    exchange.cancel_order(order['id'], ticker)
                    print(f"   - 오류 발생으로 주문(ID: {order['id']})을 취소했습니다.")
                except Exception as cancel_e:
                    print(f"   - 주문 취소 중 추가 오류 발생: {cancel_e}")
            time.sleep(RETRY_DELAY_SECONDS)
    print(f"--- 지정가 주문 {MAX_RETRIES}회 실패. 시장가 주문으로 전환합니다. ---")
    try:
        market_order = exchange.create_market_buy_order(ticker, formatted_amount, params) if side == 'buy' else exchange.create_market_sell_order(ticker, formatted_amount, params)
        print("✅ 시장가 주문이 성공적으로 제출되었습니다. 체결 상태를 확인합니다...")
        time.sleep(2) # 거래소 처리를 위한 약간의 딜레이
        fetched_market_order = exchange.fetch_order(market_order['id'], ticker)
        if fetched_market_order['status'] == 'closed':
            print(f"✅ 시장가 주문이 성공적으로 체결되었습니다! (체결가: {fetched_market_order['average']})")
            return fetched_market_order
        else:
            print(f"   - 🛑 시장가 주문 후 상태 확인 실패 (상태: {fetched_market_order['status']})")
            return fetched_market_order
    except Exception as e:
        # ✨ 추가: ReduceOnly 주문 거부 오류 처리
        if 'ReduceOnly Order is rejected' in str(e):
            print(f"   - 경고: 시장가 ReduceOnly 주문이 거부되었습니다. 포지션이 이미 청산된 것으로 보입니다.")
            return {'status': 'already_closed'}
        print(f"🛑 시장가 주문 전환 중 심각한 오류 발생: {e}")
        return None

# --- ✨ 추가: 거래소와 포지션 동기화 ---
def sync_positions_with_exchange():
    """
    거래소의 실제 포지션과 스크립트의 관리 목록을 동기화하여 '유령 포지션'을 정리합니다.
    유령 포지션: 스크립트 외부에서 청산(강제청산, 수동청산)되어 스크립트에만 남아있는 포지션
    """
    global EXCHANGE, MY_OPEN_POSITIONS, sync_counter
    sync_counter += 1
    try:
        # 1. 거래소에서 실제 보유 포지션 가져오기
        open_positions_from_exchange = EXCHANGE.fetch_positions()
        # USDT 무기한 선물, 실제 포지션이 있는 것(positionAmt != 0)만 필터링
        exchange_tickers = {
            pos['info']['symbol'] for pos in open_positions_from_exchange 
            if float(pos['info']['positionAmt']) != 0
        }
        
        # 2. 스크립트가 관리하는 포지션 목록
        script_tickers = set(MY_OPEN_POSITIONS.keys())
        
        # 3. 유령 포지션 찾기 (스크립트에는 있지만, 거래소에는 없는 포지션)
        ghost_tickers = script_tickers - exchange_tickers
        
        if ghost_tickers:
            print(f"\r👻 유령 포지션 감지: {list(ghost_tickers)}. 스크립트 외부에서 청산된 것으로 보입니다.{' '*20}\n")
            for ticker in ghost_tickers:
                position_data = MY_OPEN_POSITIONS[ticker]
                # PNL 계산 없이, 외부에서 종료되었다는 사실만 기록
                log_trade(
                    ticker=ticker, side='sell', amount=position_data['amount'],
                    price=0, # 정확한 가격을 알 수 없으므로 0으로 기록
                    reason='external_close_or_liquidation'
                )
                # 관리 목록에서 제거
                del MY_OPEN_POSITIONS[ticker]
            
            save_positions_to_file() # 파일 상태 업데이트
            print(f"✅ 유령 포지션을 정리하고 관리 목록을 업데이트했습니다: {list(MY_OPEN_POSITIONS.keys())}")
    except Exception as e:
        print(f"🛑 포지션 동기화 중 오류 발생: {e}")

# --- 포지션 관리 로직 (손절 / 트레일링 스탑) ---
def check_all_open_positions():
    """
    모든 자체 관리 포지션을 순회하며 손절 및 트레일링 스탑 조건을 확인하고 실행합니다.
    """
    global EXCHANGE, MY_OPEN_POSITIONS
    
    # 복사본을 순회하여 반복 중 딕셔너리 변경 문제를 방지
    positions_to_check = list(MY_OPEN_POSITIONS.keys())
    if not positions_to_check:
        return # 관리할 포지션이 없으면 종료

    # print(f"\n--- 포지션 관리 시작 (손절/트레일링 스탑 확인: {positions_to_check}) ---") # 로그 간소화를 위해 주석 처리
    
    try:
        # 현재가 한번에 가져오기 (1차 시도)
        tickers_info = {}
        try:
            tickers_info = EXCHANGE.fetch_tickers(positions_to_check)
        except Exception as e:
            print(f"\r  - 경고: 전체 티커 가격 일괄 조회 실패: {e}. 개별 조회를 시도합니다.", end="", flush=True)
        
        for ticker in positions_to_check:
            # 루프가 도는 동안 다른 로직에 의해 포지션이 정리되었을 수 있으므로 다시 확인
            if ticker not in MY_OPEN_POSITIONS:
                continue

            position_data = MY_OPEN_POSITIONS[ticker]
            entry_price = position_data['entry_price']
            highest_price = position_data['highest_price']
            # DB에서 가져온 전략별 종료 조건 값
            trailing_stop_pct = position_data['trailing_stop_pct']
            stop_loss_pct = position_data.get('stop_loss_pct', 0)
            take_profit_pct = position_data.get('take_profit_pct', 0)

            current_price = None
            # 1차: 일괄 조회 결과에서 가격 가져오기
            if ticker in tickers_info:
                current_price = tickers_info[ticker]['last']
            # 2차: 일괄 조회 결과에 없거나, 일괄 조회가 실패했을 경우 개별 조회
            else:
                try:
                    # print(f"  - [ {ticker} ] 일괄 조회에 정보가 없어 개별 가격 조회를 시도합니다...") # 로그 간소화
                    current_price = EXCHANGE.fetch_ticker(ticker)['last']
                except Exception as e:
                    print(f"\r  - 🛑 [ {ticker} ] 개별 가격 조회도 실패했습니다. 이번 사이클에서는 관리할 수 없습니다. 오류: {e}", end="", flush=True)
                    continue # 이번 턴에서는 이 티커를 건너뛴다.

            # --- 종료 조건 확인 (우선순위: 1.익절 -> 2.트레일링스탑 -> 3.손절) ---

            # 1. 분할 익절(Take-Profit) 조건 확인
            # ✨ 수정: 분할 익절 로직 제거 (backtest.py와 일치)

            # 2. 트레일링 스탑 발동 조건 확인 (최고가 갱신 '전에' 확인)
            # DB에 지정된 trailing_stop_pct가 있으면 사용, 없으면 하드코딩된 3% 사용
            ts_pct_to_use = trailing_stop_pct if trailing_stop_pct > 0 else 0.03

            if ts_pct_to_use > 0:
                trailing_stop_price = highest_price * (1 - ts_pct_to_use)
                # ✨ 수정: 종가 기준 확인 (backtest.py와 일치시키기 위해 현재가로 확인)
                if current_price < trailing_stop_price: 
                    print(f"\r🛑 [ {ticker} ] 트레일링 스탑({ts_pct_to_use*100:.1f}%) 발동! (최고가: {highest_price:.4f}, 현재가: {current_price:.4f}, 발동가: {trailing_stop_price:.4f})\n")
                    position_amount = position_data['amount']
                    filled_order = place_limit_order_with_retry(EXCHANGE, ticker, 'sell', position_amount, current_price, {'reduceOnly': 'true'})

                    if filled_order and filled_order.get('status') == 'closed':
                        log_trade(ticker=ticker, side='sell', amount=filled_order['filled'], price=filled_order['average'], reason='trailing_stop')
                        del MY_OPEN_POSITIONS[ticker]
                        save_positions_to_file() # ✨ 추가: 포지션 청산 후 파일 저장
                        print(f"✅ [ {ticker} ] 트레일링 스탑으로 포지션 청산 완료.")
                    continue # 청산했으므로 다음 포지션 확인

            # 3. 최고가 갱신 (청산 조건이 발동하지 않았을 때만)
            if current_price > highest_price:
                MY_OPEN_POSITIONS[ticker]['highest_price'] = current_price
                # print(f"  - [ {ticker} ] 최고가 갱신: {current_price:.4f}") # 로그 간소화
            
            # 4. 손절(Stop-Loss) 조건 확인 (9기간 신저가)
            # ✨ 수정: 9기간 신저가 손절 로직으로 변경
            try:

                ohlcv_df = pd.DataFrame(EXCHANGE.fetch_ohlcv(ticker, '15m', limit=10), columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                lowest_price_in_9_periods = ohlcv_df['low'].iloc[:-1].min() # 현재 캔들 제외
            except Exception as e:
                print(f"\r  - 🛑 [ {ticker} ] 9기간 신저가 조회 실패: {e}", end="", flush=True)
                continue
            if current_price < lowest_price_in_9_periods:
                print(f"\r🛑 [ {ticker} ] 손절(9기간 신저가) 발동! (현재가: {current_price:.4f}, 9기간 신저가: {lowest_price_in_9_periods:.4f})\n")

                position_amount = position_data['amount']
                filled_order = place_limit_order_with_retry(EXCHANGE, ticker, 'sell', position_amount, current_price, {'reduceOnly': 'true'})
                
                if filled_order and filled_order.get('status') == 'closed':
                    log_trade(ticker=ticker, side='sell', amount=filled_order['filled'], price=filled_order['average'], reason='stop_loss_9_period_low')
                    del MY_OPEN_POSITIONS[ticker]
                    save_positions_to_file() # ✨ 추가: 포지션 청산 후 파일 저장
                    print(f"✅ [ {ticker} ] 손절로 포지션 청산 완료.")
                continue # 청산했으므로 다음 포지션 확인
                
    except Exception as e:
        print(f"\r  - 🛑 포지션 관리 중 오류 발생: {e}", end="", flush=True)

    # print(f"--- 포지션 관리 종료 ---") # 로그 간소화를 위해 주석 처리

# --- 핵심 거래 로직 ---
def execute_trade_check(timeframe):
    """주어진 타임프레임에 대한 매매 신호를 확인하고 거래를 실행합니다."""
    global EXCHANGE
    print(f"\r--- [ {timeframe} ] 신호 확인 시작 ({datetime.now()}) ---")
    
    if timeframe not in STRATEGIES:
        print(f"[ {timeframe} ] 실행할 전략이 로드되지 않았습니다. 건너뜁니다.")
        return

    # ✨ 추가: API 호출을 줄이기 위한 OHLCV 데이터 캐시
    ohlcv_cache = {}

    # 2. 전략 목록 순회하며 거래 대상 찾기
    strategy_df = STRATEGIES[timeframe]
    for index, strategy in strategy_df.iterrows():
        ticker = "N/A" # 오류 발생 시 로깅을 위해 미리 초기화
        try:
            ticker = strategy['ticker']
            entry_col_name = strategy['entry_strategy']
            exit_col_name = strategy['exit_strategy']
            trailing_stop_pct_from_db = strategy.get('trailing_stop_pct', 0) / 100.0
            # ✨ 추가: DB에서 손절/익절 값 로드
            stop_loss_pct_from_db = strategy.get('stop_loss_pct', 0) / 100.0
            take_profit_pct_from_db = strategy.get('take_profit_pct', 0) / 100.0
            avg_win_volume_ratio = strategy.get('avg_entry_volume_ratio', 1.2) # DB값 없으면 기본 1.2

            # 3. 이 스크립트가 관리하는 포지션인지 확인 (중복 진입 방지)
            if ticker in MY_OPEN_POSITIONS:
                # print(f"- [ {ticker} ] 이미 자체 관리중인 포지션이 있습니다. 매도 신호만 확인합니다.") # 로그 간소화
                continue

            print(f"\r- [ {ticker:<9} ] ({index + 1:>3}순위) 신호 확인 중...{' ' * 30}", end="", flush=True)
            
            # 4. 데이터 및 지표 계산
            # --- ✨ 수정: 캐시 확인 및 데이터 로드 ---
            if ticker in ohlcv_cache:
                df = ohlcv_cache[ticker].copy()
            else:
                ohlcv = EXCHANGE.fetch_ohlcv(ticker, timeframe, limit=100) # 60일 신고가 계산을 위해 데이터 로드량 증가
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('timestamp', inplace=True)
                ohlcv_cache[ticker] = df.copy() # 원본 데이터 캐싱
            # --- 캐싱 로직 종료 ---

            # 데이터 길이는 (가장 긴 지표 기간 + 시프트 기간 + 2) 보다 길어야 함
            min_data_len = max(ICHIMOKU_PERIODS[2], SMA_PERIOD, 60 * 24 * 4) + CHIKOU_PERIOD + 2 # 60일 신고가 기간 추가
            if len(df) < 100:
                print(f"  - 데이터 부족 (필요: {min_data_len}, 보유: {len(df)}). 지표 계산 불가. 건너뜁니다.")
                continue

            df.ta.ichimoku(tenkan=ICHIMOKU_PERIODS[0], kijun=ICHIMOKU_PERIODS[1], senkou=ICHIMOKU_PERIODS[2], append=True)
            df.ta.sma(length=SMA_PERIOD, append=True)
            df.ta.bbands(length=BOLLINGER_PERIOD, append=True)
            # ✨ 추가: MACD 지표 계산
            df.ta.macd(append=True)
            # 거래량 이동평균 계산 (backtest.py와 동일하게 20일로 수정)
            df['volume_ma20'] = df['volume'].rolling(window=20).mean()

            # 체결강도 계산 (근사치)
            ti_period = 20
            df['buy_volume'] = np.where(df['close'] > df['open'], df['volume'], 0)
            df['sell_volume'] = np.where(df['close'] < df['open'], df['volume'], 0)
            df['rolling_buy_vol'] = df['buy_volume'].rolling(window=ti_period).sum()
            df['rolling_sell_vol'] = df['sell_volume'].rolling(window=ti_period).sum()
            df['trading_intensity'] = (df['rolling_buy_vol'] / (df['rolling_sell_vol'] + 1e-9)) * 100
            # ✨ 추가: 10기간 체결강도 이동평균 계산
            df['trading_intensity_ma10'] = df['trading_intensity'].rolling(window=10).mean()

            lines_map = {
                '전환선': f'ITS_{ICHIMOKU_PERIODS[0]}', '기준선': f'IKS_{ICHIMOKU_PERIODS[1]}',
                '선행스팬1': f'ISA_{ICHIMOKU_PERIODS[0]}', '선행스팬2': f'ISB_{ICHIMOKU_PERIODS[2]}',
                '60일 SMA': f'SMA_{SMA_PERIOD}', 'BB 상단': f'BBU_{BOLLINGER_PERIOD}_2.0',
                'BB 중단': f'BBM_{BOLLINGER_PERIOD}_2.0', 'BB 하단': f'BBL_{BOLLINGER_PERIOD}_2.0'
            }
            entry_col = lines_map.get(entry_col_name)
            exit_col = lines_map.get(exit_col_name)
            
            # ✨ 추가: 고정 손익비 전략인지 확인
            is_sl_tp_strategy = 'SL/TP' in exit_col_name

            if not entry_col or entry_col not in df.columns:
                # print(f"  - 매수 전략 컬럼({entry_col_name})을 찾을 수 없음. 건너뜁니다.")
                continue

            # 고정 손익비 전략이 아닌 경우에만 매도 컬럼 유효성 검사
            if not is_sl_tp_strategy and (not exit_col or exit_col not in df.columns):
                # print(f"  - 매도 전략 컬럼({exit_col_name})을 찾을 수 없음. 건너뜁니다.")
                continue

            df['shifted_entry_line'] = df[entry_col].shift(CHIKOU_PERIOD)

            dropna_subset = ['shifted_entry_line']
            # 고정 손익비(SL/TP) 전략이 아닌 경우에만 매도 신호선(shifted_exit_line)을 계산
            if not is_sl_tp_strategy:
                df['shifted_exit_line'] = df[exit_col].shift(CHIKOU_PERIOD)
                dropna_subset.append('shifted_exit_line')

            df.dropna(subset=dropna_subset, inplace=True)
            if df.empty:
                print(f"  - 데이터 부족 (지표 계산 후). 건너뜁니다.")
                continue

            # 5. 매매 신호 판단 (최근 '완성된' 봉을 기준으로 판단)
            # df.iloc[-1]은 현재 진행중인 봉, df.iloc[-2]가 가장 최근에 완성된 봉
            # 신호 유지를 위해 최소 6개 봉 필요
            if len(df) < 7:
                continue
            
            last_completed_candle = df.iloc[-2]
            current_price = df['close'].iloc[-1] # 주문 시에는 최신 가격 사용

            # .get()을 사용하여 'shifted_exit_line'이 없는 경우에도 오류 없이 None을 반환하도록 수정
            exit_signal_line = last_completed_candle.get('shifted_exit_line')

            # --- ✨ 수정: backtest.py와 동일한 진입 로직 ---
            price_signal = False
            # 1. 새로운 상향 돌파 신호 확인
            is_new_cross_up = df.iloc[-2]['close'] > df.iloc[-2]['shifted_entry_line'] and df.iloc[-3]['close'] <= df.iloc[-3]['shifted_entry_line']
            if is_new_cross_up:
                # 상향 돌파 시, 5개 캔들 동안 신호를 유효하게 설정 (실제 거래에서는 즉시 확인)
                price_signal = True
            
            # 2. 신호가 유효한 기간인지 확인 (backtest.py의 price_signal_active_for_candles 개념을 단순화)
            # live_trader는 매 캔들마다 실행되므로, 최근 5개 캔들 내 돌파가 있었는지 확인
            if not price_signal:
                for k in range(2, 7): # 최근 5개 완성된 캔들
                    if df.iloc[-k]['close'] > df.iloc[-k]['shifted_entry_line'] and df.iloc[-(k+1)]['close'] <= df.iloc[-(k+1)]['shifted_entry_line']:
                        if all(df.iloc[-j]['close'] > df.iloc[-j]['shifted_entry_line'] for j in range(2, k)):
                            price_signal = True
                            break
            # --- 로직 종료 ---
            
            if price_signal:
                # 거래량/체결강도 조건: 신호가 발생한 '완성된' 봉을 기준으로 확인
                filter_check_candle = last_completed_candle
                required_volume = filter_check_candle['volume_ma20'] * avg_win_volume_ratio
                is_volume_ok = filter_check_candle['volume'] > required_volume

                # ✨ 수정: 현재 체결강도가 10기간 이동평균보다 높은지 확인
                is_intensity_ok = filter_check_candle['trading_intensity'] > filter_check_candle['trading_intensity_ma10']

                # ✨ 추가: MACD 조건 확인 (MACD 값이 0 이상)
                is_macd_ok = filter_check_candle['MACD_12_26_9'] >= 0

                # ✨ 추가: 모멘텀 조건 확인 (현재 종가가 60기간 전 종가보다 높은가)
                is_momentum_ok = last_completed_candle['close'] > df.iloc[-2 - SMA_PERIOD]['close']

                # --- ✨ 추가: 60 SMA 및 60일 신고가 필터 (backtest.py와 일치) ---
                # 6. 현재 가격이 60 SMA 위에 있는지 확인
                is_above_sma = last_completed_candle['close'] > last_completed_candle[f'SMA_{SMA_PERIOD}']

                # 7. 60일 신고가인지 확인
                high_period = 60 * 24 * 4
                is_60d_high = last_completed_candle['close'] >= df['high'].iloc[-2-high_period:-2].max()

                # --- ✨ 추가: 윗꼬리 및 급등 필터 ---
                is_long_wick = False
                candle = last_completed_candle
                body_length = abs(candle['close'] - candle['open'])
                if body_length > 0: # 몸통이 있는 캔들만 확인
                    if candle['close'] > candle['open']: # 양봉
                        upper_wick = candle['high'] - candle['close']
                        if upper_wick >= body_length * 1.3:
                            is_long_wick = True
                    else: # 음봉
                        upper_wick = candle['high'] - candle['open']
                        if upper_wick >= body_length * 1.3:
                            is_long_wick = True
                
                is_sudden_pump = False
                prev_candle = df.iloc[-3]
                prev_body_length = abs(prev_candle['close'] - prev_candle['open'])
                if prev_body_length > 0 and body_length >= prev_body_length * 3:
                    is_sudden_pump = True

                if is_volume_ok and is_intensity_ok and is_macd_ok and is_momentum_ok and not is_long_wick and not is_sudden_pump and is_above_sma and is_60d_high:
                    print(f"\r{' '*80}\r", end="") # 상태 업데이트 줄 지우기
                    balance = EXCHANGE.fetch_balance()
                    available_balance = balance['free']['USDT']

                    if available_balance < 10:
                        print(f"  - [ {ticker} ] 사용 가능 잔고({available_balance:.2f} USDT)가 부족하여 진입 불가.")
                        continue

                    amount_to_invest = available_balance * INVESTMENT_RATIO

                    # 최소 주문 금액 처리 로직
                    MIN_ORDER_USDT = 5.0
                    if amount_to_invest < MIN_ORDER_USDT:
                        print(f"  - [ {ticker} ] 계산된 투자금({amount_to_invest:.2f} USDT)이 최소 주문액({MIN_ORDER_USDT} USDT) 미만입니다.")
                        if available_balance >= MIN_ORDER_USDT:
                            amount_to_invest = MIN_ORDER_USDT
                            print(f"  - ... 사용 가능 잔고가 충분하여 최소 주문액({MIN_ORDER_USDT} USDT)으로 투자를 조정합니다.")
                        else:
                            print(f"  - ... 사용 가능 잔고({available_balance:.2f} USDT)도 최소 주문액 미만이라 진입이 불가능합니다.")
                            continue

                    order_size = (amount_to_invest * LEVERAGE) / current_price

                    # --- ✨ 추가: 최소 주문 수량(minAmount) 체크 및 조정 ---
                    market_info = EXCHANGE.markets.get(ticker)
                    if market_info and 'limits' in market_info and 'amount' in market_info['limits']:
                        min_amount = market_info['limits']['amount'].get('min')
                        if min_amount and order_size < min_amount:
                            print(f"  - [ {ticker} ] 계산된 주문 수량({order_size:.8f})이 최소 주문 수량({min_amount})보다 작습니다.")
                            # 최소 주문 수량을 맞추기 위해 투자금 재계산
                            required_investment = min_amount * current_price * 1.01 # 슬리피지를 고려해 1% 여유
                            if available_balance >= required_investment:
                                order_size = min_amount
                                print(f"  - ... 사용 가능 잔고가 충분하여 주문 수량을 최소 수량({min_amount})으로 조정합니다.")
                            else:
                                print(f"  - ... 최소 주문 수량을 맞추기 위한 잔고({required_investment:.2f} USDT)가 부족하여 진입 불가.")
                                continue
                    else:
                        print(f"  - [ {ticker} ] 경고: 마켓 정보를 찾을 수 없어 최소 주문 수량을 확인할 수 없습니다.")

                    print(f"🚀 [ {timeframe} / {ticker} ] 매수 신호 발생! (거래량, 체결강도 충족, 수량 조정 완료) 주문을 시도합니다.")
                    try:
                        # ✨ 추가: 마진 모드를 '격리(Isolated)'로 설정하고 레버리지 설정
                        EXCHANGE.set_margin_mode('isolated', ticker)
                        print(f"  - [ {ticker} ] 마진 모드를 '격리(Isolated)'로 설정했습니다.")
                        EXCHANGE.set_leverage(LEVERAGE, ticker)
                    except Exception as e:
                        print(f"  - ⚠️ [ {ticker} ] 마진 모드 또는 레버리지 설정 중 오류 발생: {e}. 계정 기본 설정을 따를 수 있습니다.")

                    filled_order = place_limit_order_with_retry(EXCHANGE, ticker, 'buy', order_size, current_price)
                    
                    if filled_order and filled_order.get('status') == 'closed':
                        entry_price = filled_order['average']
                        log_trade(ticker=ticker, side='buy', amount=filled_order['filled'], price=entry_price, reason='entry_signal')
                        MY_OPEN_POSITIONS[ticker] = {
                            'amount': filled_order['filled'], 'entry_price': entry_price,
                            'highest_price': entry_price,
                            # 모든 종료 조건 파라미터 저장
                            'trailing_stop_pct': trailing_stop_pct_from_db,
                            'stop_loss_pct': stop_loss_pct_from_db,
                            'take_profit_pct': take_profit_pct_from_db,
                            # ✨ 추가: 분할 익절 플래그
                            'tp1_triggered': False,
                            'tp2_triggered': False
                        }
                        print(f"✅ [ {ticker} ] 포지션 진입 기록 완료: 수량 {filled_order['filled']:.4f}. 현재 관리 포지션: {list(MY_OPEN_POSITIONS.keys())}")
                    else:
                        print(f"❌ [ {ticker} ] 매수 주문이 최종적으로 체결되지 않았습니다.")
                    return
                else:
                    print(f"\r{' '*80}\r", end="") # 상태 업데이트 줄 지우기
                    reasons = []
                    if not price_signal: # 이 경우는 거의 없지만 방어적으로 추가
                        reasons.append("최근 3봉 내 상향돌파 후 유지 실패")
                    if not is_volume_ok:
                        reasons.append(f"거래량 부족 (완성봉: {filter_check_candle['volume']:.0f}, 필요(DB): >{required_volume:.0f})")
                    if not is_intensity_ok:
                        reasons.append(f"체결강도 부족 (현재: {filter_check_candle['trading_intensity']:.2f}, 필요(10MA): >{filter_check_candle['trading_intensity_ma10']:.2f})")
                    if not is_macd_ok:
                        reasons.append(f"MACD 조건 미충족 (현재: {filter_check_candle['MACD_12_26_9']:.2f}, 필요: >= 0)")
                    if not is_momentum_ok:
                        reasons.append(f"모멘텀 조건 미충족 (현재: {last_completed_candle['close']:.4f}, 필요(60봉 이전): >{df.iloc[-2 - SMA_PERIOD]['close']:.4f})")
                    if is_long_wick:
                        reasons.append("윗꼬리 김 (매도압력)")
                    if not is_above_sma:
                        reasons.append(f"SMA 추세 미충족 (현재: {last_completed_candle['close']:.4f}, 필요(60SMA): >{last_completed_candle[f'SMA_{SMA_PERIOD}']:.4f})")
                    if not is_60d_high:
                        reasons.append("60일 신고가 미달성")
                    if is_sudden_pump:
                        reasons.append("단기 급등 (과열)")
                    print(f"  - [ {ticker} ] 가격 신호는 발생했으나, 조건 미충족: {', '.join(reasons)}")
            
            # 매도 신호 (완성된 봉 기준)
            if not is_sl_tp_strategy and ticker in MY_OPEN_POSITIONS and last_completed_candle['close'] < exit_signal_line and df.iloc[-3]['close'] >= df.iloc[-3]['shifted_exit_line']:
                print(f"\r{' '*80}\r", end="") # 상태 업데이트 줄 지우기
                position_amount = MY_OPEN_POSITIONS[ticker]['amount']
                print(f"🛑 [ {timeframe} / {ticker} ] 매도 신호 발생! (자체 관리 포지션) {position_amount} 청산을 시도합니다.")
                filled_order = place_limit_order_with_retry(EXCHANGE, ticker, 'sell', position_amount, current_price, {'reduceOnly': 'true'})
                if filled_order and (filled_order.get('status') == 'closed' or filled_order.get('status') == 'already_closed'):
                    if filled_order.get('status') == 'closed':
                        log_trade(ticker=ticker, side='sell', amount=filled_order['filled'], price=filled_order['average'], reason='exit_signal')
                    if ticker in MY_OPEN_POSITIONS:
                        del MY_OPEN_POSITIONS[ticker]
                        save_positions_to_file() # ✨ 추가: 포지션 청산 후 파일 저장
                        print(f"✅ [ {ticker} ] 포지션 청산 기록 완료/확인. 현재 관리 포지션: {list(MY_OPEN_POSITIONS.keys())}")
                return # 한 타임프레임 당 하나의 거래만 실행하고 종료 (이 return은 유지)

        except Exception as e:
            import traceback
            print(f"\r- [ {ticker} ] 처리 중 오류 발생: {e}\n")
            traceback.print_exc()
            continue # 오류 발생 시 다음 티커로 넘어감

    # 모든 전략 확인 후 신규 진입이 없었을 경우
    print(f"\r--- [ {timeframe} ] 신호 확인 완료. 신규 진입 없음. ({datetime.now()}) ---{' '*30}")

# --- 메인 실행 로직 ---
def main():
    # --- 로깅 설정: 터미널 출력을 파일로 저장 ---
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_filename = os.path.join(log_dir, f"live_trader_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    sys.stdout = Logger(log_filename)
    sys.stderr = sys.stdout # 오류 출력도 동일한 파일에 기록

    if not initialize_exchange():
        return

    load_positions_from_file() # ✨ 추가: 스크립트 시작 시 포지션 정보 로드
    load_all_strategies()

    while True:
        print("\n--- 자동매매 실행 모드를 선택하세요 ---")
        print("1: 수동 실행 (신호/포지션 1회 확인 후 종료)")
        print("2: 자동 실행 (스케줄러 및 포지션 관리 지속 실행)")
        choice = input("선택 (1 또는 2): ")

        if choice == '1':
            print("\n--- 수동 모드: 신호 및 포지션 1회 확인 ---")
            print("--- 신호 확인 실행 ---")
            # ✨ 수정: 15분봉으로 확인
            execute_trade_check('15m')
            print("--- 포지션 관리 실행 ---")
            check_all_open_positions()
            print("\n--- 확인 완료. 프로그램을 종료합니다. ---")
            break

        elif choice == '2':
            print("\n--- 자동 실행 모드: 스케줄러를 시작합니다. (Ctrl+C로 종료) ---")
            # ✨ 수정: 15분마다 신호 확인 실행 (매시 0분, 15분, 30분, 45분)
            schedule.every(15).minutes.at(":00").do(execute_trade_check, timeframe='15m')

            print("\n--- 스케줄 목록 ---")
            for job in schedule.get_jobs():
                print(job)
            print("------------------\n")

            # ✨ 수정: 시작 시 15분봉 신호 확인 1회 실행
            print("--- 시작 시 신호 확인 1회 실행 ---")
            execute_trade_check('15m')

            # print()  # 다음 대기 메시지를 위한 줄바꿈. 상태 메시지 출력 로직과 충돌하므로 제거.
            wait_seconds = 0
            while True:
                # ✨ 수정: 상태 메시지를 변수로 통합 관리
                status_message = f"다음 스케줄 대기 중... ({wait_seconds}초 경과)"

                schedule.run_pending()

                # ✨ 추가: 1분마다 실제 포지션과 동기화하여 유령 포지션 정리
                if wait_seconds % 60 == 0 and wait_seconds > 0:   
                    status_message = f"🔄 포지션 동기화 확인({sync_counter + 1}번)..."
                    sync_positions_with_exchange()

                if wait_seconds % 20 == 0: # API 호출 빈도를 줄이기 위해 10초에서 20초로 변경
                    if MY_OPEN_POSITIONS:
                        check_all_open_positions()
                
                # ✨ 수정: 루프 마지막에서 상태 메시지를 한 번만 출력
                # 메시지를 출력하기 전에 이전 줄을 깨끗하게 지웁니다.
                print(f"\r{' ' * 80}\r", end="", flush=True) # 이전 줄의 잔상을 완전히 지움
                print(f"\r{status_message}", end="", flush=True)

                time.sleep(1)
                wait_seconds += 1
        else:
            print("잘못된 입력입니다. 1 또는 2를 입력해주세요.")

if __name__ == "__main__":
    main()
