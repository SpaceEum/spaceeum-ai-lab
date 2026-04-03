from binance.client import Client # 바이낸스 클라이언트 라이브러리
from binance.exceptions import BinanceAPIException # 바이낸스 예외 처리
import pandas as pd
import json
import os
import time
from datetime import datetime
import schedule
import logging
from dotenv import load_dotenv # .env 파일 로드용

# .env 파일에서 환경 변수 로드
load_dotenv()

# --- 기본 설정 ---
LOG_DIR = "logs_executor"
DATA_DIR_SELECT = "Coin_Select" # 종목 선정 결과 파일이 있는 폴더
BUY_TICKER_FILE = os.path.join(DATA_DIR_SELECT, "buy_ticker.json")
SELECTED_TICKERS_FILE = os.path.join(DATA_DIR_SELECT, "60Ma_Coinselect_5Stage.json")
MIN_NOTIONAL_VALUE_USDT = 10 # 바이낸스 선물 최소 주문 명목 가치 (예시, 실제 값 확인 필요)

# --- 로깅 설정 ---
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

log_filename_executor = datetime.now().strftime("executor_%Y-%m-%d.log")
log_filepath_executor = os.path.join(LOG_DIR, log_filename_executor)

logger = logging.getLogger('trading_executor')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# StreamHandler (콘솔 출력)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# FileHandler (파일 기록)
file_handler = logging.FileHandler(log_filepath_executor, encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# --- 바이낸스 API 클라이언트 ---
binance_client = None # 실제 거래용 클라이언트 (인증 필요)
public_binance_client = Client(None, None) # API 키 없이 공개 정보 조회용 클라이언트

BINANCE_FUTURES_API_KEY = os.getenv("BINANCE_FUTURES_API_KEY") # .env 파일에 BINANCE_FUTURES_API_KEY 로 저장 가정
BINANCE_FUTURES_API_SECRET = os.getenv("BINANCE_FUTURES_API_SECRET") # .env 파일에 BINANCE_FUTURES_API_SECRET 로 저장 가정

try:
    if BINANCE_FUTURES_API_KEY and BINANCE_FUTURES_API_SECRET: # 키가 존재하는지 확인
        # 실제 거래 시: binance_client = Client(BINANCE_FUTURES_API_KEY, BINANCE_FUTURES_API_SECRET)
        # 테스트넷 사용 시: binance_client = Client(BINANCE_FUTURES_API_KEY, BINANCE_FUTURES_API_SECRET, testnet=True)
        # 여기서는 실제 API 객체 생성은 주석 처리 (매수/매도 로직 비활성화 상태)
        # binance_client = Client(BINANCE_FUTURES_API_KEY, BINANCE_FUTURES_API_SECRET) 
        logger.info("바이낸스 선물 API 키가 감지되었습니다. (실제 거래 클라이언트 초기화는 필요시 주석 해제)")
    else:
        logger.warning("바이낸스 선물 API 키가 .env 파일에 설정되지 않았거나 로드에 실패했습니다. (인증 필요한 기능 사용 불가)")
except Exception as e:
    logger.error(f"바이낸스 선물 API 클라이언트 초기화 시도 중 오류: {e}")

# --- 헬퍼 함수 ---
def load_json_data(file_path, default_data=None):
    """JSON 파일에서 데이터를 로드합니다."""
    if default_data is None:
        default_data = {}
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError:
            logger.error(f"{file_path} 파일 디코딩 오류. 기본 데이터 반환: {default_data}")
            return default_data
    return default_data

def save_json_data(file_path, data):
    """데이터를 JSON 파일에 저장합니다."""
    try:
        # 저장 전 폴더 존재 확인 및 생성
        folder = os.path.dirname(file_path)
        if folder and not os.path.exists(folder):
            os.makedirs(folder)
            logger.info(f"폴더 생성: {folder}")

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info(f"데이터를 '{file_path}'에 저장했습니다.")
    except Exception as e:
        logger.error(f"'{file_path}'에 데이터 저장 중 오류 발생: {e}")

def get_selected_tickers():
    """선정된 티커 목록을 로드합니다."""
    data = load_json_data(SELECTED_TICKERS_FILE, default_data={"tickers": []})
    return data.get("tickers", [])

def get_futures_usdt_balance(client_to_use):
    """바이낸스 선물 계좌의 USDT 잔액을 조회합니다."""
    if not client_to_use:
        logger.warning("바이낸스 API 클라이언트가 초기화되지 않아 잔액을 조회할 수 없습니다.")
        return 0
    try:
        balances = client_to_use.futures_account_balance()
        for balance_item in balances:
            if balance_item['asset'] == 'USDT':
                return float(balance_item['balance'])
        logger.warning("선물 계좌에서 USDT 잔액 정보를 찾을 수 없습니다.")
        return 0
    except BinanceAPIException as e:
        logger.error(f"바이낸스 선물 USDT 잔액 조회 중 API 오류: {e}")
        return 0
    except Exception as e:
        logger.error(f"바이낸스 선물 USDT 잔액 조회 중 일반 오류: {e}")
        return 0
# --- Coin_Select_FF.py 에서 가져온 종목 선정 로직 함수들 ---
def cs_save_all_tickers():
    """바이낸스 USDⓈ-M 선물 USDT 페어 티커 목록을 가져옴"""
    logger.info("[종목선정] 바이낸스 USDⓈ-M 선물 USDT 페어 티커 목록을 가져옵니다...")
    # public_binance_client는 이 파일의 전역 범위에 이미 정의되어 있음
    try:
        exchange_info = public_binance_client.futures_exchange_info()
        # USDT 마진, PERPETUAL 계약, 거래 중인 상태만 필터링
        usdt_futures_tickers = [
            s['symbol'] for s in exchange_info['symbols']
            if s['quoteAsset'] == 'USDT' and s['contractType'] == 'PERPETUAL' and s['status'] == 'TRADING'
        ]
        logger.info(f"[종목선정] 총 {len(usdt_futures_tickers)}개의 바이낸스 USDT 선물 티커를 가져왔습니다.")
        return usdt_futures_tickers
    except BinanceAPIException as e:
        logger.error(f"[종목선정] 바이낸스 선물 티커 목록 조회 중 API 오류: {e}")
        return []
    except Exception as e:
        logger.error(f"[종목선정] 바이낸스 선물 티커 목록 조회 중 일반 오류: {e}")
        return []


def cs_get_ohlcv_binance(ticker, interval='4h', limit=200):
    """바이낸스 선물 API를 사용하여 OHLCV 데이터를 가져옵니다."""
    logger.debug(f"[{ticker}] 바이낸스 선물 OHLCV 데이터 요청 (간격: {interval}, 개수: {limit})")
    try:
        # 바이낸스 API는 klines 반환 시 [timestamp, open, high, low, close, volume, ...] 형식
        klines = public_binance_client.futures_klines(symbol=ticker, interval=interval, limit=limit) # 공개 API 사용
        if not klines:
            logger.warning(f"[{ticker}] OHLCV 데이터를 가져오지 못했습니다 (데이터 없음).")
            return None

        df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume',
                                           'close_time', 'quote_asset_volume', 'number_of_trades',
                                           'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        for col in ['open', 'high', 'low', 'close', 'volume', 'quote_asset_volume']:
            df[col] = pd.to_numeric(df[col])
        return df
    except BinanceAPIException as e:
        logger.error(f"[{ticker}] 바이낸스 선물 OHLCV 데이터 조회 중 API 오류: {e}")
        return None
    except Exception as e:
        logger.error(f"[{ticker}] 바이낸스 선물 OHLCV 데이터 조회 중 일반 오류: {e}")
        return None


def cs_calculate_ichimoku(df):
    """일목균형표 지표 계산 함수 - 선행스팬을 26기간 앞으로 이동"""
    # 전환선(Tenkan-sen)
    high_9 = df['high'].rolling(window=9).max()
    low_9 = df['low'].rolling(window=9).min()
    df['tenkan_sen'] = (high_9 + low_9) / 2

    # 기준선(Kijun-sen)
    high_26 = df['high'].rolling(window=26).max()
    low_26 = df['low'].rolling(window=26).min()
    df['kijun_sen'] = (high_26 + low_26) / 2

    # 선행스팬1(Senkou Span A): (전환선 + 기준선) / 2, 26기간 후에 플롯
    df['senkou_span_a'] = ((df['tenkan_sen'] + df['kijun_sen']) / 2).shift(26)

    # 선행스팬2(Senkou Span B): 52기간 (고가+저가) / 2, 26기간 후에 플롯
    high_52 = df['high'].rolling(window=52).max()
    low_52 = df['low'].rolling(window=52).min()
    df['senkou_span_b'] = ((high_52 + low_52) / 2).shift(26)

    # 후행스팬(Chikou Span): 현재 종가를 26기간 전에 플롯 (차트 표시용)
    df['chikou_span_plot'] = df['close'].shift(-26)
    return df

def cs_step_01_process_and_filter(all_tickers):
    logger.info("\n[종목선정-1단계] 4시간봉 데이터 가져오기, 지표 계산 및 조건 필터링 시작...")
    if not all_tickers:
        logger.info("[종목선정] 가져올 티커 목록이 없습니다.")
        return []
    result = []
    for ticker in all_tickers:
        try:
            df = cs_get_ohlcv_binance(ticker, interval=Client.KLINE_INTERVAL_4HOUR, limit=200) # 바이낸스용 함수 호출
            if df is None or len(df) < 62: # MA60, 60캔들 종이격 등 계산에 충분한 데이터 확인
                logger.debug(f"[{ticker}] 1단계: 데이터 부족 (가져온 캔들 수: {len(df) if df is not None else 0})")
                continue
            
            df = df.iloc[:-1] # 마지막 미완료 캔들 제거
            if len(df) < 61: # 제거 후 다시 확인
                logger.debug(f"[{ticker}] 1단계: 미완료 캔들 제거 후 데이터 부족 (캔들 수: {len(df)})")
                continue
            
            df['ma60'] = df['close'].rolling(window=60).mean()
            df = cs_calculate_ichimoku(df) # 수정된 일목균형표 계산 함수 호출

            current_price = df['close'].iloc[-1]
            if pd.isna(current_price):
                logger.debug(f"[{ticker}] 1단계: 현재가가 NaN입니다.")
                continue

            ma60_value = df['ma60'].iloc[-1]
            ma60_26_periods_ago = df['ma60'].shift(26).iloc[-1] # 26캔들 전의 MA60 값
            
            lag_60_close = df['close'].shift(60).iloc[-1] # 60캔들 전 종가
            lag_gap = current_price - lag_60_close if pd.notna(lag_60_close) else float('nan')
            lag_gap_percent = (lag_gap / lag_60_close) * 100 if pd.notna(lag_60_close) and lag_60_close != 0 else float('nan')

            highest_close_60d = df['close'].iloc[-61:-1].max() # 현재 캔들 제외, 이전 60캔들 중 최고 종가

            # cs_calculate_ichimoku에서 .shift(26) 적용됨. 
            # .iloc[-1]은 현재 캔들에 해당하는 (미래에 그려질 것으로 예상되었던) 구름대 값.
            current_senkou_span_a = df['senkou_span_a'].iloc[-1]
            current_senkou_span_b = df['senkou_span_b'].iloc[-1]

            # 조건 1: 현재 종가 > 60MA
            condition1_ma = pd.notna(ma60_value) and current_price > ma60_value
            
            # 조건 1-추가: 현재 종가가 60MA 위에 머무른 기간이 3캔들 이하
            condition1_ma_duration_check = False
            if condition1_ma:
                candles_above_ma60 = 0
                max_duration_candles = 3
                for i in range(max_duration_candles + 1): 
                    idx = -1 - i
                    if abs(idx) >= len(df): break
                    
                    close_val_at_idx = df['close'].iloc[idx]
                    ma60_val_at_idx = df['ma60'].iloc[idx]

                    if pd.notna(close_val_at_idx) and pd.notna(ma60_val_at_idx):
                        if close_val_at_idx > ma60_val_at_idx:
                            candles_above_ma60 += 1
                        else: break
                    else: break
                if 1 <= candles_above_ma60 <= max_duration_candles:
                    condition1_ma_duration_check = True
            
            # 조건 2: 후행스팬 관련 (현재 종가 > 26캔들 전 MA60)
            condition2_chikou = pd.notna(ma60_26_periods_ago) and current_price > ma60_26_periods_ago

            if condition1_ma and condition1_ma_duration_check and condition2_chikou:
                ticker_name = ticker # 바이낸스 심볼은 KRW- 접두사 없음
                processed_data = {
                    "ticker": ticker, "name": ticker_name, "current_price": float(current_price),
                    "ma60": float(ma60_value) if pd.notna(ma60_value) else float('nan'),
                    "ma60_ratio": (float(current_price) / float(ma60_value)) if pd.notna(ma60_value) and ma60_value != 0 else float('inf'),
                    "price_26_days_ago": float(ma60_26_periods_ago) if pd.notna(ma60_26_periods_ago) else float('nan'),
                    "chikou_span_current": float(current_price),
                    "chikou_ratio": (float(current_price) / float(ma60_26_periods_ago)) if pd.notna(ma60_26_periods_ago) and ma60_26_periods_ago != 0 else float('inf'),
                    "lag_60_close": float(lag_60_close) if pd.notna(lag_60_close) else float('nan'),
                    "lag_gap": lag_gap, # 60캔들 종이격 값
                    "lag_gap_percent": lag_gap_percent,
                    "senkou_span_a": float(current_senkou_span_a) if pd.notna(current_senkou_span_a) else float('nan'),
                    "senkou_span_b": float(current_senkou_span_b) if pd.notna(current_senkou_span_b) else float('nan'),
                    "cloud_ratio": (float(current_senkou_span_a) / float(current_senkou_span_b)) if pd.notna(current_senkou_span_a) and pd.notna(current_senkou_span_b) and current_senkou_span_b != 0 else float('inf'),
                    "highest_close_60d": float(highest_close_60d) if pd.notna(highest_close_60d) else 0,
                    "high_ratio": (float(current_price) / float(highest_close_60d)) if pd.notna(highest_close_60d) and highest_close_60d != 0 else float('inf')
                }
                result.append(processed_data)
            time.sleep(0.1) # 바이낸스 API 호출 간격 조절 (더 짧게 가능, 필요시 조정)
        except Exception as e:
            logger.error(f"[종목선정] {ticker} 처리 중 오류 발생 (1단계): {e}", exc_info=True)
    logger.info(f"[종목선정-1단계] 총 {len(result)}개 종목이 조건을 만족하고 다음 단계로 전달됩니다.")
    return result
def cs_step_02_find_rising_lag_gap(step01_results):
    logger.info("\n[종목선정-2단계] 60개 캔들 종이격 상승 종목 필터링 시작...")
    if not step01_results:
        logger.info("[종목선정] 1단계 결과 없음. 2단계 건너뜀.")
        return []
    rising_lag_gap_coins = []
    for coin in step01_results:
        try:
            # 조건: 60일 종이격 상승 (현재 종가 > 60캔들 전 종가)
            # lag_gap은 step01_results에 이미 계산되어 있음 (current_price - lag_60_close)
            if coin.get("lag_gap", float('-inf')) > 0:
                rising_lag_gap_coins.append(coin)
        except Exception as e:
            logger.error(f"[종목선정] {coin.get('ticker','N/A')} 처리 중 오류 발생 (2단계): {e}")
    logger.info(f"[종목선정-2단계] 총 {len(rising_lag_gap_coins)}개 종목이 60개 캔들 종이격 상승 조건을 만족합니다.")
    return rising_lag_gap_coins

def cs_step_03_analyze_cloud(step02_results):
    logger.info("\n[종목선정-3단계] 선행스팬 골든크로스 분석 시작 (4시간봉 기준)...")
    if not step02_results:
        logger.info("[종목선정] 2단계 결과 없음. 3단계 건너뜀.")
        return []
    golden_cross_tickers = []
    for coin in step02_results:
        try:
            # 조건: 선행스팬1 > 선행스팬2 (현재 캔들의 구름대 기준)
            senkou_span_a = coin.get("senkou_span_a") 
            senkou_span_b = coin.get("senkou_span_b")
            if pd.notna(senkou_span_a) and pd.notna(senkou_span_b) and senkou_span_a > senkou_span_b:
                golden_cross_tickers.append(coin)
        except Exception as e:
            logger.error(f"[종목선정] {coin.get('ticker','N/A')} 처리 중 오류 발생 (3단계): {e}")
    logger.info(f"[종목선정-3단계] 총 {len(golden_cross_tickers)}개 종목이 선행스팬 골든크로스 조건을 만족합니다.")
    return golden_cross_tickers

def cs_step_04_new_condition_filter(step03_results):
    logger.info("\n[종목선정-4단계] 새로운 조건 필터링 시작 (현재 종가 > 선행스팬 B > 선행스팬 A)...")
    if not step03_results:
        logger.info("[종목선정] 3단계 결과 없음. 4단계 건너뜀.")
        return []
    passed_step04_tickers = []
    for coin_data in step03_results:
        try:
            # 조건: 현재 종가 > 선행스팬 B AND 선행스팬 B > 선행스팬 A (현재 캔들의 구름대 기준)
            current_price = coin_data.get('current_price')
            senkou_span_a = coin_data.get('senkou_span_a') 
            senkou_span_b = coin_data.get('senkou_span_b')
            if pd.notna(current_price) and pd.notna(senkou_span_a) and pd.notna(senkou_span_b):
                if (current_price > senkou_span_b) and (senkou_span_b > senkou_span_a):
                    passed_step04_tickers.append(coin_data)
        except Exception as e:
            logger.error(f"[종목선정] {coin_data.get('ticker','N/A')} 처리 중 오류 발생 (4단계): {e}")
    logger.info(f"[종목선정-4단계] 총 {len(passed_step04_tickers)}개 종목이 조건을 만족합니다.")
    return passed_step04_tickers

def cs_step_05_find_low_and_check_period(step04_results):
    logger.info("\n[종목선정-5단계] 최저가 발생 후 기간 조건 필터링 시작...")
    if not step04_results:
        logger.info("[종목선정] 4단계 결과 없음. 5단계 건너뜀.")
        return []
    passed_step05_tickers = []
    for coin_data in step04_results:
        ticker = coin_data.get('ticker', 'N/A')
        try:
            ohlcv_count_for_step5 = 60 # 최근 60캔들
            df = cs_get_ohlcv_binance(ticker, interval=Client.KLINE_INTERVAL_4HOUR, limit=ohlcv_count_for_step5)
            min_candles_needed_for_logic = 34 # 26~33캔들 경과 확인에 필요한 최소 데이터
            if df is None or len(df) < min_candles_needed_for_logic:
                logger.debug(f"  - [{ticker}] 5단계: 데이터 부족 (요청: {ohlcv_count_for_step5}, 실제: {len(df) if df is not None else 0}, 필요: {min_candles_needed_for_logic}).")
                continue
            df = df.iloc[:-1] # 미완료 캔들 제거
            if len(df) < min_candles_needed_for_logic - 1: 
                logger.debug(f"  - [{ticker}] 5단계: 미완료 캔들 제거 후 데이터 부족 (캔들 수: {len(df)}).")
                continue

            # 최근 60캔들(실제로는 df 길이만큼) 내에서 최저 종가 찾기
            search_series = df['close']
            if search_series.empty: continue
            
            lowest_price_date_idx = search_series.idxmin() # 최저가 발생 시점의 인덱스(Timestamp)
            
            # 최저가 발생 캔들의 DataFrame 내 위치(정수 인덱스)
            idx_lowest_in_df = df.index.get_loc(lowest_price_date_idx)
            idx_current_in_df = len(df) - 1 # 현재 (완료된) 캔들의 정수 인덱스
            
            candles_since_lowest = idx_current_in_df - idx_lowest_in_df

            # 조건: 최저가 발생 후 26~33캔들 경과
            if 26 <= candles_since_lowest <= 33:
                passed_step05_tickers.append(coin_data)
            time.sleep(0.1) # 바이낸스 API 호출 간격
        except Exception as e:
            logger.error(f"[종목선정] {ticker} 처리 중 오류 발생 (5단계): {e}", exc_info=True)
    logger.info(f"[종목선정-5단계] 총 {len(passed_step05_tickers)}개 종목이 최저가 발생 후 기간 조건을 만족합니다.")
    return passed_step05_tickers
def run_coin_selection_logic():
    logger.info("--- 종목 선정 로직 시작 ---")
    all_tickers = cs_save_all_tickers()
    if not all_tickers: return

    step01_results = cs_step_01_process_and_filter(all_tickers)
    save_json_data(os.path.join(DATA_DIR_SELECT, "60Ma_Coinselect_1Stage.json"), {"tickers": step01_results, "update_time": datetime.now().isoformat()})

    step02_results = cs_step_02_find_rising_lag_gap(step01_results)
    save_json_data(os.path.join(DATA_DIR_SELECT, "60Ma_Coinselect_2Stage.json"), {"tickers": step02_results, "update_time": datetime.now().isoformat()})

    step03_results = cs_step_03_analyze_cloud(step02_results)
    save_json_data(os.path.join(DATA_DIR_SELECT, "60Ma_Coinselect_3Stage.json"), {"tickers": step03_results, "update_time": datetime.now().isoformat()})

    step04_results = cs_step_04_new_condition_filter(step03_results)
    save_json_data(os.path.join(DATA_DIR_SELECT, "60Ma_Coinselect_4Stage.json"), {"tickers": step04_results, "update_time": datetime.now().isoformat()})

    step05_results = cs_step_05_find_low_and_check_period(step04_results)
    final_selected_data = {"tickers": step05_results, "update_time": datetime.now().isoformat()}
    if step05_results:
         final_selected_data["tickers"] = sorted(step05_results, key=lambda x: x.get('high_ratio', 0), reverse=True)
    save_json_data(SELECTED_TICKERS_FILE, final_selected_data)
    
    logger.info(f"--- 종목 선정 로직 완료 --- (최종 {len(step05_results)}개 티커 선정)")

# --- 1단계 매수 로직 ---
def execute_buy_orders(): # 함수 이름을 execute_stage1_buy_orders 등으로 변경 고려
    logger.info("--- 1단계 매수 로직 시작 ---")
    if not binance_client: # 실제 거래 시 클라이언트 확인
        logger.warning("바이낸스 API 클라이언트(인증 필요)가 초기화되지 않아 1단계 매수를 진행할 수 없습니다.")
        logger.info("매수/매도 기능은 API 키/시크릿 설정 및 binance_client 객체 활성화 필요.")
        return
    logger.info("현재 1단계 매수 주문 로직은 비활성화되어 있습니다 (바이낸스 선물용).")
    return # 실제 매수 로직은 없으므로 여기서 종료
    
    # 아래는 바이낸스 기준으로 수정한 코드 예시 (현재는 위의 return 문으로 인해 실행되지 않음)
    selected_tickers_info = get_selected_tickers() 
    if not selected_tickers_info:
        logger.info("1단계 매수: 선정된 티커가 없습니다.")
        return

    usdt_balance = get_futures_usdt_balance(binance_client)
    logger.info(f"1단계 매수: 현재 USDT 잔액: {usdt_balance:,.2f} USDT")

    if usdt_balance < MIN_NOTIONAL_VALUE_USDT: # 바이낸스 최소 주문 명목 가치 사용
        logger.info(f"1단계 매수: USDT 잔액이 최소 주문 명목 가치({MIN_NOTIONAL_VALUE_USDT} USDT)보다 적어 매수를 진행할 수 없습니다.")
        return

    current_futures_positions = {}
    try:
        # 바이낸스 선물 포지션 정보 조회 (실제 구현 시 필요)
        # positions = binance_client.futures_position_information()
        # for pos in positions:
        #     if float(pos.get('positionAmt', 0)) != 0: # 실제 포지션이 있는 경우
        #         current_futures_positions[pos['symbol']] = {
        #             "entry_price": float(pos.get('entryPrice', 0)),
        #             "quantity": float(pos.get('positionAmt', 0)), # 양수면 롱, 음수면 숏
        #             # ... 기타 필요한 정보
        #         }
        logger.info(f"1단계 매수: 현재 바이낸스 선물 포지션 (가정): {list(current_futures_positions.keys())}")
    except Exception as e_balance:
        logger.error(f"1단계 매수: 바이낸스 선물 포지션 조회 중 오류: {e_balance}. 이전 buy_ticker.json 사용.")
        current_futures_positions = load_json_data(BUY_TICKER_FILE, default_data={})

    tickers_to_consider_for_buy = [
        info for info in selected_tickers_info if info['ticker'] not in current_futures_positions
    ]

    if not tickers_to_consider_for_buy:
        logger.info("1단계 매수: 새롭게 매수할 티커가 없습니다.")
        return

    logger.info(f"1단계 매수: 신규 매수 고려 대상 티커 수: {len(tickers_to_consider_for_buy)}개")
    
    eligible_tickers_for_buy = []
    buy_notional_per_ticker_usdt_actual = 0 # USDT 기준 명목 가치

    if len(tickers_to_consider_for_buy) > 0:
        # 1단계 매수: 총 투자금의 20%를 각 티커에 분배
        # 자금 관리 전략: 사용 가능한 USDT 잔액을 매수 대상 티커 수로 나누어
        # 각 티커에 대한 "총 투자 가능액(명목)"을 정하고, 그 금액의 20%를 1단계 매수 명목 금액으로 사용.
        # 레버리지 미고려, 순수 USDT 투자금액 기준.
        
        total_investment_per_ticker_base = usdt_balance / len(tickers_to_consider_for_buy) # 각 티커에 할당될 수 있는 총 투자금 (이론상)
        stage1_buy_target_percentage = 0.20 # 1단계 매수 비율
        
        buy_notional_per_ticker_usdt_target = total_investment_per_ticker_base * stage1_buy_target_percentage
        logger.info(f"1단계 매수: 티커당 총 할당 가능 금액(이론상): {total_investment_per_ticker_base:,.2f} USDT, 1단계 목표 매수 명목 금액(20%): {buy_notional_per_ticker_usdt_target:,.2f} USDT")

        if buy_notional_per_ticker_usdt_target < MIN_NOTIONAL_VALUE_USDT:
            logger.warning(f"1단계 매수: 목표 티커당 매수 명목 금액({buy_notional_per_ticker_usdt_target:,.2f} USDT)이 최소 주문 명목 가치({MIN_NOTIONAL_VALUE_USDT:,.2f} USDT)보다 작습니다.")
            # 사용 가능한 총 1단계 투자금 (전체 잔액의 20%)
            total_stage1_allocatable_usdt = usdt_balance * stage1_buy_target_percentage
            max_tickers_can_buy_stage1 = int(total_stage1_allocatable_usdt / MIN_NOTIONAL_VALUE_USDT)
            
            if max_tickers_can_buy_stage1 > 0 and max_tickers_can_buy_stage1 <= len(tickers_to_consider_for_buy):
                logger.info(f"1단계 매수: 매수 티커 수를 {max_tickers_can_buy_stage1}개로 조정합니다.")
                eligible_tickers_for_buy = tickers_to_consider_for_buy[:max_tickers_can_buy_stage1]
                if eligible_tickers_for_buy: 
                    buy_notional_per_ticker_usdt_actual = total_stage1_allocatable_usdt / len(eligible_tickers_for_buy)
            elif max_tickers_can_buy_stage1 == 0 :
                logger.info("1단계 매수: 조정 후에도 매수 가능한 티커가 없습니다.")
                return
            else: 
                eligible_tickers_for_buy = tickers_to_consider_for_buy
                buy_notional_per_ticker_usdt_actual = buy_notional_per_ticker_usdt_target
        else:
            eligible_tickers_for_buy = tickers_to_consider_for_buy
            buy_notional_per_ticker_usdt_actual = buy_notional_per_ticker_usdt_target
    
    if not eligible_tickers_for_buy:
        logger.info("1단계 매수: 최종적으로 매수할 티커가 없습니다.")
        return

    logger.info(f"1단계 매수: 실제 매수 진행 티커 수: {len(eligible_tickers_for_buy)}개, 티커당 실제 매수 명목 금액: {buy_notional_per_ticker_usdt_actual:,.2f} USDT")

    for ticker_info in eligible_tickers_for_buy:
        ticker = ticker_info['ticker']
        current_price_at_selection = ticker_info.get('current_price') 
        if current_price_at_selection is None:
            logger.warning(f"[{ticker}] 1단계 매수: 선정 시점 가격 정보가 없어 현재가로 대체 시도.")
            try:
                # 바이낸스 현재가 조회 (futures_ticker)
                ticker_price_info = public_binance_client.futures_ticker(symbol=ticker)
                if ticker_price_info and 'lastPrice' in ticker_price_info:
                    current_price_at_selection = float(ticker_price_info['lastPrice'])
                else:
                    logger.error(f"[{ticker}] 1단계 매수: 현재가 조회 실패. 매수 건너뜀.")
                    continue
            except Exception as e:
                logger.error(f"[{ticker}] 1단계 매수: 현재가 조회 중 오류: {e}. 매수 건너뜀.")
                continue
        
        buy_notional_for_api = buy_notional_per_ticker_usdt_actual

        if buy_notional_for_api < MIN_NOTIONAL_VALUE_USDT: 
            logger.info(f"  - [{ticker}] 1단계 매수: 최종 매수 명목 금액({buy_notional_for_api:,.2f} USDT) 부족으로 매수 건너뜀.")
            continue

        try:
            # 주문 수량 계산 (명목금액 / 현재가) - 실제로는 정밀도, 최소 주문 수량 등 고려 필요
            quantity_to_buy = buy_notional_for_api / current_price_at_selection
            # TODO: 바이낸스 `get_symbol_info` 등으로 quantityPrecision, minQty 등 확인 후 수량 조정 필요
            logger.info(f"  -> [{ticker}] 1단계 시장가 매수 시도 (명목: {buy_notional_for_api:,.2f} USDT, 예상 수량: {quantity_to_buy})")
            
            # --- 실제 1단계 매수 주문 (바이낸스 선물용 - 주의!) ---
            # order = binance_client.futures_create_order(
            #     symbol=ticker,
            #     side=Client.SIDE_BUY,
            #     type=Client.ORDER_TYPE_MARKET,
            #     quantity=quantity_to_buy # 수량 정밀도 맞춰야 함
            # )
            # logger.info(f"    1단계 매수 주문 결과: {order}")
            # if order and order.get('orderId'):
            #     time.sleep(1) 
            #     try:
            #         # 주문 상세 정보 조회 (체결가, 체결 수량 등 확인)
            #         # filled_order_info = binance_client.futures_get_order(symbol=ticker, orderId=order['orderId'])
            #         # logger.info(f"    1단계 주문 상세 정보: {filled_order_info}")
            #         # avg_filled_price = float(filled_order_info.get('avgPrice',0))
            #         # filled_volume = float(filled_order_info.get('executedQty',0))
                        
            #         # if filled_volume > 0:
            #         initial_total_investment_for_this_ticker = buy_notional_for_api / stage1_buy_target_percentage
                        
            #         current_futures_positions[ticker] = {
            #             "entry_price": avg_filled_price, 
            #             "quantity": filled_volume,    
            #             "stage1_buy_time_str": datetime.now().isoformat(),
            #             "stage1_buy_timestamp": datetime.now().timestamp(),
            #             "stage1_buy_notional_value_usdt": buy_notional_for_api, 
            #             "initial_total_investment_allocation_usdt": initial_total_investment_for_this_ticker,
            #             "current_stage": 1 
            #         }
            #         logger.info(f"    [{ticker}] 1단계 매수 정보 저장 완료 (체결가: {avg_filled_price}, 수량: {filled_volume}).")
            #     except Exception as e_order_detail:
            #         logger.error(f"    [{ticker}] 1단계 매수 주문 상세 정보 조회 중 오류: {e_order_detail}")
            # --- 실제 1단계 매수 주문 끝 ---
            time.sleep(0.5) 
        except Exception as e:
            logger.error(f"  - [{ticker}] 1단계 매수 주문 중 오류 발생: {e}")
    
    save_json_data(BUY_TICKER_FILE, current_futures_positions)
    logger.info(f"1단계 매수 후 buy_ticker.json 업데이트. 총 {len(current_futures_positions)}개 티커 정보 저장.")
    logger.info("--- 1단계 매수 로직 종료 ---")
# --- 2단계 매수 로직 ---
def check_and_execute_stage2_buys():
    logger.info("--- 2단계 매수 조건 확인 및 실행 시작 ---")
    if not binance_client:
        logger.warning("바이낸스 API 클라이언트(인증 필요)가 초기화되지 않아 2단계 매수를 진행할 수 없습니다.")
        logger.info("매수/매도 기능은 API 키/시크릿 설정 및 binance_client 객체 활성화 필요.")
        return

    bought_data = load_json_data(BUY_TICKER_FILE, default_data={})
    if not bought_data:
        logger.info("1단계 매수된 티커가 없어 2단계 매수를 진행할 수 없습니다.")
        return

    usdt_balance = get_futures_usdt_balance(binance_client)
    logger.info(f"2단계 매수 시도 전 USDT 잔액: {usdt_balance:,.2f} USDT")

    updated_bought_data = bought_data.copy() # 변경사항 반영용

    for ticker, trade_info in bought_data.items():
        if trade_info.get("current_stage") != 1:
            # logger.debug(f"[{ticker}] 1단계 매수 상태가 아니므로 2단계 매수 건너뜀 (현재 상태: {trade_info.get('current_stage')}).")
            continue

        stage1_buy_timestamp = trade_info.get("stage1_buy_timestamp")
        if not stage1_buy_timestamp:
            logger.warning(f"[{ticker}] 1단계 매수 시간이 기록되지 않아 2단계 타이밍 체크 불가.")
            continue
        
        current_timestamp = datetime.now().timestamp()
        time_diff_seconds = current_timestamp - stage1_buy_timestamp
        candles_passed = time_diff_seconds / (4 * 60 * 60) # 4시간봉 기준 1캔들 = 4시간

        max_candles_for_stage2 = 13 # 13 기간(캔들) 이내 조건
        if candles_passed > max_candles_for_stage2:
            logger.info(f"[{ticker}] 1단계 매수 후 {max_candles_for_stage2}캔들 초과 ({candles_passed:.2f}캔들 경과). 2단계 매수 기간 종료.")
            updated_bought_data[ticker]["current_stage"] = "stage2_timeout" # 상태 업데이트
            continue
        
        logger.info(f"[{ticker}] 2단계 매수 조건 확인 중 (1단계 매수 후 {candles_passed:.2f}캔들 경과)...")

        try:
            # 2단계 조건 확인을 위한 데이터 가져오기 (최근 60캔들 + 현재캔들, 넉넉히 80개)
            df = cs_get_ohlcv_binance(ticker, interval=Client.KLINE_INTERVAL_4HOUR, limit=80)
            if df is None or len(df) < 61: # 60기간 전 종가, 60기간 신고가 등 계산에 필요
                logger.warning(f"[{ticker}] 2단계 분석용 데이터 부족 (최소 61개 필요). 건너뜀.")
                continue
            df = df.iloc[:-1] # 마지막 미완료 캔들 제거
            if len(df) < 60: continue

            df = cs_calculate_ichimoku(df) # 일목균형표 지표 추가

            current_close = df['close'].iloc[-1]
            senkou_a_val = df['senkou_span_a'].iloc[-1] # 현재 캔들의 선행스팬 A
            senkou_b_val = df['senkou_span_b'].iloc[-1] # 현재 캔들의 선행스팬 B
            
            # 60기간 전 종가 (DataFrame에서 직접 계산)
            close_60_periods_ago = df['close'].shift(60).iloc[-1] if len(df) >= 60 else float('nan')


            # 조건 1: 신고가 (최근 52 또는 60 기간 종가 기준)
            # 현재 캔들을 제외하고, 그 이전 52개 또는 60개 캔들의 종가 중 최고가를 찾음
            high_52_lookback = df['close'].iloc[-53:-1].max() if len(df) >= 53 else float('-inf')
            high_60_lookback = df['close'].iloc[-61:-1].max() if len(df) >= 61 else float('-inf')
            
            is_new_high = (pd.notna(high_52_lookback) and current_close > high_52_lookback) or \
                          (pd.notna(high_60_lookback) and current_close > high_60_lookback)

            # 조건 2: 60일 종이격 상승 (현재 종가 > 60기간 전 종가)
            is_price_gap_rising = pd.notna(close_60_periods_ago) and current_close > close_60_periods_ago

            # 조건 3: 가격과 구름대 유지 (현재 종가 > 선행스팬1 AND 현재 종가 > 선행스팬2)
            is_above_cloud = pd.notna(senkou_a_val) and pd.notna(senkou_b_val) and \
                             current_close > senkou_a_val and current_close > senkou_b_val

            logger.debug(f"[{ticker}] 2단계 조건: 신고가={is_new_high}, 종이격상승={is_price_gap_rising}, 구름위={is_above_cloud}")

            if is_new_high and is_price_gap_rising and is_above_cloud:
                logger.info(f"  -> [{ticker}] 2단계 매수 조건 모두 충족!")
                initial_total_investment = trade_info.get("initial_total_investment_allocation_usdt", 0) # USDT 기준
                if initial_total_investment <= 0:
                    logger.warning(f"    [{ticker}] 2단계 매수: 초기 총 투자 할당액 정보 없음. 건너뜀.")
                    continue

                stage2_buy_notional_usdt = initial_total_investment * 0.80 # 총 투자 예정액의 80% (명목 가치)

                if stage2_buy_notional_usdt < MIN_NOTIONAL_VALUE_USDT:
                    logger.warning(f"    [{ticker}] 2단계 매수 예정 명목 금액({stage2_buy_notional_usdt:,.2f} USDT)이 최소 주문 명목 가치 미만. 건너뜀.")
                    continue
                if usdt_balance < stage2_buy_notional_usdt: # 실제 투자금(마진)과 비교해야 하지만, 단순화하여 명목가치로 비교
                    logger.warning(f"    [{ticker}] USDT 잔액 부족 ({usdt_balance:,.2f} USDT)으로 2단계 매수 불가 (필요 명목: {stage2_buy_notional_usdt:,.2f} USDT).")
                    # 잔액 부족 시, 현재 티커는 건너뛰고 다음 티커로 진행. 또는 전체 2단계 매수 중단 결정 가능.
                    # 여기서는 현재 티커만 건너뜀.
                    continue 
                
                # 주문 수량 계산 (명목금액 / 현재가)
                quantity_to_buy_s2 = stage2_buy_notional_usdt / current_close
                # TODO: 바이낸스 `get_symbol_info` 등으로 quantityPrecision, minQty 등 확인 후 수량 조정 필요
                logger.info(f"    -> [{ticker}] 2단계 시장가 매수 시도 (명목: {stage2_buy_notional_usdt:,.2f} USDT, 예상 수량: {quantity_to_buy_s2})")
                # """ # --- 실제 2단계 매수 주문 (주의!) ---
                # ret_stage2 = upbit.buy_market_order(ticker, stage2_buy_amount_krw)
                # logger.info(f"      2단계 매수 주문 결과: {ret_stage2}")
                # if ret_stage2 and 'uuid' in ret_stage2:
                #     time.sleep(1)
                #     order_info_s2 = upbit.get_order(ret_stage2['uuid'])
                #     logger.info(f"      2단계 주문 상세 정보: {order_info_s2}")
                #     if order_info_s2 and order_info_s2.get('state') == 'done' and order_info_s2.get('trades_count', 0) > 0:
                #         s2_avg_price = float(order_info_s2['trades'][0]['price'])
                #         s2_volume = float(order_info_s2['executed_volume'])
                        
                #         # 1단계 정보와 합산하여 평균 단가 및 총 수량 업데이트
                #         stage1_value = trade_info['buy_price'] * trade_info['quantity']
                #         stage2_value = s2_avg_price * s2_volume
                #         total_value_accumulated = stage1_value + stage2_value
                #         total_quantity_accumulated = trade_info['quantity'] + s2_volume
                        
                #         new_avg_buy_price = total_value_accumulated / total_quantity_accumulated if total_quantity_accumulated > 0 else s2_avg_price

                #         updated_bought_data[ticker].update({
                #             "buy_price": new_avg_buy_price, 
                #             "quantity": total_quantity_accumulated, 
                #             "current_stage": 2,
                #             "stage2_buy_time_str": datetime.now().isoformat(),
                #             "stage2_buy_notional_value_usdt": stage2_buy_notional_usdt
                #         })
                #         logger.info(f"      [{ticker}] 2단계 매수 정보 업데이트 완료. 새 평단가: {new_avg_buy_price:,.0f}, 총 수량: {total_quantity_accumulated}")
                #         krw_balance -= stage2_buy_amount_krw # 임시 잔액 업데이트 (다음 티커 계산용)
                #     else: 
                #         logger.error(f"      [{ticker}] 2단계 매수 주문은 되었으나 체결되지 않았거나 체결 정보를 가져올 수 없음: {order_info_s2}")
                # else: 
                #     logger.error(f"      [{ticker}] 2단계 매수 주문 실패 또는 유효하지 않은 응답: {ret_stage2}")
                # """
                # --- 시뮬레이션용 ---
                logger.info(f"    [시뮬레이션] [{ticker}] 2단계 매수 성공.")
                updated_bought_data[ticker]["current_stage"] = 2 # 상태를 2단계로 변경
                updated_bought_data[ticker]["stage2_buy_time_str"] = datetime.now().isoformat()
                # 시뮬레이션에서는 수량, 평단가 업데이트는 생략 (실제 주문 시 필요)
                # 실제로는 1단계 매수 정보에 2단계 매수 정보를 더해서 평균단가, 총수량 등을 업데이트해야 함.
                usdt_balance -= stage2_buy_notional_usdt # 시뮬레이션용 잔액 차감
                # --- 시뮬레이션 끝 ---
            
            time.sleep(0.3) # 각 티커 처리 후 API 호출 딜레이
        except Exception as e:
            logger.error(f"[{ticker}] 2단계 매수 조건 확인 중 오류 발생: {e}", exc_info=True)

    save_json_data(BUY_TICKER_FILE, updated_bought_data) # 모든 티커 확인 후 최종 상태 저장
    logger.info("--- 2단계 매수 조건 확인 및 실행 종료 ---")


# --- 스케줄링 실행 ---
def main():
    print("-----------------------------------------------------")
    print("          바이낸스 선물 자동 매수 실행기 시작")
    print("-----------------------------------------------------")
    
    while True:
        print("\n실행 모드를 선택하세요:")
        print("1. 수동 실행 (즉시 1회 매수 로직 실행)")
        print("2. 스케줄 실행 (매일 09:30에 매수 로직 실행)")
        print("3. 종료")
        choice = input("선택 (1, 2, 또는 3): ")

        if choice == '1':
            logger.info("수동 실행 모드를 선택했습니다.")
            logger.info("수동 실행: 종목 선정 로직을 먼저 실행합니다.")
            run_coin_selection_logic()
            logger.info("수동 실행: 1단계 매수 로직을 실행합니다.")
            execute_buy_orders()
            logger.info("수동 실행: 2단계 매수 조건 확인 로직을 실행합니다.")
            check_and_execute_stage2_buys()
            logger.info("수동 실행 완료.")
            break 
        elif choice == '2':
            logger.info("스케줄 실행 모드를 선택했습니다.")
            logger.info("스케줄 실행: 초기 종목 선정 로직을 먼저 1회 실행합니다.")
            run_coin_selection_logic() 
            
            schedule.every().day.at("09:28").do(run_coin_selection_logic) 
            schedule.every().day.at("09:30").do(execute_buy_orders) # 1단계 매수
            schedule.every().day.at("09:35").do(check_and_execute_stage2_buys) # 2단계 매수 확인
            logger.info("매수 로직 스케줄: 09:28(종목선정), 09:30(1차매수), 09:35(2차매수 확인)")
            logger.info("스케줄러가 시작되었습니다. Ctrl+C 로 종료할 수 있습니다.")
            while True:
                schedule.run_pending()
                time.sleep(1)
        elif choice == '3':
            logger.info("프로그램을 종료합니다.")
            break
        else:
            print("잘못된 선택입니다. 1, 2, 또는 3 중에서 선택해주세요.")

if __name__ == "__main__":
    main()
