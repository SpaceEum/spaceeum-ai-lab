import pandas as pd # 데이터 분석
import json # JSON 파일 처리
import time # 시간 관련
from datetime import datetime # 날짜 및 시간
import os # 운영체제 기능 (파일, 폴더)
import schedule # 스케줄 라이브러리 추가
import logging # 로깅 라이브러리 추가
from binance.client import Client # 바이낸스 클라이언트 라이브러리
from binance.exceptions import BinanceAPIException # 바이낸스 예외 처리

# --- 기본 설정 ---
LOG_DIR = "logs_coin_select_binance"
DATA_DIR_SELECT = "Coin_Select" # 종목 선정 결과 파일이 있는 폴더 (trading_executor_binance.py와 동일하게 사용)

# --- 로깅 설정 ---
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

log_filename_selector = datetime.now().strftime("coin_select_binance_%Y-%m-%d.log")
log_filepath_selector = os.path.join(LOG_DIR, log_filename_selector)

logger = logging.getLogger('coin_select_binance')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# StreamHandler (콘솔 출력)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# FileHandler (파일 기록)
file_handler = logging.FileHandler(log_filepath_selector, encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# --- 바이낸스 API 클라이언트 ---
# API 키 없이 공개 정보 조회용 클라이언트
public_binance_client = Client(None, None)

def save_all_tickers_binance():
    """바이낸스 USDⓈ-M 선물 USDT 페어 티커 목록을 가져옴"""
    logger.info("바이낸스 USDⓈ-M 선물 USDT 페어 티커 목록을 가져옵니다...")
    try:
        exchange_info = public_binance_client.futures_exchange_info()
        # USDT 마진, PERPETUAL 계약, 거래 중인 상태만 필터링
        usdt_futures_tickers = [
            s['symbol'] for s in exchange_info['symbols']
            if s['quoteAsset'] == 'USDT' and s['contractType'] == 'PERPETUAL' and s['status'] == 'TRADING'
        ]
        logger.info(f"총 {len(usdt_futures_tickers)}개의 바이낸스 USDT 선물 티커를 가져왔습니다.")
        return usdt_futures_tickers
    except BinanceAPIException as e:
        logger.error(f"바이낸스 선물 티커 목록 조회 중 API 오류: {e}")
        return []
    except Exception as e:
        logger.error(f"바이낸스 선물 티커 목록 조회 중 일반 오류: {e}")
        return []

def get_ohlcv_binance(ticker, interval='4h', limit=200):
    """바이낸스 선물 API를 사용하여 OHLCV 데이터를 가져옵니다."""
    logger.debug(f"[{ticker}] 바이낸스 선물 OHLCV 데이터 요청 (간격: {interval}, 개수: {limit})")
    try:
        # 바이낸스 API는 klines 반환 시 [timestamp, open, high, low, close, volume, ...] 형식
        klines = public_binance_client.futures_klines(symbol=ticker, interval=interval, limit=limit)
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

def calculate_ichimoku(df):
    """일목균형표 지표 계산 함수 (Coin_Select_FF.py 원본 로직 유지)"""
    # 전환선(Tenkan-sen): 9일 기간의 (최고가 + 최저가) / 2
    high_9 = df['high'].rolling(window=9).max()
    low_9 = df['low'].rolling(window=9).min()
    df['tenkan_sen'] = (high_9 + low_9) / 2

    # 기준선(Kijun-sen): 26일 기간의 (최고가 + 최저가) / 2
    high_26 = df['high'].rolling(window=26).max()
    low_26 = df['low'].rolling(window=26).min()
    df['kijun_sen'] = (high_26 + low_26) / 2

    # 선행스팬1(Senkou Span A): (전환선 + 기준선) / 2. (차트 플롯 시 26기간 앞으로 이동)
    df['senkou_span_a_unshifted'] = (df['tenkan_sen'] + df['kijun_sen']) / 2

    # 선행스팬2(Senkou Span B): 52일 기간의 (최고가 + 최저가) / 2. (차트 플롯 시 26기간 앞으로 이동)
    high_52 = df['high'].rolling(window=52).max()
    low_52 = df['low'].rolling(window=52).min()
    df['senkou_span_b_unshifted'] = (high_52 + low_52) / 2

    # 후행스팬(Chikou Span): 현재 종가를 26일 전에 플롯 (차트 표시용 데이터)
    df['chikou_span_plot'] = df['close'].shift(-26)
    return df


def step_01_process_and_filter(all_tickers):
    """
    Step 01: 모든 티커의 4시간봉 데이터 가져오기, 필요 지표 계산 및
    60 이동평균선 상승 + 일목균형표 후행스팬 조건을 만족하는 종목 필터링
    """
    logger.info("\n[1단계] 4시간봉 데이터 가져오기, 지표 계산 및 60MA/후행스팬 조건 필터링 시작...")

    if not all_tickers:
        logger.info("가져올 티커 목록이 없습니다.")
        return []

    result = []
    # KLINE_INTERVAL_4HOUR은 Client 객체에 정의되어 있음
    kline_interval = Client.KLINE_INTERVAL_4HOUR

    for ticker in all_tickers:
        try:
            df = get_ohlcv_binance(ticker, interval=kline_interval, limit=200)
            
            if df is None or len(df) < 2: # 최소 2개는 있어야 마지막 행 제거 가능
                 logger.debug(f"[{ticker}] 1단계: 초기 데이터 부족 (가져온 캔들 수: {len(df) if df is not None else 0})")
                 continue
            
            df = df.iloc[:-1] # 마지막 미완료 캔들 제거
            
            # MA60, 60캔들 전 종가, 26캔들 전 MA60 등을 계산하기에 충분한 데이터 확인 (최소 61개 캔들 필요)
            if df.empty or len(df) < 61:
                 logger.debug(f"[{ticker}] 1단계: 미완료 캔들 제거 후 데이터 부족 (캔들 수: {len(df)}), 최소 61개 필요")
                 continue
            
            logger.debug(f"--- 티커: {ticker} ---")
                       
            df['ma60'] = df['close'].rolling(window=60).mean()
            df = calculate_ichimoku(df) # 일목균형표 계산 (unshifted spans)

            current_price = df['close'].iloc[-1]
            if pd.isna(current_price):
                logger.debug(f"[{ticker}] 1단계: 현재가가 NaN입니다.")
                continue

            ma60_value = df['ma60'].iloc[-1]
            # "26캔들 전 MA60" -> 현재 완료된 캔들(-1) 기준으로 26캔들 이전(-1-26 = -27)의 MA60 값
            ma60_26_periods_ago = df['ma60'].shift(26).iloc[-1] if len(df) >= 27 else float('nan')
            
            # "60캔들 전 종가" -> 현재 완료된 캔들(-1) 기준으로 60캔들 이전(-1-60 = -61)의 종가
            lag_60_close = df['close'].shift(60).iloc[-1] if len(df) >= 61 else float('nan')

            lag_gap = current_price - lag_60_close if pd.notna(lag_60_close) else float('nan')
            lag_gap_percent = (lag_gap / lag_60_close) * 100 if pd.notna(lag_60_close) and lag_60_close != 0 else float('nan')

            # 현재 캔들 제외, 이전 60캔들 중 최고 종가 (df.iloc[-61] 부터 df.iloc[-2] 까지)
            highest_close_60d = df['close'].iloc[-61:-1].max() if len(df) >= 61 else float('nan')
            
            # "미래" 선행스팬 값 (현재 데이터 기준으로 계산되어 26기간 뒤에 그려질 값들)
            senkou_span_a_future = df['senkou_span_a_unshifted'].iloc[-1]
            senkou_span_b_future = df['senkou_span_b_unshifted'].iloc[-1]
            
            # "현재" 선행스팬 값 (26기간 전 데이터 기준으로 계산되어 현재 그려질 값들)
            # df['senkou_span_a_unshifted'].iloc[-26] -> 현재 기준 -25번째 캔들의 값 (26개 이전)
            senkou_span_a_current = df['senkou_span_a_unshifted'].shift(26).iloc[-1] if len(df) >= 27 else float('nan')
            senkou_span_b_current = df['senkou_span_b_unshifted'].shift(26).iloc[-1] if len(df) >= 27 else float('nan')


            condition1_ma = pd.notna(ma60_value) and current_price > ma60_value
            condition2_chikou = pd.notna(ma60_26_periods_ago) and current_price > ma60_26_periods_ago

            if condition1_ma and condition2_chikou:
                ticker_name = ticker # 바이낸스 심볼은 KRW- 접두사 없음
                
                current_price_float = float(current_price)
                ma60_float = float(ma60_value) if pd.notna(ma60_value) else float('nan')
                price_26_periods_ago_float = float(ma60_26_periods_ago) if pd.notna(ma60_26_periods_ago) else float('nan')
                lag_60_close_float = float(lag_60_close) if pd.notna(lag_60_close) else float('nan')
                highest_close_60d_float = float(highest_close_60d) if pd.notna(highest_close_60d) else 0
                
                senkou_a_future_float = float(senkou_span_a_future) if pd.notna(senkou_span_a_future) else float('nan')
                senkou_b_future_float = float(senkou_span_b_future) if pd.notna(senkou_span_b_future) else float('nan')
                senkou_a_current_float = float(senkou_span_a_current) if pd.notna(senkou_span_a_current) else float('nan')
                senkou_b_current_float = float(senkou_span_b_current) if pd.notna(senkou_span_b_current) else float('nan')

                ma60_ratio_val = current_price_float / ma60_float if ma60_float != 0 and pd.notna(ma60_float) else float('inf')
                chikou_ratio_val = current_price_float / price_26_periods_ago_float if price_26_periods_ago_float != 0 and pd.notna(price_26_periods_ago_float) else float('inf')
                lag_gap_val = lag_gap # 이미 계산됨
                lag_gap_percent_val = lag_gap_percent # 이미 계산됨
                cloud_ratio_val = senkou_a_future_float / senkou_b_future_float if senkou_b_future_float != 0 and pd.notna(senkou_a_future_float) and pd.notna(senkou_b_future_float) else float('inf')
                high_ratio_val = current_price_float / highest_close_60d_float if highest_close_60d_float != 0 and pd.notna(highest_close_60d_float) else float('inf')

                processed_data = {
                    "ticker": ticker, "name": ticker_name, "current_price": current_price_float,
                    "ma60": ma60_float, "ma60_ratio": ma60_ratio_val,
                    "price_26_periods_ago": price_26_periods_ago_float, # MA60 of 26 periods ago
                    "chikou_span_current": current_price_float, "chikou_ratio": chikou_ratio_val,
                    "lag_60_close": lag_60_close_float, "lag_gap": lag_gap_val, "lag_gap_percent": lag_gap_percent_val,
                    "senkou_span_a_future": senkou_a_future_float, # For Step 3
                    "senkou_span_b_future": senkou_b_future_float, # For Step 3
                    "senkou_span_a_current": senkou_a_current_float, # For Step 4
                    "senkou_span_b_current": senkou_b_current_float, # For Step 4
                    "cloud_ratio": cloud_ratio_val, # Based on future spans
                    "highest_close_60d": highest_close_60d_float, "high_ratio": high_ratio_val
                }
                result.append(processed_data)
            time.sleep(0.15) # API 호출 간격 조절
                           
        except Exception as e:
            logger.error(f"[{ticker}] 처리 중 오류 발생 (1단계): {e}", exc_info=True)

    logger.info(f"\n[1단계] 총 {len(result)}개 종목이 조건을 만족하고 다음 단계로 전달됩니다.")
    return result

def step_02_find_rising_lag_gap(step01_results):
    logger.info("\n[2단계] 60개 캔들 종이격 상승 종목 필터링 시작...")
    if not step01_results:
        logger.info("1단계에서 필터링된 종목이 없습니다. 2단계를 건너뜁니다.")
        return []
    rising_lag_gap_coins = []
    for coin in step01_results:
        try:
            lag_gap = coin.get("lag_gap", float('-inf')) # lag_gap이 없을 경우 대비
            if pd.notna(lag_gap) and lag_gap > 0:
                rising_lag_gap_coins.append(coin)
        except Exception as e:
            logger.error(f"[{coin.get('ticker','N/A')}] 처리 중 오류 발생 (2단계): {e}")
    logger.info(f"\n[2단계] 총 {len(rising_lag_gap_coins)}개 종목이 60개 캔들 종이격 상승 조건을 만족합니다.")
    return rising_lag_gap_coins

def step_03_analyze_cloud(step02_results):
    logger.info("\n[3단계] 선행스팬(미래) 골든크로스 분석 시작 (4시간봉 기준)...")
    if not step02_results:
        logger.info("2단계에서 필터링된 종목이 없습니다. 3단계를 건너뜁니다.")
        return [] 
    golden_cross_tickers = []
    for coin in step02_results:
        try:
            # Step 1에서 저장한 "미래" 선행스팬 값 사용
            senkou_span_a = coin.get("senkou_span_a_future")
            senkou_span_b = coin.get("senkou_span_b_future")
            if pd.notna(senkou_span_a) and pd.notna(senkou_span_b) and senkou_span_a > senkou_span_b:
                golden_cross_tickers.append(coin)
        except Exception as e:
            logger.error(f"[{coin.get('ticker','N/A')}] 처리 중 오류 발생 (3단계): {e}")
    logger.info(f"\n[3단계] 총 {len(golden_cross_tickers)}개 종목이 선행스팬(미래) 골든크로스 조건을 만족합니다.")
    return golden_cross_tickers

def step_04_new_condition_filter(step03_results):
    logger.info("\n[4단계] 새로운 조건 필터링 시작 (현재 종가 > 현재 선행스팬 B > 현재 선행스팬 A)...")
    if not step03_results:
        logger.info("3단계에서 필터링된 종목이 없습니다. 4단계를 건너뜁니다.")
        return []
    passed_step04_tickers = []
    for coin_data in step03_results:
        try:
            current_price = coin_data.get('current_price')
            # Step 1에서 저장한 "현재" 선행스팬 값 사용
            senkou_span_a_now = coin_data.get('senkou_span_a_current')
            senkou_span_b_now = coin_data.get('senkou_span_b_current')

            if pd.notna(current_price) and pd.notna(senkou_span_a_now) and pd.notna(senkou_span_b_now):
                condition_met = (current_price > senkou_span_b_now) and \
                                (senkou_span_b_now > senkou_span_a_now)
                if condition_met:
                    passed_step04_tickers.append(coin_data)
        except Exception as e:
            logger.error(f"[{coin_data.get('ticker','N/A')}] 처리 중 오류 발생 (4단계): {e}")
    logger.info(f"\n[4단계] 총 {len(passed_step04_tickers)}개 종목이 (현재 종가 > 현재 선행스팬 B > 현재 선행스팬 A) 조건을 만족합니다.")
    return passed_step04_tickers

def step_05_find_low_and_check_period(step04_results):
    logger.info("\n[5단계] 최저가 발생 후 기간 조건 필터링 시작...")
    if not step04_results:
        logger.info("4단계에서 필터링된 종목이 없습니다. 5단계를 건너뜁니다.")
        return []
    
    passed_step05_tickers = []
    kline_interval = Client.KLINE_INTERVAL_4HOUR

    for coin_data in step04_results:
        ticker = coin_data.get('ticker', 'N/A')
        try:
            ohlcv_count_for_step5 = 60 # 최근 60캔들 조회
            df = get_ohlcv_binance(ticker, interval=kline_interval, limit=ohlcv_count_for_step5)
            
            min_candles_needed_for_logic = 34 
            if df is None or len(df) < min_candles_needed_for_logic:
                logger.debug(f"  - [{ticker}] 5단계 데이터 부족 (요청: {ohlcv_count_for_step5}, 실제: {len(df) if df is not None else 0}, 필요: {min_candles_needed_for_logic}). 건너뜁니다.")
                continue
            
            df = df.iloc[:-1] # 마지막 미완료 캔들 제거
            if len(df) < min_candles_needed_for_logic -1: 
                logger.debug(f"  - [{ticker}] 5단계: 미완료 캔들 제거 후 데이터 부족 (캔들 수: {len(df)}).")
                continue

            # 조회한 df (최근 60개 미만 완료 캔들) 내에서 최저 종가 찾기
            search_series = df['close']
            if search_series.empty:
                continue
            
            lowest_price_date_idx = search_series.idxmin()
            idx_lowest_in_df = df.index.get_loc(lowest_price_date_idx)
            idx_current_in_df = len(df) - 1

            candles_since_lowest = idx_current_in_df - idx_lowest_in_df

            if 26 <= candles_since_lowest <= 33:
                passed_step05_tickers.append(coin_data)
            time.sleep(0.15) 
        except Exception as e:
            logger.error(f"[{ticker}] 처리 중 오류 발생 (5단계): {e}", exc_info=True)

    logger.info(f"\n[5단계] 총 {len(passed_step05_tickers)}개 종목이 최저가 발생 후 기간 조건을 만족합니다.")
    return passed_step05_tickers

def save_final_results(final_tickers, filename_override=None):
    """최종 결과를 JSON 파일로 저장하고 기존 파일을 업데이트"""
    # DATA_DIR_SELECT 전역 변수 사용
    filename = filename_override if filename_override else "60Ma_CoinSelect_Binance_Final.json" # 기본 파일명 변경
    
    if not os.path.exists(DATA_DIR_SELECT):
        os.makedirs(DATA_DIR_SELECT)
        logger.info(f"'{DATA_DIR_SELECT}' 폴더를 생성했습니다.")

    file_path = os.path.join(DATA_DIR_SELECT, filename)

    final_result = {
        "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(final_tickers),
        "tickers": final_tickers
    }

    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(final_result, f, ensure_ascii=False, indent=4)
        if final_tickers:
            logger.info(f"\n{len(final_tickers)}개 종목 결과가 {file_path} 파일에 저장되었습니다.")
        else:
            logger.info(f"\n조건 만족 종목 없음. {file_path} 파일이 (빈 목록으로) 업데이트되었습니다.")
    except Exception as e:
        logger.error(f"결과 저장 중 오류 발생: {e}")
    return file_path

def run_all_steps():
    """모든 스텝을 순차적으로 실행"""
    logger.info("바이낸스 선물 종목 분석 시작 (4시간봉 기준)...")
    logger.info("분석 조건: 1단계 -> 2단계 -> 3단계 -> 4단계 -> 5단계(최저가 후 기간체크)")

    all_tickers = save_all_tickers_binance()
    if not all_tickers:
        logger.warning("티커 목록을 가져오지 못했습니다. 분석을 중단합니다.")
        return

    step01_results = step_01_process_and_filter(all_tickers)
    save_final_results(step01_results, filename_override="60Ma_Coinselect_Binance_1Stage.json")

    step02_results = step_02_find_rising_lag_gap(step01_results)
    save_final_results(step02_results, filename_override="60Ma_Coinselect_Binance_2Stage.json")

    step03_results = step_03_analyze_cloud(step02_results)
    save_final_results(step03_results, filename_override="60Ma_Coinselect_Binance_3Stage.json")

    step04_results = step_04_new_condition_filter(step03_results)
    save_final_results(step04_results, filename_override="60Ma_Coinselect_Binance_4Stage.json")

    step05_results = step_05_find_low_and_check_period(step04_results)
    final_results = step05_results
    
    if final_results:
         final_results = sorted(final_results, key=lambda x: x.get('high_ratio', 0), reverse=True)

    # 최종 결과 파일명은 trading_executor_binance.py의 SELECTED_TICKERS_FILE과 일치시킬 수 있음
    # SELECTED_TICKERS_FILE = "60Ma_Coinselect_5Stage.json" (executor 기준)
    save_final_results(final_results, filename_override="60Ma_Coinselect_5Stage.json") # 최종 파일명 일치
    
    if final_results:
        logger.info("\n===== 최종 선별된 종목 (5단계 결과) =====")
        for i, item in enumerate(final_results, 1):
            logger.info(f"{i}. {item['name']} ({item['ticker']})")
            logger.info(f"   - 현재가: {item.get('current_price', 0):.4f}") # USDT 페어는 소수점 많이 표시
            logger.info(f"   - 비율(MA60/후행/종이격/구름/신고가): "
                        f"{item.get('ma60_ratio', 0):.4f} / "
                        f"{item.get('chikou_ratio', 0):.4f} / "
                        f"{item.get('lag_gap_percent', 0):.2f}% / "
                        f"{item.get('cloud_ratio', 0):.4f} / "
                        f"{item.get('high_ratio', 0):.4f}")
        logger.info("-" * 30)
    else:
        logger.info("\n5단계 조건을 만족하는 종목이 없습니다.")
    logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 종목 분석 실행 완료.")

if __name__ == "__main__":
    logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 바이낸스 선물 종목 분석 스크립트 실행 시작...")
    run_all_steps()
    # # 스케줄링이 필요하다면 아래 주석 해제 및 조정
    # logger.info("스케줄러를 설정하려면 해당 코드를 활성화하세요.")
    # schedule.every().day.at("09:00").do(run_all_steps) # 예시: 매일 09:00 실행
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
