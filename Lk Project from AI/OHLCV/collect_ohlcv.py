# -*- coding: utf-8 -*- 
import os
import sys
import time
import shutil
import logging
import pandas as pd
import ccxt
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import pytz

# ==============================================================================
# 설정
# ==============================================================================
# 이 파일의 상위 폴더를 프로젝트 루트로 간주합니다.
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_ROOT = PROJECT_ROOT
LOG_FILE = os.path.join(PROJECT_ROOT, "collector.log") 
ARCHIVE_DIR = os.path.join(DATA_ROOT, 'delisted_archive')

EXCLUDED_SYMBOLS = [
    'BTCST/USDT:USDT'
]

TIMEFRAME_DIRS = {
    '30m': 'Binance_Fureres_USDT_30Minute_ohlcv',
    '1h': 'Binance_Fureres_USDT_1Hour_ohlcv',
    '4h': 'Binance_Fureres_USDT_4Hour_ohlcv',
    '1d': 'Binance_Fureres_USDT_1Day_ohlcv',
}

class BinanceDataCollector:
    """바이낸스 선물 데이터를 객체지향(OOP) 방식으로 수집/관리하는 클래스"""
    
    def __init__(self):
        self.setup_logging()
        self.kst = pytz.timezone('Asia/Seoul')
        self.utc = pytz.utc
        self.start_timestamp = int(datetime(2019, 9, 1, tzinfo=self.utc).timestamp() * 1000)
        self.client = self._get_binance_client()
        self._create_directories()

    def setup_logging(self):
        self.logger = logging.getLogger()
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        self.logger.addHandler(stream_handler)
        
        self.logger.info("OOP 기반 데이터 수집기 로깅이 설정되었습니다.")

    def _create_directories(self):
        os.makedirs(ARCHIVE_DIR, exist_ok=True)
        for _, dir_name in TIMEFRAME_DIRS.items():
            os.makedirs(os.path.join(DATA_ROOT, dir_name), exist_ok=True)

    def clear_all_data_folders(self):
        """기존 수집된 모든 데이터 폴더를 비웁니다 (완전 초기화)."""
        self.logger.info("모든 데이터 폴더의 기존 데이터를 완전 삭제합니다...")
        for _, dir_name in TIMEFRAME_DIRS.items():
            full_dir_path = os.path.join(DATA_ROOT, dir_name)
            if os.path.exists(full_dir_path):
                for file_name in os.listdir(full_dir_path):
                    file_path = os.path.join(full_dir_path, file_name)
                    try:
                        if os.path.isfile(file_path):
                            os.remove(file_path)
                    except Exception as e:
                        self.logger.error(f"초기화 중 파일 삭제 실패: {file_path} - {e}")
        self.logger.info("모든 데이터 폴더가 성공적으로 비워졌습니다. 이제 완전히 빈 상태로 새로 수집됩니다.")

    def _get_binance_client(self):
        try:
            return ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}})
        except Exception as e:
            self.logger.error(f"CCXT 클라이언트 초기화 실패: {e}")
            return None

    def get_active_usdt_future_tickers(self):
        if not self.client: return []
        try:
            markets = self.client.load_markets()
            active_usdt_futures = []

            for market in markets.values():
                is_usdt_future = (
                    market.get('quote') and market['quote'].upper() == 'USDT' and
                    market.get('settle') and market['settle'].upper() == 'USDT' and
                    market.get('type') == 'swap' and
                    not market.get('inverse')
                )
                if is_usdt_future:
                    # 'active' 키 검사를 통해 거래 중지 및 상장 폐지 종목 원천 차단
                    if market.get('active') == True:
                        active_usdt_futures.append((market['id'], market['symbol']))

            self.logger.info(f"바이낸스에서 살아있는 {len(active_usdt_futures)}개의 USDT 무기한 종목을 스캔했습니다.")
            return sorted(active_usdt_futures)
        except Exception as e:
            self.logger.error(f"티커 목록 스캔 오류: {e}")
            return []

    def archive_delisted_tickers(self, active_tickers):
        """활성화 상태가 아닌 상폐/중지 종목의 파일을 분리 보관(Archive) 폴더로 옮깁니다."""
        active_ids = {t[0] for t in active_tickers}
        archived_count = 0

        for tf, dir_name in TIMEFRAME_DIRS.items():
            full_dir_path = os.path.join(DATA_ROOT, dir_name)
            if not os.path.exists(full_dir_path):
                continue
            
            for file_name in os.listdir(full_dir_path):
                if file_name.endswith('.csv') or file_name.endswith('.parquet'):
                    ticker_id = file_name.rsplit('.', 1)[0]
                    
                    if ticker_id not in active_ids:
                        src_path = os.path.join(full_dir_path, file_name)
                        
                        target_sub_dir = os.path.join(ARCHIVE_DIR, dir_name)
                        os.makedirs(target_sub_dir, exist_ok=True)
                        dest_path = os.path.join(target_sub_dir, file_name)
                        
                        try:
                            shutil.move(src_path, dest_path)
                            self.logger.info(f"상장 폐지 적발 - 파일 이동: {ticker_id} -> archive")
                            archived_count += 1
                        except Exception as e:
                            self.logger.error(f"파일 이동 실패: {file_name} - {e}")
                            
        if archived_count > 0:
            self.logger.info(f"총 {archived_count}개의 상장 폐지 파일을 Archive로 무사히 옮겼습니다.")

    def update_ohlcv_data(self, timeframe_str):
        if not self.client: return False
        
        # 1. 활성 티커 스캔 및 상장 폐지 티커 아카이브 분리
        active_tickers = self.get_active_usdt_future_tickers()
        if not active_tickers: 
            return False
            
        self.archive_delisted_tickers(active_tickers)
        
        self.logger.info(f"\n--- {timeframe_str} 단위 데이터 파케이 업데이트 시작 ---")
        data_dir = os.path.join(DATA_ROOT, TIMEFRAME_DIRS.get(timeframe_str))
        
        total_tickers = len(active_tickers)
        for i, (ticker_id, symbol) in enumerate(active_tickers):
            if symbol in EXCLUDED_SYMBOLS:
                continue
            try:
                progress = (i + 1) / total_tickers
                sys.stdout.write(f"\r[{'=' * int(progress * 20):<20}] {i+1}/{total_tickers} - {symbol} ({timeframe_str}) 동기화...")
                sys.stdout.flush()

                parquet_path = os.path.join(data_dir, f"{ticker_id}.parquet")
                
                since = self.start_timestamp
                existing_df = None

                # 기존 파케이 파일 먼저 찾기
                if os.path.exists(parquet_path):
                    try:
                        existing_df = pd.read_parquet(parquet_path)
                        if not existing_df.empty:
                            last_time_kst = existing_df['Open time'].iloc[-1]
                            if last_time_kst.tzinfo is None:
                                last_time_kst = self.kst.localize(last_time_kst)
                            since = int(last_time_kst.timestamp() * 1000)
                    except Exception as e:
                        self.logger.warning(f'\n{symbol} Parquet 읽기 오류. 손상되었으므로 삭제하고 재수집합니다.')
                        existing_df = None
                        since = self.start_timestamp
                        os.remove(parquet_path)
                        
                # 데이터 별도 수집
                all_ohlcv = []
                current_since = since
                while True:
                    ohlcv = None
                    for limit_retries in range(3):
                        try:
                            ohlcv = self.client.fetch_ohlcv(symbol, timeframe_str, since=current_since, limit=1500)
                            break
                        except Exception as e:
                            time.sleep(5)
                    
                    if ohlcv is None or not ohlcv:
                        break 

                    all_ohlcv.extend(ohlcv)
                    current_since = ohlcv[-1][0] + 1
                    time.sleep(self.client.rateLimit / 1000)

                if not all_ohlcv: 
                    continue

                new_df = pd.DataFrame(all_ohlcv, columns=['Open time', 'Open', 'High', 'Low', 'Close', 'Volume'])
                new_df['Open time'] = pd.to_datetime(new_df['Open time'], unit='ms', utc=True)
                new_df = new_df.astype({'Open': 'float64', 'High': 'float64', 'Low': 'float64', 'Close': 'float64', 'Volume': 'float64'})

                timeframe_map = {
                    '1M': 'MS',   
                    '1w': 'W-MON', 
                    '1d': 'D', 
                    '4h': '4h', 
                    '1h': 'h', 
                    '30m': '30min', 
                    '15m': '15min', 
                    '5m': '5min', 
                    '3m': '3min', 
                    '1m': 'min'
                }
                
                if timeframe_str == '1M':
                    current_candle_start_utc = pd.to_datetime(datetime.now(self.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0))
                elif timeframe_str == '1w':
                    now = datetime.now(self.utc)
                    monday = now - pd.Timedelta(days=now.weekday())
                    current_candle_start_utc = pd.to_datetime(monday.replace(hour=0, minute=0, second=0, microsecond=0))
                else:
                    floor_unit = timeframe_map.get(timeframe_str, 'h')
                    current_candle_start_utc = pd.to_datetime(datetime.now(self.utc)).floor(floor_unit)

                # 최신(아직 완성되지 않은) 캔들은 과감히 버림
                new_df = new_df[new_df['Open time'] < current_candle_start_utc]

                if new_df.empty: 
                    continue

                new_df['Open time'] = new_df['Open time'].dt.tz_convert(self.kst).dt.tz_localize(None)
                
                if existing_df is not None:
                    final_df = pd.concat([existing_df, new_df]).drop_duplicates(subset='Open time', keep='last').sort_values(by='Open time')
                else:
                    final_df = new_df
                
                # 강제 Parquet 저장 (AI 사용에 가장 유리)
                final_df.to_parquet(parquet_path)
                    
            except ccxt.BadRequest:
                pass 
            except Exception as e:
                sys.stdout.write(f"\r{' ' * 80}\r")
                self.logger.error(f"\n수집 중 예외: {ticker_id} - {e}")
        
        sys.stdout.write(f"\r{' ' * 80}\r")
        self.logger.info(f"--- {timeframe_str} 동기화 완료 ---")
        return True

# ==============================================================================
# 스케줄러 관리기
# ==============================================================================
class CollectScheduler:
    def __init__(self):
        self.collector = BinanceDataCollector()
        
    def run_all_sequential(self):
        self.collector.logger.info("선택된 타임프레임 강제 순차 업데이트를 지시했습니다.")
        tfs = ['30m', '1h', '4h', '1d']
        for tf in tfs:
            self.collector.update_ohlcv_data(tf)
        self.collector.logger.info("강제 순차 업데이트가 무사히 성료되었습니다.")

    def run_auto_scheduler(self):
        self.collector.logger.info("최적 방어형 24H 스케줄러가 활성화되었습니다.")
        scheduler = BlockingScheduler(timezone='Asia/Seoul')
        
        # 시봉 이상 계열: 하루 한 번 아침 9시 이후
        scheduler.add_job(lambda: self.collector.update_ohlcv_data('30m'), 'cron', hour=9, minute=10, misfire_grace_time=300)
        scheduler.add_job(lambda: self.collector.update_ohlcv_data('1h'), 'cron', hour=9, minute=20, misfire_grace_time=300)
        scheduler.add_job(lambda: self.collector.update_ohlcv_data('4h'), 'cron', hour=9, minute=30, misfire_grace_time=300)
        scheduler.add_job(lambda: self.collector.update_ohlcv_data('1d'), 'cron', hour=9, minute=40, misfire_grace_time=300)
        
        print("\n================== 자동매매 데이터 펌프 ==================")
        print("  [수집 대상] 30m, 1h, 4h, 1d")
        print("  [시봉급 차트] 매일 아침 오전 9시 이후 동결된 장 일괄 동기화")
        print("     -> 해당 최적화를 통해 밴을 방지하고 CPU 점유를 최소화")
        print("==========================================================")
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            self.collector.logger.info("작업자 강제 인터럽트로 스케줄러가 정지되었습니다.")

def main():
    manager = CollectScheduler()

    while True:
        print("\n--- 바이낸스(Parquet / OOP / Archive) 콘솔 ---")
        print("1. [명령] 전체 즉시 동기화 실행 (신규 수집)")
        print("2. [데몬] 24H 장기 스케줄러 모드 가동")
        print("3. [초기화] 기존 수집 폴더 비우기 (경고: 모든 CSV/Parquet 영구삭제)")
        print("q. [종료] 패널 닫기")
        choice = input("입력: ").strip()

        if choice == '1':
            manager.run_all_sequential()
        elif choice == '2':
            manager.run_auto_scheduler()
            break
        elif choice == '3':
            confirm = input("정말로 하위 폴더들의 기존 데이터를 다 지우시겠습니까? (y/n): ")
            if confirm.lower() == 'y':
                manager.collector.clear_all_data_folders()
            else:
                print("초기화를 취소했습니다.")
        elif choice.lower() == 'q':
            break
        else:
            print("올바른 키를 눌러주십시오.")

if __name__ == "__main__":
    main()