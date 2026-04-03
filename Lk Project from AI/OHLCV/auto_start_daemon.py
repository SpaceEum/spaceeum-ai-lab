import time
import os
import shutil
import subprocess
import sys

LOG_FILE = 'collector.log'
SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
# 코인투자 폴더가 가장 높은 최상위 폴더이므로 그 바로 하위에 전용 폴더 배치
TARGET_DIR = r"g:\내 드라이브\코인투자\Binance_OHLCV_Collector"

def is_sync_finished():
    try:
        with open(LOG_FILE, 'r', encoding='utf-8') as f:
            if "성료되었습니다" in f.read():
                return True
    except FileNotFoundError:
        pass
    return False

def copy_files_to_new_folder():
    print(f"\n[마이그레이션] 새 전용 폴더({TARGET_DIR})로 필요한 데이터 복사를 시작합니다.")
    os.makedirs(TARGET_DIR, exist_ok=True)
    
    # 1. 수집 파이썬 소스코드 복사
    shutil.copy2(os.path.join(SOURCE_DIR, 'collect_ohlcv.py'), TARGET_DIR)
    print(" - 수집 스크립트 복사 완료")
    
    # 2. 수집 대상인 30m, 1h, 4h, 1d 차트 및 아카이브 폴더만 선별하여 복사
    timeframe_dirs = [
        'Binance_Fureres_USDT_30Minute_ohlcv',
        'Binance_Fureres_USDT_1Hour_ohlcv',
        'Binance_Fureres_USDT_4Hour_ohlcv',
        'Binance_Fureres_USDT_1Day_ohlcv',
        'delisted_archive'
    ]
    
    for d in timeframe_dirs:
        src_d = os.path.join(SOURCE_DIR, d)
        dst_d = os.path.join(TARGET_DIR, d)
        if os.path.exists(src_d):
            shutil.copytree(src_d, dst_d, dirs_exist_ok=True)
            print(f" - 데이터 폴더 이전 완료: {d}")

if __name__ == "__main__":
    print("==========================================================================")
    print(" 감시 타이머: 초기화 수집이 완료되면 '코인투자' 하위 새 폴더로 자동 구성 시작")
    print("==========================================================================")
    
    while not is_sync_finished():
        time.sleep(60)
        
    print("\n[감지] 초기 1회성 데이터 백업이 완전히 끝났습니다!")
    
    try:
        copy_files_to_new_folder()
        print("\n필요한 소스코드와 4개 타임프레임 Parquet 데이터가 새 폴더로 구성되었습니다.")
        
        # CWD를 새 폴더로 변경
        os.chdir(TARGET_DIR)
        print(f"\n현 위치({os.getcwd()})에서 24시간 데몬 스케줄러를 자동 가동합니다!")
        
        # 새 폴더에서 1일 반복(24H 데몬) 스크립트 실행
        subprocess.Popen([
            sys.executable, "-c", 
            "from collect_ohlcv import CollectScheduler; m = CollectScheduler(); m.run_auto_scheduler()"
        ])
        
    except Exception as e:
        print(f"폴더 자동 구성 중 예외 발생: {e}")
