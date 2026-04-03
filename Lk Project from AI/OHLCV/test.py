import pandas as pd
from pathlib import Path
import json # To handle potential JSONDecodeError with more information

# --- 설정 ---
# 저장된 데이터 디렉토리 이름 (OHLCV_UPBIT_KRW_MIN15.PY 스크립트의 SAVE_DIR_NAME과 동일해야 함)
SAVE_DIR_NAME = "upbit_ohlcv_min60"
# 확인할 티커 (예: "krw-btc") - 파일명에 사용된 티커명과 일치해야 함
TICKER_SYMBOL = "krw-btc"
# OHLCV 간격 (예: "minute15") - 파일명에 사용된 간격과 일치해야 함
OHLCV_INTERVAL = "min60"

# 파일 이름 구성 (OHLCV_UPBIT_KRW_MIN15.PY 스크립트의 파일명 규칙과 동일하게)
# 예: upbit_btc_minute15.json
FILENAME = f"upbit_{TICKER_SYMBOL.lower()}_{OHLCV_INTERVAL}.json"
FILE_PATH = Path(SAVE_DIR_NAME) / FILENAME

def load_and_display_btc_data(file_path: Path):
    """
    지정된 경로의 BTC OHLCV JSON 파일을 로드하고 데이터를 표시합니다.

    Args:
        file_path (Path): 불러올 JSON 파일의 경로.
    """
    if not file_path.exists():
        print(f"오류: 파일을 찾을 수 없습니다 - {file_path}")
        return

    print(f"'{file_path}' 파일에서 데이터를 불러옵니다...")
    try:
        # JSON 파일을 DataFrame으로 읽어옵니다.
        # OHLCV_UPBIT_KRW_MIN15.PY 에서 'iso' 형식으로 날짜를 저장했으므로,
        # pandas가 자동으로 DatetimeIndex로 변환하려고 시도합니다.
        # 명시적으로 인덱스를 datetime으로 변환하고 UTC로 설정할 수 있습니다.
        df = pd.read_json(file_path)
        if df.empty:
            print("파일이 비어있습니다.")
            return

        # 인덱스를 datetime 객체로 변환하고 UTC 시간대로 설정합니다.
        # (저장 시점에 UTC로 저장되었거나, 시간대 정보가 없는 ISO 문자열로 저장된 경우)
        df.index = pd.to_datetime(df.index, utc=True)
        df.sort_index(inplace=True) # 혹시 모르니 시간순 정렬

        print("\n--- 데이터 정보 ---")
        df.info()

        print("\n--- 데이터 처음 5행 ---")
        print(df.head())

        print("\n--- 데이터 마지막 5행 ---")
        print(df.tail())

        print("\n--- 기본 통계 ---")
        print(df.describe())

        # 특정 날짜/시간 범위의 데이터 조회 예시 (필요에 따라 주석 해제)
        # if not df.empty:
        #     try:
        #         # 예: 2023년 10월 26일 데이터 조회 (UTC 기준)
        #         specific_day_data = df.loc['2023-10-26']
        #         print("\n--- 2023-10-26 데이터 (UTC) ---")
        #         print(specific_day_data)
        #
        #         # 예: 특정 시간 이후 데이터 조회
        #         # specific_time = pd.Timestamp('2023-10-26 12:00:00', tz='UTC')
        #         # data_after_time = df[df.index >= specific_time]
        #         # print(f"\n--- {specific_time} 이후 데이터 ---")
        #         # print(data_after_time.head())
        #     except KeyError:
        #         print("\n지정한 날짜/시간에 해당하는 데이터가 없습니다.")
        #     except Exception as e:
        #         print(f"\n특정 날짜/시간 조회 중 오류 발생: {e}")


    except json.JSONDecodeError as jde:
        print(f"오류: JSON 파일 파싱 중 오류가 발생했습니다. 파일이 손상되었거나 유효한 JSON 형식이 아닐 수 있습니다. - {file_path}")
        print(f"오류 상세: {jde}")
    except Exception as e:
        print(f"오류: 데이터를 불러오거나 처리하는 중 예외가 발생했습니다 - {file_path}")
        print(f"오류 상세: {e}")

if __name__ == "__main__":
    load_and_display_btc_data(FILE_PATH)

    # 만약 다른 티커나 간격의 파일을 보고 싶다면,
    # FILENAME과 FILE_PATH를 직접 지정하여 함수를 호출할 수 있습니다.
    # 예시:
    # other_file = Path(SAVE_DIR_NAME) / "upbit_eth_minute15.json"
    # if other_file.exists():
    #     load_and_display_btc_data(other_file)
    # else:
    #     print(f"추가 파일 예시: {other_file}을 찾을 수 없습니다.")
