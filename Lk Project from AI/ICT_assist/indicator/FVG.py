import pandas as pd

def find_bullish_fvg(df: pd.DataFrame):
    """
    데이터프레임에서 이상적인 상승 FVG(Ideal Bullish Fair Value Gap)를 찾습니다.

    상승 FVG는 3개의 연속된 캔들에서 첫 번째 캔들의 고점보다
    세 번째 캔들의 저점이 더 높을 때 형성됩니다.
    - [추가 조건 1] 2번째 캔들의 시가는 1번째 캔들의 고가보다 같거나 낮아야 합니다.
    - [추가 조건 2] 2번째 캔들의 종가는 3번째 캔들의 저가보다 같거나 높아야 합니다.

    Args:
        df (pd.DataFrame): 'open', 'high', 'low', 'close' 컬럼과
                           datetime 인덱스를 포함하는 OHLC 데이터.

    Returns:
        list[dict]: 발견된 모든 상승 FVG 목록.
                    각 FVG는 딕셔너리 형태입니다.
                    [{'time': 2번째 캔들 시간, 'top': 3번째 캔들 저점, 'bottom': 1번째 캔들 고점}, ...]
    """
    fvg_list = []
    # 데이터프레임의 마지막 2개 행은 3번째 캔들이 없으므로 제외하고 반복
    for i in range(len(df) - 2):
        candle1 = df.iloc[i]
        candle2 = df.iloc[i+1]
        candle3 = df.iloc[i+2]

        # 상승 FVG 조건: 1번째 캔들 고점 < 3번째 캔들 저점
        if (candle1['high'] < candle3['low'] and          # 기본 FVG 조건
            candle2['open'] <= candle1['high'] and        # 조건 1
            candle2['close'] >= candle3['low']):          # 조건 2
            fvg_info = {
                'time': candle2.name,          # 2번째 캔들의 시간 (인덱스)
                'top': candle3['low'],         # FVG 상단 (3번째 캔들 저점)
                'bottom': candle1['high'],     # FVG 하단 (1번째 캔들 고점)
            }
            fvg_list.append(fvg_info)

    return fvg_list

def find_bearish_fvg(df: pd.DataFrame):
    """
    데이터프레임에서 이상적인 하락 FVG(Ideal Bearish Fair Value Gap)를 찾습니다.

    하락 FVG는 3개의 연속된 캔들에서 첫 번째 캔들의 저점보다
    세 번째 캔들의 고점이 더 낮을 때 형성됩니다.
    - [추가 조건 1] 2번째 캔들의 시가는 1번째 캔들의 저가보다 같거나 높아야 합니다.
    - [추가 조건 2] 2번째 캔들의 종가는 3번째 캔들의 고가보다 같거나 낮아야 합니다.

    Args:
        df (pd.DataFrame): 'open', 'high', 'low', 'close' 컬럼과
                           datetime 인덱스를 포함하는 OHLC 데이터.

    Returns:
        list[dict]: 발견된 모든 하락 FVG 목록.
                    각 FVG는 딕셔너리 형태입니다.
                    [{'time': 2번째 캔들 시간, 'top': 1번째 캔들 저점, 'bottom': 3번째 캔들 고점}, ...]
    """
    fvg_list = []
    # 데이터프레임의 마지막 2개 행은 3번째 캔들이 없으므로 제외하고 반복
    for i in range(len(df) - 2):
        candle1 = df.iloc[i]
        candle2 = df.iloc[i+1]
        candle3 = df.iloc[i+2]

        # 하락 FVG 조건: 1번째 캔들 저점 > 3번째 캔들 고점
        if (candle1['low'] > candle3['high'] and          # 기본 FVG 조건
            candle2['open'] >= candle1['low'] and         # 조건 1
            candle2['close'] <= candle3['high']):         # 조건 2
            fvg_info = {
                'time': candle2.name,          # 2번째 캔들의 시간 (인덱스)
                'top': candle1['low'],         # FVG 상단 (1번째 캔들 저점)
                'bottom': candle3['high'],     # FVG 하단 (3번째 캔들 고점)
            }
            fvg_list.append(fvg_info)

    return fvg_list

# --- 이 아래는 FVG.py 파일을 직접 실행했을 때만 동작하는 예제 코드입니다. ---
if __name__ == '__main__':
    # 가상의 데이터 생성 (예: 1시간 봉)
    # 실제 사용 시에는 pyupbit, ccxt 등을 통해 데이터를 받아와야 합니다.
    timeframe = '1h'
    data = {
        'open':  [100, 110, 105, 130, 125, 120, 110, 95, 90, 80],
        'high':  [115, 120, 135, 140, 130, 125, 115, 100, 98, 85],
        'low':   [95,  108, 100, 128, 122, 118, 90,  88, 82, 75],
        'close': [110, 118, 130, 135, 128, 122, 95,  90, 85, 78]
    }
    # 타임프레임에 맞는 DatetimeIndex 생성
    index = pd.to_datetime(pd.date_range(start='2023-01-01', periods=len(data['open']), freq=timeframe))
    ohlc_df = pd.DataFrame(data, index=index)

    print("--- OHLC 데이터 ---")
    print(ohlc_df)
    print("\n" + "="*30 + "\n")

    # 상승 FVG 찾기
    bullish_fvgs = find_bullish_fvg(ohlc_df)
    print(f"--- [{timeframe}] 상승 FVG(Bullish FVG) 목록 ---")
    for fvg in bullish_fvgs:
        print(f"발생 시간: {fvg['time']}, FVG 구간: ({fvg['bottom']:.2f} ~ {fvg['top']:.2f})")

    print("\n" + "="*30 + "\n")

    # 하락 FVG 찾기
    bearish_fvgs = find_bearish_fvg(ohlc_df)
    print(f"--- [{timeframe}] 하락 FVG(Bearish FVG) 목록 ---")
    for fvg in bearish_fvgs:
        print(f"발생 시간: {fvg['time']}, FVG 구간: ({fvg['bottom']:.2f} ~ {fvg['top']:.2f})")