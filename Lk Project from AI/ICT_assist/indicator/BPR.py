import pandas as pd
# 같은 indicator 폴더에 있는 FVG.py에서 함수들을 가져옵니다.
from FVG import find_bullish_fvg, find_bearish_fvg

def find_bpr(df: pd.DataFrame):
    """
    데이터프레임에서 BPR(Balanced Price Range)을 찾습니다.

    BPR은 상승 FVG와 하락 FVG가 겹치는(overlapping) 영역입니다.

    Args:
        df (pd.DataFrame): 'open', 'high', 'low', 'close' 컬럼과
                           datetime 인덱스를 포함하는 OHLC 데이터.

    Returns:
        list[dict]: 발견된 모든 BPR 목록.
                    각 BPR은 딕셔너리 형태입니다.
                    [{'bullish_fvg_time': ..., 'bearish_fvg_time': ..., 'top': ..., 'bottom': ...}, ...]
    """
    bullish_fvgs = find_bullish_fvg(df)
    bearish_fvgs = find_bearish_fvg(df)
    bpr_list = []

    # 모든 상승 FVG와 하락 FVG 조합을 비교하여 겹치는 구간을 찾습니다.
    for bullish_fvg in bullish_fvgs:
        for bearish_fvg in bearish_fvgs:
            # 두 FVG의 가격 범위
            bull_bottom, bull_top = bullish_fvg['bottom'], bullish_fvg['top']
            bear_bottom, bear_top = bearish_fvg['bottom'], bearish_fvg['top']

            # 겹치는지 확인하는 조건:
            # (한 구간의 시작점이 다른 구간의 끝점보다 작고) and (한 구간의 끝점이 다른 구간의 시작점보다 크다)
            if bull_bottom < bear_top and bull_top > bear_bottom:
                # 겹치는 영역(BPR) 계산
                bpr_bottom = max(bull_bottom, bear_bottom)
                bpr_top = min(bull_top, bear_top)

                # BPR 구간이 유효한지 확인 (top이 bottom보다 커야 함)
                if bpr_top > bpr_bottom:
                    bpr_info = {
                        'bullish_fvg_time': bullish_fvg['time'],
                        'bearish_fvg_time': bearish_fvg['time'],
                        'top': bpr_top,
                        'bottom': bpr_bottom
                    }
                    bpr_list.append(bpr_info)

    return bpr_list

# --- 이 아래는 BPR.py 파일을 직접 실행했을 때만 동작하는 예제 코드입니다. ---
if __name__ == '__main__':
    # BPR이 형성되는 가상의 데이터 생성
    timeframe = '1h'
    data = {
        'open':  [150, 140, 125, 120, 130, 145, 155],
        'high':  [155, 145, 130, 128, 138, 150, 160],
        'low':   [138, 128, 110, 118, 122, 142, 152],
        'close': [142, 129, 112, 125, 135, 148, 158]
    }
    index = pd.to_datetime(pd.date_range(start='2023-01-01', periods=len(data['open']), freq=timeframe))
    ohlc_df = pd.DataFrame(data, index=index)

    print("--- OHLC 데이터 ---")
    print(ohlc_df)
    print("\n" + "="*30 + "\n")

    # BPR 찾기
    bprs = find_bpr(ohlc_df)
    print(f"--- [{timeframe}] BPR(Balanced Price Range) 목록 ---")
    if bprs:
        for bpr in bprs:
            print(f"BPR 구간: ({bpr['bottom']:.2f} ~ {bpr['top']:.2f})")
            print(f"  - 하락 FVG 시간: {bpr['bearish_fvg_time']}")
            print(f"  - 상승 FVG 시간: {bpr['bullish_fvg_time']}")
    else:
        print("BPR을 찾지 못했습니다.")