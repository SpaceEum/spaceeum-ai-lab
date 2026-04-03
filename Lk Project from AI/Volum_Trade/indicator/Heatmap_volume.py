import pandas as pd
import numpy as np

def calculate_heatmap_volume(df: pd.DataFrame, length: int = 610, slength: int = 610) -> pd.DataFrame:
    """
    Heatmap Volume 지표를 계산하여 DataFrame에 추가합니다.
    
    Args:
        df (pd.DataFrame): 'open', 'close', 'volume' 컬럼을 포함하는 데이터프레임
        length (int): 이동평균(SMA) 기간 (기본값: 610)
        slength (int): 표준편차(StDev) 기간 (기본값: 610)
        
    Returns:
        pd.DataFrame: 지표가 추가된 데이터프레임
    """
    # 데이터프레임 복사본 생성 (원본 변경 방지)
    df = df.copy()
    
    # 입력 파라미터 (Pine Script Inputs)
    thresholdExtraHigh = 3.0
    thresholdHigh = 1.5
    thresholdMedium = 0.5
    thresholdNormal = -0.5
    
    # 색상 설정 (Hex Codes)
    # Heatmap Colors
    chm1 = '#ff0000'  # Extra High (Red)
    chm2 = '#ff7800'  # High (Orange)
    chm3 = '#ffcf03'  # Medium (Yellow)
    chm4 = '#a0d6dc'  # Normal
    chm5 = '#1f9cac'  # Low
    
    # Up Colors
    cup_xh = '#00FF00'
    cup_h = '#30FF30'
    cup_m = '#60FF60'
    cup_n = '#8FFF8F'
    cup_l = '#BFFFBF'
    
    # Down Colors
    cdn_xh = '#FF0000'
    cdn_h = '#FF3030'
    cdn_m = '#FF6060'
    cdn_n = '#FF8F8F'
    cdn_l = '#FFBFBF'
    
    # 기본 계산
    # dir = close > open
    df['dir'] = df['close'] > df['open']
    
    # mean = ta.sma(volume, length)
    df['mean_volume'] = df['volume'].rolling(window=length).mean()
    
    # std = ta.stdev(volume, slength)
    # pandas rolling std uses N-1 by default (ddof=1), similar to sample stdev
    df['std_volume'] = df['volume'].rolling(window=slength).std()
    
    # stdbar = (volume - mean) / std
    df['stdbar'] = (df['volume'] - df['mean_volume']) / df['std_volume']
    
    # 색상 및 카테고리 결정 로직
    # Pine Script 로직:
    # bcolor = stdbar > thresholdExtraHigh ? dir ? cthresholdExtraHighUp : cthresholdExtraHighDn : ...
    
    # 여기서는 'Heatmap' 모드와 'Up/Down' 모드 중 기본적으로 'Heatmap' 모드의 색상을 우선적으로 적용하는 로직을 구현하거나,
    # 사용자가 쉽게 선택할 수 있도록 구조화합니다. 
    # 요청하신 코드는 변환이 주 목적이므로, 가장 정보량이 많은 'Up/Down' 모드 색상 로직을 기반으로 
    # 각 상태(Extra High, High 등)를 식별하는 컬럼을 추가합니다.
    
    conditions = [
        (df['stdbar'] > thresholdExtraHigh),
        (df['stdbar'] > thresholdHigh),
        (df['stdbar'] > thresholdMedium),
        (df['stdbar'] > thresholdNormal)
    ]
    
    choices_category = ['Extra High', 'High', 'Medium', 'Normal']
    
    df['vol_category'] = np.select(conditions, choices_category, default='Low')
    
    # 색상 할당 함수 (Up/Down 모드 기준 - Pine Script의 기본값 로직 참조)
    def get_color(row):
        cat = row['vol_category']
        is_up = row['dir']
        
        if cat == 'Extra High':
            return cup_xh if is_up else cdn_xh
        elif cat == 'High':
            return cup_h if is_up else cdn_h
        elif cat == 'Medium':
            return cup_m if is_up else cdn_m
        elif cat == 'Normal':
            return cup_n if is_up else cdn_n
        else: # Low
            return cup_l if is_up else cdn_l

    # Heatmap 모드 색상 (Volume 만 고려)
    def get_heatmap_color(row):
        cat = row['vol_category']
        if cat == 'Extra High': return chm1
        elif cat == 'High': return chm2
        elif cat == 'Medium': return chm3
        elif cat == 'Normal': return chm4
        else: return chm5

    df['color_updown'] = df.apply(get_color, axis=1)
    df['color_heatmap'] = df.apply(get_heatmap_color, axis=1)
    
    return df

if __name__ == "__main__":
    # 테스트용 더미 데이터 생성
    np.random.seed(42)
    periods = 1000
    data = {
        'open': np.random.uniform(100, 200, periods),
        'close': np.random.uniform(100, 200, periods),
        'volume': np.random.uniform(1000, 50000, periods)
    }
    # Volume에 스파이크 추가 (테스트용)
    data['volume'][990:] = data['volume'][990:] * 5
    
    df = pd.DataFrame(data)
    
    # 지표 계산 (기간을 짧게 조정하여 테스트)
    result_df = calculate_heatmap_volume(df, length=20, slength=20)
    
    print("=== 결과 데이터프레임 (상위 5개) ===")
    print(result_df[['volume', 'mean_volume', 'stdbar', 'vol_category', 'color_heatmap']].tail())
    print("\n작업이 완료되었습니다.")
