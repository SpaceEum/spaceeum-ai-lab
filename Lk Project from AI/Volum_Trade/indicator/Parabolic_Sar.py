import pandas as pd
import numpy as np

def calculate_parabolic_sar(df: pd.DataFrame, start: float = 0.02, increment: float = 0.02, maximum: float = 0.2) -> pd.DataFrame:
    """
    Parabolic SAR 지표를 계산하여 DataFrame에 추가합니다.
    
    Args:
        df (pd.DataFrame): 'high', 'low', 'close' 컬럼을 포함하는 데이터프레임
        start (float): 시작 가속도 계수 (기본값: 0.02)
        increment (float): 가속도 계수 증가량 (기본값: 0.02)
        maximum (float): 최대 가속도 계수 (기본값: 0.2)
        
    Returns:
        pd.DataFrame: 지표가 추가된 데이터프레임
    """
    df = df.copy()
    
    # 데이터 확인
    if len(df) < 2:
        return df
    
    high = df['high'].values
    low = df['low'].values
    close = df['close'].values
    
    # 결과 배열 초기화
    psar = np.zeros(len(df))
    psar_dir = np.zeros(len(df)) # 1: Up (Long), -1: Down (Short)
    
    # 초기값 설정 (첫 번째 봉 기준)
    # 일반적으로 첫 추세는 첫 봉의 종가가 시가보다 높으면 상승, 아니면 하락으로 가정하거나
    # 단순히 첫 봉의 High/Low를 기준으로 잡기도 함.
    # 여기서는 간단히 첫 봉의 종가와 시가 비교 또는 High/Low 비교로 초기화
    
    # 초기 추세 결정 (임의로 첫 번째 봉이 상승이면 Long, 하락이면 Short)
    # Pine Script의 ta.sar는 초기화 로직이 내부적으로 처리됨.
    # 일반적인 관례: 첫 SAR는 이전 추세의 극점(EP).
    
    trend = 1 # 1: Up, -1: Down
    sar = low[0] # 초기 SAR (상승 추세 가정 시 Low)
    ep = high[0] # 초기 EP (상승 추세 가정 시 High)
    af = start # 초기 가속도
    
    # 첫 번째 값 설정
    psar[0] = sar
    psar_dir[0] = trend

    for i in range(1, len(df)):
        prev_sar = psar[i-1]
        prev_trend = psar_dir[i-1]
        
        # SAR 계산
        sar = prev_sar + af * (ep - prev_sar)
        
        # 추세 반전 확인
        if prev_trend == 1: # 상승 추세였을 때
            if low[i] < sar: # 저가가 SAR보다 낮아지면 하락 반전
                trend = -1
                sar = ep # 반전 시 SAR는 이전 추세의 EP
                ep = low[i] # 새로운 하락 추세의 EP 초기화
                af = start # AF 초기화
            else:
                trend = 1
                if high[i] > ep: # 신고가 갱신
                    ep = high[i]
                    af = min(af + increment, maximum)
                
                # 상승 추세에서 SAR는 이전 두 봉의 저가보다 높을 수 없음
                if i >= 1:
                    sar = min(sar, low[i-1])
                if i >= 2:
                    sar = min(sar, low[i-2])
                    
        else: # 하락 추세였을 때
            if high[i] > sar: # 고가가 SAR보다 높아지면 상승 반전
                trend = 1
                sar = ep # 반전 시 SAR는 이전 추세의 EP
                ep = high[i] # 새로운 상승 추세의 EP 초기화
                af = start # AF 초기화
            else:
                trend = -1
                if low[i] < ep: # 신저가 갱신
                    ep = low[i]
                    af = min(af + increment, maximum)
                
                # 하락 추세에서 SAR는 이전 두 봉의 고가보다 낮을 수 없음
                if i >= 1:
                    sar = max(sar, high[i-1])
                if i >= 2:
                    sar = max(sar, high[i-2])
        
        psar[i] = sar
        psar_dir[i] = trend
        
    df['psar'] = psar
    df['psar_dir'] = psar_dir
    
    # Buy/Sell Signal
    # buySignal = dir == 1 and dir[1] == -1
    # sellSignal = dir == -1 and dir[1] == 1
    df['psar_buy'] = (df['psar_dir'] == 1) & (df['psar_dir'].shift(1) == -1)
    df['psar_sell'] = (df['psar_dir'] == -1) & (df['psar_dir'].shift(1) == 1)
    
    # Colors (Hex)
    # psarColor = dir == 1 ? #3388bb : #fdcc02
    df['psar_color'] = np.where(df['psar_dir'] == 1, '#3388bb', '#fdcc02')
    
    return df

if __name__ == "__main__":
    # 테스트용 더미 데이터 생성
    np.random.seed(42)
    periods = 100
    
    # 간단한 추세 데이터 생성
    close = np.cumsum(np.random.randn(periods)) + 100
    high = close + np.random.rand(periods)
    low = close - np.random.rand(periods)
    
    data = {
        'high': high,
        'low': low,
        'close': close
    }
    
    df = pd.DataFrame(data)
    
    # 지표 계산
    result_df = calculate_parabolic_sar(df)
    
    print("=== 결과 데이터프레임 (상위 10개) ===")
    print(result_df[['close', 'psar', 'psar_dir', 'psar_buy', 'psar_sell', 'psar_color']].head(10))
    
    print("\n=== 결과 데이터프레임 (하위 5개) ===")
    print(result_df[['close', 'psar', 'psar_dir', 'psar_buy', 'psar_sell', 'psar_color']].tail())
    print("\n작업이 완료되었습니다.")
