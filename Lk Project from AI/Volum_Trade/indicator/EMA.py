import pandas as pd
import numpy as np

def calculate_ema(df: pd.DataFrame, period: int = 60, source: str = 'close') -> pd.DataFrame:
    """
    지수이동평균(EMA)을 계산하여 DataFrame에 추가합니다.
    
    Args:
        df (pd.DataFrame): 데이터프레임
        period (int): EMA 기간 (기본값: 60)
        source (str): 계산에 사용할 컬럼명 (기본값: 'close')
        
    Returns:
        pd.DataFrame: EMA 컬럼이 추가된 데이터프레임
    """
    df = df.copy()
    
    # EMA 계산
    # span=period는 alpha = 2 / (span + 1) 공식을 사용합니다.
    # adjust=False는 재귀적 공식을 사용하여 초기값의 영향이 시간이 지남에 따라 감소하는 방식입니다.
    # 이는 기술적 분석에서 일반적으로 사용되는 방식입니다.
    col_name = f'ema_{period}'
    df[col_name] = df[source].ewm(span=period, adjust=False).mean()
    
    return df

if __name__ == "__main__":
    # 테스트용 더미 데이터 생성
    np.random.seed(42)
    periods = 200
    
    # 간단한 추세 데이터 생성
    close = np.cumsum(np.random.randn(periods)) + 100
    
    data = {
        'close': close
    }
    
    df = pd.DataFrame(data)
    
    # 지표 계산 (기본값 60)
    result_df = calculate_ema(df, period=60)
    
    print(f"=== 결과 데이터프레임 (상위 5개) ===")
    print(result_df.head())
    
    print(f"\n=== 결과 데이터프레임 (하위 5개) ===")
    print(result_df.tail())
    
    print("\n작업이 완료되었습니다.")
