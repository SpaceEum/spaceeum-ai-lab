import requests
import datetime
from typing import List, Dict, Any, Optional
import pandas as pd # Pandas 라이브러리 임포트
import pandas_ta as ta # Pandas TA 라이브러리 임포트

# API URL들을 상수로 정의하여 명확성 및 수정 용이성 확보
BINANCE_FUTURES_EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_FUTURES_KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"

def get_usdt_futures_symbols() -> List[str]:
    """
    Binance API로부터 USDT 기반 선물 거래 종목 리스트를 가져옵니다.

    Returns:
        List[str]: 종목 문자열 리스트 (예: ['BTCUSDT', 'ETHUSDT']).
                   오류 발생 또는 종목을 찾을 수 없는 경우 빈 리스트를 반환합니다.
    """
    print("Binance에서 USDT 선물 티커 목록을 가져오는 중입니다...")
    try:
        response = requests.get(BINANCE_FUTURES_EXCHANGE_INFO_URL, timeout=10)
        response.raise_for_status()  # HTTP 오류 발생 시 예외 발생
        data: Dict[str, Any] = response.json()
        
        symbols_data = data.get('symbols', [])
        usdt_symbols_list = [
            s['symbol']
            for s in symbols_data
            if isinstance(s, dict) and s.get('quoteAsset') == 'USDT' and 'symbol' in s
        ]
        if not usdt_symbols_list:
            print("USDT 기반 선물 티커를 찾을 수 없습니다.")
        return usdt_symbols_list
        
    except requests.exceptions.Timeout:
        print(f"오류: {BINANCE_FUTURES_EXCHANGE_INFO_URL} 요청 시간이 초과되었습니다.")
        return []
    except requests.exceptions.HTTPError as http_err:
        print(f"오류: USDT 티커를 가져오는 중 HTTP 오류가 발생했습니다: {http_err}")
        return []
    except requests.exceptions.RequestException as req_err:
        print(f"오류: Binance API에서 USDT 티커를 요청하는 중 오류가 발생했습니다: {req_err}")
        return []
    except ValueError:  # JSON 디코딩 오류 처리
        print("오류: Binance API로부터 받은 USDT 티커 JSON 응답을 디코딩할 수 없습니다.")
        return []

def get_candlestick_data(symbol: str, interval: str, limit: int) -> Optional[List[List[Any]]]:
    """
    Binance API로부터 특정 티커의 캔들스틱 데이터를 가져옵니다.

    Args:
        symbol (str): 티커 심볼 (예: "BTCUSDT")
        interval (str): 캔들 간격 (예: "1m", "5m", "1h", "1d")
        limit (int): 가져올 캔들 개수 (최대 1500)

    Returns:
        Optional[List[List[Any]]]: 캔들 데이터 리스트. 각 캔들은 [open_time, open, high, low, close, volume, ...] 형식.
                                   오류 발생 시 None을 반환합니다.
    """
    print(f"\n{symbol} {interval} 캔들 데이터 (최근 {limit}개)를 가져오는 중입니다...")
    params = {
        'symbol': symbol.upper(),
        'interval': interval,
        'limit': limit
    }
    try:
        response = requests.get(BINANCE_FUTURES_KLINES_URL, params=params, timeout=10)
        response.raise_for_status()
        candlestick_data: List[List[Any]] = response.json()
        if not candlestick_data:
            print(f"{symbol}에 대한 캔들 데이터를 찾을 수 없습니다.")
        return candlestick_data
    except requests.exceptions.Timeout:
        print(f"오류: {symbol} 캔들 데이터 요청({BINANCE_FUTURES_KLINES_URL}) 시간이 초과되었습니다.")
        return None
    except requests.exceptions.HTTPError as http_err:
        print(f"오류: {symbol} 캔들 데이터를 가져오는 중 HTTP 오류가 발생했습니다: {http_err} (응답: {response.text})")
        return None
    except requests.exceptions.RequestException as req_err:
        print(f"오류: {symbol} 캔들 데이터를 요청하는 중 오류가 발생했습니다: {req_err}")
        return None
    except ValueError:
        print(f"오류: {symbol} 캔들 데이터에 대한 JSON 응답을 디코딩할 수 없습니다.")
        return None

if __name__ == '__main__':
    usdt_symbols = get_usdt_futures_symbols()
    
    if usdt_symbols:
        print(f"\n총 USDT 선물 티커 개수: {len(usdt_symbols)}개")
    else:
        print("\nUSDT 선물 티커를 가져오지 못했습니다.")

    symbol_to_fetch = "BTCUSDT"
    interval_to_fetch = "5m"
    
    # DMI 계산을 위해 충분한 데이터 요청 (예: 14기간 DMI, ADX는 추가 기간 필요)
    # 최근 19개 완성봉에 대한 DMI를 보려면, 19(출력) + 14(DMI) + 14(ADX) + 1(미완성) ~ 48개. 50개로 설정.
    limit_to_fetch = 50 
    num_candles_to_display = 19 # 화면에 표시할 최근 완성된 캔들 수
    dmi_period = 14 # DMI 계산 기간

    btcusdt_candles_raw = get_candlestick_data(
        symbol=symbol_to_fetch,
        interval=interval_to_fetch,
        limit=limit_to_fetch
    )

    if btcusdt_candles_raw:
        # API 응답을 Pandas DataFrame으로 변환
        # 컬럼명: Open time, Open, High, Low, Close, Volume, Close time, Quote asset volume, Number of trades, Taker buy base asset volume, Taker buy quote asset volume, Ignore
        df = pd.DataFrame(btcusdt_candles_raw, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume', 
            'close_time', 'quote_asset_volume', 'number_of_trades', 
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
        ])

        # 숫자형으로 변환
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col])

        # 가장 최근 캔들(미완성일 수 있음)을 제외
        df_completed = df.iloc[:-1].copy() # .copy()를 사용하여 SettingWithCopyWarning 방지

        if not df_completed.empty:
            # DMI 계산 (pandas_ta 사용, 기본 period 14)
            adx_col = f'ADX_{dmi_period}'
            dmp_col = f'DMP_{dmi_period}' # +DI (Positive Directional Movement)
            dmn_col = f'DMN_{dmi_period}' # -DI (Negative Directional Movement)
            dmi_calculated_successfully = False

            try:
                # df.ta.dmi()는 ADX_14, DMP_14, DMN_14 컬럼을 생성 (DMP: +DI, DMN: -DI)
                # append=True가 기본 동작이므로, df_completed에 직접 컬럼이 추가됩니다.
                print("정보: df.ta.dmi 접근자를 사용하여 DMI 계산 시도 중...")
                df_completed.ta.dmi(length=dmi_period, append=True)

                # DMI 컬럼이 정상적으로 추가되었는지 확인
                if adx_col in df_completed.columns and dmp_col in df_completed.columns and dmn_col in df_completed.columns:
                    dmi_calculated_successfully = True
                    print("정보: df.ta.dmi를 통해 DMI 계산 성공.")
                else:
                    print("경고: df.ta.dmi 호출 후 DMI 컬럼이 생성되지 않았습니다. 직접 임포트를 시도합니다.")
                    # 컬럼이 없다면 다음 except 블록에서 처리되거나, 여기서 NA로 채울 수 있습니다.
                    for col_name_check in [adx_col, dmp_col, dmn_col]:
                        if col_name_check not in df_completed.columns:
                            df_completed[col_name_check] = pd.NA

            except AttributeError as ae:
                if "'AnalysisIndicators' object has no attribute 'dmi'" in str(ae):
                    print(f"경고: df.ta.dmi 접근자 사용 실패 ({ae}). pandas_ta.trend.dmi 직접 임포트 시도 중...")
                    try:
                        from pandas_ta.trend import dmi as dmi_trend_function
                        dmi_results = dmi_trend_function(high=df_completed['high'], low=df_completed['low'], close=df_completed['close'], length=dmi_period)
                        if dmi_results is not None and not dmi_results.empty and all(col in dmi_results.columns for col in [adx_col, dmp_col, dmn_col]):
                            # 필요한 컬럼만 선택하여 병합
                            df_completed = pd.concat([df_completed, dmi_results[[adx_col, dmp_col, dmn_col]]], axis=1)
                            dmi_calculated_successfully = True
                            print("정보: pandas_ta.trend.dmi 직접 임포트를 통해 DMI 계산 성공.")
                        else:
                            print("오류: pandas_ta.trend.dmi 직접 임포트 후 결과가 비어있거나 필요한 DMI 컬럼이 없습니다.")
                    except ImportError:
                        print("오류: 'from pandas_ta.trend import dmi' 실패. pandas_ta 설치가 손상되었거나 매우 오래된 버전일 수 있습니다.")
                    except Exception as e_direct:
                        print(f"오류: pandas_ta.trend.dmi 직접 호출 중 예외 발생: {e_direct}")
                else:
                    print(f"오류: DMI 지표 계산 중 예기치 않은 AttributeError 발생: {ae}")
            except Exception as e:
                print(f"오류: DMI 지표 계산 중 예외 발생: {e}")

            if not dmi_calculated_successfully:
                print("경고: DMI 지표를 계산하지 못했습니다. DMI 값은 NaN으로 표시됩니다.")
                # 모든 시도 실패 시 DMI 컬럼이 없다면 NaN으로 채움
                for col_name in [adx_col, dmp_col, dmn_col]:
                    if col_name not in df_completed.columns:
                        df_completed[col_name] = pd.NA

            # 화면에 표시할 최근 N개 캔들 선택
            df_to_display = df_completed.tail(num_candles_to_display)

            if not df_to_display.empty:
                print(f"\n--- {symbol_to_fetch} {interval_to_fetch} (최근 {len(df_to_display)}개 완성봉) 캔들 및 DMI({dmi_period}) 데이터 ---")
                header = f"시작 시간 (KST)     | {'Open':>8} | {'High':>8} | {'Low':>8} | {'Close':>8} | {'Volume':>10} | {'ADX':>7} | {'+DI':>7} | {'-DI':>7}"
                print(header)
                print("-" * len(header))
                
                kst_tz = datetime.timezone(datetime.timedelta(hours=9))
                
                for index, row in df_to_display.iterrows():
                    open_time_utc_dt = datetime.datetime.fromtimestamp(row['timestamp'] / 1000, tz=datetime.timezone.utc)
                    open_time_kst_str = open_time_utc_dt.astimezone(kst_tz).strftime('%Y-%m-%d %H:%M:%S')

                    adx_val = f"{row[adx_col]:.2f}" if pd.notna(row[adx_col]) else "NaN"
                    dmp_val = f"{row[dmp_col]:.2f}" if pd.notna(row[dmp_col]) else "NaN"
                    dmn_val = f"{row[dmn_col]:.2f}" if pd.notna(row[dmn_col]) else "NaN"

                    print(f"{open_time_kst_str} | {row['open']:>8} | {row['high']:>8} | {row['low']:>8} | {row['close']:>8} | {row['volume']:>10.2f} | {adx_val:>7} | {dmp_val:>7} | {dmn_val:>7}")
            else:
                print(f"\n{symbol_to_fetch} {interval_to_fetch} DMI 계산 후 표시할 데이터가 없습니다 (요청된 캔들 수: {num_candles_to_display}).")
        else:
            print(f"\n{symbol_to_fetch} {interval_to_fetch} 데이터를 가져왔으나, 가장 최근 봉을 제외하면 DMI를 계산할 데이터가 없습니다.")
    else:
        print(f"\n{symbol_to_fetch} 캔들 데이터를 가져오지 못했습니다.")
