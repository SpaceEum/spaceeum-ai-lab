"""
업비트 데이터 수집 모듈
binance_data.py를 대체합니다 — 동일한 인터페이스, 업비트 데이터
"""

import time
import pyupbit

TOP_N_SYMBOLS = 50
CANDLE_COUNT = 120


def get_top_symbols(n: int = TOP_N_SYMBOLS) -> list:
    """업비트 KRW 마켓 상위 N개 티커 반환 (예: KRW-BTC)"""
    try:
        tickers = pyupbit.get_tickers(fiat="KRW")
        print(f"[Upbit] 상위 {n}개 심볼 로딩: {tickers[:3]}... 등")
        return tickers[:n]
    except Exception as e:
        print(f"[Upbit] 심볼 목록 조회 실패: {e}")
        return ["KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-SOL", "KRW-ADA"]


def get_klines(symbol: str, interval: str = "day", limit: int = CANDLE_COUNT) -> list:
    """
    업비트 일봉 캔들 데이터 조회
    반환: [{"time", "open", "high", "low", "close", "volume"}, ...]
    """
    try:
        df = pyupbit.get_ohlcv(symbol, count=limit, interval="day")
        if df is None or len(df) == 0:
            return []
        candles = []
        for idx, row in df.iterrows():
            candles.append({
                "time": str(idx),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
            })
        return candles
    except Exception as e:
        print(f"[Upbit] {symbol} 캔들 조회 실패: {e}")
        return []


def get_current_price(symbol: str) -> float:
    """현재가 조회"""
    try:
        price = pyupbit.get_current_price(symbol)
        return float(price) if price else 0.0
    except Exception as e:
        print(f"[Upbit] {symbol} 현재가 조회 실패: {e}")
        return 0.0


def get_batch_prices(symbols: list) -> dict:
    """여러 심볼 현재가 일괄 조회"""
    result = {}
    for symbol in symbols:
        try:
            price = pyupbit.get_current_price(symbol)
            if price:
                result[symbol] = float(price)
            time.sleep(0.05)
        except Exception:
            continue
    return result
