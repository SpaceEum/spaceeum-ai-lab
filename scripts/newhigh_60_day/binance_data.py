"""
신창환 에이전트 - 바이낸스 선물 데이터 수집 모듈
공개 API 사용 (인증 불필요)
"""

import requests
import time
from config import BINANCE_BASE_URL, TOP_N_SYMBOLS, CANDLE_INTERVAL, CANDLE_LIMIT


HEADERS = {
    "User-Agent": "Mozilla/5.0 ShinAgent/1.0"
}


def get_top_symbols(n: int = TOP_N_SYMBOLS) -> list:
    """
    바이낸스 선물 거래량 상위 N개 USDT 페어 반환
    """
    try:
        url = f"{BINANCE_BASE_URL}/fapi/v1/ticker/24hr"
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        tickers = resp.json()

        # USDT 페어만 필터링 + 거래량(quoteVolume) 기준 정렬
        usdt_pairs = [
            t for t in tickers
            if t["symbol"].endswith("USDT") and float(t["quoteVolume"]) > 0
        ]
        usdt_pairs.sort(key=lambda x: float(x["quoteVolume"]), reverse=True)

        symbols = [t["symbol"] for t in usdt_pairs[:n]]
        print(f"[Binance] 상위 {n}개 심볼 로딩: {symbols[:5]}... 등")
        return symbols

    except Exception as e:
        print(f"[Binance] 심볼 목록 조회 실패: {e}")
        # 기본 심볼 반환
        return ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]


def get_klines(symbol: str, interval: str = CANDLE_INTERVAL,
               limit: int = CANDLE_LIMIT) -> list:
    """
    바이낸스 선물 캔들 데이터 조회
    반환: [{"time", "open", "high", "low", "close", "volume"}, ...]
    """
    try:
        url = f"{BINANCE_BASE_URL}/fapi/v1/klines"
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        resp = requests.get(url, params=params, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        raw = resp.json()

        candles = []
        for k in raw:
            candles.append({
                "time": k[0],           # 시작 시간 (ms)
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "close_time": k[6]
            })
        return candles

    except Exception as e:
        print(f"[Binance] {symbol} 캔들 조회 실패: {e}")
        return []


def get_current_price(symbol: str) -> float:
    """현재가 조회"""
    try:
        url = f"{BINANCE_BASE_URL}/fapi/v1/ticker/price"
        resp = requests.get(url, params={"symbol": symbol}, headers=HEADERS, timeout=5)
        resp.raise_for_status()
        return float(resp.json()["price"])
    except Exception as e:
        print(f"[Binance] {symbol} 현재가 조회 실패: {e}")
        return 0.0


def get_batch_prices(symbols: list) -> dict:
    """여러 심볼 현재가 일괄 조회"""
    try:
        url = f"{BINANCE_BASE_URL}/fapi/v1/ticker/price"
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return {item["symbol"]: float(item["price"]) for item in data
                if item["symbol"] in symbols}
    except Exception as e:
        print(f"[Binance] 배치 현재가 조회 실패: {e}")
        return {}
