"""
신창환 에이전트 - 매매 전략 모음

5가지 전략 구현:
  1. Shin_60MA_Basic    : 60MA 기본 돌파 전략
  2. Shin_60MA_Bounce   : 60MA 눌림목/반등 전략 (핵심)
  3. Shin_60MA_Cross    : 20MA×60MA 골든/데드크로스
  4. Shin_60MA_RSI      : 60MA + RSI 복합 전략
  5. Shin_60MA_Volume   : 60MA + 거래량 급등 전략
"""

import numpy as np


# ──────────────────────────────────────────────────────────
# 보조 지표 계산
# ──────────────────────────────────────────────────────────

def calc_ma(closes: list, period: int) -> list:
    """단순 이동평균 (SMA)"""
    result = [None] * len(closes)
    for i in range(period - 1, len(closes)):
        result[i] = sum(closes[i - period + 1:i + 1]) / period
    return result


def calc_rsi(closes: list, period: int = 14) -> list:
    """RSI 계산"""
    result = [None] * len(closes)
    if len(closes) < period + 1:
        return result

    gains, losses = [], []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0))
        losses.append(max(-delta, 0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(closes)):
        if avg_loss == 0:
            result[i] = 100
        else:
            rs = avg_gain / avg_loss
            result[i] = 100 - (100 / (1 + rs))

        if i < len(closes) - 1:
            g = gains[i] if i < len(gains) else 0
            l = losses[i] if i < len(losses) else 0
            avg_gain = (avg_gain * (period - 1) + g) / period
            avg_loss = (avg_loss * (period - 1) + l) / period

    return result


def calc_volume_ma(volumes: list, period: int = 20) -> list:
    """거래량 이동평균"""
    return calc_ma(volumes, period)


# ──────────────────────────────────────────────────────────
# 전략 기본 클래스
# ──────────────────────────────────────────────────────────

class BaseStrategy:
    """모든 전략의 기본 클래스 (업비트 현물 기준 — 매수/매도만)"""

    name = "BaseStrategy"
    description = "기본 전략"

    # 리스크 파라미터
    STOP_LOSS_PCT = 0.02    # 2% 손절
    TAKE_PROFIT_PCT = 0.04  # 4% 익절

    def analyze(self, candles: list) -> dict:
        """
        캔들 데이터 분석 후 시그널 반환

        candles: [{"time", "open", "high", "low", "close", "volume"}, ...]

        반환값:
          {"signal": "BUY"|"SELL"|"NONE",
           "reason": str,
           "entry": float,
           "stop_loss": float,
           "take_profit": float,
           "indicators": dict}
        """
        raise NotImplementedError

    def _no_signal(self, indicators: dict = None) -> dict:
        return {
            "signal": "NONE",
            "reason": "조건 미충족",
            "entry": None,
            "stop_loss": None,
            "take_profit": None,
            "indicators": indicators or {}
        }

    def _buy_signal(self, entry: float, reason: str, indicators: dict = None) -> dict:
        return {
            "signal": "BUY",
            "reason": reason,
            "entry": entry,
            "stop_loss": round(entry * (1 - self.STOP_LOSS_PCT), 6),
            "take_profit": round(entry * (1 + self.TAKE_PROFIT_PCT), 6),
            "indicators": indicators or {}
        }

    def _sell_signal(self, entry: float, reason: str, indicators: dict = None) -> dict:
        return {
            "signal": "SELL",
            "reason": reason,
            "entry": entry,
            "stop_loss": None,
            "take_profit": None,
            "indicators": indicators or {}
        }


# ──────────────────────────────────────────────────────────
# 전략 1 : 60MA 기본 돌파
# ──────────────────────────────────────────────────────────

class Shin60MABasic(BaseStrategy):
    """
    신창환 60이평선 기본 돌파 전략 (업비트 현물)

    매수: 직전 종가 < 60MA → 현재 종가 > 60MA (상향 돌파)
    매도: 직전 종가 > 60MA → 현재 종가 < 60MA (60MA 이탈 청산)
    """

    name = "Shin_60MA_Basic"
    description = "60MA 기본 돌파 전략 - 60이평선 상향 돌파 시 매수, 이탈 시 매도"
    STOP_LOSS_PCT = 0.02
    TAKE_PROFIT_PCT = 0.04

    def analyze(self, candles: list) -> dict:
        if len(candles) < 62:
            return self._no_signal()

        closes = [c["close"] for c in candles]
        ma60 = calc_ma(closes, 60)

        cur_close = candles[-1]["close"]
        prev_close = candles[-2]["close"]
        cur_ma60 = ma60[-1]
        prev_ma60 = ma60[-2]

        if None in [cur_ma60, prev_ma60]:
            return self._no_signal()

        indicators = {
            "MA60": round(cur_ma60, 6),
            "현재종가": round(cur_close, 6),
            "직전종가": round(prev_close, 6)
        }

        # 매수: 60MA 상향 돌파
        if prev_close < prev_ma60 and cur_close > cur_ma60:
            return self._buy_signal(
                cur_close,
                f"60MA 상향 돌파 (종가 {cur_close:.4f} > MA60 {cur_ma60:.4f})",
                indicators
            )

        # 매도: 60MA 아래로 이탈 (보유 중 청산 신호)
        if prev_close > prev_ma60 and cur_close < cur_ma60:
            return self._sell_signal(
                cur_close,
                f"60MA 이탈 청산 (종가 {cur_close:.4f} < MA60 {cur_ma60:.4f})",
                indicators
            )

        return self._no_signal(indicators)


# ──────────────────────────────────────────────────────────
# 전략 2 : 60MA 눌림목/반등 (신창환 핵심 전략)
# ──────────────────────────────────────────────────────────

class Shin60MABounce(BaseStrategy):
    """
    신창환 60이평선 눌림목 반등 전략 — 핵심 (업비트 현물)

    매수: 상승추세(MA5>MA60) + 최근 3봉 중 저가가 60MA 0.8% 이내 터치 + 양봉 반등
    매도: 손절(-2.5%) or 익절(+6%)
    """

    name = "Shin_60MA_Bounce"
    description = "60MA 눌림목 반등 전략 - 상승추세에서 60MA 지지 반등 시 매수 (신창환 핵심)"
    STOP_LOSS_PCT = 0.025
    TAKE_PROFIT_PCT = 0.06
    TOUCH_RANGE = 0.008

    def analyze(self, candles: list) -> dict:
        if len(candles) < 62:
            return self._no_signal()

        closes = [c["close"] for c in candles]
        ma60 = calc_ma(closes, 60)
        ma5 = calc_ma(closes, 5)

        cur = candles[-1]
        cur_close = cur["close"]
        cur_open = cur["open"]
        cur_ma60 = ma60[-1]
        cur_ma5 = ma5[-1]

        if None in [cur_ma60, cur_ma5]:
            return self._no_signal()

        indicators = {
            "MA60": round(cur_ma60, 6),
            "MA5": round(cur_ma5, 6),
            "현재종가": round(cur_close, 6)
        }

        # 최근 3봉 중 저가가 60MA에 터치했는지 확인
        touched_from_below = False
        for i in range(-4, -1):
            c = candles[i]
            m = ma60[i]
            if m is None:
                continue
            if abs(c["low"] - m) / m <= self.TOUCH_RANGE and c["close"] > m:
                touched_from_below = True

        is_bullish = cur_close > cur_open

        # 매수: 상승추세 + 60MA 눌림 반등
        if (cur_ma5 > cur_ma60 and
                cur_close > cur_ma60 and
                touched_from_below and
                is_bullish):
            return self._buy_signal(
                cur_close,
                f"60MA 눌림 반등 (상승추세, MA60={cur_ma60:.4f} 지지 후 양봉 반등)",
                indicators
            )

        return self._no_signal(indicators)


# ──────────────────────────────────────────────────────────
# 전략 3 : 20MA × 60MA 골든/데드크로스
# ──────────────────────────────────────────────────────────

class Shin60MACross(BaseStrategy):
    """
    신창환 20MA × 60MA 골든/데드크로스 전략 (업비트 현물)

    매수: 전봉 MA20 < MA60 → 현봉 MA20 > MA60 (골든크로스)
    매도: 전봉 MA20 > MA60 → 현봉 MA20 < MA60 (데드크로스 — 보유 청산)
    """

    name = "Shin_60MA_Cross"
    description = "20MA×60MA 골든/데드크로스 전략 - 중기 추세 전환 매수, 반전 시 매도"
    STOP_LOSS_PCT = 0.03
    TAKE_PROFIT_PCT = 0.08

    def analyze(self, candles: list) -> dict:
        if len(candles) < 65:
            return self._no_signal()

        closes = [c["close"] for c in candles]
        ma20 = calc_ma(closes, 20)
        ma60 = calc_ma(closes, 60)

        cur_ma20 = ma20[-1]
        prev_ma20 = ma20[-2]
        cur_ma60 = ma60[-1]
        prev_ma60 = ma60[-2]

        if None in [cur_ma20, prev_ma20, cur_ma60, prev_ma60]:
            return self._no_signal()

        cur_close = candles[-1]["close"]

        indicators = {
            "MA20": round(cur_ma20, 6),
            "MA60": round(cur_ma60, 6),
        }

        # 매수: 골든크로스
        if prev_ma20 < prev_ma60 and cur_ma20 > cur_ma60:
            return self._buy_signal(
                cur_close,
                f"골든크로스 발생 MA20({cur_ma20:.4f}) > MA60({cur_ma60:.4f})",
                indicators
            )

        # 매도: 데드크로스 (보유 청산)
        if prev_ma20 > prev_ma60 and cur_ma20 < cur_ma60:
            return self._sell_signal(
                cur_close,
                f"데드크로스 청산 MA20({cur_ma20:.4f}) < MA60({cur_ma60:.4f})",
                indicators
            )

        return self._no_signal(indicators)


# ──────────────────────────────────────────────────────────
# 전략 4 : 60MA + RSI 복합
# ──────────────────────────────────────────────────────────

class Shin60MARSI(BaseStrategy):
    """
    신창환 60MA + RSI 복합 전략 (업비트 현물)

    매수: 종가 > MA60 AND RSI < 40 (상승추세에서 과매도 반등 타이밍)
    매도: 손절(-2%) or 익절(+5%)
    """

    name = "Shin_60MA_RSI"
    description = "60MA + RSI 복합 전략 - 상승추세 안에서 RSI 과매도 반등 시 매수"
    STOP_LOSS_PCT = 0.02
    TAKE_PROFIT_PCT = 0.05
    RSI_OVERSOLD = 40

    def analyze(self, candles: list) -> dict:
        if len(candles) < 75:
            return self._no_signal()

        closes = [c["close"] for c in candles]
        ma60 = calc_ma(closes, 60)
        rsi = calc_rsi(closes, 14)

        cur_close = candles[-1]["close"]
        cur_ma60 = ma60[-1]
        cur_rsi = rsi[-1]

        if None in [cur_ma60, cur_rsi]:
            return self._no_signal()

        indicators = {
            "MA60": round(cur_ma60, 6),
            "RSI": round(cur_rsi, 2),
            "현재종가": round(cur_close, 6)
        }

        # 매수: 60MA 위 + RSI 과매도 반등
        if cur_close > cur_ma60 and cur_rsi < self.RSI_OVERSOLD:
            return self._buy_signal(
                cur_close,
                f"60MA 위 + RSI 과매도({cur_rsi:.1f} < {self.RSI_OVERSOLD}) 반등 매수",
                indicators
            )

        return self._no_signal(indicators)


# ──────────────────────────────────────────────────────────
# 전략 5 : 60MA + 거래량 급등
# ──────────────────────────────────────────────────────────

class Shin60MAVolume(BaseStrategy):
    """
    신창환 60MA + 거래량 급등 전략 (업비트 현물)

    매수: 종가 > MA60 AND 거래량 > 20봉 평균의 1.5배 AND 양봉
    매도: 손절(-2.5%) or 익절(+6%)
    """

    name = "Shin_60MA_Volume"
    description = "60MA + 거래량 급등 전략 - 거래량 수반 60MA 위 양봉 돌파 시 매수"
    STOP_LOSS_PCT = 0.025
    TAKE_PROFIT_PCT = 0.06
    VOLUME_MULTIPLIER = 1.5

    def analyze(self, candles: list) -> dict:
        if len(candles) < 65:
            return self._no_signal()

        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]

        ma60 = calc_ma(closes, 60)
        vol_ma20 = calc_volume_ma(volumes, 20)

        cur = candles[-1]
        cur_close = cur["close"]
        cur_open = cur["open"]
        cur_vol = cur["volume"]
        cur_ma60 = ma60[-1]
        cur_vol_ma = vol_ma20[-1]

        if None in [cur_ma60, cur_vol_ma]:
            return self._no_signal()

        vol_ratio = cur_vol / cur_vol_ma if cur_vol_ma > 0 else 0
        is_bullish = cur_close > cur_open

        indicators = {
            "MA60": round(cur_ma60, 6),
            "거래량비율": round(vol_ratio, 2)
        }

        # 매수: 60MA 위 + 거래량 급증 + 양봉
        if cur_close > cur_ma60 and vol_ratio >= self.VOLUME_MULTIPLIER and is_bullish:
            return self._buy_signal(
                cur_close,
                f"거래량 급증 양봉 (MA60 위, 거래량 평균대비 {vol_ratio:.1f}배)",
                indicators
            )

        return self._no_signal(indicators)


# ──────────────────────────────────────────────────────────
# 전략 목록 (실행 순서)
# ──────────────────────────────────────────────────────────

ALL_STRATEGIES = [
    Shin60MABounce(),    # 핵심 전략
    Shin60MABasic(),
    Shin60MACross(),
    Shin60MARSI(),
    Shin60MAVolume(),
]
