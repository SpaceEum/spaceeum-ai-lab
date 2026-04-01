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
    """모든 전략의 기본 클래스"""

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
          {"signal": "LONG"|"SHORT"|"NONE",
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

    def _signal(self, direction: str, entry: float, reason: str, indicators: dict = None) -> dict:
        if direction == "LONG":
            sl = round(entry * (1 - self.STOP_LOSS_PCT), 6)
            tp = round(entry * (1 + self.TAKE_PROFIT_PCT), 6)
        else:  # SHORT
            sl = round(entry * (1 + self.STOP_LOSS_PCT), 6)
            tp = round(entry * (1 - self.TAKE_PROFIT_PCT), 6)

        return {
            "signal": direction,
            "reason": reason,
            "entry": entry,
            "stop_loss": sl,
            "take_profit": tp,
            "indicators": indicators or {}
        }


# ──────────────────────────────────────────────────────────
# 전략 1 : 60MA 기본 돌파
# ──────────────────────────────────────────────────────────

class Shin60MABasic(BaseStrategy):
    """
    신창환 60이평선 기본 돌파 전략

    원리:
    - 청송촌놈 신창환의 핵심 원칙: 60이평선이 지지/저항
    - 캔들이 60MA 위로 돌파(종가) = 매수 신호
    - 캔들이 60MA 아래로 이탈(종가) = 매도/공매도 신호

    진입 조건:
    - LONG : 직전 캔들 종가 < 60MA  AND  현재 캔들 종가 > 60MA (상향 돌파)
    - SHORT: 직전 캔들 종가 > 60MA  AND  현재 캔들 종가 < 60MA (하향 이탈)
    """

    name = "Shin_60MA_Basic"
    description = "60MA 기본 돌파 전략 - 캔들이 60이평선 위/아래 돌파 시 진입"
    STOP_LOSS_PCT = 0.02
    TAKE_PROFIT_PCT = 0.04

    def analyze(self, candles: list) -> dict:
        if len(candles) < 62:
            return self._no_signal()

        closes = [c["close"] for c in candles]
        ma60 = calc_ma(closes, 60)

        cur = candles[-1]
        prev = candles[-2]

        cur_close = cur["close"]
        prev_close = prev["close"]
        cur_ma60 = ma60[-1]
        prev_ma60 = ma60[-2]

        if None in [cur_ma60, prev_ma60]:
            return self._no_signal()

        indicators = {
            "MA60": round(cur_ma60, 6),
            "현재종가": round(cur_close, 6),
            "직전종가": round(prev_close, 6)
        }

        # 롱 진입: 60MA 상향 돌파
        if prev_close < prev_ma60 and cur_close > cur_ma60:
            return self._signal(
                "LONG", cur_close,
                f"60MA 상향 돌파 (종가 {cur_close:.4f} > MA60 {cur_ma60:.4f})",
                indicators
            )

        # 숏 진입: 60MA 하향 이탈
        if prev_close > prev_ma60 and cur_close < cur_ma60:
            return self._signal(
                "SHORT", cur_close,
                f"60MA 하향 이탈 (종가 {cur_close:.4f} < MA60 {cur_ma60:.4f})",
                indicators
            )

        return self._no_signal(indicators)


# ──────────────────────────────────────────────────────────
# 전략 2 : 60MA 눌림목/반등 (신창환 핵심 전략)
# ──────────────────────────────────────────────────────────

class Shin60MABounce(BaseStrategy):
    """
    신창환 60이평선 눌림목/반등 전략 (핵심 전략)

    원리:
    - 신창환의 핵심: "60이평선은 지지선이다"
    - 상승 추세에서 가격이 60MA까지 내려왔다가 반등할 때 매수
    - 하락 추세에서 가격이 60MA까지 올라왔다가 반락할 때 공매도

    진입 조건:
    LONG:
    - 최근 3개 캔들 중 하나의 저가가 60MA에 근접(±0.5%)
    - 현재 캔들 종가가 60MA 위에 있음
    - 현재 캔들이 양봉 (종가 > 시가)
    - 5MA > 60MA (상승 추세 확인)

    SHORT:
    - 최근 3개 캔들 중 하나의 고가가 60MA에 근접(±0.5%)
    - 현재 캔들 종가가 60MA 아래에 있음
    - 현재 캔들이 음봉 (종가 < 시가)
    - 5MA < 60MA (하락 추세 확인)
    """

    name = "Shin_60MA_Bounce"
    description = "60MA 눌림목/반등 전략 - 60이평선 지지/저항 반등 진입 (신창환 핵심)"
    STOP_LOSS_PCT = 0.025
    TAKE_PROFIT_PCT = 0.06
    TOUCH_RANGE = 0.008  # MA에 0.8% 이내 근접 = 터치로 판단

    def analyze(self, candles: list) -> dict:
        if len(candles) < 62:
            return self._no_signal()

        closes = [c["close"] for c in candles]
        opens = [c["open"] for c in candles]
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]

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

        # 최근 3 캔들 중 MA60에 터치한 적 있는지 확인
        touched_from_below = False  # 저가로 MA60 터치
        touched_from_above = False  # 고가로 MA60 터치

        for i in range(-4, -1):
            c = candles[i]
            m = ma60[i]
            if m is None:
                continue
            # 아래에서 터치 (저가가 MA60 근처)
            if abs(c["low"] - m) / m <= self.TOUCH_RANGE and c["close"] > m:
                touched_from_below = True
            # 위에서 터치 (고가가 MA60 근처)
            if abs(c["high"] - m) / m <= self.TOUCH_RANGE and c["close"] < m:
                touched_from_above = True

        is_bullish = cur_close > cur_open  # 양봉
        is_bearish = cur_close < cur_open  # 음봉

        # LONG: 상승 추세 + 60MA 눌림 반등
        if (cur_ma5 > cur_ma60 and        # 상승 추세
            cur_close > cur_ma60 and       # 60MA 위에 있음
            touched_from_below and          # 최근 60MA 터치
            is_bullish):                    # 양봉으로 반등
            return self._signal(
                "LONG", cur_close,
                f"60MA 눌림 반등 (상승추세, MA60={cur_ma60:.4f} 지지 후 양봉 반등)",
                indicators
            )

        # SHORT: 하락 추세 + 60MA 반락
        if (cur_ma5 < cur_ma60 and        # 하락 추세
            cur_close < cur_ma60 and       # 60MA 아래에 있음
            touched_from_above and          # 최근 60MA 터치
            is_bearish):                    # 음봉으로 반락
            return self._signal(
                "SHORT", cur_close,
                f"60MA 저항 반락 (하락추세, MA60={cur_ma60:.4f} 저항 후 음봉 반락)",
                indicators
            )

        return self._no_signal(indicators)


# ──────────────────────────────────────────────────────────
# 전략 3 : 20MA × 60MA 골든/데드크로스
# ──────────────────────────────────────────────────────────

class Shin60MACross(BaseStrategy):
    """
    신창환 20MA × 60MA 골든크로스/데드크로스 전략

    원리:
    - 신창환: "20이평선이 60이평선을 상향 돌파 = 중기 골든크로스 = 강한 매수 신호"
    - 20MA가 60MA 위로 골든크로스 = LONG
    - 20MA가 60MA 아래로 데드크로스 = SHORT

    진입 조건:
    LONG : 전봉에서 MA20 < MA60  AND  현봉에서 MA20 > MA60
    SHORT: 전봉에서 MA20 > MA60  AND  현봉에서 MA20 < MA60
    """

    name = "Shin_60MA_Cross"
    description = "20MA×60MA 골든/데드크로스 전략 - 중기 추세 전환 포착"
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
            "MA20_prev": round(prev_ma20, 6),
            "MA60_prev": round(prev_ma60, 6)
        }

        # 골든크로스: MA20이 MA60을 상향 돌파
        if prev_ma20 < prev_ma60 and cur_ma20 > cur_ma60:
            return self._signal(
                "LONG", cur_close,
                f"골든크로스 발생! MA20({cur_ma20:.4f}) > MA60({cur_ma60:.4f}) 상향 돌파",
                indicators
            )

        # 데드크로스: MA20이 MA60을 하향 이탈
        if prev_ma20 > prev_ma60 and cur_ma20 < cur_ma60:
            return self._signal(
                "SHORT", cur_close,
                f"데드크로스 발생! MA20({cur_ma20:.4f}) < MA60({cur_ma60:.4f}) 하향 이탈",
                indicators
            )

        return self._no_signal(indicators)


# ──────────────────────────────────────────────────────────
# 전략 4 : 60MA + RSI 복합
# ──────────────────────────────────────────────────────────

class Shin60MARSI(BaseStrategy):
    """
    신창환 60MA + RSI 복합 전략

    원리:
    - 60MA 추세 방향 확인 + RSI 과매수/과매도로 타이밍 포착
    - 60MA 위에서 RSI가 40 미만(과매도) → 반등 매수
    - 60MA 아래에서 RSI가 60 초과(과매수) → 반락 공매도

    진입 조건:
    LONG : 종가 > MA60 AND RSI < 40 (과매도 반등)
    SHORT: 종가 < MA60 AND RSI > 60 (과매수 반락)
    """

    name = "Shin_60MA_RSI"
    description = "60MA + RSI 복합 전략 - 추세 방향 + 과매수/과매도 타이밍"
    STOP_LOSS_PCT = 0.02
    TAKE_PROFIT_PCT = 0.05
    RSI_OVERSOLD = 40
    RSI_OVERBOUGHT = 60

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

        # LONG: 60MA 위 + RSI 과매도 (반등 타이밍)
        if cur_close > cur_ma60 and cur_rsi < self.RSI_OVERSOLD:
            return self._signal(
                "LONG", cur_close,
                f"60MA 위({cur_ma60:.4f}) + RSI 과매도({cur_rsi:.1f} < {self.RSI_OVERSOLD}) → 반등 매수",
                indicators
            )

        # SHORT: 60MA 아래 + RSI 과매수 (반락 타이밍)
        if cur_close < cur_ma60 and cur_rsi > self.RSI_OVERBOUGHT:
            return self._signal(
                "SHORT", cur_close,
                f"60MA 아래({cur_ma60:.4f}) + RSI 과매수({cur_rsi:.1f} > {self.RSI_OVERBOUGHT}) → 반락 매도",
                indicators
            )

        return self._no_signal(indicators)


# ──────────────────────────────────────────────────────────
# 전략 5 : 60MA + 거래량 급등
# ──────────────────────────────────────────────────────────

class Shin60MAVolume(BaseStrategy):
    """
    신창환 60MA + 거래량 급등 전략

    원리:
    - 신창환: "거래량을 동반한 돌파가 진짜 돌파"
    - 60MA 위/아래에서 거래량 급등하며 강한 캔들 발생 시 진입

    진입 조건:
    LONG : 종가 > MA60 AND 현재 거래량 > 20기간 평균 거래량 × 1.5 AND 양봉
    SHORT: 종가 < MA60 AND 현재 거래량 > 20기간 평균 거래량 × 1.5 AND 음봉
    """

    name = "Shin_60MA_Volume"
    description = "60MA + 거래량 급등 전략 - 거래량 수반 강한 돌파 포착"
    STOP_LOSS_PCT = 0.025
    TAKE_PROFIT_PCT = 0.06
    VOLUME_MULTIPLIER = 1.5  # 평균 거래량의 1.5배 이상

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

        is_bullish = cur_close > cur_open
        is_bearish = cur_close < cur_open
        vol_ratio = cur_vol / cur_vol_ma if cur_vol_ma > 0 else 0

        indicators = {
            "MA60": round(cur_ma60, 6),
            "현재거래량": round(cur_vol, 2),
            "평균거래량": round(cur_vol_ma, 2),
            "거래량비율": round(vol_ratio, 2)
        }

        # LONG: 60MA 위 + 거래량 급증 + 양봉
        if (cur_close > cur_ma60 and
            vol_ratio >= self.VOLUME_MULTIPLIER and
            is_bullish):
            return self._signal(
                "LONG", cur_close,
                f"거래량 급증 양봉 상승 (MA60 위, 거래량 평균대비 {vol_ratio:.1f}배)",
                indicators
            )

        # SHORT: 60MA 아래 + 거래량 급증 + 음봉
        if (cur_close < cur_ma60 and
            vol_ratio >= self.VOLUME_MULTIPLIER and
            is_bearish):
            return self._signal(
                "SHORT", cur_close,
                f"거래량 급증 음봉 하락 (MA60 아래, 거래량 평균대비 {vol_ratio:.1f}배)",
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
