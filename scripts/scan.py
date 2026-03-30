#!/usr/bin/env python3
"""
SpaceEum AI Lab - 업비트 KRW 60일 이평 자동 스캔
pyupbit 사용 (미국/해외 서버에서도 접속 가능)
"""

import json
import math
import os
import time
from datetime import datetime, timezone, timedelta

import pyupbit

# ── 설정 ─────────────────────────────────────────
TOP_N = 200
MIN_SCORE = 5
BUY_SCORE = 6
STRONG_SCORE = 8
OUTPUT_PATH = "data/scan_latest.json"
KST = timezone(timedelta(hours=9))


def log(msg):
    now = datetime.now(KST).strftime("%H:%M:%S")
    print(f"[{now}] {msg}")


# ── STEP 1: KRW 마켓 상위 티커 목록 ───────────────
def get_top_tickers(n=200):
    log("업비트 KRW 마켓 티커 목록 가져오는 중...")
    tickers = pyupbit.get_tickers(fiat="KRW")
    log(f"전체 KRW 마켓: {len(tickers)}개 → 상위 {n}개 추출")
    return tickers[:n]


# ── STEP 2: 개별 티커 OHLCV 데이터 ───────────────
def get_ohlcv(ticker, count=90):
    df = pyupbit.get_ohlcv(ticker, count=count, interval="day")
    return df


# ── STEP 3: 개별 티커 분석 ────────────────────────
def analyze_ticker(ticker):
    try:
        df = get_ohlcv(ticker, count=90)
        if df is None or len(df) < 62:
            return None

        closes = df['close'].tolist()
        volumes = df['volume'].tolist()
        highs = df['high'].tolist()
        lows = df['low'].tolist()
        current_price = closes[-1]

        # 60일 이평 및 종이격
        ma60 = sum(closes[-60:]) / 60
        ma60_prev = sum(closes[-61:-1]) / 60
        ma60_prev2 = sum(closes[-62:-2]) / 60
        jongi_gap_today = ma60 - ma60_prev
        jongi_gap_yesterday = ma60_prev - ma60_prev2

        # 1차 필터: 현재가가 60이평 아래면 제외
        if current_price <= ma60:
            return None

        # 이평선
        ma9 = sum(closes[-9:]) / 9
        ma10 = sum(closes[-10:]) / 10
        ma26 = sum(closes[-26:]) / 26

        # MACD
        def ema(prices, period):
            k = 2 / (period + 1)
            v = prices[0]
            for p in prices[1:]:
                v = p * k + v * (1 - k)
            return v

        macd = ema(closes[-40:], 12) - ema(closes[-40:], 26)
        macd_above_zero = macd > 0

        # OBV
        obv_list = [0]
        for j in range(1, len(closes)):
            if closes[j] > closes[j - 1]:
                obv_list.append(obv_list[-1] + volumes[j])
            elif closes[j] < closes[j - 1]:
                obv_list.append(obv_list[-1] - volumes[j])
            else:
                obv_list.append(obv_list[-1])
        obv_rising = sum(obv_list[-5:]) / 5 > sum(obv_list[-15:-5]) / 10

        # 거래량
        volume_increasing = sum(volumes[-5:]) / 5 > sum(volumes[-25:-5]) / 20

        # 일목균형표 구름대
        span_a = (ma9 + ma26) / 2
        span_b = (max(highs[-52:]) + min(lows[-52:])) / 2
        above_cloud = current_price > max(span_a, span_b)
        is_positive_cloud = span_a > span_b
        is_new_high_60d = current_price >= max(closes[-60:]) * 0.99

        # 볼린저 밴드
        ma20 = sum(closes[-20:]) / 20
        std20 = math.sqrt(sum((c - ma20) ** 2 for c in closes[-20:]) / 20)
        bb_upper = ma20 + 2 * std20
        bb_lower = ma20 - 2 * std20
        bb_pos = round((current_price - bb_lower) / (bb_upper - bb_lower) * 100, 1) if bb_upper != bb_lower else 50

        # 조건 체크 (9개)
        conditions = {
            '현재가_60이평_위': current_price > ma60,
            '60이평_우상향': jongi_gap_today > 0,
            '종이격_증가중': jongi_gap_today > jongi_gap_yesterday,
            'MACD_영선_위': macd_above_zero,
            'OBV_상승중': obv_rising,
            '거래량_증가': volume_increasing,
            '정배열_전환선_10이평_기준선': ma9 > ma10 > ma26,
            '구름대_위_및_양운': above_cloud and is_positive_cloud,
            '60일_신고가_근처': is_new_high_60d,
        }

        satisfied = [k for k, v in conditions.items() if v]
        score = len(satisfied)

        if score < MIN_SCORE:
            return None

        if score >= STRONG_SCORE:
            signal = 'STRONG BUY'
        elif score >= BUY_SCORE:
            signal = 'BUY'
        else:
            signal = 'WATCH'

        return {
            'symbol': ticker,
            'signal': signal,
            'score': score,
            'max_score': 9,
            'current_price': current_price,
            'ma60': round(ma60, 2),
            'jongi_gap': round(jongi_gap_today, 2),
            'jongi_gap_trend': '증가' if jongi_gap_today > jongi_gap_yesterday else '감소',
            'macd_signal': '영선위' if macd_above_zero else '영선아래',
            'obv_signal': '상승중' if obv_rising else '하락중',
            'volume_signal': '증가' if volume_increasing else '감소',
            'cloud_status': '구름대위_양운' if (above_cloud and is_positive_cloud) else (
                '구름대위_음운' if above_cloud else '구름대아래'),
            'bb_position': bb_pos,
            'price_vs_ma60_pct': round((current_price - ma60) / ma60 * 100, 2),
            'satisfied_conditions': satisfied,
        }
    except Exception as e:
        return None


# ── STEP 4: 전체 스캔 ────────────────────────────
def run_scan():
    today = datetime.now(KST).strftime("%Y-%m-%d")
    log(f"=== SpaceEum AI Lab 자동 스캔 시작: {today} ===")
    log(f"거래소: 업비트 (KRW 마켓)")

    tickers = get_top_tickers(TOP_N)
    signals = []

    log(f"{len(tickers)}개 티커 스캔 중...")

    for i, ticker in enumerate(tickers):
        result = analyze_ticker(ticker)
        if result:
            signals.append(result)
        if (i + 1) % 50 == 0:
            log(f"진행: {i + 1}/{len(tickers)} | 신호: {len(signals)}개")
        time.sleep(0.1)  # API 속도 제한 준수

    signals.sort(key=lambda x: x['score'], reverse=True)

    buy_signals = [s for s in signals if s['signal'] in ['BUY', 'STRONG BUY']]
    watch_signals = [s for s in signals if s['signal'] == 'WATCH']

    log(f"\n=== 스캔 완료 ===")
    log(f"STRONG BUY: {len([s for s in signals if s['signal'] == 'STRONG BUY'])}개")
    log(f"BUY: {len([s for s in signals if s['signal'] == 'BUY'])}개")
    log(f"WATCH: {len(watch_signals)}개")

    if buy_signals:
        log("★ BUY 신호 종목:")
        for s in buy_signals[:10]:
            log(f"  {s['signal']} | {s['symbol']} | {s['score']}/9점 | MA60대비 {s['price_vs_ma60_pct']}%")

    os.makedirs('data', exist_ok=True)

    result_data = {
        'date': today,
        'scan_time': datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        'exchange': 'Upbit',
        'market': 'KRW',
        'total_scanned': len(tickers),
        'strong_buy_count': len([s for s in signals if s['signal'] == 'STRONG BUY']),
        'buy_count': len([s for s in signals if s['signal'] == 'BUY']),
        'watch_count': len(watch_signals),
        'top_signals': signals[:10],
        'all_buy_signals': buy_signals,
        'all_watch_signals': watch_signals[:20],
    }

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(result_data, f, indent=2, ensure_ascii=False)

    log(f"결과 저장 완료: {OUTPUT_PATH}")
    log("=== 완료 ===")


if __name__ == '__main__':
    run_scan()
