#!/usr/bin/env python3
"""
SpaceEum AI Lab - 바이낸스 선물 60일 이평 자동 스캔
매일 오전 9시 (KST) GitHub Actions에서 자동 실행
"""

import json
import urllib.request
import time
import math
import os
from datetime import datetime, timezone, timedelta

# ── 설정 ─────────────────────────────────────────
TOP_N = 300          # 상위 몇 개 티커 스캔
MIN_SCORE = 5        # 최소 신호 점수
BUY_SCORE = 6        # BUY 신호 점수
STRONG_SCORE = 8     # STRONG BUY 점수
OUTPUT_PATH = "data/scan_latest.json"

# 한국 시간
KST = timezone(timedelta(hours=9))

def log(msg):
    now = datetime.now(KST).strftime("%H:%M:%S")
    print(f"[{now}] {msg}")

# ── STEP 1: 거래량 상위 300개 티커 가져오기 ──────
def get_top_tickers(n=300):
    log(f"바이낸스 선물 티커 목록 가져오는 중...")
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    with urllib.request.urlopen(url, timeout=10) as r:
        data = json.loads(r.read())
    usdt = [d for d in data if d['symbol'].endswith('USDT')]
    sorted_data = sorted(usdt, key=lambda x: float(x['quoteVolume']), reverse=True)
    top = sorted_data[:n]
    symbols = [d['symbol'] for d in top]
    log(f"전체 USDT 선물: {len(usdt)}개 → 상위 {n}개 추출")
    return symbols

# ── STEP 2: 개별 티커 분석 ────────────────────────
def analyze_ticker(symbol):
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1d&limit=90"
    with urllib.request.urlopen(url, timeout=5) as r:
        data = json.loads(r.read())

    if len(data) < 62:
        return None

    closes = [float(d[4]) for d in data]
    volumes = [float(d[5]) for d in data]
    highs = [float(d[2]) for d in data]
    lows = [float(d[3]) for d in data]

    current_price = closes[-1]

    # 60일 이평 및 종이격
    ma60 = sum(closes[-60:]) / 60
    ma60_prev = sum(closes[-61:-1]) / 60
    ma60_prev2 = sum(closes[-62:-2]) / 60
    jongi_gap_today = ma60 - ma60_prev
    jongi_gap_yesterday = ma60_prev - ma60_prev2

    # 1차 필터: 현재가 > 60일 이평
    if current_price <= ma60:
        return None

    # 이평선들
    ma9 = sum(closes[-9:]) / 9
    ma10 = sum(closes[-10:]) / 10
    ma26 = sum(closes[-26:]) / 26

    # MACD (12/26)
    def ema(prices, period):
        k = 2 / (period + 1)
        v = prices[0]
        for p in prices[1:]:
            v = p * k + v * (1 - k)
        return v

    ema12 = ema(closes[-40:], 12)
    ema26_val = ema(closes[-40:], 26)
    macd = ema12 - ema26_val
    macd_prev = ema(closes[-41:-1], 12) - ema(closes[-41:-1], 26)
    macd_above_zero = macd > 0

    # OBV
    obv_list = [0]
    for j in range(1, len(closes)):
        if closes[j] > closes[j-1]:
            obv_list.append(obv_list[-1] + volumes[j])
        elif closes[j] < closes[j-1]:
            obv_list.append(obv_list[-1] - volumes[j])
        else:
            obv_list.append(obv_list[-1])
    obv_rising = sum(obv_list[-5:]) / 5 > sum(obv_list[-15:-5]) / 10

    # 거래량 증가
    vol_recent = sum(volumes[-5:]) / 5
    vol_prev = sum(volumes[-25:-5]) / 20
    volume_increasing = vol_recent > vol_prev

    # 일목균형표 구름대
    span_a = (ma9 + ma26) / 2
    span_b = (max(highs[-52:]) + min(lows[-52:])) / 2
    above_cloud = current_price > max(span_a, span_b)
    is_positive_cloud = span_a > span_b

    # 60일 신고가
    high_60d = max(closes[-60:])
    is_new_high_60d = current_price >= high_60d * 0.99

    # 볼린저 밴드
    ma20 = sum(closes[-20:]) / 20
    std20 = math.sqrt(sum((c - ma20)**2 for c in closes[-20:]) / 20)
    bb_upper = ma20 + 2 * std20
    bb_lower = ma20 - 2 * std20
    bb_position = round((current_price - bb_lower) / (bb_upper - bb_lower) * 100, 1) if bb_upper != bb_lower else 50

    # 조건 체크 (총 9개)
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

    # 신호 등급
    if score >= STRONG_SCORE:
        signal = 'STRONG BUY'
    elif score >= BUY_SCORE:
        signal = 'BUY'
    else:
        signal = 'WATCH'

    return {
        'symbol': symbol,
        'signal': signal,
        'score': score,
        'max_score': len(conditions),
        'current_price': current_price,
        'ma60': round(ma60, 6),
        'jongi_gap': round(jongi_gap_today, 6),
        'jongi_gap_trend': '증가' if jongi_gap_today > jongi_gap_yesterday else '감소',
        'macd': round(macd, 6),
        'macd_signal': '영선위' if macd_above_zero else '영선아래',
        'obv_signal': '상승중' if obv_rising else '하락중',
        'volume_signal': '증가' if volume_increasing else '감소',
        'cloud_status': '구름대위_양운' if (above_cloud and is_positive_cloud) else ('구름대위_음운' if above_cloud else '구름대아래'),
        'bb_position': bb_position,
        'price_vs_ma60_pct': round((current_price - ma60) / ma60 * 100, 2),
        'satisfied_conditions': satisfied,
    }

# ── STEP 3: 전체 스캔 실행 ────────────────────────
def run_scan():
    today = datetime.now(KST).strftime("%Y-%m-%d")
    log(f"=== SpaceEum AI Lab 자동 스캔 시작: {today} ===")

    # 티커 목록 가져오기
    symbols = get_top_tickers(TOP_N)

    # 스캔 실행
    signals = []
    passed_step1 = 0

    log(f"{len(symbols)}개 티커 스캔 중...")

    for i, symbol in enumerate(symbols):
        try:
            result = analyze_ticker(symbol)
            if result:
                if result['score'] >= MIN_SCORE:
                    passed_step1 += 1
                signals.append(result)

            if (i + 1) % 50 == 0:
                log(f"진행: {i+1}/{len(symbols)} | 신호: {len(signals)}개")

            time.sleep(0.08)  # API 부하 방지

        except Exception as e:
            continue

    # 점수 높은 순 정렬
    signals.sort(key=lambda x: x['score'], reverse=True)

    buy_signals = [s for s in signals if s['signal'] in ['BUY', 'STRONG BUY']]
    watch_signals = [s for s in signals if s['signal'] == 'WATCH']

    log(f"\n=== 스캔 완료 ===")
    log(f"STRONG BUY: {len([s for s in signals if s['signal'] == 'STRONG BUY'])}개")
    log(f"BUY: {len([s for s in signals if s['signal'] == 'BUY'])}개")
    log(f"WATCH: {len(watch_signals)}개")

    if buy_signals:
        log(f"\n★ BUY 신호 종목:")
        for s in buy_signals[:10]:
            log(f"  {s['signal']} | {s['symbol']} | {s['score']}/9점 | MA60대비 {s['price_vs_ma60_pct']}%")

    # 결과 저장
    result_data = {
        'date': today,
        'scan_time': datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        'total_scanned': len(symbols),
        'strong_buy_count': len([s for s in signals if s['signal'] == 'STRONG BUY']),
        'buy_count': len([s for s in signals if s['signal'] == 'BUY']),
        'watch_count': len(watch_signals),
        'top_signals': signals[:10],  # 상위 10개만 홈페이지 표시
        'all_buy_signals': buy_signals,
        'all_watch_signals': watch_signals[:20],
    }

    # data 폴더 없으면 생성
    os.makedirs('data', exist_ok=True)

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(result_data, f, indent=2, ensure_ascii=False)

    log(f"\n결과 저장 완료: {OUTPUT_PATH}")
    log(f"=== 완료 ===")

    return result_data

if __name__ == '__main__':
    run_scan()
