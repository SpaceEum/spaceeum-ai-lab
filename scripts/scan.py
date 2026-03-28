#!/usr/bin/env python3
"""
SpaceEum AI Lab - 바이낸스 선물 60일 이평 자동 스캔
ccxt 라이브러리 사용 (GitHub Actions 미국 서버 우회)
"""

import json
import time
import math
import os
from datetime import datetime, timezone, timedelta

try:
    import ccxt
except ImportError:
    os.system("pip install ccxt -q")
    import ccxt

# ── 설정 ─────────────────────────────────────────
TOP_N = 300
MIN_SCORE = 5
BUY_SCORE = 6
STRONG_SCORE = 8
OUTPUT_PATH = "data/scan_latest.json"
KST = timezone(timedelta(hours=9))

def log(msg):
    now = datetime.now(KST).strftime("%H:%M:%S")
    print(f"[{now}] {msg}")

def init_exchange():
    exchange = ccxt.binanceusdm({
        'enableRateLimit': True,
        'options': {
            'defaultType': 'future',
            'adjustForTimeDifference': True,
        }
    })
    return exchange

def get_top_tickers(exchange, n=300):
    log("바이낸스 선물 티커 목록 가져오는 중...")
    tickers = exchange.fetch_tickers()
    usdt = {k: v for k, v in tickers.items() if k.endswith('/USDT')}
    sorted_tickers = sorted(
        usdt.items(),
        key=lambda x: float(x[1].get('quoteVolume') or 0),
        reverse=True
    )
    top = sorted_tickers[:n]
    symbols = [t[0] for t in top]
    log(f"전체 USDT 선물: {len(usdt)}개 → 상위 {n}개 추출")
    return symbols

def analyze_ticker(exchange, symbol):
    ohlcv = exchange.fetch_ohlcv(symbol, '1d', limit=90)
    if len(ohlcv) < 62:
        return None

    closes = [d[4] for d in ohlcv]
    volumes = [d[5] for d in ohlcv]
    highs = [d[2] for d in ohlcv]
    lows = [d[3] for d in ohlcv]
    current_price = closes[-1]

    ma60 = sum(closes[-60:]) / 60
    ma60_prev = sum(closes[-61:-1]) / 60
    ma60_prev2 = sum(closes[-62:-2]) / 60
    jongi_gap_today = ma60 - ma60_prev
    jongi_gap_yesterday = ma60_prev - ma60_prev2

    if current_price <= ma60:
        return None

    ma9 = sum(closes[-9:]) / 9
    ma10 = sum(closes[-10:]) / 10
    ma26 = sum(closes[-26:]) / 26

    def ema(prices, period):
        k = 2 / (period + 1)
        v = prices[0]
        for p in prices[1:]:
            v = p * k + v * (1 - k)
        return v

    macd = ema(closes[-40:], 12) - ema(closes[-40:], 26)
    macd_above_zero = macd > 0

    obv_list = [0]
    for j in range(1, len(closes)):
        if closes[j] > closes[j-1]:
            obv_list.append(obv_list[-1] + volumes[j])
        elif closes[j] < closes[j-1]:
            obv_list.append(obv_list[-1] - volumes[j])
        else:
            obv_list.append(obv_list[-1])
    obv_rising = sum(obv_list[-5:]) / 5 > sum(obv_list[-15:-5]) / 10

    volume_increasing = sum(volumes[-5:]) / 5 > sum(volumes[-25:-5]) / 20

    span_a = (ma9 + ma26) / 2
    span_b = (max(highs[-52:]) + min(lows[-52:])) / 2
    above_cloud = current_price > max(span_a, span_b)
    is_positive_cloud = span_a > span_b
    is_new_high_60d = current_price >= max(closes[-60:]) * 0.99

    ma20 = sum(closes[-20:]) / 20
    std20 = math.sqrt(sum((c - ma20)**2 for c in closes[-20:]) / 20)
    bb_upper = ma20 + 2 * std20
    bb_lower = ma20 - 2 * std20
    bb_pos = round((current_price - bb_lower) / (bb_upper - bb_lower) * 100, 1) if bb_upper != bb_lower else 50

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
        'symbol': symbol.replace('/USDT', 'USDT'),
        'signal': signal,
        'score': score,
        'max_score': 9,
        'current_price': current_price,
        'ma60': round(ma60, 6),
        'jongi_gap': round(jongi_gap_today, 6),
        'jongi_gap_trend': '증가' if jongi_gap_today > jongi_gap_yesterday else '감소',
        'macd_signal': '영선위' if macd_above_zero else '영선아래',
        'obv_signal': '상승중' if obv_rising else '하락중',
        'volume_signal': '증가' if volume_increasing else '감소',
        'cloud_status': '구름대위_양운' if (above_cloud and is_positive_cloud) else ('구름대위_음운' if above_cloud else '구름대아래'),
        'bb_position': bb_pos,
        'price_vs_ma60_pct': round((current_price - ma60) / ma60 * 100, 2),
        'satisfied_conditions': satisfied,
    }

def run_scan():
    today = datetime.now(KST).strftime("%Y-%m-%d")
    log(f"=== SpaceEum AI Lab 자동 스캔 시작: {today} ===")

    exchange = init_exchange()
    symbols = get_top_tickers(exchange, TOP_N)
    signals = []

    log(f"{len(symbols)}개 티커 스캔 중...")

    for i, symbol in enumerate(symbols):
        try:
            result = analyze_ticker(exchange, symbol)
            if result:
                signals.append(result)
            if (i + 1) % 50 == 0:
                log(f"진행: {i+1}/{len(symbols)} | 신호: {len(signals)}개")
            time.sleep(0.1)
        except Exception as e:
            continue

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
        'total_scanned': len(symbols),
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
