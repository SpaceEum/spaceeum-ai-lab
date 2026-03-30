#!/usr/bin/env python3
"""
SpaceEum AI Lab - 업비트 KRW 60일 이평 자동 스캔 + 8등분 주기 분석 + 페이퍼 트레이딩
신창환 60MA 이론 기반

매수 조건: STRONG BUY(8점+) + 1~2번 자리
매도 조건: 손절(-7%) | 익절(+15%) | 3~4번 자리 도달 | 다음날 스캔 5점 이하
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

STOP_LOSS_PCT = -7.0    # 손절: -7%
TAKE_PROFIT_PCT = 15.0  # 익절: +15%
SELL_SCORE_THRESHOLD = 5  # 이 점수 미만이면 매도신호

OUTPUT_PATH = "data/scan_latest.json"
TRADES_PATH = "data/paper_trades.json"
PERF_PATH = "data/performance.json"

INITIAL_CAPITAL = 10_000_000  # 1천만원
TRADE_UNIT = 2_000_000        # 거래당 200만원
MAX_POSITIONS = 5             # 최대 동시 보유

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


# ── STEP 3: 60MA 8등분 주기 판단 ─────────────────
def get_cycle_zone(jongi_gaps):
    """
    신창환 60MA 8등분 이론 기반 현재 주기 위치 판단
    jongi_gaps: 최근 5일 종이격 리스트 [oldest → newest]

    1~2번: MA60 상승 + 종이격 증가 (상승 초기 → 최적 매수 타점)
    3~4번: MA60 상승이지만 종이격 감소 (고점권 → 꼭지 경고)
    5~6번: MA60 하락 + 종이격 더 악화 (하락 중 → 관망)
    7~8번: MA60 하락이지만 종이격 완만해짐 (바닥권 → 매수 준비)
    """
    if len(jongi_gaps) < 2:
        return "판단불가"

    latest = jongi_gaps[-1]
    prev = jongi_gaps[-2]

    if latest > 0:  # MA60 상승 중 (우상향)
        if latest >= prev:
            return "1~2번"  # 종이격 증가 → 상승 초기
        else:
            return "3~4번"  # 종이격 감소 → 꼭지 경고
    else:  # MA60 하락 중 (우하향)
        if abs(latest) <= abs(prev):
            return "7~8번"  # 하락 완만해짐 → 바닥권
        else:
            return "5~6번"  # 하락 심화 → 하락 중


def get_cycle_label(zone):
    labels = {
        "1~2번": "📈 상승초기",
        "3~4번": "⚠️ 고점경고",
        "5~6번": "📉 하락중",
        "7~8번": "🔄 바닥권",
        "판단불가": "❓ 판단불가",
    }
    return labels.get(zone, zone)


# ── STEP 4: 개별 티커 분석 ────────────────────────
def analyze_ticker(ticker):
    try:
        df = get_ohlcv(ticker, count=90)
        if df is None or len(df) < 65:
            return None

        closes = df['close'].tolist()
        volumes = df['volume'].tolist()
        highs = df['high'].tolist()
        lows = df['low'].tolist()
        current_price = closes[-1]

        # 60일 이평 (최근 5일치 계산 → 종이격 트렌드용)
        ma60_series = []
        for offset in range(5, 0, -1):  # 5일전 ~ 오늘
            start = -(60 + offset)
            end = -offset if offset > 0 else None
            if end is None:
                ma60_series.append(sum(closes[-60:]) / 60)
            else:
                ma60_series.append(sum(closes[start:end]) / 60)

        # 사실 위 계산을 다시 명확하게
        ma60_series = []
        for i in range(4, -1, -1):
            s = -(60 + i)
            e = -i if i > 0 else len(closes)
            window = closes[s:e]
            if len(window) == 60:
                ma60_series.append(sum(window) / 60)

        ma60 = sum(closes[-60:]) / 60
        ma60_prev = sum(closes[-61:-1]) / 60
        ma60_prev2 = sum(closes[-62:-2]) / 60

        jongi_gap_today = ma60 - ma60_prev
        jongi_gap_yesterday = ma60_prev - ma60_prev2

        # 종이격 히스토리 (5일)
        jongi_gaps = []
        for i in range(len(ma60_series) - 1):
            jongi_gaps.append(ma60_series[i + 1] - ma60_series[i])

        # 현재 주기 위치
        cycle_zone = get_cycle_zone(jongi_gaps if jongi_gaps else [jongi_gap_yesterday, jongi_gap_today])
        cycle_label = get_cycle_label(cycle_zone)

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
            'cycle_zone': cycle_zone,
            'cycle_label': cycle_label,
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
        log(f"오류 ({ticker}): {e}")
        return None


# ── STEP 5: 페이퍼 트레이딩 로직 ─────────────────

def load_trades():
    """기존 거래 데이터 로드 (없으면 신규 생성)"""
    if os.path.exists(TRADES_PATH):
        with open(TRADES_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {
        "initial_capital": INITIAL_CAPITAL,
        "trade_unit": TRADE_UNIT,
        "max_positions": MAX_POSITIONS,
        "trades": []
    }


def save_trades(data):
    os.makedirs('data', exist_ok=True)
    with open(TRADES_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def update_performance(trades_data):
    """누적 성과 집계"""
    trades = trades_data['trades']
    closed = [t for t in trades if t['status'] == 'CLOSED']
    open_pos = [t for t in trades if t['status'] == 'OPEN']

    wins = [t for t in closed if (t.get('pnl_pct') or 0) > 0]
    losses = [t for t in closed if (t.get('pnl_pct') or 0) <= 0]

    total_pnl = sum(t.get('pnl_pct', 0) or 0 for t in closed)
    avg_pnl = total_pnl / len(closed) if closed else 0
    win_rate = len(wins) / len(closed) * 100 if closed else 0

    best = max(closed, key=lambda t: t.get('pnl_pct', 0) or 0) if closed else None
    worst = min(closed, key=lambda t: t.get('pnl_pct', 0) or 0) if closed else None

    perf = {
        "last_updated": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "total_trades": len(trades),
        "open_positions": len(open_pos),
        "closed_trades": len(closed),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(win_rate, 1),
        "avg_pnl_pct": round(avg_pnl, 2),
        "total_pnl_pct": round(total_pnl, 2),
        "best_trade": {
            "symbol": best['symbol'],
            "pnl_pct": best['pnl_pct'],
            "exit_reason": best['exit_reason'],
            "entry_date": best['entry_date'],
            "exit_date": best['exit_date'],
        } if best else None,
        "worst_trade": {
            "symbol": worst['symbol'],
            "pnl_pct": worst['pnl_pct'],
            "exit_reason": worst['exit_reason'],
            "entry_date": worst['entry_date'],
            "exit_date": worst['exit_date'],
        } if worst else None,
        "open_position_list": [
            {
                "symbol": t['symbol'],
                "entry_date": t['entry_date'],
                "entry_price": t['entry_price'],
                "entry_cycle": t['entry_cycle'],
                "entry_score": t['entry_score'],
                "current_pnl_pct": t.get('current_pnl_pct'),
            } for t in open_pos
        ],
    }

    os.makedirs('data', exist_ok=True)
    with open(PERF_PATH, 'w', encoding='utf-8') as f:
        json.dump(perf, f, indent=2, ensure_ascii=False)

    return perf


def run_paper_trading(scan_results, today):
    """
    페이퍼 트레이딩 실행
    매수: STRONG BUY(8+) + 1~2번 자리
    매도: 손절(-7%) | 익절(+15%) | 3~4번 자리 | 5점 미만으로 하락
    """
    log("=== 페이퍼 트레이딩 업데이트 시작 ===")
    trades_data = load_trades()
    trades = trades_data['trades']

    # 오늘 스캔 결과를 심볼 기준 딕셔너리로
    scan_map = {r['symbol']: r for r in scan_results}

    # ── 1. 오픈 포지션 청산 조건 체크 ──────────────
    closed_count = 0
    for trade in trades:
        if trade['status'] != 'OPEN':
            continue

        symbol = trade['symbol']
        entry_price = trade['entry_price']

        # 현재가 조회
        try:
            current_price = pyupbit.get_current_price(symbol)
            time.sleep(0.1)
        except Exception as e:
            log(f"현재가 조회 실패 ({symbol}): {e}")
            continue

        if not current_price:
            continue

        pnl_pct = (current_price - entry_price) / entry_price * 100
        trade['current_pnl_pct'] = round(pnl_pct, 2)

        exit_reason = None
        exit_cycle = None

        # 오늘 스캔에서 해당 종목 결과 확인
        today_result = scan_map.get(symbol)

        if pnl_pct <= STOP_LOSS_PCT:
            exit_reason = f"손절 ({pnl_pct:.1f}%)"
        elif pnl_pct >= TAKE_PROFIT_PCT:
            exit_reason = f"익절 ({pnl_pct:.1f}%)"
        elif today_result and today_result['cycle_zone'] == '3~4번':
            exit_reason = f"꼭지경고 (3~4번 자리 진입)"
            exit_cycle = '3~4번'
        elif today_result is None or today_result['score'] < SELL_SCORE_THRESHOLD:
            exit_reason = f"매도신호 (스캔 점수 하락: {today_result['score'] if today_result else 0}점)"

        if exit_reason:
            trade['status'] = 'CLOSED'
            trade['exit_date'] = today
            trade['exit_price'] = current_price
            trade['exit_reason'] = exit_reason
            trade['exit_cycle'] = exit_cycle or (today_result['cycle_zone'] if today_result else '알수없음')
            trade['pnl_pct'] = round(pnl_pct, 2)
            trade['pnl_krw'] = round(TRADE_UNIT * pnl_pct / 100)
            log(f"  ✅ 청산: {symbol} | {exit_reason} | PnL: {pnl_pct:+.2f}%")
            closed_count += 1

    # ── 2. 신규 매수 조건 체크 ────────────────────
    open_positions = [t for t in trades if t['status'] == 'OPEN']
    open_symbols = {t['symbol'] for t in open_positions}
    new_entries = 0

    if len(open_positions) < MAX_POSITIONS:
        # STRONG BUY + 1~2번 자리인 종목 추출
        candidates = [
            r for r in scan_results
            if r['signal'] == 'STRONG BUY'
            and r['cycle_zone'] == '1~2번'
            and r['symbol'] not in open_symbols
        ]
        candidates.sort(key=lambda x: x['score'], reverse=True)

        for candidate in candidates:
            if len(open_positions) >= MAX_POSITIONS:
                break

            trade_id = f"{today}_{candidate['symbol']}"
            # 같은 날 같은 종목 중복 방지
            if any(t['id'] == trade_id for t in trades):
                continue

            new_trade = {
                "id": trade_id,
                "symbol": candidate['symbol'],
                "status": "OPEN",
                "entry_date": today,
                "entry_price": candidate['current_price'],
                "entry_score": candidate['score'],
                "entry_cycle": candidate['cycle_zone'],
                "entry_cycle_label": candidate['cycle_label'],
                "entry_jongi_gap": candidate['jongi_gap'],
                "entry_reason": f"STRONG BUY({candidate['score']}/9) + {candidate['cycle_zone']} (종이격 {candidate['jongi_gap']:+.0f})",
                "exit_date": None,
                "exit_price": None,
                "exit_reason": None,
                "exit_cycle": None,
                "pnl_pct": None,
                "pnl_krw": None,
                "current_pnl_pct": 0.0,
            }
            trades.append(new_trade)
            open_positions.append(new_trade)
            open_symbols.add(candidate['symbol'])
            new_entries += 1
            log(f"  🟢 신규 매수: {candidate['symbol']} | {candidate['score']}/9점 | {candidate['cycle_zone']} | {candidate['current_price']:,.0f}원")

    log(f"페이퍼 트레이딩: 청산 {closed_count}건 | 신규 매수 {new_entries}건 | 오픈 포지션 {len([t for t in trades if t['status']=='OPEN'])}건")

    trades_data['trades'] = trades
    save_trades(trades_data)

    perf = update_performance(trades_data)
    log(f"누적 성과: 총 {perf['total_trades']}건 | 승률 {perf['win_rate']}% | 평균 PnL {perf['avg_pnl_pct']:+.2f}%")

    return perf


# ── STEP 6: 전체 스캔 실행 ────────────────────────
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

    strong_buy = [s for s in signals if s['signal'] == 'STRONG BUY']
    buy_signals = [s for s in signals if s['signal'] == 'BUY']
    watch_signals = [s for s in signals if s['signal'] == 'WATCH']

    log(f"\n=== 스캔 완료 ===")
    log(f"STRONG BUY: {len(strong_buy)}개")
    log(f"BUY: {len(buy_signals)}개")
    log(f"WATCH: {len(watch_signals)}개")

    # 주기별 분포
    zone_counts = {}
    for s in strong_buy + buy_signals:
        zone = s.get('cycle_zone', '알수없음')
        zone_counts[zone] = zone_counts.get(zone, 0) + 1
    log(f"주기 분포: {zone_counts}")

    buy_all = strong_buy + buy_signals
    if buy_all:
        log("★ STRONG BUY 신호 종목 (1~2번 자리):")
        for s in strong_buy[:10]:
            log(f"  {s['signal']} | {s['symbol']} | {s['score']}/9점 | {s['cycle_label']} | MA60대비 {s['price_vs_ma60_pct']}%")

    os.makedirs('data', exist_ok=True)

    # 페이퍼 트레이딩 실행
    all_scan = strong_buy + buy_signals + watch_signals
    perf = run_paper_trading(all_scan, today)

    result_data = {
        'date': today,
        'scan_time': datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        'exchange': 'Upbit',
        'market': 'KRW',
        'total_scanned': len(tickers),
        'strong_buy_count': len(strong_buy),
        'buy_count': len(buy_signals),
        'watch_count': len(watch_signals),
        # 1~2번 자리 STRONG BUY = 실제 매수 대상
        'buy_targets_count': len([s for s in strong_buy if s['cycle_zone'] == '1~2번']),
        'top_signals': signals[:10],
        'all_buy_signals': strong_buy + buy_signals,
        'all_watch_signals': watch_signals[:20],
        'paper_trading': {
            'open_positions': perf['open_positions'],
            'total_trades': perf['total_trades'],
            'win_rate': perf['win_rate'],
            'avg_pnl_pct': perf['avg_pnl_pct'],
            'open_list': perf.get('open_position_list', []),
        },
    }

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(result_data, f, indent=2, ensure_ascii=False)

    log(f"결과 저장 완료: {OUTPUT_PATH}")
    log("=== 완료 ===")


if __name__ == '__main__':
    run_scan()
