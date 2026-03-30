#!/usr/bin/env python3
"""
SpaceEum AI Lab - 업비트 KRW 멀티 타임프레임 스캔 (1D + 4H)
신창환 60MA 이론 기반

1D: MA60 = 60일 이평 | 4H: MA60 = 60개 4시간봉 (= 약 10일)

매수 조건: STRONG BUY(8점+) + 1~2번 자리  (1D 기준)
  → 1D + 4H 동시 STRONG BUY = 최강 매수 타점
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

STOP_LOSS_PCT = -7.0
TAKE_PROFIT_PCT = 15.0
SELL_SCORE_THRESHOLD = 5

OUTPUT_PATH = "data/scan_latest.json"
TRADES_PATH = "data/paper_trades.json"
PERF_PATH = "data/performance.json"

INITIAL_CAPITAL = 10_000_000
TRADE_UNIT = 2_000_000
MAX_POSITIONS = 5

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


# ── STEP 2: OHLCV 데이터 ──────────────────────────
def get_ohlcv(ticker, count, interval):
    """interval: 'day' | 'minute240'"""
    df = pyupbit.get_ohlcv(ticker, count=count, interval=interval)
    return df


# ── STEP 3: 60MA 8등분 주기 판단 ─────────────────
def get_cycle_zone(jongi_gaps):
    """
    jongi_gaps: 최근 종이격 리스트 [oldest → newest] (최소 2개)
    1~2번: MA60 상승 + 종이격 증가 (상승 초기 → 최적 매수 타점)
    3~4번: MA60 상승 + 종이격 감소 (고점권 → 꼭지 경고)
    5~6번: MA60 하락 + 더 악화    (하락 중 → 관망)
    7~8번: MA60 하락 + 완만해짐   (바닥권 → 매수 준비)
    """
    if len(jongi_gaps) < 2:
        return "판단불가"
    latest = jongi_gaps[-1]
    prev = jongi_gaps[-2]
    if latest > 0:
        return "1~2번" if latest >= prev else "3~4번"
    else:
        return "7~8번" if abs(latest) <= abs(prev) else "5~6번"


def get_cycle_label(zone):
    return {
        "1~2번": "📈 상승초기",
        "3~4번": "⚠️ 고점경고",
        "5~6번": "📉 하락중",
        "7~8번": "🔄 바닥권",
        "판단불가": "❓ 판단불가",
    }.get(zone, zone)


# ── STEP 4: 공통 기술적 분석 함수 ────────────────
def analyze_ohlcv(ticker, closes, volumes, highs, lows, timeframe_label):
    """
    9개 조건 분석 (1D / 4H 공통 사용)
    timeframe_label: '1D' or '4H'
    """
    try:
        n = len(closes)
        if n < 65:
            return None

        current_price = closes[-1]

        # ── MA60 + 종이격 ─────────────────────────
        def ma(offset_from_end, period):
            s = -(period + offset_from_end)
            e = -offset_from_end if offset_from_end > 0 else n
            window = closes[s:e] if offset_from_end > 0 else closes[-period:]
            return sum(window) / period if len(window) == period else None

        ma60 = ma(0, 60)
        ma60_p1 = ma(1, 60)
        ma60_p2 = ma(2, 60)
        if not all([ma60, ma60_p1, ma60_p2]):
            return None

        jongi_today = ma60 - ma60_p1
        jongi_yesterday = ma60_p1 - ma60_p2

        # 종이격 히스토리 (5일치)
        jongi_gaps = []
        prev_val = None
        for offset in range(4, -1, -1):
            v = ma(offset, 60)
            if v and prev_val:
                jongi_gaps.append(v - prev_val)
            prev_val = v

        cycle_zone = get_cycle_zone(jongi_gaps if jongi_gaps else [jongi_yesterday, jongi_today])
        cycle_label = get_cycle_label(cycle_zone)

        # 1차 필터: 현재가 MA60 아래면 제외
        if current_price <= ma60:
            return None

        # ── 이평선 ────────────────────────────────
        ma9 = sum(closes[-9:]) / 9
        ma10 = sum(closes[-10:]) / 10
        ma26 = sum(closes[-26:]) / 26

        # ── MACD ─────────────────────────────────
        def ema(prices, period):
            k = 2 / (period + 1)
            v = prices[0]
            for p in prices[1:]:
                v = p * k + v * (1 - k)
            return v

        macd = ema(closes[-40:], 12) - ema(closes[-40:], 26)
        macd_above_zero = macd > 0

        # ── OBV ──────────────────────────────────
        obv = [0]
        for j in range(1, n):
            if closes[j] > closes[j - 1]:
                obv.append(obv[-1] + volumes[j])
            elif closes[j] < closes[j - 1]:
                obv.append(obv[-1] - volumes[j])
            else:
                obv.append(obv[-1])
        obv_rising = sum(obv[-5:]) / 5 > sum(obv[-15:-5]) / 10

        # ── 거래량 ────────────────────────────────
        volume_increasing = sum(volumes[-5:]) / 5 > sum(volumes[-25:-5]) / 20

        # ── 일목균형표 ────────────────────────────
        span_a = (ma9 + ma26) / 2
        span_b = (max(highs[-52:]) + min(lows[-52:])) / 2
        above_cloud = current_price > max(span_a, span_b)
        is_positive_cloud = span_a > span_b

        # ── 60봉 신고가 근처 ──────────────────────
        is_new_high = current_price >= max(closes[-60:]) * 0.99

        # ── 볼린저 밴드 ───────────────────────────
        ma20 = sum(closes[-20:]) / 20
        std20 = math.sqrt(sum((c - ma20) ** 2 for c in closes[-20:]) / 20)
        bb_upper = ma20 + 2 * std20
        bb_lower = ma20 - 2 * std20
        bb_pos = round((current_price - bb_lower) / (bb_upper - bb_lower) * 100, 1) \
            if bb_upper != bb_lower else 50

        # ── 9개 조건 ──────────────────────────────
        conditions = {
            '현재가_MA60_위': current_price > ma60,
            'MA60_우상향': jongi_today > 0,
            '종이격_증가중': jongi_today > jongi_yesterday,
            'MACD_영선_위': macd_above_zero,
            'OBV_상승중': obv_rising,
            '거래량_증가': volume_increasing,
            '정배열_전환선_10이평_기준선': ma9 > ma10 > ma26,
            '구름대_위_및_양운': above_cloud and is_positive_cloud,
            f'{timeframe_label}_신고가_근처': is_new_high,
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
            'signal': signal,
            'score': score,
            'max_score': 9,
            'ma60': round(ma60, 4),
            'jongi_gap': round(jongi_today, 4),
            'jongi_gap_trend': '증가' if jongi_today > jongi_yesterday else '감소',
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
        return None


# ── STEP 5: 개별 티커 1D + 4H 분석 ──────────────
def analyze_ticker(ticker):
    try:
        # ── 1D 분석 ──────────────────────────────
        df_1d = get_ohlcv(ticker, count=90, interval="day")
        result_1d = None
        if df_1d is not None and len(df_1d) >= 65:
            result_1d = analyze_ohlcv(
                ticker,
                df_1d['close'].tolist(),
                df_1d['volume'].tolist(),
                df_1d['high'].tolist(),
                df_1d['low'].tolist(),
                '1D'
            )

        time.sleep(0.05)

        # ── 4H 분석 ──────────────────────────────
        # 60 4H봉 = 60 × 4h = 10일
        # 분석에 필요한 최소 캔들: MA60(60) + 여유(30) = 90개
        df_4h = get_ohlcv(ticker, count=120, interval="minute240")
        result_4h = None
        if df_4h is not None and len(df_4h) >= 65:
            result_4h = analyze_ohlcv(
                ticker,
                df_4h['close'].tolist(),
                df_4h['volume'].tolist(),
                df_4h['high'].tolist(),
                df_4h['low'].tolist(),
                '4H'
            )

        # 1D 결과가 없으면 스킵 (현재가 MA60 아래이거나 데이터 부족)
        if result_1d is None:
            return None

        current_price = df_1d['close'].tolist()[-1]

        # 1D+4H 동시 STRONG BUY 여부
        dual_strong = (
            result_1d['signal'] == 'STRONG BUY' and
            result_4h is not None and
            result_4h['signal'] == 'STRONG BUY'
        )

        return {
            'symbol': ticker,
            'current_price': current_price,
            # 1D
            'signal': result_1d['signal'],
            'score': result_1d['score'],
            'max_score': 9,
            'ma60': round(result_1d['ma60'], 2),
            'jongi_gap': round(result_1d['jongi_gap'], 2),
            'jongi_gap_trend': result_1d['jongi_gap_trend'],
            'cycle_zone': result_1d['cycle_zone'],
            'cycle_label': result_1d['cycle_label'],
            'macd_signal': result_1d['macd_signal'],
            'obv_signal': result_1d['obv_signal'],
            'volume_signal': result_1d['volume_signal'],
            'cloud_status': result_1d['cloud_status'],
            'bb_position': result_1d['bb_position'],
            'price_vs_ma60_pct': result_1d['price_vs_ma60_pct'],
            'satisfied_conditions': result_1d['satisfied_conditions'],
            # 4H
            'tf_4h': {
                'signal': result_4h['signal'] if result_4h else None,
                'score': result_4h['score'] if result_4h else None,
                'cycle_zone': result_4h['cycle_zone'] if result_4h else None,
                'cycle_label': result_4h['cycle_label'] if result_4h else None,
                'jongi_gap': round(result_4h['jongi_gap'], 4) if result_4h else None,
                'macd_signal': result_4h['macd_signal'] if result_4h else None,
                'obv_signal': result_4h['obv_signal'] if result_4h else None,
                'satisfied_conditions': result_4h['satisfied_conditions'] if result_4h else [],
            },
            # 멀티 타임프레임
            'dual_strong': dual_strong,
        }
    except Exception as e:
        log(f"오류 ({ticker}): {e}")
        return None


# ── STEP 6: 페이퍼 트레이딩 ──────────────────────

def load_trades():
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
            "symbol": best['symbol'], "pnl_pct": best['pnl_pct'],
            "exit_reason": best['exit_reason'],
            "entry_date": best['entry_date'], "exit_date": best['exit_date'],
        } if best else None,
        "worst_trade": {
            "symbol": worst['symbol'], "pnl_pct": worst['pnl_pct'],
            "exit_reason": worst['exit_reason'],
            "entry_date": worst['entry_date'], "exit_date": worst['exit_date'],
        } if worst else None,
        "open_position_list": [
            {
                "symbol": t['symbol'],
                "entry_date": t['entry_date'],
                "entry_price": t['entry_price'],
                "entry_cycle": t['entry_cycle'],
                "entry_score": t['entry_score'],
                "dual_strong": t.get('dual_strong', False),
                "current_pnl_pct": t.get('current_pnl_pct'),
            } for t in open_pos
        ],
    }

    os.makedirs('data', exist_ok=True)
    with open(PERF_PATH, 'w', encoding='utf-8') as f:
        json.dump(perf, f, indent=2, ensure_ascii=False)
    return perf


def run_paper_trading(scan_results, today):
    log("=== 페이퍼 트레이딩 업데이트 시작 ===")
    trades_data = load_trades()
    trades = trades_data['trades']
    scan_map = {r['symbol']: r for r in scan_results}

    # ── 오픈 포지션 청산 조건 체크 ────────────────
    closed_count = 0
    for trade in trades:
        if trade['status'] != 'OPEN':
            continue
        symbol = trade['symbol']
        entry_price = trade['entry_price']
        try:
            current_price = pyupbit.get_current_price(symbol)
            time.sleep(0.1)
        except Exception:
            continue
        if not current_price:
            continue

        pnl_pct = (current_price - entry_price) / entry_price * 100
        trade['current_pnl_pct'] = round(pnl_pct, 2)

        today_result = scan_map.get(symbol)
        exit_reason = None

        if pnl_pct <= STOP_LOSS_PCT:
            exit_reason = f"손절 ({pnl_pct:.1f}%)"
        elif pnl_pct >= TAKE_PROFIT_PCT:
            exit_reason = f"익절 ({pnl_pct:.1f}%)"
        elif today_result and today_result['cycle_zone'] == '3~4번':
            exit_reason = "꼭지경고 (3~4번 자리 진입)"
        elif today_result is None or today_result['score'] < SELL_SCORE_THRESHOLD:
            score_now = today_result['score'] if today_result else 0
            exit_reason = f"매도신호 (점수 하락: {score_now}점)"

        if exit_reason:
            trade['status'] = 'CLOSED'
            trade['exit_date'] = today
            trade['exit_price'] = current_price
            trade['exit_reason'] = exit_reason
            trade['exit_cycle'] = today_result['cycle_zone'] if today_result else '알수없음'
            trade['pnl_pct'] = round(pnl_pct, 2)
            trade['pnl_krw'] = round(TRADE_UNIT * pnl_pct / 100)
            log(f"  ✅ 청산: {symbol} | {exit_reason} | PnL: {pnl_pct:+.2f}%")
            closed_count += 1

    # ── 신규 매수: 1D STRONG BUY + 1~2번 자리 ─────
    open_positions = [t for t in trades if t['status'] == 'OPEN']
    open_symbols = {t['symbol'] for t in open_positions}
    new_entries = 0

    if len(open_positions) < MAX_POSITIONS:
        candidates = [
            r for r in scan_results
            if r['signal'] == 'STRONG BUY'
            and r['cycle_zone'] == '1~2번'
            and r['symbol'] not in open_symbols
        ]
        # 1D+4H 동시 STRONG BUY 우선 정렬
        candidates.sort(key=lambda x: (x.get('dual_strong', False), x['score']), reverse=True)

        for c in candidates:
            if len(open_positions) >= MAX_POSITIONS:
                break
            trade_id = f"{today}_{c['symbol']}"
            if any(t['id'] == trade_id for t in trades):
                continue

            tf4h = c.get('tf_4h', {})
            dual = c.get('dual_strong', False)
            entry_reason = f"{'★ 1D+4H STRONG BUY' if dual else 'STRONG BUY'}({c['score']}/9) + {c['cycle_zone']}"

            new_trade = {
                "id": trade_id,
                "symbol": c['symbol'],
                "status": "OPEN",
                "entry_date": today,
                "entry_price": c['current_price'],
                "entry_score": c['score'],
                "entry_score_4h": tf4h.get('score'),
                "entry_cycle": c['cycle_zone'],
                "entry_cycle_4h": tf4h.get('cycle_zone'),
                "dual_strong": dual,
                "entry_reason": entry_reason,
                "exit_date": None, "exit_price": None,
                "exit_reason": None, "exit_cycle": None,
                "pnl_pct": None, "pnl_krw": None, "current_pnl_pct": 0.0,
            }
            trades.append(new_trade)
            open_positions.append(new_trade)
            open_symbols.add(c['symbol'])
            new_entries += 1
            log(f"  🟢 {'★ ' if dual else ''}매수: {c['symbol']} | 1D {c['score']}/9 | 4H {tf4h.get('score', '-')}/9 | {c['cycle_zone']} | {c['current_price']:,.0f}원")

    log(f"페이퍼 트레이딩: 청산 {closed_count}건 | 신규 매수 {new_entries}건 | 오픈 {len([t for t in trades if t['status']=='OPEN'])}건")
    trades_data['trades'] = trades
    save_trades(trades_data)
    return update_performance(trades_data)


# ── STEP 7: 전체 스캔 실행 ────────────────────────
def run_scan():
    today = datetime.now(KST).strftime("%Y-%m-%d")
    log(f"=== SpaceEum AI Lab 멀티 타임프레임 스캔 시작: {today} ===")
    log(f"거래소: 업비트 (KRW 마켓) | 타임프레임: 1D + 4H")

    tickers = get_top_tickers(TOP_N)
    signals = []

    log(f"{len(tickers)}개 티커 스캔 중 (1D + 4H 각각 분석)...")

    for i, ticker in enumerate(tickers):
        result = analyze_ticker(ticker)
        if result:
            signals.append(result)
        if (i + 1) % 50 == 0:
            log(f"진행: {i + 1}/{len(tickers)} | 신호: {len(signals)}개")
        time.sleep(0.15)  # 1D + 4H 두 번 요청하므로 간격 확보

    signals.sort(key=lambda x: (x.get('dual_strong', False), x['score']), reverse=True)

    strong_buy = [s for s in signals if s['signal'] == 'STRONG BUY']
    buy_signals = [s for s in signals if s['signal'] == 'BUY']
    watch_signals = [s for s in signals if s['signal'] == 'WATCH']
    dual_strong = [s for s in signals if s.get('dual_strong')]

    log(f"\n=== 스캔 완료 ===")
    log(f"STRONG BUY (1D): {len(strong_buy)}개")
    log(f"BUY (1D): {len(buy_signals)}개")
    log(f"WATCH (1D): {len(watch_signals)}개")
    log(f"★ 1D+4H 동시 STRONG BUY: {len(dual_strong)}개")

    if dual_strong:
        log("★★★ 최강 매수 타점:")
        for s in dual_strong[:5]:
            log(f"  {s['symbol']} | 1D {s['score']}/9 | 4H {s['tf_4h']['score']}/9 | {s['cycle_zone']}")

    os.makedirs('data', exist_ok=True)
    all_scan = strong_buy + buy_signals + watch_signals
    perf = run_paper_trading(all_scan, today)

    result_data = {
        'date': today,
        'scan_time': datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        'exchange': 'Upbit',
        'market': 'KRW',
        'timeframes': ['1D', '4H'],
        'total_scanned': len(tickers),
        'strong_buy_count': len(strong_buy),
        'buy_count': len(buy_signals),
        'watch_count': len(watch_signals),
        'dual_strong_count': len(dual_strong),
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
