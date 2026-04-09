#!/usr/bin/env python3
"""
60MA 통합 전략 백테스트
타임프레임 : 1시간봉 (1H)
기간       : 최근 1년
대상       : 업비트 KRW 마켓 전체
방식       : 5개 전략 가중 점수 합산 → 임계점 이상 시 매수

실행:
  python scripts/backtest.py
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyupbit

# shin_agent 경로 추가
sys.path.insert(0, str(Path(__file__).parent))
from shin_agent.strategies import ALL_STRATEGIES

# ── 설정 ─────────────────────────────────────────────────────
KST = timezone(timedelta(hours=9))

# 가중치 (1단계: 균등)
WEIGHTS = {s.name: 1 for s in ALL_STRATEGIES}

# 임계점: 이 이상일 때 매수 (3~5 사이에서 비교)
BUY_THRESHOLD = 2

# 리스크 파라미터
STOP_LOSS_PCT  = -5.0   # -5% 손절
TAKE_PROFIT_PCT = 10.0  # +10% 익절

# 데이터 설정
CANDLE_INTERVAL = "minute60"       # 1시간봉
CANDLES_PER_YEAR = 365 * 24       # 8,760개
WARMUP_CANDLES   = 80              # 전략 계산에 필요한 최소 봉 수
FETCH_BATCH      = 200             # pyupbit 1회 최대 요청 수

# ohlcv parquet 경로 (수집 완료된 경우 API 대신 사용)
OHLCV_DIR = Path(__file__).parent.parent / "data" / "ohlcv" / "1H_upbit"

OUTPUT_PATH = f"data/backtest_result_threshold{BUY_THRESHOLD}.json"


# ── 로그 ──────────────────────────────────────────────────────
def log(msg):
    print(f"[{datetime.now(KST).strftime('%H:%M:%S')}] {msg}")


# ── STEP 1: 데이터 수집 ───────────────────────────────────────
def load_from_parquet(ticker: str) -> list:
    """로컬 parquet에서 1H 캔들 로드 (ohlcv_update 수집 데이터 사용)"""
    safe = ticker.replace("-", "_")
    path = OHLCV_DIR / f"{safe}.parquet"
    if not path.exists():
        return []
    try:
        import pandas as pd
        df = pd.read_parquet(path, engine="pyarrow")
        # 최근 1년치만 사용
        df = df.tail(CANDLES_PER_YEAR).reset_index(drop=True)
        return [
            {
                "time":   str(row["datetime"]),
                "open":   row["open"],
                "high":   row["high"],
                "low":    row["low"],
                "close":  row["close"],
                "volume": row["volume"],
            }
            for _, row in df.iterrows()
        ]
    except Exception as e:
        log(f"  parquet 로드 실패 ({ticker}): {e}")
        return []


def fetch_1y_1h(ticker: str) -> list:
    """티커의 1년치 1H 캔들 가져오기 — parquet 우선, 없으면 API"""
    # parquet 우선 시도
    candles = load_from_parquet(ticker)
    if candles:
        return candles

    # parquet 없으면 API에서 직접 수집
    all_candles = []
    to = None

    while len(all_candles) < CANDLES_PER_YEAR:
        try:
            kwargs = dict(ticker=ticker, interval=CANDLE_INTERVAL, count=FETCH_BATCH)
            if to:
                kwargs["to"] = to
            df = pyupbit.get_ohlcv(**kwargs)

            if df is None or df.empty:
                break

            batch = [
                {
                    "time":   str(idx),
                    "open":   row["open"],
                    "high":   row["high"],
                    "low":    row["low"],
                    "close":  row["close"],
                    "volume": row["volume"],
                }
                for idx, row in df.iterrows()
            ]

            # 오래된 데이터를 앞에 붙임
            all_candles = batch + all_candles
            to = df.index[0].strftime("%Y-%m-%d %H:%M:%S")

            if len(df) < FETCH_BATCH:
                break

            time.sleep(0.12)

        except Exception as e:
            break

    # 1년치만 유지
    return all_candles[-CANDLES_PER_YEAR:] if len(all_candles) > CANDLES_PER_YEAR else all_candles


# ── STEP 2: 단일 티커 백테스트 ────────────────────────────────
def backtest_ticker(ticker: str, candles: list) -> dict | None:
    """1H 캔들로 통합 전략 백테스트"""
    if len(candles) < WARMUP_CANDLES + 10:
        return None

    trades  = []
    position = None  # 현재 보유 포지션

    for i in range(WARMUP_CANDLES, len(candles)):
        window        = candles[:i + 1]   # 현재 봉까지의 전체 슬라이딩 윈도우
        current_price = candles[i]["close"]
        current_time  = candles[i]["time"]

        # ── 보유 중: 손절 / 익절 / 매도 신호 체크 ─────────────
        if position:
            pnl_pct = (current_price - position["entry_price"]) / position["entry_price"] * 100

            exit_reason = None
            if pnl_pct <= STOP_LOSS_PCT:
                exit_reason = f"손절 ({pnl_pct:.1f}%)"
            elif pnl_pct >= TAKE_PROFIT_PCT:
                exit_reason = f"익절 ({pnl_pct:.1f}%)"
            else:
                # 전략 SELL 신호 집계
                sell_count = 0
                for strategy in ALL_STRATEGIES:
                    try:
                        r = strategy.analyze(window)
                        if r["signal"] == "SELL":
                            sell_count += 1
                    except:
                        pass
                if sell_count >= 2:
                    exit_reason = f"매도신호 ({sell_count}개 전략 SELL)"

            if exit_reason:
                position.update({
                    "exit_price":  current_price,
                    "exit_time":   current_time,
                    "pnl_pct":     round(pnl_pct, 2),
                    "exit_reason": exit_reason,
                })
                trades.append(position)
                position = None
                continue

        # ── 미보유: BUY 스코어 계산 ───────────────────────────
        if position is None:
            score   = 0
            signals = []
            for strategy in ALL_STRATEGIES:
                try:
                    r = strategy.analyze(window)
                    if r["signal"] == "BUY":
                        score += WEIGHTS.get(strategy.name, 1)
                        signals.append(strategy.name)
                except:
                    pass

            if score >= BUY_THRESHOLD:
                position = {
                    "symbol":        ticker,
                    "entry_price":   current_price,
                    "entry_time":    current_time,
                    "entry_score":   score,
                    "entry_signals": signals,
                    "exit_price":    None,
                    "exit_time":     None,
                    "pnl_pct":       None,
                    "exit_reason":   None,
                }

    # 백테스트 종료 시 미청산 포지션 → 마지막 가격으로 강제 청산
    if position:
        last_price = candles[-1]["close"]
        pnl_pct    = (last_price - position["entry_price"]) / position["entry_price"] * 100
        position.update({
            "exit_price":  last_price,
            "exit_time":   candles[-1]["time"],
            "pnl_pct":     round(pnl_pct, 2),
            "exit_reason": "백테스트 종료",
        })
        trades.append(position)

    if not trades:
        return None

    wins      = [t for t in trades if t["pnl_pct"] > 0]
    losses    = [t for t in trades if t["pnl_pct"] <= 0]
    total_pnl = sum(t["pnl_pct"] for t in trades)
    best      = max(trades, key=lambda t: t["pnl_pct"])
    worst     = min(trades, key=lambda t: t["pnl_pct"])

    return {
        "symbol":        ticker,
        "total_trades":  len(trades),
        "wins":          len(wins),
        "losses":        len(losses),
        "win_rate":      round(len(wins) / len(trades) * 100, 1),
        "avg_pnl_pct":   round(total_pnl / len(trades), 2),
        "total_pnl_pct": round(total_pnl, 2),
        "best_trade_pct":  best["pnl_pct"],
        "worst_trade_pct": worst["pnl_pct"],
        "candles_used":  len(candles),
        "recent_trades": trades[-5:],  # 최근 5개만 저장
    }


# ── STEP 3: 전체 실행 ─────────────────────────────────────────
def run_backtest():
    log("=" * 60)
    log("60MA 통합 전략 백테스트 시작")
    log(f"타임프레임: 1H | 기간: 1년 | 임계점: {BUY_THRESHOLD}점")
    log(f"손절: {STOP_LOSS_PCT}% | 익절: {TAKE_PROFIT_PCT}%")
    log("=" * 60)

    tickers = pyupbit.get_tickers(fiat="KRW")
    log(f"대상 코인: {len(tickers)}개")

    results      = []
    failed       = []
    total        = len(tickers)

    for idx, ticker in enumerate(tickers, 1):
        log(f"[{idx}/{total}] {ticker} 데이터 수집 중...")

        candles = fetch_1y_1h(ticker)

        if len(candles) < WARMUP_CANDLES + 10:
            log(f"  → 데이터 부족 ({len(candles)}개), 스킵")
            failed.append(ticker)
            time.sleep(0.1)
            continue

        log(f"  → {len(candles)}개 캔들 수집 완료, 백테스트 실행 중...")
        result = backtest_ticker(ticker, candles)

        if result:
            results.append(result)
            log(f"  → 거래 {result['total_trades']}건 | 승률 {result['win_rate']}% | "
                f"평균수익 {result['avg_pnl_pct']:+.2f}%")
        else:
            log(f"  → 거래 없음 (조건 미충족)")
            failed.append(ticker)

        time.sleep(0.2)

    # ── 전체 요약 ─────────────────────────────────────────────
    if not results:
        log("결과 없음")
        return

    all_trades   = sum(r["total_trades"] for r in results)
    all_wins     = sum(r["wins"] for r in results)
    total_pnl    = sum(r["total_pnl_pct"] for r in results)
    overall_wr   = round(all_wins / all_trades * 100, 1) if all_trades else 0
    overall_avg  = round(total_pnl / all_trades, 2) if all_trades else 0

    # 수익률 상위 10개
    top10 = sorted(results, key=lambda r: r["avg_pnl_pct"], reverse=True)[:10]
    # 거래 횟수 상위 10개
    top10_active = sorted(results, key=lambda r: r["total_trades"], reverse=True)[:10]

    summary = {
        "run_date":         datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "timeframe":        "1H",
        "period":           "1년",
        "threshold":        BUY_THRESHOLD,
        "weights":          WEIGHTS,
        "stop_loss_pct":    STOP_LOSS_PCT,
        "take_profit_pct":  TAKE_PROFIT_PCT,
        "total_coins_scanned": total,
        "coins_with_trades":   len(results),
        "coins_no_trades":     len(failed),
        "all_trades":       all_trades,
        "all_wins":         all_wins,
        "overall_win_rate": overall_wr,
        "overall_avg_pnl":  overall_avg,
        "top10_by_avg_pnl": [
            {
                "symbol":      r["symbol"],
                "trades":      r["total_trades"],
                "win_rate":    r["win_rate"],
                "avg_pnl":     r["avg_pnl_pct"],
                "total_pnl":   r["total_pnl_pct"],
            }
            for r in top10
        ],
        "top10_by_activity": [
            {
                "symbol":    r["symbol"],
                "trades":    r["total_trades"],
                "win_rate":  r["win_rate"],
                "avg_pnl":   r["avg_pnl_pct"],
            }
            for r in top10_active
        ],
        "all_results": results,
    }

    os.makedirs("data", exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    log("=" * 60)
    log("백테스트 완료!")
    log(f"분석 코인: {len(results)}개 | 총 거래: {all_trades}건")
    log(f"전체 승률: {overall_wr}% | 평균 수익: {overall_avg:+.2f}%")
    log("\n📊 평균 수익률 TOP 10:")
    for i, r in enumerate(top10, 1):
        log(f"  {i:2}. {r['symbol']:20} 승률 {r['win_rate']:5.1f}% | "
            f"평균 {r['avg_pnl_pct']:+6.2f}% | 거래 {r['total_trades']}건")
    log(f"\n결과 저장: {OUTPUT_PATH}")
    log("=" * 60)


if __name__ == "__main__":
    run_backtest()
