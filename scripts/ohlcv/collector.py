#!/usr/bin/env python3
"""
SpaceEum OHLCV 수집기
업비트 KRW 마켓 1달 거래량 상위 200개 × 6개 타임프레임
GitHub 레포 data/ohlcv/ 에 parquet 저장 / 매일 00:00 KST 자동 실행
"""

import os
import json
import time
import requests
import pandas as pd
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── 상수 ──────────────────────────────────────────────────────

KST = timezone(timedelta(hours=9))
UPBIT_BASE = "https://api.upbit.com/v1"
TOP_N = 200
DATA_DIR = Path("data/ohlcv")

# 초기 전체 수집 시 최근 N년치만 가져옴 (전체 이력은 너무 오래 걸림)
HISTORY_YEARS = 2
# 이 시간(초) 초과 시 수집 중단하고 지금까지 수집한 것만 커밋
MAX_RUNTIME_SEC = 5 * 3600 + 30 * 60  # 5시간 30분
TICKER_LIST_FILE = DATA_DIR / "ticker_list.json"

TIMEFRAMES = {
    "15min": {"unit": "minutes", "value": 15,  "folder": "15min_upbit"},
    "1H":    {"unit": "minutes", "value": 60,  "folder": "1H_upbit"},
    "4H":    {"unit": "minutes", "value": 240, "folder": "4H_upbit"},
    "1D":    {"unit": "days",    "value": 1,   "folder": "1D_upbit"},
    "1W":    {"unit": "weeks",   "value": 1,   "folder": "1W_upbit"},
    "1M":    {"unit": "months",  "value": 1,   "folder": "1M_upbit"},
}

# 완성된 봉 판단 기준 (분 단위, None = 월봉 별도 처리)
TF_MINUTES = {
    "15min": 15,
    "1H":    60,
    "4H":    240,
    "1D":    1440,
    "1W":    10080,
    "1M":    None,
}


# ── 로컬 파일 I/O ─────────────────────────────────────────────

def get_tf_dir(tf_key):
    """타임프레임 폴더 경로 반환 + 생성"""
    d = DATA_DIR / TIMEFRAMES[tf_key]["folder"]
    d.mkdir(parents=True, exist_ok=True)
    return d


def load_df(tf_key, market):
    """로컬 parquet → DataFrame (없으면 빈 DataFrame)"""
    safe = market.replace("-", "_")
    path = get_tf_dir(tf_key) / f"{safe}.parquet"
    if path.exists():
        return pd.read_parquet(path, engine="pyarrow")
    return pd.DataFrame()


def save_df(df, tf_key, market):
    """DataFrame → 로컬 parquet"""
    safe = market.replace("-", "_")
    path = get_tf_dir(tf_key) / f"{safe}.parquet"
    df.to_parquet(path, index=False, engine="pyarrow")


def delete_local(tf_key, market):
    """로컬 parquet 파일 삭제"""
    safe = market.replace("-", "_")
    path = get_tf_dir(tf_key) / f"{safe}.parquet"
    if path.exists():
        path.unlink()
        print(f"  [삭제] {path.name}")


def load_ticker_list():
    """저장된 티커 목록 로드 (없으면 빈 리스트)"""
    if TICKER_LIST_FILE.exists():
        with open(TICKER_LIST_FILE, encoding="utf-8") as f:
            data = json.load(f)
        return data.get("tickers", [])
    return []


def save_ticker_list(tickers, now_kst):
    """티커 목록 저장"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(TICKER_LIST_FILE, "w", encoding="utf-8") as f:
        json.dump(
            {"tickers": tickers, "updated_at": now_kst.strftime("%Y-%m-%d %H:%M KST")},
            f, ensure_ascii=False, indent=2
        )


# ── Upbit API ─────────────────────────────────────────────────

def upbit_get(url, params=None, retries=3):
    """Upbit API GET (재시도 포함)"""
    for i in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if i == retries - 1:
                raise
            print(f"  [API] 재시도 {i+1}/{retries}: {e}")
            time.sleep(2 ** i)


def get_all_krw_markets():
    """현재 업비트에 상장된 모든 KRW 마켓 반환"""
    data = upbit_get(f"{UPBIT_BASE}/market/all", {"isDetails": "false"})
    return [m["market"] for m in data if m["market"].startswith("KRW-")]


def get_top200_by_monthly_volume(all_markets):
    """모든 KRW 마켓의 30일 거래대금 합산 후 상위 200개 반환"""
    print(f"[선정] 전체 {len(all_markets)}개 마켓 1달 거래대금 계산 중...")
    volumes = []

    for i, market in enumerate(all_markets):
        try:
            candles = upbit_get(
                f"{UPBIT_BASE}/candles/days",
                {"market": market, "count": 30}
            )
            vol = sum(c.get("candle_acc_trade_price", 0) for c in candles)
        except Exception:
            vol = 0
        volumes.append((market, vol))

        if (i + 1) % 50 == 0:
            print(f"  {i+1}/{len(all_markets)} 완료...")
        time.sleep(0.13)

    volumes.sort(key=lambda x: x[1], reverse=True)
    top = [m for m, _ in volumes[:TOP_N]]
    print(f"[선정] 상위 {len(top)}개 선정 완료")
    return top


def fetch_candles(market, tf_key, to=None, count=200):
    """업비트 캔들 API 호출"""
    tf = TIMEFRAMES[tf_key]
    unit = tf["unit"]
    value = tf["value"]

    if unit == "minutes":
        url = f"{UPBIT_BASE}/candles/minutes/{value}"
    elif unit == "days":
        url = f"{UPBIT_BASE}/candles/days"
    elif unit == "weeks":
        url = f"{UPBIT_BASE}/candles/weeks"
    elif unit == "months":
        url = f"{UPBIT_BASE}/candles/months"
    else:
        raise ValueError(f"지원하지 않는 unit: {unit}")

    params = {"market": market, "count": count}
    if to:
        params["to"] = to
    return upbit_get(url, params)


def candles_to_df(candles):
    """캔들 리스트 → DataFrame (KST datetime, 오름차순 정렬)"""
    if not candles:
        return pd.DataFrame()
    df = pd.DataFrame(candles)
    df = df.rename(columns={
        "candle_date_time_kst":      "datetime",
        "opening_price":             "open",
        "high_price":                "high",
        "low_price":                 "low",
        "trade_price":               "close",
        "candle_acc_trade_volume":   "volume",
        "candle_acc_trade_price":    "trade_price_krw",
    })
    cols = [c for c in ["datetime", "open", "high", "low", "close", "volume", "trade_price_krw"] if c in df.columns]
    df = df[cols].copy()
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.sort_values("datetime").reset_index(drop=True)


# ── 완성된 봉 필터링 ──────────────────────────────────────────

def is_candle_complete(open_dt, tf_key):
    """
    해당 봉이 완성된 봉인지 판단
    open_dt: KST naive datetime (봉의 시작 시각)
    """
    now_kst = datetime.now(KST).replace(tzinfo=None)
    tf_min = TF_MINUTES[tf_key]

    if tf_min is None:
        # 월봉: 다음 달 1일이 되어야 완성
        if open_dt.month == 12:
            next_month_start = datetime(open_dt.year + 1, 1, 1)
        else:
            next_month_start = datetime(open_dt.year, open_dt.month + 1, 1)
        return now_kst >= next_month_start

    return now_kst >= open_dt + timedelta(minutes=tf_min)


def filter_incomplete(df, tf_key):
    """미완성 봉(현재 진행 중인 봉) 제거"""
    if df.empty:
        return df
    last_dt = df.iloc[-1]["datetime"]
    if isinstance(last_dt, pd.Timestamp):
        last_dt = last_dt.to_pydatetime().replace(tzinfo=None)
    if not is_candle_complete(last_dt, tf_key):
        df = df.iloc[:-1].copy().reset_index(drop=True)
    return df


# ── 데이터 수집 ───────────────────────────────────────────────

def fetch_all_history(market, tf_key):
    """최근 HISTORY_YEARS년치 이력 수집 (초기 수집 시간 단축)"""
    all_candles = []
    to = None
    cutoff = datetime.now(KST).replace(tzinfo=None) - timedelta(days=365 * HISTORY_YEARS)

    print(f"  [전체수집] {market} {tf_key} 최근 {HISTORY_YEARS}년치 시작...", flush=True)

    for _ in range(2000):
        try:
            candles = fetch_candles(market, tf_key, to=to, count=200)
        except Exception as e:
            print(f"  [오류] {e} — 5초 후 재시도", flush=True)
            time.sleep(5)
            continue

        if not candles:
            break

        all_candles.extend(candles)

        if len(candles) < 200:
            break  # 상장 초기에 도달

        # cutoff 이전 데이터에 도달하면 중단
        oldest_str = candles[-1]["candle_date_time_kst"]
        oldest_dt = datetime.strptime(oldest_str[:19], "%Y-%m-%dT%H:%M:%S")
        if oldest_dt <= cutoff:
            break

        to = oldest_str
        time.sleep(0.13)

    if not all_candles:
        return pd.DataFrame()

    df = candles_to_df(all_candles)
    # cutoff 이후 데이터만 유지
    df = df[df["datetime"] >= pd.Timestamp(cutoff)]
    df = df.drop_duplicates("datetime").sort_values("datetime").reset_index(drop=True)
    print(f"  → {len(df)}행 수집 완료", flush=True)
    return df


def fetch_incremental(market, tf_key, last_dt):
    """last_dt 이후 신규 캔들만 수집"""
    all_candles = []
    to = None

    for _ in range(200):
        try:
            candles = fetch_candles(market, tf_key, to=to, count=200)
        except Exception as e:
            print(f"  [오류] {e}")
            break

        if not candles:
            break

        # last_dt 이전 캔들이 나오면 거기서 잘라냄
        new = []
        reached_old = False
        for c in candles:
            dt = pd.to_datetime(c["candle_date_time_kst"])
            if dt > last_dt:
                new.append(c)
            else:
                reached_old = True
                break

        all_candles.extend(new)

        if reached_old or len(candles) < 200:
            break

        to = candles[-1]["candle_date_time_kst"]
        time.sleep(0.13)

    return candles_to_df(all_candles)


# ── 단일 티커 × 타임프레임 수집 ──────────────────────────────

def collect_ticker(market, tf_key):
    """기존 데이터 로드 → 신규 수집 → 저장, 행 수 반환"""
    existing = load_df(tf_key, market)

    if existing.empty:
        df = fetch_all_history(market, tf_key)
    else:
        last_dt = existing["datetime"].max()
        if isinstance(last_dt, pd.Timestamp):
            last_dt = last_dt.to_pydatetime()

        df_new = fetch_incremental(market, tf_key, last_dt)

        if df_new.empty:
            df = existing
        else:
            df = pd.concat([existing, df_new], ignore_index=True)
            df = df.drop_duplicates("datetime").sort_values("datetime").reset_index(drop=True)
            added = len(df) - len(existing)
            if added > 0:
                print(f"  +{added}행 추가 (총 {len(df)}행)")

    df = filter_incomplete(df, tf_key)

    if not df.empty:
        save_df(df, tf_key, market)

    return len(df)


# ── 텔레그램 ─────────────────────────────────────────────────

def send_telegram(text):
    token = os.environ.get("TELEGRAM_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("[텔레그램] 환경변수 없음 — 알림 스킵")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
    }).encode()
    try:
        urllib.request.urlopen(urllib.request.Request(url, data), timeout=10)
        print("[텔레그램] 전송 완료")
    except Exception as e:
        print(f"[텔레그램] 실패: {e}")


# ── 메인 ─────────────────────────────────────────────────────

def main():
    start_time = time.time()
    now_kst = datetime.now(KST)
    print(f"\n{'='*60}")
    print(f" SpaceEum OHLCV 수집 시작: {now_kst.strftime('%Y-%m-%d %H:%M KST')}")
    print(f"{'='*60}\n")

    errors = []
    delisted_removed = []

    # ── 폴더 초기화
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for tf_key in TIMEFRAMES:
        get_tf_dir(tf_key)
    print("[초기화] data/ohlcv/ 폴더 준비 완료")

    # ── 1달 거래량 상위 200개 선정
    all_markets = get_all_krw_markets()
    current_tickers = get_top200_by_monthly_volume(all_markets)
    active_set = set(all_markets)

    # ── 폐지 티커 처리 (이전 목록에 있었지만 업비트에서 사라진 것)
    prev_tickers = load_ticker_list()

    for ticker in prev_tickers:
        if ticker not in active_set:
            print(f"[폐지] {ticker} 감지 → 파일 삭제")
            for tf_key in TIMEFRAMES:
                delete_local(tf_key, ticker)
            delisted_removed.append(ticker)

    # 현재 티커 목록 저장
    save_ticker_list(current_tickers, now_kst)

    # ── 데이터 수집 (200 티커 × 6 타임프레임)
    total_tasks = len(current_tickers) * len(TIMEFRAMES)
    done = 0

    for market in current_tickers:
        for tf_key in TIMEFRAMES:
            # 최대 실행 시간 초과 시 조기 종료
            elapsed_so_far = time.time() - start_time
            if elapsed_so_far > MAX_RUNTIME_SEC:
                print(f"\n⏰ 최대 실행 시간({MAX_RUNTIME_SEC//3600}h {(MAX_RUNTIME_SEC%3600)//60}m) 초과 — 수집 중단 후 커밋", flush=True)
                break

            done += 1
            try:
                rows = collect_ticker(market, tf_key)
                print(f"[{done}/{total_tasks}] ✓ {market} {tf_key} ({rows}행)", flush=True)
            except Exception as e:
                err_msg = f"{market} {tf_key}: {e}"
                print(f"[{done}/{total_tasks}] ❌ {err_msg}", flush=True)
                errors.append(err_msg)
            time.sleep(0.05)
        else:
            continue
        break  # 내부 루프에서 break 시 외부 루프도 중단

    elapsed = int(time.time() - start_time)
    status = "success" if not errors else "partial"

    # ── 상태 JSON 저장 (홈페이지에서 읽음)
    status_data = {
        "last_updated": now_kst.strftime("%Y-%m-%d %H:%M KST"),
        "status": status,
        "tickers": len(current_tickers),
        "timeframes": list(TIMEFRAMES.keys()),
        "total_files": len(current_tickers) * len(TIMEFRAMES),
        "delisted_removed": delisted_removed,
        "errors": errors[:10],
        "duration_sec": elapsed,
    }
    os.makedirs("data", exist_ok=True)
    with open("data/ohlcv_status.json", "w", encoding="utf-8") as f:
        json.dump(status_data, f, ensure_ascii=False, indent=2)
    print("\n[상태] data/ohlcv_status.json 저장 완료")

    # ── 텔레그램 알림
    icon = "✅" if status == "success" else "⚠️"
    msg = (
        f"{icon} <b>SpaceEum OHLCV 수집</b>\n"
        f"📅 {now_kst.strftime('%Y-%m-%d %H:%M KST')}\n\n"
        f"📊 수집 결과\n"
        f"├ 상태: {'성공' if status == 'success' else '부분 성공'}\n"
        f"├ 티커: {len(current_tickers)}개\n"
        f"├ 파일: {len(current_tickers) * len(TIMEFRAMES)}개\n"
        f"├ 폐지 삭제: {len(delisted_removed)}개\n"
        f"├ 오류: {len(errors)}건\n"
        f"└ 소요시간: {elapsed // 60}분 {elapsed % 60}초"
    )
    if delisted_removed:
        msg += f"\n\n🗑 폐지 티커: {', '.join(delisted_removed)}"
    if errors:
        msg += "\n\n❌ 오류 (최대 3건):\n" + "\n".join(errors[:3])

    send_telegram(msg)
    print(f"\n{'='*60}")
    print(f" 완료: {elapsed // 60}분 {elapsed % 60}초 소요")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
