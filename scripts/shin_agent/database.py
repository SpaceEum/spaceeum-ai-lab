"""
신창환 에이전트 - 거래 기록 데이터베이스
"""

import sqlite3
import json
import os
from datetime import datetime
from config import DB_PATH


def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """DB 초기화 - 테이블 생성"""
    conn = get_conn()
    c = conn.cursor()

    # 거래 기록 테이블
    c.execute("""
    CREATE TABLE IF NOT EXISTS trades (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy    TEXT NOT NULL,          -- 전략명
        symbol      TEXT NOT NULL,          -- 코인 심볼
        direction   TEXT NOT NULL,          -- LONG / SHORT
        status      TEXT NOT NULL DEFAULT 'OPEN',  -- OPEN / CLOSED

        entry_time  TEXT NOT NULL,          -- 진입 일시
        exit_time   TEXT,                   -- 청산 일시

        entry_price REAL NOT NULL,          -- 진입 가격
        exit_price  REAL,                   -- 청산 가격
        stop_loss   REAL NOT NULL,          -- 손절 가격
        take_profit REAL NOT NULL,          -- 익절 가격

        entry_reason TEXT NOT NULL,         -- 진입 사유
        exit_reason  TEXT,                  -- 청산 사유

        size_usdt   REAL NOT NULL,          -- 포지션 크기 (USDT)
        leverage    INTEGER NOT NULL,       -- 레버리지

        pnl_pct     REAL,                   -- 이번 거래 수익률 (%)
        pnl_usdt    REAL,                   -- 이번 거래 수익 (USDT)

        indicators  TEXT,                   -- 진입 시 지표 (JSON)
        created_at  TEXT DEFAULT (datetime('now','localtime'))
    )
    """)

    # 전략별 잔고 테이블
    c.execute("""
    CREATE TABLE IF NOT EXISTS strategy_balance (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy    TEXT NOT NULL UNIQUE,
        initial_balance  REAL NOT NULL,
        current_balance  REAL NOT NULL,
        total_trades     INTEGER DEFAULT 0,
        win_trades       INTEGER DEFAULT 0,
        total_pnl_pct    REAL DEFAULT 0,
        max_drawdown     REAL DEFAULT 0,
        updated_at  TEXT DEFAULT (datetime('now','localtime'))
    )
    """)

    # 감시 종목 시그널 로그
    c.execute("""
    CREATE TABLE IF NOT EXISTS signal_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy    TEXT NOT NULL,
        symbol      TEXT NOT NULL,
        signal      TEXT NOT NULL,          -- LONG / SHORT / NONE
        price       REAL NOT NULL,
        reason      TEXT,
        indicators  TEXT,
        created_at  TEXT DEFAULT (datetime('now','localtime'))
    )
    """)

    conn.commit()
    conn.close()
    print("[DB] 데이터베이스 초기화 완료")


def open_trade(strategy: str, symbol: str, direction: str,
               entry_price: float, stop_loss: float, take_profit: float,
               entry_reason: str, size_usdt: float, leverage: int,
               indicators: dict = None) -> int:
    """신규 거래 기록 저장 (진입)"""
    conn = get_conn()
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""
    INSERT INTO trades
    (strategy, symbol, direction, status, entry_time, entry_price,
     stop_loss, take_profit, entry_reason, size_usdt, leverage, indicators)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        strategy, symbol, direction, "OPEN", now, entry_price,
        stop_loss, take_profit, entry_reason,
        size_usdt, leverage,
        json.dumps(indicators or {}, ensure_ascii=False)
    ))
    trade_id = c.lastrowid
    conn.commit()
    conn.close()
    return trade_id


def close_trade(trade_id: int, exit_price: float, exit_reason: str) -> dict:
    """거래 청산 기록"""
    conn = get_conn()
    c = conn.cursor()

    c.execute("SELECT * FROM trades WHERE id=?", (trade_id,))
    trade = c.fetchone()
    if not trade:
        conn.close()
        return None

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 수익률 계산
    entry = trade["entry_price"]
    direction = trade["direction"]
    leverage = trade["leverage"]
    size_usdt = trade["size_usdt"]

    if direction == "LONG":
        raw_pct = (exit_price - entry) / entry
    else:
        raw_pct = (entry - exit_price) / entry

    pnl_pct = raw_pct * leverage * 100      # 레버리지 반영 %
    pnl_usdt = size_usdt * raw_pct * leverage

    status = "WIN" if pnl_pct > 0 else "LOSS"

    c.execute("""
    UPDATE trades SET
        exit_time=?, exit_price=?, exit_reason=?,
        pnl_pct=?, pnl_usdt=?, status=?
    WHERE id=?
    """, (now, exit_price, exit_reason, pnl_pct, pnl_usdt, status, trade_id))

    # 전략 잔고 업데이트
    _update_strategy_balance(c, trade["strategy"], size_usdt, pnl_usdt, pnl_pct > 0)

    conn.commit()
    conn.close()

    return {
        "trade_id": trade_id,
        "symbol": trade["symbol"],
        "direction": direction,
        "entry": entry,
        "exit": exit_price,
        "pnl_pct": round(pnl_pct, 2),
        "pnl_usdt": round(pnl_usdt, 2),
        "status": status
    }


def _update_strategy_balance(c, strategy: str, size_usdt: float, pnl_usdt: float, is_win: bool):
    """전략별 잔고 업데이트 (내부용)"""
    from config import PAPER_BALANCE
    c.execute("SELECT * FROM strategy_balance WHERE strategy=?", (strategy,))
    row = c.fetchone()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if row:
        new_balance = row["current_balance"] + pnl_usdt
        new_trades = row["total_trades"] + 1
        new_wins = row["win_trades"] + (1 if is_win else 0)
        new_pnl = ((new_balance - row["initial_balance"]) / row["initial_balance"]) * 100
        c.execute("""
        UPDATE strategy_balance
        SET current_balance=?, total_trades=?, win_trades=?, total_pnl_pct=?, updated_at=?
        WHERE strategy=?
        """, (new_balance, new_trades, new_wins, new_pnl, now, strategy))
    else:
        new_balance = PAPER_BALANCE + pnl_usdt
        c.execute("""
        INSERT INTO strategy_balance
        (strategy, initial_balance, current_balance, total_trades, win_trades, total_pnl_pct)
        VALUES (?,?,?,?,?,?)
        """, (strategy, PAPER_BALANCE, new_balance, 1, 1 if is_win else 0,
              ((new_balance - PAPER_BALANCE) / PAPER_BALANCE) * 100))


def ensure_strategy_balance(strategy: str):
    """전략 잔고 레코드 초기화"""
    from config import PAPER_BALANCE
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT 1 FROM strategy_balance WHERE strategy=?", (strategy,))
    if not c.fetchone():
        c.execute("""
        INSERT OR IGNORE INTO strategy_balance
        (strategy, initial_balance, current_balance, total_trades, win_wins, total_pnl_pct)
        VALUES (?,?,?,0,0,0)
        """, (strategy, PAPER_BALANCE, PAPER_BALANCE))
        conn.commit()
    conn.close()


def get_open_trades() -> list:
    """현재 열린 포지션 조회"""
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM trades WHERE status='OPEN' ORDER BY entry_time DESC")
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def get_all_trades(limit: int = 200) -> list:
    """전체 거래 기록 조회"""
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
    SELECT * FROM trades
    ORDER BY COALESCE(exit_time, entry_time) DESC
    LIMIT ?
    """, (limit,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def get_strategy_stats() -> list:
    """전략별 통계 조회"""
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
    SELECT
        strategy,
        COUNT(*) as total,
        SUM(CASE WHEN status='WIN' THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN status='LOSS' THEN 1 ELSE 0 END) as losses,
        ROUND(AVG(pnl_pct), 2) as avg_pnl,
        ROUND(SUM(pnl_usdt), 2) as total_pnl_usdt,
        ROUND(MAX(pnl_pct), 2) as best_trade,
        ROUND(MIN(pnl_pct), 2) as worst_trade
    FROM trades
    WHERE status IN ('WIN','LOSS')
    GROUP BY strategy
    ORDER BY total_pnl_usdt DESC
    """)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()

    # 승률 추가
    for r in rows:
        r["win_rate"] = round(r["wins"] / r["total"] * 100, 1) if r["total"] > 0 else 0

    return rows


def get_strategy_balance() -> list:
    """전략별 잔고 현황"""
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM strategy_balance ORDER BY total_pnl_pct DESC")
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def log_signal(strategy: str, symbol: str, signal: str,
               price: float, reason: str, indicators: dict):
    """시그널 로그 저장"""
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
    INSERT INTO signal_log (strategy, symbol, signal, price, reason, indicators)
    VALUES (?,?,?,?,?,?)
    """, (strategy, symbol, signal, price, reason,
          json.dumps(indicators, ensure_ascii=False)))
    conn.commit()
    conn.close()


def get_recent_signals(limit: int = 50) -> list:
    """최근 시그널 조회"""
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
    SELECT * FROM signal_log
    WHERE signal != 'NONE'
    ORDER BY created_at DESC
    LIMIT ?
    """, (limit,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows
