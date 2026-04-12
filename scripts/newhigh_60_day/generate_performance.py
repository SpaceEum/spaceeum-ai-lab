#!/usr/bin/env python3
# ============================================================
# shin_trades.db → data/shin_performance.json 내보내기
# GitHub Actions에서 run_cycle.py 실행 후 호출
# ============================================================
import sqlite3, json, os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH   = os.path.join(BASE_DIR, "data", "shin_trades.db")
OUT_PATH  = os.path.join(BASE_DIR, "data", "shin_performance.json")

def main():
    if not os.path.exists(DB_PATH):
        print(f"DB 없음: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # 전략별 성과
    balances = conn.execute(
        "SELECT * FROM strategy_balance ORDER BY total_pnl_pct DESC"
    ).fetchall()
    strategies = []
    for b in balances:
        b = dict(b)
        win_rate = round(b["win_trades"] / b["total_trades"] * 100, 1) if b["total_trades"] > 0 else 0
        strategies.append({
            "name":            b["strategy"],
            "initial_balance": b["initial_balance"],
            "current_balance": round(b["current_balance"], 2),
            "total_trades":    b["total_trades"],
            "win_trades":      b["win_trades"],
            "loss_trades":     b["total_trades"] - b["win_trades"],
            "win_rate":        win_rate,
            "total_pnl_pct":   round(b["total_pnl_pct"], 2),
        })

    # 오픈 포지션
    opens_raw = conn.execute(
        "SELECT * FROM trades WHERE status='OPEN' ORDER BY entry_time DESC"
    ).fetchall()
    opens = [{
        "strategy":    t["strategy"],
        "symbol":      t["symbol"],
        "direction":   t["direction"],
        "entry_price": t["entry_price"],
        "stop_loss":   t["stop_loss"],
        "take_profit": t["take_profit"],
        "entry_time":  t["entry_time"],
        "entry_reason":t["entry_reason"],
    } for t in opens_raw]

    # 최근 거래 10개
    recent_raw = conn.execute(
        "SELECT * FROM trades WHERE status!='OPEN' ORDER BY exit_time DESC LIMIT 10"
    ).fetchall()
    recent = [{
        "strategy":   t["strategy"],
        "symbol":     t["symbol"],
        "direction":  t["direction"],
        "entry_price":t["entry_price"],
        "exit_price": t["exit_price"],
        "pnl_pct":    round(t["pnl_pct"], 2) if t["pnl_pct"] else 0,
        "exit_reason":t["exit_reason"],
        "entry_time": t["entry_time"],
        "exit_time":  t["exit_time"],
        "status":     t["status"],
    } for t in recent_raw]

    total_closed = conn.execute("SELECT COUNT(*) as c FROM trades WHERE status!='OPEN'").fetchone()["c"]
    total_wins   = conn.execute("SELECT COUNT(*) as c FROM trades WHERE status='WIN'").fetchone()["c"]
    overall_win_rate = round(total_wins / total_closed * 100, 1) if total_closed > 0 else 0

    result = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "summary": {
            "total_closed_trades": total_closed,
            "total_wins":          total_wins,
            "overall_win_rate":    overall_win_rate,
            "open_positions":      len(opens),
            "best_strategy":       strategies[0]["name"] if strategies else None,
        },
        "strategies":    strategies,
        "open_positions": opens,
        "recent_trades": recent,
    }

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(f"성과 JSON 저장 완료: {OUT_PATH}")

if __name__ == "__main__":
    main()
