#!/usr/bin/env python3
# ============================================================
# GitHub Actions 전용 — 신창환 60MA 봇 1회 사이클 실행
# ============================================================
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import init_db
from paper_trader import PaperTrader

if __name__ == "__main__":
    print("=== 신창환 60MA 전략 — 1회 사이클 실행 ===")
    init_db()
    trader = PaperTrader()
    trader._run_scan()
    print("=== 사이클 완료 ===")
