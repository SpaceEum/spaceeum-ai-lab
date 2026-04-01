#!/usr/bin/env python3
# ============================================================
# GitHub Actions 전용 — 신창환 60MA 봇 1회 사이클 실행
# ============================================================
import sys, os, traceback
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import init_db
from paper_trader import PaperTrader

if __name__ == "__main__":
    print("=== 신창환 60MA 전략 — 1회 사이클 실행 ===")
    try:
        init_db()
        trader = PaperTrader()
        trader._run_scan()
        print("=== 사이클 완료 ===")
    except Exception as e:
        print(f"[ERROR] 사이클 실행 오류: {e}")
        traceback.print_exc()
        print("=== 사이클 오류 — 워크플로우 계속 진행 ===")
        sys.exit(0)
