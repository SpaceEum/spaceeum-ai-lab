"""
신창환 에이전트 설정 파일 — 업비트 버전
"""

# ──────────────────────────────────────────
# 모니터링 설정
# ──────────────────────────────────────────
TOP_N_SYMBOLS = 50          # 거래량 상위 N개 감시
CANDLE_INTERVAL = "day"     # 일봉 (신창환 60일 이평선 기준)
CANDLE_LIMIT = 120          # 최근 120개 캔들 (60MA 계산에 충분)
SCAN_INTERVAL_MINUTES = 60  # GitHub Actions는 매일 1회 실행

# ──────────────────────────────────────────
# 페이퍼 트레이딩 설정
# ──────────────────────────────────────────
PAPER_BALANCE = 1_000_000   # 초기 자본 (1,000,000 KRW)
LEVERAGE = 1                # 레버리지 없음 (현물 기준)
POSITION_SIZE_PCT = 0.1     # 포지션당 자본의 10% 사용
MAX_POSITIONS = 5           # 최대 동시 포지션

# ──────────────────────────────────────────
# DB 설정
# ──────────────────────────────────────────
import os as _os
DB_PATH = _os.path.join(
    _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))),
    "data", "shin_trades.db"
)
