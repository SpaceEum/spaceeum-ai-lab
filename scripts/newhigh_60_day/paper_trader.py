"""
신창환 에이전트 - 페이퍼 트레이딩 엔진

실시간 시장 데이터로 가상 매매를 수행합니다.
실제 주문은 실행하지 않으며, 모든 거래는 시뮬레이션입니다.
"""

import time
import threading
from datetime import datetime
from config import (PAPER_BALANCE, LEVERAGE, POSITION_SIZE_PCT,
                    MAX_POSITIONS, SCAN_INTERVAL_MINUTES)
from upbit_data import get_top_symbols, get_klines, get_batch_prices
from strategies import ALL_STRATEGIES
from database import (init_db, open_trade, close_trade, get_open_trades,
                      log_signal, ensure_strategy_balance)


class PaperTrader:
    """페이퍼 트레이딩 엔진"""

    def __init__(self):
        self.running = False
        self.symbols = []
        self.last_scan = None
        self.scan_count = 0
        self.lock = threading.Lock()

        # 전략별 잔고 관리
        self.balances = {s.name: PAPER_BALANCE for s in ALL_STRATEGIES}

        # DB 초기화 및 전략 잔고 레코드 생성
        init_db()
        self._init_balances()

        print(f"[PaperTrader] 초기화 완료 - 전략 {len(ALL_STRATEGIES)}개, 초기자본 {PAPER_BALANCE:,.0f} USDT")

    def _init_balances(self):
        """DB에 전략별 초기 잔고 레코드 생성"""
        from config import PAPER_BALANCE
        from database import get_conn
        conn = get_conn()
        c = conn.cursor()
        for strategy in ALL_STRATEGIES:
            c.execute("SELECT 1 FROM strategy_balance WHERE strategy=?", (strategy.name,))
            if not c.fetchone():
                c.execute("""
                INSERT INTO strategy_balance
                (strategy, initial_balance, current_balance, total_trades, win_trades, total_pnl_pct)
                VALUES (?,?,?,0,0,0)
                """, (strategy.name, PAPER_BALANCE, PAPER_BALANCE))
        conn.commit()
        conn.close()

    def start(self):
        """모니터링 시작"""
        self.running = True
        thread = threading.Thread(target=self._scan_loop, daemon=True)
        thread.start()
        print(f"[PaperTrader] 모니터링 시작 - {SCAN_INTERVAL_MINUTES}분 간격")

    def stop(self):
        """모니터링 중지"""
        self.running = False
        print("[PaperTrader] 모니터링 중지")

    def _scan_loop(self):
        """메인 스캔 루프"""
        while self.running:
            try:
                self._run_scan()
            except Exception as e:
                print(f"[PaperTrader] 스캔 오류: {e}")
            # 다음 스캔까지 대기
            time.sleep(SCAN_INTERVAL_MINUTES * 60)

    def _run_scan(self):
        """전체 종목 스캔 실행"""
        start = time.time()
        self.last_scan = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.scan_count += 1

        print(f"\n[{self.last_scan}] ===== 스캔 #{self.scan_count} 시작 =====")

        # 1. 상위 종목 업데이트
        with self.lock:
            self.symbols = get_top_symbols()

        # 2. 열린 포지션 청산 체크
        self._check_open_positions()

        # 3. 신규 진입 신호 스캔
        self._scan_for_signals()

        elapsed = time.time() - start
        print(f"[PaperTrader] 스캔 완료 ({elapsed:.1f}초) - 종목 {len(self.symbols)}개")

    def _check_open_positions(self):
        """열린 포지션의 손절/익절 체크"""
        open_trades = get_open_trades()
        if not open_trades:
            return

        symbols_needed = list(set(t["symbol"] for t in open_trades))
        prices = get_batch_prices(symbols_needed)

        for trade in open_trades:
            symbol = trade["symbol"]
            current_price = prices.get(symbol, 0)
            if current_price == 0:
                continue

            direction = trade["direction"]
            entry = trade["entry_price"]
            sl = trade["stop_loss"]
            tp = trade["take_profit"]

            should_close = False
            exit_reason = ""

            # 현물 매수만 — 손절/익절 모두 현재가 기준 하락/상승
            if current_price <= sl:
                should_close = True
                exit_reason = f"손절 (현재가 {current_price:.4f} ≤ SL {sl:.4f})"
            elif tp and current_price >= tp:
                should_close = True
                exit_reason = f"익절 (현재가 {current_price:.4f} ≥ TP {tp:.4f})"

            if should_close:
                result = close_trade(trade["id"], current_price, exit_reason)
                if result:
                    emoji = "✅" if result["pnl_pct"] > 0 else "❌"
                    print(f"  {emoji} [{trade['strategy']}] {symbol} {direction} 청산 "
                          f"| {exit_reason} | PnL: {result['pnl_pct']:+.2f}%")

    def _scan_for_signals(self):
        """신규 진입 신호 스캔"""
        open_trades = get_open_trades()
        open_count = len(open_trades)

        # 전략별 이미 열린 포지션 수 집계
        strategy_open = {}
        symbol_open = set()
        for t in open_trades:
            strategy_open[t["strategy"]] = strategy_open.get(t["strategy"], 0) + 1
            symbol_open.add(f"{t['strategy']}_{t['symbol']}")

        new_trades = 0

        for symbol in self.symbols:
            # 전체 최대 포지션 초과 시 중단
            if open_count >= MAX_POSITIONS * len(ALL_STRATEGIES):
                break

            # 캔들 데이터 조회
            candles = get_klines(symbol)
            if len(candles) < 62:
                continue

            current_price = candles[-1]["close"]

            for strategy in ALL_STRATEGIES:
                # 해당 전략+심볼 포지션이 이미 열려있으면 스킵
                key = f"{strategy.name}_{symbol}"
                if key in symbol_open:
                    continue

                # 전략별 최대 포지션 (심볼당 3개)
                if strategy_open.get(strategy.name, 0) >= 5:
                    continue

                # 전략 분석
                try:
                    result = strategy.analyze(candles)
                except Exception as e:
                    print(f"  [오류] {strategy.name} {symbol}: {e}")
                    continue

                # 시그널 로그 저장 (NONE 제외)
                if result["signal"] != "NONE":
                    log_signal(
                        strategy.name, symbol, result["signal"],
                        current_price, result["reason"], result["indicators"]
                    )

                # 매수 진입
                if result["signal"] == "BUY":
                    size_usdt = PAPER_BALANCE * POSITION_SIZE_PCT
                    trade_id = open_trade(
                        strategy=strategy.name,
                        symbol=symbol,
                        direction="BUY",
                        entry_price=result["entry"],
                        stop_loss=result["stop_loss"],
                        take_profit=result["take_profit"],
                        entry_reason=result["reason"],
                        size_usdt=size_usdt,
                        leverage=LEVERAGE,
                        indicators=result["indicators"]
                    )

                    print(f"  🟢 [{strategy.name}] {symbol} "
                          f"매수 @ {result['entry']:.4f} "
                          f"| SL:{result['stop_loss']:.4f} TP:{result['take_profit']:.4f}")
                    print(f"     이유: {result['reason']}")

                # SELL 신호 = 보유 포지션 청산
                elif result["signal"] == "SELL":
                    key = f"{strategy.name}_{symbol}"
                    matching = [t for t in open_trades if f"{t['strategy']}_{t['symbol']}" == key]
                    for t in matching:
                        close_trade(t["id"], current_price, result["reason"])
                        print(f"  🔴 [{strategy.name}] {symbol} 매도 청산 @ {current_price:.4f} | {result['reason']}")

                    strategy_open[strategy.name] = strategy_open.get(strategy.name, 0) + 1
                    symbol_open.add(key)
                    open_count += 1
                    new_trades += 1

            # API 레이트 제한 방지
            time.sleep(0.1)

        if new_trades > 0:
            print(f"  → 신규 진입: {new_trades}건")

    def get_status(self) -> dict:
        """현재 상태 반환"""
        return {
            "running": self.running,
            "last_scan": self.last_scan,
            "scan_count": self.scan_count,
            "watching_symbols": len(self.symbols),
            "open_positions": len(get_open_trades())
        }
