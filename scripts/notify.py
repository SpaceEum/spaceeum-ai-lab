#!/usr/bin/env python3
"""
공간이음 AI — 텔레그램 보고 발송
매일 09:00 스캔 완료 후 자동 실행
"""
import json
import os
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))


def send_telegram(token: str, chat_id: str, text: str):
    """텔레그램 메시지 발송"""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }).encode("utf-8")
    try:
        req = urllib.request.Request(url, data=data)
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"[텔레그램] 전송 완료 ({resp.status})")
    except Exception as e:
        print(f"[텔레그램] 전송 실패: {e}")


def build_scan_message() -> str:
    """매매법 1 (업비트 60MA 스캔) 보고 메시지"""
    try:
        with open("data/scan_latest.json", "r", encoding="utf-8") as f:
            d = json.load(f)
        pt = d.get("paper_trading", {})
        open_list = pt.get("open_list", [])

        msg = (
            f"📊 <b>공간이음 — 매매법 1 보고</b>\n"
            f"📅 {d.get('date', '')} 09:00 KST\n\n"
            f"🔍 스캔 결과\n"
            f"├ 전체 스캔: {d.get('total_scanned', 0)}개\n"
            f"├ STRONG BUY: {d.get('strong_buy_count', 0)}개\n"
            f"└ ⭐ 1D+4H 동시: {d.get('dual_strong_count', 0)}개\n\n"
            f"💼 페이퍼 트레이딩\n"
            f"├ 현재 보유: {pt.get('open_positions', 0)}개\n"
            f"├ 누적 거래: {pt.get('total_trades', 0)}건\n"
            f"├ 승률: {pt.get('win_rate', 0)}%\n"
            f"└ 평균 수익: {pt.get('avg_pnl_pct', 0):+.2f}%"
        )

        if open_list:
            msg += "\n\n📋 보유 종목:"
            for p in open_list[:5]:
                pnl = p.get("current_pnl_pct", 0) or 0
                emoji = "📈" if pnl >= 0 else "📉"
                msg += f"\n{emoji} {p.get('symbol', '')} {pnl:+.1f}%"

        return msg
    except Exception as e:
        return f"📊 <b>매매법 1 보고 오류</b>\n{e}"


def build_shin_message() -> str:
    """매매법 2 (신창환 5전략) 보고 메시지"""
    try:
        with open("data/shin_performance.json", "r", encoding="utf-8") as f:
            d = json.load(f)

        summary = d.get("summary", {})
        strategies = d.get("strategies", [])
        opens = d.get("open_positions", [])

        msg = (
            f"⚡ <b>공간이음 — 매매법 2 보고</b>\n"
            f"📅 {d.get('updated_at', '')}\n\n"
            f"📊 전체 성과\n"
            f"├ 총 거래: {summary.get('total_closed_trades', 0)}건\n"
            f"├ 승률: {summary.get('overall_win_rate', 0)}%\n"
            f"├ 오픈 포지션: {summary.get('open_positions', 0)}개\n"
            f"└ 최고 전략: {summary.get('best_strategy', '-')}\n"
        )

        if strategies:
            msg += "\n🏆 전략별 성과:"
            for s in strategies[:3]:
                msg += (f"\n├ [{s['name']}] 승률 {s['win_rate']}% "
                        f"| 수익 {s['total_pnl_pct']:+.1f}%")

        if opens:
            msg += "\n\n📋 보유 포지션:"
            for p in opens[:3]:
                msg += f"\n└ {p['symbol']} [{p['strategy']}] {p['direction']}"

        return msg
    except Exception as e:
        return f"⚡ <b>매매법 2 보고 오류</b>\n{e}"


def main():
    token = os.environ.get("TELEGRAM_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        print("[오류] TELEGRAM_TOKEN 또는 TELEGRAM_CHAT_ID 환경변수가 없습니다")
        sys.exit(1)

    source = sys.argv[1] if len(sys.argv) > 1 else "scan"

    if source == "scan":
        msg = build_scan_message()
    elif source == "shin":
        msg = build_shin_message()
    else:
        msg = f"⚠️ 알 수 없는 소스: {source}"

    send_telegram(token, chat_id, msg)


if __name__ == "__main__":
    main()
