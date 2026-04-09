#!/usr/bin/env python3
"""
Claude 세션 기록을 파싱하여 AI 협업 작업 요약 및 홍보 콘텐츠를 생성합니다.
~/.claude/projects/ 아래 최근 N일 JSONL 파일을 읽고 Claude API로 요약문을 만듭니다.
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


# Windows/Linux 둘 다 대응
def _get_home() -> Path:
    # Git Bash on Windows: HOME=/c/Users/... 형식
    home_env = os.environ.get("HOME") or os.environ.get("USERPROFILE")
    if home_env:
        return Path(home_env)
    return Path.home()

CLAUDE_DIR = _get_home() / ".claude"
PROJECTS_DIR = CLAUDE_DIR / "projects"
SCRIPTS_DIR = CLAUDE_DIR / "scripts"


def find_recent_jsonl_files(days: int) -> list[Path]:
    """최근 N일 이내 수정된 JSONL 파일 목록 반환."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    result = []
    for jsonl_path in PROJECTS_DIR.rglob("*.jsonl"):
        mtime = datetime.fromtimestamp(jsonl_path.stat().st_mtime, tz=timezone.utc)
        if mtime >= cutoff:
            result.append(jsonl_path)
    return sorted(result)


def extract_messages(jsonl_path: Path) -> list[dict]:
    """JSONL 파일에서 user/assistant 메시지 추출."""
    messages = []
    with open(jsonl_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            record_type = record.get("type")
            ts = record.get("timestamp") or record.get("message", {}).get("timestamp")

            if record_type == "user":
                content = record.get("message", {}).get("content", "")
                if isinstance(content, list):
                    text = " ".join(
                        block.get("text", "") for block in content
                        if isinstance(block, dict) and block.get("type") == "text"
                    )
                else:
                    text = str(content)
                if "<system-reminder>" in text:
                    continue
                if text.strip():
                    messages.append({"role": "user", "text": text[:1000], "timestamp": ts})

            elif record_type == "assistant":
                content_blocks = record.get("message", {}).get("content", [])
                for block in content_blocks:
                    if isinstance(block, dict) and block.get("type") == "text":
                        text = block.get("text", "").strip()
                        if text:
                            messages.append({"role": "assistant", "text": text[:1000], "timestamp": ts})
                        break

    return messages


def get_git_log(days: int, cwd: str | None = None) -> str:
    """최근 N일 git 커밋 로그 반환."""
    try:
        result = subprocess.run(
            ["git", "log", f"--since={days} days ago", "--oneline", "--no-merges"],
            capture_output=True, text=True, cwd=cwd or "."
        )
        return result.stdout.strip()
    except Exception:
        return ""


def build_prompt(conversations: str, git_log: str, period_label: str) -> str:
    return f"""당신은 개발자/크리에이터의 AI 협업 작업 일지를 블로그와 SNS용으로 정리하는 전문 에디터입니다.

아래는 최근 {period_label} 동안의 Claude AI와 나눈 대화 요약과 Git 커밋 기록입니다.

=== 대화 기록 ===
{conversations}

=== Git 커밋 기록 ===
{git_log if git_log else "(커밋 없음)"}

위 내용을 바탕으로 다음 두 가지 포맷의 홍보 콘텐츠를 작성해주세요.

---

## [BLOG]
마크다운 형식으로 500~800자 분량의 블로그 포스트를 작성하세요.
- 제목: "AI와 함께한 N일 작업 일지 (날짜)"
- 섹션: 이번에 한 일 / 주요 성과 / 배운 점 / 다음 계획
- 해시태그 5개 포함 (#ClaudeCode #AI개발 등)

---

## [SNS]
트위터/X 또는 텔레그램용 단문 포스트를 작성하세요.
- 200자 이내
- 핵심 성과 1~2줄
- 해시태그 3개
- 공감을 이끌어내는 캐주얼한 톤
"""


TELEGRAM_CHANNEL = "@spaceeum_ai_lab"


def extract_sns_text(result: str) -> str:
    """생성된 콘텐츠에서 [SNS] 섹션만 추출."""
    lines = result.split("\n")
    sns_lines = []
    in_sns = False
    for line in lines:
        if "## [SNS]" in line:
            in_sns = True
            continue
        if in_sns and line.startswith("## ["):
            break
        if in_sns:
            sns_lines.append(line)
    return "\n".join(sns_lines).strip()


def send_to_telegram_channel(text: str) -> bool:
    """텔레그램 채널에 메시지 전송."""
    import urllib.request
    import urllib.parse

    token = os.environ.get("TELEGRAM_TOKEN")
    if not token:
        print("  [텔레그램] TELEGRAM_TOKEN 환경변수 없음 — 전송 스킵")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": TELEGRAM_CHANNEL,
        "text": text,
        "parse_mode": "HTML",
    }).encode()

    try:
        urllib.request.urlopen(urllib.request.Request(url, data), timeout=10)
        print(f"  [텔레그램] {TELEGRAM_CHANNEL} 채널 전송 완료")
        return True
    except Exception as e:
        print(f"  [텔레그램] 전송 실패: {e}")
        return False


def call_claude_api(prompt: str) -> str:
    """Claude API를 호출하여 요약문을 생성합니다."""
    try:
        import anthropic
    except ImportError:
        return "[오류] anthropic 패키지가 설치되지 않았습니다. pip install anthropic 실행 후 재시도하세요."

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return "[오류] ANTHROPIC_API_KEY 환경변수가 설정되지 않았습니다."

    client = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text


def main():
    parser = argparse.ArgumentParser(description="Claude 세션 기록 기반 홍보 콘텐츠 생성")
    parser.add_argument("--days", type=int, default=2, help="최근 며칠치 세션을 분석할지 (기본: 2)")
    parser.add_argument("--output-dir", type=str, default=str(SCRIPTS_DIR / "output"), help="출력 폴더")
    parser.add_argument("--dry-run", action="store_true", help="API 호출 없이 파싱 결과만 출력")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"[1] 최근 {args.days}일 JSONL 파일 검색 중...")
    jsonl_files = find_recent_jsonl_files(args.days)
    if not jsonl_files:
        print("  분석할 세션 파일이 없습니다. (Git 커밋 기록만으로 진행)")
    else:
        print(f"  {len(jsonl_files)}개 파일 발견: {[f.name for f in jsonl_files]}")

    print("[2] 대화 내용 파싱 중...")
    all_messages = []
    for path in jsonl_files:
        msgs = extract_messages(path)
        all_messages.extend(msgs)
    print(f"  총 {len(all_messages)}개 메시지 추출")

    convo_lines = []
    for m in all_messages[:30]:
        role = "사용자" if m["role"] == "user" else "Claude"
        convo_lines.append(f"{role}: {m['text'][:300]}")
    conversations = "\n\n".join(convo_lines)[:3000] if convo_lines else "(세션 기록 없음)"

    print("[3] Git 커밋 기록 수집 중...")
    git_log = get_git_log(args.days)
    if git_log:
        print(f"  커밋 {len(git_log.splitlines())}개 발견")
    else:
        print("  커밋 없음 (또는 git 저장소 아님)")

    if not conversations.strip() or conversations == "(세션 기록 없음)":
        if not git_log:
            print("  세션 기록도 Git 커밋도 없습니다. 종료합니다.")
            sys.exit(0)

    if args.dry_run:
        print("\n=== [DRY RUN] 파싱 결과 ===")
        try:
            print(f"대화 내용 (앞 500자):\n{conversations[:500]}")
            print(f"\nGit 로그:\n{git_log[:300] if git_log else '(없음)'}")
        except UnicodeEncodeError:
            print("(인코딩 문제로 미리보기 생략 — 실제 실행 시 정상 작동)")
        print("\n[dry-run 완료] API 호출 없이 종료합니다.")
        return

    print("[4] Claude API로 요약 생성 중...")
    period_label = f"{args.days}일"
    prompt = build_prompt(conversations, git_log, period_label)
    result = call_claude_api(prompt)

    date_str = datetime.now().strftime("%Y-%m-%d")
    out_path = output_dir / f"promote_{date_str}.md"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(f"# AI 협업 작업 홍보 콘텐츠 — {date_str}\n\n")
        f.write(result)
    print(f"[5] 저장 완료: {out_path}")
    print("\n" + "=" * 60)
    print(result[:600])
    print("=" * 60)

    print("\n[6] 텔레그램 채널 포스팅 중...")
    sns_text = extract_sns_text(result)
    if sns_text:
        send_to_telegram_channel(sns_text)
    else:
        print("  [텔레그램] SNS 섹션을 찾지 못했습니다.")


if __name__ == "__main__":
    main()
