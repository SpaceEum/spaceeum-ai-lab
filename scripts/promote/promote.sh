#!/bin/bash
# promote.sh — 2일마다 cron으로 실행되는 AI 작업 홍보 콘텐츠 생성 스크립트
#
# 사용법:
#   수동 실행:  bash ~/.claude/scripts/promote.sh
#   dry-run:   bash ~/.claude/scripts/promote.sh --dry-run
#   crontab:   0 9 * * * ~/.claude/scripts/promote.sh >> ~/.claude/scripts/cron.log 2>&1

set -euo pipefail

# Windows 한글 경로 인코딩 문제 방지
export PYTHONIOENCODING=utf-8
export PYTHONUTF8=1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="$SCRIPT_DIR/summarize_sessions.py"
OUTPUT_DIR="$SCRIPT_DIR/output"
LOG_PREFIX="[$(date '+%Y-%m-%d %H:%M:%S')]"

echo "$LOG_PREFIX promote.sh 시작"

if [[ -z "${ANTHROPIC_API_KEY:-}" ]]; then
    if [[ -f "$HOME/.anthropic_key" ]]; then
        export ANTHROPIC_API_KEY="$(cat "$HOME/.anthropic_key")"
        echo "$LOG_PREFIX API 키를 ~/.anthropic_key에서 로드했습니다."
    else
        echo "$LOG_PREFIX 경고: ANTHROPIC_API_KEY가 설정되지 않았습니다."
        echo "$LOG_PREFIX  → export ANTHROPIC_API_KEY='sk-...' 또는 ~/.anthropic_key 파일을 만드세요."
    fi
fi

EXTRA_ARGS=""
if [[ "${1:-}" == "--dry-run" ]]; then
    EXTRA_ARGS="--dry-run"
    echo "$LOG_PREFIX dry-run 모드로 실행합니다."
fi

cd "$SCRIPT_DIR"
python3 summarize_sessions.py \
    --days 1 \
    --output-dir "$OUTPUT_DIR" \
    $EXTRA_ARGS

echo "$LOG_PREFIX promote.sh 완료"

LATEST=$(ls -t "$OUTPUT_DIR"/promote_*.md 2>/dev/null | head -1 || true)
if [[ -n "$LATEST" ]]; then
    echo "$LOG_PREFIX 생성된 파일: $LATEST"
fi
