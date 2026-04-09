# promote.ps1 — 매일 실행되는 AI 작업 홍보 콘텐츠 생성 스크립트 (PowerShell)
param(
    [switch]$DryRun
)

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PythonScript = Join-Path $ScriptDir "summarize_sessions.py"
$OutputDir = Join-Path $ScriptDir "output"
$LogPrefix = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')]"

Write-Host "$LogPrefix promote.ps1 시작"

# API 키 로드
if (-not $env:ANTHROPIC_API_KEY) {
    $KeyFile = "$env:USERPROFILE\.anthropic_key"
    if (Test-Path $KeyFile) {
        $env:ANTHROPIC_API_KEY = (Get-Content $KeyFile -Raw).Trim()
        Write-Host "$LogPrefix API 키를 .anthropic_key에서 로드했습니다."
    } else {
        Write-Host "$LogPrefix 경고: ANTHROPIC_API_KEY가 설정되지 않았습니다."
    }
}

# 인코딩 설정
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONUTF8 = "1"

# Python 실행
$Args = @("--days", "1", "--output-dir", $OutputDir)
if ($DryRun) {
    $Args += "--dry-run"
    Write-Host "$LogPrefix dry-run 모드로 실행합니다."
}

Set-Location $ScriptDir
python $PythonScript @Args

Write-Host "$LogPrefix promote.ps1 완료"

# 최신 파일 출력
$Latest = Get-ChildItem "$OutputDir\promote_*.md" -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1
if ($Latest) {
    Write-Host "$LogPrefix 생성된 파일: $($Latest.FullName)"
}
