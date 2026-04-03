@echo off
echo Installing Backend Dependencies...
call pip install -r backend/requirements.txt

echo Starting Backend...
start cmd /k "python -m uvicorn backend.main:app --reload"

echo Starting Frontend...
cd frontend
echo NOTE: If npm install failed, please run 'npm install' manually.
call npm install
call npm run dev
