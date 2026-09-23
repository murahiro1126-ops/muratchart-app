@echo off
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo [ERROR] .venv was not found.
  echo Create the environment and install dependencies first.
  pause
  exit /b 1
)

start "Stock Board Server" "%ComSpec%" /k ""%~dp0.venv\Scripts\python.exe" -m uvicorn main:app --host 127.0.0.1 --port 8000"
timeout /t 2 /nobreak >nul
start "" "http://127.0.0.1:8000/"
endlocal
