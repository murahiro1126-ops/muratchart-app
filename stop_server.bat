@echo off
setlocal

taskkill /FI "WINDOWTITLE eq Stock Board Server*" /T /F >nul 2>&1
if errorlevel 1 (
  echo Stock Board Server window was not found.
) else (
  echo Stock Board Server stopped.
)

endlocal
pause
