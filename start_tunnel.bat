@echo off
echo Starting Flask App + Public Tunnel...
echo.

REM Start Flask app in a new window
start "Flask App" cmd /k "cd /d %~dp0 && python run.py"

REM Wait 3 seconds for Flask to start
timeout /t 3 /nobreak >nul

REM Start Cloudflare tunnel in a new window
start "Public Tunnel" cmd /k "cloudflared tunnel --url http://localhost:5050"

echo.
echo Both windows opened!
echo - Flask App window: localhost:5050
echo - Tunnel window: shows your public URL
echo.
pause
