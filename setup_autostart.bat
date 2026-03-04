@echo off
cd /d %~dp0
echo Setting up auto-start for Flask + Tunnel...
echo.

schtasks /create /tn "SocialMonitorFlask" /tr "\"%~dp0.venv\Scripts\pythonw.exe\" \"%~dp0run.py\"" /sc ONLOGON /ru "%USERNAME%" /f
if %errorlevel%==0 (
    echo [OK] Flask task created
) else (
    echo [FAIL] Flask task failed
)

schtasks /create /tn "SocialMonitorTunnel" /tr "cloudflared tunnel --url http://localhost:5050" /sc ONLOGON /ru "%USERNAME%" /f /delay 0001:00
if %errorlevel%==0 (
    echo [OK] Tunnel task created
) else (
    echo [FAIL] Tunnel task failed
)

echo.
echo Starting Flask now...
schtasks /run /tn "SocialMonitorFlask"
timeout /t 5 /nobreak >nul

echo Starting Tunnel now...
schtasks /run /tn "SocialMonitorTunnel"

echo.
echo Both are running! Check tunnel.log for the live URL.
echo File: %~dp0tunnel.log
echo.
pause
