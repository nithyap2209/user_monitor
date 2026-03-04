@echo off
echo Setting up Flask as auto-start service...

schtasks /create /tn "SocialMonitorFlask" /tr "\".venv\Scripts\pythonw.exe\" \"run.py\"" /sc ONLOGON /ru "%USERNAME%" /f /st 00:00

schtasks /create /tn "SocialMonitorTunnel" /tr "cloudflared tunnel --url http://localhost:5050" /sc ONLOGON /ru "%USERNAME%" /f /st 00:00 /delay 0000:10

echo.
echo Done! Flask and tunnel will now auto-start on every Windows login.
echo To start them RIGHT NOW, run:
echo   schtasks /run /tn "SocialMonitorFlask"
echo   schtasks /run /tn "SocialMonitorTunnel"
echo.
pause
