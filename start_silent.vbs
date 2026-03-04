Dim WshShell, appDir
Set WshShell = CreateObject("WScript.Shell")

appDir = "C:\Users\ADMIN\Downloads\user_monitor\social_monitor"

' Start Flask silently (pythonw = no console window)
WshShell.Run Chr(34) & appDir & "\.venv\Scripts\pythonw.exe" & Chr(34) & " " & Chr(34) & appDir & "\run.py" & Chr(34), 0, False

' Wait 4 seconds for Flask to start
WScript.Sleep 4000

' Start serveo tunnel with fixed subdomain (always same URL)
' URL will always be: https://socialpulseapp.serveo.net
WshShell.Run "cmd /c ssh -o StrictHostKeyChecking=no -o ServerAliveInterval=30 -R socialpulseapp:80:localhost:5050 serveo.net > " & Chr(34) & appDir & "\tunnel.log" & Chr(34) & " 2>&1", 0, False
