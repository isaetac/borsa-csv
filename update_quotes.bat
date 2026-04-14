@echo off
cd /d C:\Users\ARMA\Desktop\borsa-csv

if not exist logs mkdir logs

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd"') do set LOGDATE=%%i

py build_quotes.py >> logs\quotes_update_%LOGDATE%.log 2>&1

powershell -NoProfile -Command "Get-ChildItem 'C:\Users\ARMA\Desktop\borsa-csv\logs\*.log' -ErrorAction SilentlyContinue | Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-14) } | Remove-Item -Force"