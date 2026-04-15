@echo off
cd /d D:\armaDOCs\arma\Desktop\Kodlama\borsa-csv

if not exist logs mkdir logs

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd"') do set LOGDATE=%%i
set LOGFILE=logs\quotes_update_%LOGDATE%.log

echo [%date% %time%] Build basliyor... >> %LOGFILE%
py build_quotes.py >> %LOGFILE% 2>&1

if %errorlevel% neq 0 (
    echo [%date% %time%] HATA: build_quotes.py basarisiz oldu, push atlanıyor. >> %LOGFILE%
    goto cleanup
)

echo [%date% %time%] Git push basliyor... >> %LOGFILE%
git add data\universe.csv data\tefas_cache.csv public\latest.csv public\missing_symbols.csv public\candidate_exclusions.csv >> %LOGFILE% 2>&1
git diff --cached --quiet && (
    echo [%date% %time%] Degisiklik yok, push atlanıyor. >> %LOGFILE%
    goto cleanup
)
git commit -m "Auto update: TEFAS + market data" >> %LOGFILE% 2>&1
git pull --rebase origin main >> %LOGFILE% 2>&1
git push origin main >> %LOGFILE% 2>&1
echo [%date% %time%] Push tamamlandi. >> %LOGFILE%

:cleanup
powershell -NoProfile -Command "Get-ChildItem 'D:\armaDOCs\arma\Desktop\Kodlama\borsa-csv\logs\*.log' -ErrorAction SilentlyContinue | Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-14) } | Remove-Item -Force"
echo [%date% %time%] Bitti. >> %LOGFILE%
