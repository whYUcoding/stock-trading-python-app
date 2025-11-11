@echo off
setlocal enabledelayedexpansion

rem === config ===
set "PROJECT=D:\DataExpertBootCamp\stock-trading-python-app"
set "SCRIPT=%PROJECT%\script.py"
set "LOGDIR=%PROJECT%\logs"
set "CONDA_BAT=D:\anaconda\condabin\conda.bat"
set "ENVNAME=dtxprt"
rem =============

if not exist "%LOGDIR%" mkdir "%LOGDIR%"

rem Safe timestamp: 2025-11-10_01-23-45
for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd_HH-mm-ss"') do set "TS=%%I"

call "%CONDA_BAT%" run -n "%ENVNAME%" python "%SCRIPT%" >> "%LOGDIR%\run_%TS%.log" 2>&1

endlocal