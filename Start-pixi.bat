@echo off
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0pixi-powershell-script.ps1"
if %ERRORLEVEL% neq 0 (
    echo.
    echo  Something went wrong. See above for details.
    pause
)