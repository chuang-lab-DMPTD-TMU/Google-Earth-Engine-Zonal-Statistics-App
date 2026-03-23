@echo off
setlocal EnableDelayedExpansion
cd /d "%~dp0"

echo.
echo  Stopping GEE Web App (Pixi)...
echo.

:: Kill by saved port
if exist ".pixi.port" (
    set /p PORT=<.pixi.port
    for /f "tokens=5" %%a in ('netstat -aon 2^>nul ^| findstr ":!PORT! " ^| findstr "LISTENING"') do (
        taskkill /pid %%a /f /t >nul 2>&1
    )
    del .pixi.port 2>nul
    echo  Stopped.
) else (
    :: Fallback: try the default port
    for /f "tokens=5" %%a in ('netstat -aon 2^>nul ^| findstr ":8000 " ^| findstr "LISTENING"') do (
        taskkill /pid %%a /f /t >nul 2>&1
    )
    echo  Stopped (used default port 8000).
)

:: Also close the minimized cmd window if still open
taskkill /fi "WINDOWTITLE eq GEE App (Pixi)" /t /f >nul 2>&1

echo.
pause
