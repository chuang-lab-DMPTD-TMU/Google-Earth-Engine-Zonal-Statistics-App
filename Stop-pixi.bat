@echo off
setlocal EnableDelayedExpansion
cd /d "%~dp0"
echo.
echo  Stopping GEE Web App (Pixi)...
echo.

:: Determine port
set PORT=8000
if exist ".pixi.port" (
    set /p PORT=<.pixi.port
)

:: Kill anything listening on that port (full process tree)
for /f "tokens=5" %%a in ('netstat -aon 2^>nul ^| findstr /r ":%PORT%\s" ^| findstr "LISTENING"') do (
    echo  Killing PID %%a on port %PORT%...
    taskkill /pid %%a /f /t >nul 2>&1
)

:: Safety net - kill orphaned uvicorn/pixi processes by name
taskkill /im uvicorn.exe /f /t >nul 2>&1
taskkill /im pixi.exe /f /t >nul 2>&1

:: Clean up port file
if exist ".pixi.port" del .pixi.port 2>nul

echo  Done.
echo.
pause