@echo off
setlocal EnableDelayedExpansion
cd /d "%~dp0"

echo.
echo  GEE Web App - Pixi (no Docker)
echo  ================================
echo.

:: --- Check pixi ---
where pixi >nul 2>&1
if errorlevel 1 (
    echo  Pixi not found. Install it with:
    echo    powershell -c "iwr -useb https://pixi.sh/install.ps1 ^| iex"
    echo  Then open a new terminal and try again.
    echo.
    pause
    exit /b 1
)

:: --- Find a free port (8000-8003) ---
set PORT=
for %%p in (8000 8001 8002 8003) do (
    if not defined PORT (
        netstat -an 2>nul | findstr /C:":%%p " | findstr "LISTENING" >nul 2>&1
        if errorlevel 1 set PORT=%%p
    )
)
if not defined PORT (
    echo  No free port (tried 8000-8003). Free a port and try again.
    echo.
    pause
    exit /b 1
)

:: --- Warn if GEE key missing ---
if not exist "config\gee-key.json" (
    echo  WARNING: config\gee-key.json not found.
    echo  The app will start but GEE operations will fail until a key is uploaded.
    echo.
)

echo  App port : %PORT%
echo.

:: --- Build frontend ---
echo  Building frontend...
pixi run build-frontend
if errorlevel 1 (
    echo  Frontend build failed.
    pause
    exit /b 1
)

:: --- Launch uvicorn in a new minimized window ---
echo  Starting backend...
set GOOGLE_APPLICATION_CREDENTIALS=config\gee-key.json
start "GEE App (Pixi)" /min cmd /c "pixi run uvicorn backend.app:app --host 0.0.0.0 --port %PORT% > pixi.log 2>&1"
echo %PORT% > .pixi.port

:: --- Wait for app to be ready ---
echo  Waiting for app...
set ready=0
powershell -NoProfile -Command ^
  "$port='%PORT%';" ^
  "for($i=0;$i-lt 60;$i++){" ^
  "  try { $r=(Invoke-WebRequest -Uri \"http://localhost:$port/api/gee-key\" -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop).StatusCode;" ^
  "    if($r -eq 200){exit 0} } catch {}; Start-Sleep 1 }; exit 1"
if not errorlevel 1 set ready=1

if "%ready%"=="0" (
    echo.
    echo  ERROR: App did not respond after 60 s.
    echo  Check pixi.log for details.
    echo.
    pause
    exit /b 1
)

echo.
echo  ==========================================
echo   GEE Web App is ready
echo   http://localhost:%PORT%
echo  ==========================================
echo.
start "" "http://localhost:%PORT%"
echo  Run Stop-pixi.bat when you are done.
echo.
