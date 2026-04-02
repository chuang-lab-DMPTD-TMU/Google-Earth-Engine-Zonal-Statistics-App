@echo off
cd /d "%~dp0"

set COMMAND=%1

if "%COMMAND%"=="start" goto :start
if "%COMMAND%"=="stop" goto :stop

echo.
echo  Usage: docker.bat [start^|stop]
echo.
echo    start  -- build and start production
echo    stop   -- stop all containers
echo.
pause
exit /b 1

:: ============================================================
:stop
:: ============================================================
echo.
echo  Stopping GEE Web App...
echo.

docker compose --profile prod down
if errorlevel 1 (
    echo  Some containers may still be running.
    echo  Run: docker compose --profile prod ps
) else (
    echo  All services stopped.
)

echo.
pause
exit /b 0

:: ============================================================
:start
:: ============================================================
echo.
echo  GEE Web App - React + FastAPI
echo  ================================
echo.

:: --- Docker check ---
echo STEP - DOCKER CHECK
docker info >nul 2>&1
if errorlevel 1 (
    echo  Docker is not running.
    echo  Please start Docker Desktop, then try again.
    echo.
    pause
    exit /b 1
)

:: --- Check for conflicting Pixi process ---
echo STEP - CHECK FOR PIXI CONFLICT
if exist ".pixi.pid" (
    set /p PIXI_PID=<.pixi.pid
    if defined PIXI_PID (
        powershell -NoProfile -Command "Get-Process -Id %PIXI_PID% -ErrorAction SilentlyContinue" >nul 2>&1
        if not errorlevel 1 (
            set /p PIXI_PORT=<.pixi.port
            echo  ERROR: A Pixi-managed backend is already running (PID %PIXI_PID%, port %PIXI_PORT%).
            echo  Stop it first with: Stop-pixi.bat
            echo.
            pause
            exit /b 1
        ) else (
            del /f /q .pixi.pid 2>nul
            del /f /q .pixi.port 2>nul
        )
    )
)

:: --- Find a free port for the backend (8000-8003) ---
echo STEP - FIND BACKEND FREE PORT
set BACKEND_PORT=
setlocal EnableDelayedExpansion
for /f %%p in ('powershell -NoProfile -Command "8000,8001,8002,8003 | Where-Object { -not (Get-NetTCPConnection -LocalPort $_ -ErrorAction SilentlyContinue) } | Select-Object -First 1"') do set "BACKEND_PORT=%%p"
endlocal & set "BACKEND_PORT=%BACKEND_PORT%"
if not defined BACKEND_PORT (
    echo  No free port for backend (tried 8000-8003^). Free a port and try again.
    pause & exit /b 1
)

:: --- Find a free port for the frontend (3000-3003) ---
echo STEP - FIND FRONTEND FREE PORT
set FRONTEND_PORT=
setlocal EnableDelayedExpansion
for /f %%p in ('powershell -NoProfile -Command "3000,3001,3002,3003 | Where-Object { -not (Get-NetTCPConnection -LocalPort $_ -ErrorAction SilentlyContinue) } | Select-Object -First 1"') do set "FRONTEND_PORT=%%p"
endlocal & set "FRONTEND_PORT=%FRONTEND_PORT%"
if not defined FRONTEND_PORT (
    echo  No free port for frontend (tried 3000-3003^). Free a port and try again.
    pause & exit /b 1
)

echo  Backend  port : %BACKEND_PORT%
echo  Frontend port : %FRONTEND_PORT%
echo.

:: --- Update .env ---
if exist ".env" (
    type ".env" | findstr /v /r "^BACKEND_PORT= ^APP_PORT=" > ".env.tmp"
    move /y ".env.tmp" ".env" >nul
)
echo BACKEND_PORT=%BACKEND_PORT%>> .env
echo APP_PORT=%FRONTEND_PORT%>> .env

:: --- Build images ---
echo  Building backend image...
docker compose build backend
if errorlevel 1 ( echo  Backend build failed. & pause & exit /b 1 )

echo  Building frontend image...
docker compose build frontend
if errorlevel 1 ( echo  Frontend build failed. & pause & exit /b 1 )

:: --- Start services ---
echo  Starting services...
docker compose --profile prod up -d --force-recreate backend frontend
if errorlevel 1 ( echo  Failed to start containers. & pause & exit /b 1 )

:: --- Wait for backend ---
echo  Waiting for backend...
set backend_ready=0
powershell -NoProfile -Command "$port='%BACKEND_PORT%'; for($i=0;$i-lt 40;$i++){ try { $r=(Invoke-WebRequest -Uri \"http://localhost:$port/api/gee-key\" -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop).StatusCode; if($r -eq 200){exit 0} } catch {}; Start-Sleep 1 }; exit 1"
if not errorlevel 1 set backend_ready=1

if "%backend_ready%"=="0" (
    echo.
    echo  WARNING: Backend did not respond after 40 s.
    echo  Check logs: docker compose logs -f backend
    echo.
)

:: --- Wait for frontend ---
echo  Waiting for frontend...
powershell -NoProfile -Command "$port='%FRONTEND_PORT%'; for($i=0;$i-lt 60;$i++){ try { $r=(Invoke-WebRequest -Uri \"http://localhost:$port/\" -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop).StatusCode; if($r -lt 500){exit 0} } catch {}; Start-Sleep 1 }; exit 1"
if errorlevel 1 (
    echo.
    echo  Frontend did not respond after 60 s.
    echo  Check logs: docker compose logs -f frontend
    echo.
    pause
    exit /b 1
)

echo.
echo  ==========================================
echo   GEE Web App is ready
echo   Frontend : http://localhost:%FRONTEND_PORT%
echo   Backend  : http://localhost:%BACKEND_PORT%
echo  ==========================================
echo.
start "" "http://localhost:%FRONTEND_PORT%"
echo  Run: docker.bat stop -- when you are done.
echo.