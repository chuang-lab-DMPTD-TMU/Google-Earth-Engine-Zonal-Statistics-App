@echo off
setlocal EnableDelayedExpansion
cd /d "%~dp0"

echo.
echo  GEE Web App - React + FastAPI
echo  ================================
echo.

:: --- Docker check ---
docker info >nul 2>&1
if errorlevel 1 (
    echo  Docker is not running.
    echo  Please start Docker Desktop, then try again.
    echo.
    pause
    exit /b 1
)

:: --- Find a free port for the backend (8000-8003) ---
set BACKEND_PORT=
for %%p in (8000 8001 8002 8003) do (
    if not defined BACKEND_PORT (
        netstat -an 2>nul | findstr /C:":%%p " | findstr "LISTENING" >nul 2>&1
        if errorlevel 1 set BACKEND_PORT=%%p
    )
)
if not defined BACKEND_PORT (
    echo  No free port for backend (tried 8000-8003). Free a port and try again.
    pause
    exit /b 1
)

:: --- Find a free port for the frontend (3000-3003) ---
set FRONTEND_PORT=
for %%p in (3000 3001 3002 3003) do (
    if not defined FRONTEND_PORT (
        netstat -an 2>nul | findstr /C:":%%p " | findstr "LISTENING" >nul 2>&1
        if errorlevel 1 set FRONTEND_PORT=%%p
    )
)
if not defined FRONTEND_PORT (
    echo  No free port for frontend (tried 3000-3003). Free a port and try again.
    pause
    exit /b 1
)

echo  Backend  port : %BACKEND_PORT%
echo  Frontend port : %FRONTEND_PORT%
echo.

:: --- Update .env (preserve existing lines, update/add our keys) ---
powershell -NoProfile -Command ^
  "$lines = if (Test-Path '.env') { Get-Content '.env' } else { @() };" ^
  "$new = $lines | Where-Object { $_ -notmatch '^(BACKEND_PORT|APP_PORT)=' };" ^
  "$new += 'BACKEND_PORT=%BACKEND_PORT%';" ^
  "$new += 'APP_PORT=%FRONTEND_PORT%';" ^
  "$new | Set-Content '.env'"

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
powershell -NoProfile -Command ^
  "$port='%BACKEND_PORT%';" ^
  "for($i=0;$i-lt 40;$i++){" ^
  "  try { $r=(Invoke-WebRequest -Uri \"http://localhost:$port/api/gee-key\" -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop).StatusCode;" ^
  "    if($r -eq 200){exit 0} } catch {}; Start-Sleep 1 }; exit 1"
if not errorlevel 1 set backend_ready=1

if "%backend_ready%"=="0" (
    echo.
    echo  WARNING: Backend did not respond after 40 s.
    echo  Check logs: docker compose logs -f backend
    echo.
)

:: --- Wait for frontend, then open browser ---
echo  Waiting for frontend...
powershell -NoProfile -Command ^
  "$port='%FRONTEND_PORT%';" ^
  "for($i=0;$i-lt 60;$i++){" ^
  "  try { $r=(Invoke-WebRequest -Uri \"http://localhost:$port/\" -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop).StatusCode;" ^
  "    if($r -lt 500){exit 0} } catch {}; Start-Sleep 1 }; exit 1"
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
echo  Run Stop.bat when you are done.
echo.
