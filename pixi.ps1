param(
    [Parameter(Position=0)]
    [string]$Command = ""
)

Set-Location $PSScriptRoot

switch ($Command) {
    "start" { Start-App }
    "stop"  { Stop-App }
    default {
        Write-Host ""
        Write-Host "  Usage: pixi.bat [start|stop]"
        Write-Host ""
        Write-Host "    start  -- build frontend and start backend via Pixi"
        Write-Host "    stop   -- stop all Pixi-managed processes"
        Write-Host ""
        exit 1
    }
}


function Stop-App {
    Write-Host ""
    Write-Host "  Stopping GEE Web App (Pixi)..."
    Write-Host ""

    $PORT = 8000
    if (Test-Path ".pixi.port") {
        $PORT = Get-Content ".pixi.port"
    }

    # Kill by port
    $listeners = netstat -aon 2>$null | Select-String ":${PORT}\s+.*LISTENING"
    foreach ($line in $listeners) {
        $pid = ($line -split '\s+')[-1]
        Write-Host "  Killing PID $pid on port $PORT..."
        taskkill /pid $pid /f /t 2>$null | Out-Null
    }

    # Safety net
    taskkill /im uvicorn.exe /f /t 2>$null | Out-Null
    taskkill /im pixi.exe    /f /t 2>$null | Out-Null

    # Clean up state files
    Remove-Item -Force ".pixi.port" -ErrorAction SilentlyContinue
    Remove-Item -Force ".pixi.pid"  -ErrorAction SilentlyContinue

    Write-Host "  Done."
    Write-Host ""
}

function Start-App {
    Write-Host ""
    Write-Host "  GEE Web App - Pixi (no Docker)"
    Write-Host "  ================================"
    Write-Host ""

    # --- Check pixi ---
    if (-not (Get-Command pixi -ErrorAction SilentlyContinue)) {
        Write-Host "  Pixi not found."
        Write-Host ""
        $answer = Read-Host "  Install Pixi now? [Y/N]"
        if ($answer -imatch '^y') {
            Write-Host "  Installing Pixi..."
            try {
                Invoke-RestMethod https://pixi.sh/install.ps1 | Invoke-Expression
            } catch {
                Write-Host "  Pixi installation failed. Please install manually."
                exit 1
            }
            $env:PATH = "$env:USERPROFILE\.pixi\bin;$env:PATH"
            if (-not (Get-Command pixi -ErrorAction SilentlyContinue)) {
                Write-Host "  Pixi installed but not found in PATH."
                Write-Host "  Please open a new terminal and run again."
                exit 1
            }
        } else {
            Write-Host "  Pixi is required. Exiting."
            exit 1
        }
    }

    # --- Check for conflicting Docker containers ---
    if (Get-Command docker -ErrorAction SilentlyContinue) {
        docker info 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) {
            $running = docker ps --format '{{.Names}}' 2>$null |
                Where-Object { $_ -match '^gee_' }
            if ($running) {
                Write-Host "  ERROR: Docker containers are already running:"
                $running | ForEach-Object { Write-Host "    $_" }
                Write-Host "  Stop them first with: docker.bat stop"
                Write-Host ""
                exit 1
            }
        }
    }

    # --- Find a free port ---
    $PORT = $null
    foreach ($p in 8000, 8001, 8002, 8003) {
        $inUse = netstat -an 2>$null | Select-String ":$p\s+.*LISTENING"
        if (-not $inUse) { $PORT = $p; break }
    }
    if (-not $PORT) {
        Write-Host "  No free port found (tried 8000-8003). Free a port and try again."
        exit 1
    }

    # --- Warn if GEE key missing ---
    if (-not (Test-Path "config\gee-key.json")) {
        Write-Host "  WARNING: config\gee-key.json not found."
        Write-Host "  The app will start but GEE operations will fail."
        Write-Host ""
    }

    Write-Host "  App port : $PORT"
    Write-Host ""

    # --- Build frontend ---
    $needsInstall = (-not (Test-Path "frontend\node_modules")) -or
                    ((Get-Item "frontend\package.json").LastWriteTime -gt
                     (Get-Item "frontend\node_modules").LastWriteTime)
    if ($needsInstall) {
        Write-Host "  Installing frontend dependencies..."
        & pixi run npm-install-frontend
        if ($LASTEXITCODE -ne 0) { Write-Host "  Frontend install failed."; exit 1 }
    } else {
        Write-Host "  Frontend dependencies up to date, skipping install."
    }

    Write-Host "  Building frontend..."
    & pixi run build-frontend
    if ($LASTEXITCODE -ne 0) { Write-Host "  Frontend build failed."; exit 1 }

    # --- Launch backend ---
    Write-Host "  Starting backend..."
    $env:GOOGLE_APPLICATION_CREDENTIALS = "config\gee-key.json"
    $logPath = Join-Path $PSScriptRoot "pixi.log"
    $proc = Start-Process cmd `
        -ArgumentList "/c pixi run uvicorn backend.app:app --host 0.0.0.0 --port $PORT >> `"$logPath`" 2>&1" `
        -WindowStyle Hidden `
        -PassThru

    Set-Content -Path ".pixi.port" -Value $PORT
    Set-Content -Path ".pixi.pid"  -Value $proc.Id

    # --- Wait for backend ---
    Write-Host "  Waiting for backend..."
    Write-Host ""
    $ready = $false
    for ($i = 0; $i -lt 60; $i++) {
        try {
            $r = Invoke-WebRequest -Uri "http://localhost:$PORT/api/gee-key" `
                     -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop
            if ($r.StatusCode -ge 200 -and $r.StatusCode -lt 500) {
                $ready = $true
                break
            }
        } catch {}
        Write-Host -NoNewline "`r  Still waiting... ($i s)  "
        Start-Sleep 1
    }
    Write-Host -NoNewline "`r                                    `r"

    if (-not $ready) {
        Write-Host ""
        Write-Host "  ERROR: App did not respond after 60 s."
        Write-Host "  Check pixi.log for details."
        exit 1
    }

    Write-Host ""
    Write-Host "  =========================================="
    Write-Host "   GEE Web App is ready"
    Write-Host "   http://localhost:$PORT"
    Write-Host "  =========================================="
    Write-Host ""
    Start-Process "http://localhost:$PORT"
    Write-Host "  Run: pixi.bat stop -- when you are done."
    Write-Host ""
}