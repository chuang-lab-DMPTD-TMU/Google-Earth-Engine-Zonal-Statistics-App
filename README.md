# GEE Web App

A browser-based tool for downloading satellite data (precipitation, temperature, vegetation, land cover, and more) from Google Earth Engine for any area you choose.

---

## Before you start

You need two things:

**1. A way to run the app — choose one:**

| Option | Best for | Requirement |
|--------|----------|-------------|
| **Docker** | Most users | Install [Docker Desktop](https://www.docker.com/products/docker-desktop) |
| **Pixi** | Users without Docker, or if Docker is not allowed | Run one install command (see below) |

Both options give the same app in your browser. Docker runs everything in containers; Pixi runs everything directly on your machine without containers.

> **Installing Pixi** (if you choose this option):
> - **Mac / Linux:** open a terminal and run `curl -fsSL https://pixi.sh/install.sh | sh`, then close and reopen the terminal.
> - **Windows:** open PowerShell and run `iwr -useb https://pixi.sh/install.ps1 | iex`, then close and reopen the terminal.

**2. A Google Earth Engine key file**
A small file (ending in `.json`) that gives the app access to Google Earth Engine. If you do not have one yet, see [Getting a key](#getting-a-key) below.

---

## Starting the app

**Docker option:**

| Platform | Action |
|----------|--------|
| macOS | Double-click `Start.command` |
| Windows | Double-click `Start.bat` |
| Linux | Open a terminal in the folder and run `./quickstart-react.sh` |

**Pixi option:**

| Platform | Action |
|----------|--------|
| macOS | Double-click `Start-pixi.command` |
| Windows | Double-click `Start-pixi.bat` |
| Linux | Open a terminal in the folder and run `./start-pixi.sh` |

> **Mac — first launch only (both options):** if you see "cannot be opened because the developer cannot be verified", right-click the file, choose **Open**, then click **Open**. You will not be asked again.

The first launch takes a few minutes to set up. You can use this time to read the [User Manual](USER_MANUAL.md). When it is ready, your browser will open automatically.

---

## Stopping the app

**Docker option:**

| Platform | Action |
|----------|--------|
| macOS | Double-click `Stop.command` |
| Windows | Double-click `Stop.bat` |
| Linux | Run `./stop.sh` in a terminal |

**Pixi option:**

| Platform | Action |
|----------|--------|
| macOS | Double-click `Stop-pixi.command` |
| Windows | Double-click `Stop-pixi.bat` |
| Linux | Run `./stop-pixi.sh` in a terminal |

Closing the browser tab does **not** stop the app — any running download will continue in the background until you use the stop file.

---

## Getting a key

1. Go to [Google Cloud Console](https://console.cloud.google.com/iam-admin/serviceaccounts) and select your project.
2. Open or create a service account that has the **Earth Engine** role.
3. Click **Keys → Add Key → Create new key → JSON** and download the file.

The first time you open the app it will ask you to upload this file. After that it remembers it and you will not be asked again.

---

# GEE Web App（繁體中文說明）

簡單從 Google Earth Engine 下載您所選區域的衛星資料（降水量、氣溫、植被、土地覆蓋等）。

---

## 開始之前

您需要準備兩樣東西：

**1. 執行應用程式的方式——請選擇其一：**

| 選項 | 適合對象 | 需求 |
|------|----------|------|
| **Docker** | 一般使用者 | 安裝 [Docker Desktop](https://www.docker.com/products/docker-desktop) |
| **Pixi** | 無法使用 Docker 的使用者 | 執行一行安裝指令（見下方說明） |

兩種選項都會在瀏覽器中呈現相同的應用程式。Docker 使用容器執行所有程式；Pixi 則直接在您的電腦上執行，不需要容器。

> **安裝 Pixi**（若選擇此選項）：
> - **Mac / Linux：** 開啟終端機，執行 `curl -fsSL https://pixi.sh/install.sh | sh`，然後關閉並重新開啟終端機。
> - **Windows：** 開啟 PowerShell，執行 `iwr -useb https://pixi.sh/install.ps1 | iex`，然後關閉並重新開啟。

**2. Google Earth Engine 金鑰檔案**
一個小檔案（副檔名為 `.json`），用於授權應用程式存取 Google Earth Engine。如果您還沒有，請參閱下方的[取得金鑰](#取得金鑰)說明。

---

## 啟動應用程式

**Docker 選項：**

| 平台 | 操作方式 |
|------|----------|
| macOS | 雙擊 `Start.command` |
| Windows | 雙擊 `Start.bat` |
| Linux | 在資料夾中開啟終端機並執行 `./quickstart-react.sh` |

**Pixi 選項：**

| 平台 | 操作方式 |
|------|----------|
| macOS | 雙擊 `Start-pixi.command` |
| Windows | 雙擊 `Start-pixi.bat` |
| Linux | 在資料夾中開啟終端機並執行 `./start-pixi.sh` |

> **Mac — 首次啟動（兩種選項皆適用）：** 若出現「無法開啟，因為無法驗證開發者」的提示，請右鍵點擊該檔案，選擇**開啟**，然後點擊**開啟**。之後不會再出現此提示。

首次啟動需要幾分鐘進行初始化。您可以利用這段時間閱讀[使用手冊](USER_MANUAL.md)。準備就緒後，瀏覽器將自動開啟。

---

## 停止應用程式

**Docker 選項：**

| 平台 | 操作方式 |
|------|----------|
| macOS | 雙擊 `Stop.command` |
| Windows | 雙擊 `Stop.bat` |
| Linux | 在終端機中執行 `./stop.sh` |

**Pixi 選項：**

| 平台 | 操作方式 |
|------|----------|
| macOS | 雙擊 `Stop-pixi.command` |
| Windows | 雙擊 `Stop-pixi.bat` |
| Linux | 在終端機中執行 `./stop-pixi.sh` |

關閉瀏覽器分頁不會停止應用程式——任何正在進行的下載都會在背景繼續，直到您使用對應的停止檔案為止。

---

## 取得金鑰

1. 前往 [Google Cloud Console](https://console.cloud.google.com/iam-admin/serviceaccounts) 並選擇您的專案。
2. 開啟或建立一個具有 **Earth Engine** 角色的服務帳戶。
3. 點擊**金鑰 → 新增金鑰 → 建立新金鑰 → JSON**，然後下載該檔案。

首次開啟應用程式時，系統會要求您上傳此檔案。之後應用程式會記住它，不會再次詢問。
