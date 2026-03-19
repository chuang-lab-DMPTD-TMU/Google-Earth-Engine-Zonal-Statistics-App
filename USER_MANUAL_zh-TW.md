# GEE Batch Processor — 使用手冊

本手冊提供應用程式的實用操作指南，包含設定步驟與常見問題排解。

---

## 目錄

1. [使用下載器（Streamlit UI）](#1-使用下載器streamlit-ui)
   - [第 0 節 — 執行工作階段（側邊欄）](#第-0-節--執行工作階段側邊欄)
   - [第 1 節 — 資料參數（側邊欄）](#第-1-節--資料參數側邊欄)
   - [第 2 節 — 關注區域](#第-2-節--關注區域)
   - [第 3 節 — 結果](#第-3-節--結果)
   - [第 4 節 — 部分提取](#第-4-節--部分提取)
2. [前置需求](#2-前置需求)
3. [取得程式碼](#3-取得程式碼)
4. [GEE 憑證](#4-gee-憑證)
5. [建置與啟動 Docker](#5-建置與啟動-docker)
6. [停止、恢復與維護](#6-停止恢復與維護)
7. [疑難排解](#7-疑難排解)
8. [開發者說明](#8-開發者說明)

---

## 1. 使用下載器（Streamlit UI）

介面分為數個編號區塊。執行新任務時，請由上至下依序操作。

---

### 第 0 節 — 執行工作階段（側邊欄）

位於左側邊欄頂端。

| 欄位 | 用途 |
|------|------|
| **RUN ID**（文字方塊） | 留白以自動產生 6 字元 ID 來建立新任務。輸入現有 ID 可恢復或檢查先前的任務。 |

輸入現有 RUN ID 後，輸入框下方會顯示狀態標籤（`running`、`completed`、`failed`）。若任務仍在執行中，也會顯示 **Stop This RUN ID** 按鈕。

下方的**本機 RUN 紀錄**展開區會以表格列出所有曾啟動的任務及其狀態與時間戳記。點選**檢視已儲存的 RUN** 選擇器中的任意列，可查看其完整設定。

---

### 第 1 節 — 資料參數（側邊欄）

每個資料集以獨立的展開區收納。展開一個或多個來進行設定。

**可用資料集：**

| 資料集 | 測量內容 | 原始解析度 | 日期範圍 |
|--------|----------|------------|----------|
| **CHIRPS** | 降水量 | 0.05°（約 5.6 km） | 1981-01 → 至今 |
| **ERA5_LAND** | 氣溫、降水量、蒸發量（11 個變數） | 約 9 km | 1950-01 → 至今 |
| **MODIS_LST** | 地表溫度（白天 + 夜間） | 1 km | 2000-02 → 至今 |
| **MODIS_NDVI_EVI** | 植被指數 | 250 m | 2000-02 → 至今 |
| **WorldCover_v100** | 2020 年土地覆蓋分類 | 10 m | 僅 2020 年 |
| **WorldCover_v200** | 2021 年土地覆蓋分類 | 10 m | 僅 2021 年 |
| **MODIS_LULC** | 土地覆蓋類型 | 500 m | 2001–2023 |

**啟用每個資料集後：**

1. 勾選**啟用 `<資料集>`**。
2. **選擇波段** — 選取要擷取的變數（預設全部勾選）。
3. **選擇統計量（Reducers）** — `mean`、`sum`、`min`、`max`、`median`。各資料集已預填預設值。
4. **選擇日期範圍** — 具有合成週期的產品會自動擷取所選範圍內的資料。

單次任務可啟用多個資料集，它們會在 GEE 速率限制下平行處理。

---

### 第 2 節 — 關注區域

位於主欄左側，標示為 **2. Area of Interest**。

#### 上傳幾何圖形

點擊**瀏覽檔案**並上傳以下其中一種格式：

| 格式 | 說明 |
|------|------|
| **ZIP shapefile** | 必須將 `.shp`、`.shx`、`.dbf`、`.prj` 一起打包成一個 ZIP |
| **GeoJSON** | 單一 `.geojson` 檔案 |
| **GeoParquet** | 含幾何欄位的 `.parquet` 或 `.geoparquet` |

應用程式會驗證幾何圖形，並將其儲存至 `data/runs/<run_id>/inputs/`，讓所有輸入與輸出集中於同一位置。Geoparquet會在運算時自動轉換為GeoJSON。

#### 新任務與恢復任務

- 在第 0 節中將 **RUN ID** 留白以開始全新任務。
- 輸入現有 RUN ID 以恢復任務——幾何圖形已隨該任務儲存，無需重新上傳。

#### 執行處理流程

至少設定一個資料集並上傳 AOI 後，**Run Analysis** 按鈕即會啟用。點擊後會：

1. 鎖定執行設定。
2. 自動產生或沿用 RUN ID。
3. 顯示含工作計數的**執行計劃**。
4. 在背景啟動擷取流程。

進度會以區塊和最終檔案計數的形式即時更新顯示。您可以安全地離開頁面後再返回——處理流程會在容器背景中持續執行。

---

### 第 3 節 — 結果

位於主欄右側，標示為 **3. Results**。

任務完成後，結果檔案會出現在介面中，提供兩種選項：

- **GeoParquet（建議）** — 流程完成後自動寫入 `data/runs/<run_id>/results/`。檔案已存在您的電腦上，介面按鈕僅為便利捷徑。
- **CSV** — 點擊 CSV 按鈕會將 GeoParquet 轉換為不含幾何的平面表格，並以 `<product>_<start>_to_<end>.csv` 格式儲存於 parquet 旁邊。後續點擊會直接讀取已儲存的檔案。與 parquet 相同，可直接從 `data/runs/<run_id>/results/` 存取。

檔案命名規則：

```
<product>_<start_date>_to_<end_date>.parquet
# 例如：CHIRPS_1986-01-01_to_2026-02-28.parquet
```

---

### 第 4 節 — 部分提取

輸入結果 RUN ID 後出現。適用於長時間任務仍在進行中但需要中間結果的情況。

1. 點擊**準備／重新整理部分提取檔案**。
2. 應用程式將所有已完成的區塊檔案（即使完整流程尚未結束）合併為每個產品的單一 GeoParquet。
3. 合併後的部分檔案下載按鈕隨即出現。

可隨時再次點擊按鈕以納入更新的已完成區塊。

---

## 2. 前置需求

| 需求 | 最低版本 | 說明 |
|------|----------|------|
| Docker Engine | 24+ | 包含 `docker compose` 外掛 |
| Docker Compose | v2 | 以 `docker compose` 呼叫（無連字號） |
| 磁碟空間 | 至少 5 GB | 每次任務；依 AOI 大小與日期範圍而異 |
| GEE 服務帳戶 | — | JSON 金鑰檔案；詳見第 4 節 |

**macOS／Windows** 請使用 Docker Desktop。**Linux** 請從官方套件安裝 Docker Engine 與 Compose 外掛。

驗證安裝：

```bash
docker --version        # Docker version 24.x 或更新版本
docker compose version  # Docker Compose version v2.x 或更新版本
```

---

## 3. 取得程式碼

複製儲存庫並進入目錄：

```bash
git clone https://github.com/<your-org>/gee_web_app.git
cd gee_web_app
```

若您收到的是 ZIP 壓縮檔，請解壓縮後 `cd` 進入該資料夾，後續步驟完全相同。

**資料夾內容：**

```
gee_web_app/
├── main.py               ← Streamlit 應用程式（UI + 流程啟動器）
├── Snakefile_parquet     ← Snakemake 工作流程（GeoParquet 流程）
├── scripts/              ← Snakemake 呼叫的工作腳本
├── config/               ← 首次設定後金鑰儲存位置（詳見第 4 節）
├── data/                 ← 自動建立；存放所有任務資料
├── Start.command         ← macOS 啟動器（雙擊）
├── Start.bat             ← Windows 啟動器（雙擊）
├── Stop.command          ← macOS 停止（雙擊）
├── Stop.bat              ← Windows 停止（雙擊）
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

---

## 4. GEE 憑證

應用程式使用**服務帳戶金鑰**連接 Google Earth Engine。所有設定均在瀏覽器內完成，無需手動複製檔案。

**從 Google Cloud 取得金鑰：**

1. 開啟 [Google Cloud Console](https://console.cloud.google.com/iam-admin/serviceaccounts) 並選擇您的專案。
2. 建立（或開啟）具有 **Earth Engine** 角色的服務帳戶。
3. 前往**金鑰 → 新增金鑰 → 建立新金鑰 → JSON**，然後下載檔案。

**在應用程式中上傳金鑰：**

首次開啟應用程式時，會顯示設定畫面而非一般介面。將下載的 `.json` 檔案拖放（或點擊瀏覽）至上傳元件。應用程式會驗證並確認服務帳戶電子郵件，然後自動載入完整介面。

金鑰會儲存至主機的 `config/gee-key.json`，並在每次重新啟動時重複使用。除非您將其移除，否則不會再次出現提示。

**移除或更換金鑰：**

開啟左側邊欄中的 **GEE credentials** 展開區，點擊 **Remove / replace key**。設定畫面會重新出現，讓您上傳其他金鑰。

> `config/gee-key.json` 已列於 `.gitignore`，不會被提交至 git。

---

## 5. 建置與啟動 Docker

**macOS** — 在 Finder 中雙擊 `Start.command`。

**Windows** — 雙擊 `Start.bat`。

**Linux** — 在專案資料夾的終端機中執行 `./quickstart.sh`。

三者執行的操作相同：

1. 確認 Docker Desktop 是否正在執行，若未執行則顯示清楚的提示訊息。
2. 首次啟動時建置映像檔（下載基礎映像並安裝相依套件——**首次約需 3–10 分鐘**，之後每次啟動均使用快取）。
3. 啟動容器，從 8501–8505 中選取第一個空閒連接埠，等待 UI 回應後自動在瀏覽器中開啟。

> **macOS — 首次啟動：** 若出現「無法開啟，因為無法驗證開發者」，請右鍵點擊 `Start.command` 選擇**開啟**，然後在對話框中點擊**開啟**。之後不會再次詢問。

**完成後停止應用程式：**

- **macOS：** 雙擊 `Stop.command`
- **Windows：** 雙擊 `Stop.bat`
- **Linux：** 在終端機中執行 `./stop.sh`

**查看即時記錄：**

```bash
docker compose logs -f app
```

---

## 6. 停止、恢復與維護

### 停止執行中的處理流程

- **從 UI：** 在第 0 節輸入 RUN ID → 點擊 **Stop This RUN ID**。
- **從終端機：** `docker compose exec app kill -TERM <snakemake_pid>`（PID 顯示於側邊欄）。
- **強制停止：** `docker compose down` — 停止所有服務；之後可重新啟動並恢復。

### 恢復失敗或已停止的任務

1. 在第 0 節側邊欄輸入 RUN ID。
2. 設定相同的產品與日期範圍。
3. 點擊 **Run Analysis** — 已儲存的幾何圖形會自動重複使用，流程從中斷處繼續（僅重新擷取缺少的區塊）。

### 解除過時的 Snakemake 鎖定

若應用程式在任務執行中途崩潰，Snakemake 可能會在任務資料夾內留下 `.snakemake/locks/` 目錄。應用程式會在每次新任務前自動嘗試解鎖。若問題持續，請針對特定任務執行解鎖指令：

```bash
docker compose exec app bash -c "cd /app/data/runs/<run_id> && snakemake --unlock --snakefile /app/Snakefile_parquet --directory /app/data/runs/<run_id>"
```

---

## 7. 疑難排解

| 症狀 | 可能原因 | 解決方法 |
|------|----------|----------|
| 雙擊啟動器後應用程式未開啟 | Docker 未執行 | 啟動 Docker Desktop，然後再試一次 |
| UI 未載入（瀏覽器空白） | 連接埠衝突或映像建置失敗 | 執行 `docker compose logs -f app` 查看詳情 |
| 「GEE authentication error」 | `gee-key.json` 遺失或路徑錯誤 | 確認檔案位於 `config/gee-key.json` |
| 處理流程卡在 0 個區塊 | GEE 速率限制或網路問題 | 等待 2 分鐘；檢查 `data/runs/<run_id>/logs/snakemake_run.log` |
| 任務立即顯示 `failed` | 先前崩潰留下的 Snakemake 鎖定 | 執行上方的解鎖指令，然後重試 |
| Linux 上的檔案擁有權問題 | UID/GID 不符 | `./quickstart.sh` 會透過匯出 `HOST_UID`／`HOST_GID` 自動修正 |

**記錄檔位置：**

| 記錄 | 路徑 |
|------|------|
| Snakemake 執行記錄 | `data/runs/<run_id>/logs/snakemake_run.log` |
| 容器標準輸出 | `docker compose logs app` |
| 任務事件歷史 | `data/runs/run_state.duckdb` → `run_events` 資料表 |

---

## 8. 開發者說明

### 目錄結構

```
gee_web_app/
├── data/              # 執行時資料（不提交至 git）
│   └── runs/          # 所有每次任務的輸出
│       ├── <run_id>/
│       │   ├── inputs/       # 上傳的幾何檔案（shapefile、GeoJSON 或 GeoParquet）
│       │   ├── results/      # 最終合併的 .parquet 檔案
│       │   ├── logs/         # snakemake_run.log 與各工作記錄
│       │   └── intermediate/ # 區塊工作檔案（GeoJSON → GeoParquet）
│       └── run_state.duckdb  # 任務狀態與事件歷史
├── config/            # GEE 服務帳戶金鑰（不提交至 git）
│   └── gee-key.json
├── scripts/           # Snakemake 呼叫的工作腳本
├── main.py            # Streamlit 應用程式（UI + 流程啟動器）
├── Snakefile_parquet  # Snakemake 工作流程協調器
├── quickstart.sh      # Linux 啟動器；選取空閒連接埠（8501–8505）
├── stop.sh            # Linux 停止腳本
├── Start.command      # macOS 啟動器（雙擊）
├── Start.bat          # Windows 啟動器（雙擊）
├── Stop.command       # macOS 停止（雙擊）
├── Stop.bat           # Windows 停止（雙擊）
├── docker-compose.yml # 服務定義
├── Dockerfile         # 基礎映像、系統相依套件、pip 安裝
└── requirements.txt
```

### 查看容器即時記錄

```bash
docker compose logs -f app
```

### 進入容器 Shell

```bash
docker compose exec app bash
```

### 連接埠選擇

啟動器會嘗試 8501–8505 連接埠，並使用第一個空閒的連接埠。若要更改候選清單，請編輯 `quickstart.sh` 或 `Start.command`／`Start.bat` 頂部的 `PORTS` 陣列。

### Linux 上的檔案擁有權

`quickstart.sh` 在啟動容器前會匯出 `HOST_UID` 和 `HOST_GID`，確保容器內寫入的檔案由您的主機使用者擁有。若您手動啟動容器（例如 `docker compose up -d`），請先匯出這些變數：

```bash
export HOST_UID=$(id -u) HOST_GID=$(id -g)
docker compose up -d app
```
