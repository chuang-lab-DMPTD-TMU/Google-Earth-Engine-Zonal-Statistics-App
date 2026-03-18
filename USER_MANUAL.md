# GEE Batch Processor — User Manual

A practical guide from cloning the repository to querying your downloaded results.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Getting the code](#2-getting-the-code)
3. [GEE credentials](#3-gee-credentials)
4. [Building and launching Docker](#4-building-and-launching-docker)
5. [Using the downloader (Streamlit UI)](#5-using-the-downloader-streamlit-ui)
   - [Section 0 — Run Session (sidebar)](#section-0--run-session-sidebar)
   - [Section 1 — Data Parameters (sidebar)](#section-1--data-parameters-sidebar)
   - [Section 2 — Area of Interest](#section-2--area-of-interest)
   - [Section 3 — Results](#section-3--results)
   - [Section 4 — Partial Checkout](#section-4--partial-checkout)
6. [Looking at past downloads with the Data Explorer](#6-looking-at-past-downloads-with-the-data-explorer)
7. [Querying with DuckDB directly](#7-querying-with-duckdb-directly)
8. [Stopping, resuming, and housekeeping](#8-stopping-resuming-and-housekeeping)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Prerequisites

| Requirement | Minimum version | Notes |
|-------------|-----------------|-------|
| Docker Engine | 24+ | Includes `docker compose` plugin |
| Docker Compose | v2 | Called as `docker compose` (no hyphen) |
| Disk space | ~5 GB free | Per run; varies with AOI size and date range |
| GEE service account | — | JSON key file; see §3 |

On **macOS/Windows** use Docker Desktop. On **Linux** install Docker Engine and the Compose plugin from the official packages.

Verify your install:

```bash
docker --version        # Docker version 24.x or later
docker compose version  # Docker Compose version v2.x or later
```

---

## 2. Getting the code

Clone the repository and enter the directory:

```bash
git clone https://github.com/<your-org>/gee_web_app.git
cd gee_web_app
```

If you received the code as a ZIP archive instead, unzip it and `cd` into the resulting folder. The rest of the steps are identical.

Make the launcher script executable:

```bash
chmod +x quickstart.sh
```

**What you'll see inside the folder:**

```
gee_web_app/
├── main.py               ← Streamlit application (UI + pipeline launcher)
├── Snakefile_parquet     ← Snakemake workflow (GeoParquet pipeline)
├── scripts/              ← Worker scripts called by Snakemake
├── config/               ← Key stored here after first-run setup (see §3)
├── data/                 ← Created automatically; holds all run data
├── docker-compose.yml
├── Dockerfile
├── quickstart.sh
└── requirements.txt
```

---

## 3. GEE credentials

The app connects to Google Earth Engine using a **service account key**. You set this up entirely inside the browser — no manual file copying required.

**Getting a key from Google Cloud:**

1. Open the [Google Cloud Console](https://console.cloud.google.com/iam-admin/serviceaccounts) and select your project.
2. Create (or open) a service account that has the **Earth Engine** role.
3. Go to **Keys → Add Key → Create new key → JSON** and download the file.

**Uploading the key in the app:**

The first time you open `http://localhost:8501`, the app shows a setup screen instead of the normal UI. Drag-and-drop (or click to browse) the downloaded `.json` file into the upload widget. The app validates it and confirms the service-account email — then loads the full interface automatically.

The key is saved to `config/gee-key.json` on the host and reused on every subsequent restart. You will not be prompted again unless you remove it.

**Removing or replacing the key:**

Open the **GEE credentials** expander in the left sidebar and click **Remove / replace key**. The setup screen reappears so you can upload a different key.

> `config/gee-key.json` is listed in `.gitignore` and will not be committed to git.

---

## 4. Building and launching Docker

Run the quickstart script from inside the project folder:

```bash
./quickstart.sh
```

The script does three things:

1. Exports your host UID/GID so files created inside the container are owned by you.
2. Runs `docker compose build app` — downloads the base image and installs Python dependencies. **This takes 3–10 minutes on the first run** and is cached for subsequent launches.
3. Starts the container in detached mode and polls `http://localhost:8501` until the UI responds, then prints:

   ```
   🚀 Streamlit UI is ready at http://localhost:8501
   ```

Open **http://localhost:8501** in your browser.

**To stop the app** when you are done:

```bash
docker compose down
```

**To restart without rebuilding** (after the image already exists):

```bash
docker compose up -d app
```

**To follow live logs:**

```bash
docker compose logs -f app
```

---

## 5. Using the downloader (Streamlit UI)

The UI is divided into numbered sections. Work through them top-to-bottom for a new run.

---

### Section 0 — Run Session (sidebar)

Located at the top of the left sidebar.

| Field | Purpose |
|-------|---------|
| **RUN ID** (text box) | Leave blank to auto-generate a 6-character ID for a new run. Type an existing ID to resume or monitor a previous run. |

When you type an existing RUN ID, a status badge (`running`, `completed`, `failed`) appears below the input. If the run is still active, a **Stop This RUN ID** button is also shown.

The **Local RUN registry** expander beneath shows a table of every run ever started, with status and timestamps. Click any row in the **Inspect a stored RUN** selector to see its full configuration.

---

### Section 1 — Data Parameters (sidebar)

Each dataset is collapsed inside its own expander. Expand one or more to configure them.

**Available datasets:**

| Dataset | What it measures | Native resolution | Date range |
|---------|-----------------|-------------------|------------|
| **CHIRPS** | Precipitation | 0.05° (~5.6 km) | 1981-01 → present |
| **ERA5_LAND** | Temperature, precipitation, evaporation (11 variables) | ~9 km | 1950-01 → present |
| **MODIS_LST** | Land Surface Temperature (day + night) | 1 km | 2000-02 → present |
| **MODIS_NDVI_EVI** | Vegetation indices | 250 m | 2000-02 → present |
| **WorldCover_v100** | Land cover classification 2020 | 10 m | 2020 only |
| **WorldCover_v200** | Land cover classification 2021 | 10 m | 2021 only |
| **MODIS_LULC** | Land cover types | 500 m | 2001–2023 |

**For each dataset you enable:**

1. Tick **Enable `<dataset>`**.
2. **Select Bands** — choose which variables to extract (all are ticked by default).
3. **Select Statistics (Reducers)** — `mean`, `sum`, `min`, `max`, `median`. The default is pre-filled per dataset.
4. **Select date range** — monthly products show month dropdowns; annual products show year dropdowns.

You can enable multiple datasets in one run; they are processed in parallel subject to GEE rate limits.

---

### Section 2 — Area of Interest

This is the left main column, labelled **2. Area of Interest**.

#### Uploading your geometry

Click **Browse files** and upload one of:

| Format | Notes |
|--------|-------|
| **ZIP shapefile** | Must contain `.shp`, `.shx`, `.dbf`, `.prj` together in one ZIP |
| **GeoJSON** | Single `.geojson` file |
| **GeoParquet** | `.parquet` or `.geoparquet` with a geometry column |

The app validates the geometry, hashes it (for deduplication), and stores it in `data/uploads/`. Geoparquet is converted automatically to GeoJSON for GEE compatility.

#### New run vs. resume

- Leave **RUN ID** blank in Section 0 to start fresh.
- Enter an existing RUN ID to re-use the same geometry and configuration (e.g. after a failure).

#### Running the pipeline

Once at least one dataset is configured and the AOI is uploaded, the **Run Analysis** button becomes active. Click it to:

1. Freeze the run configuration.
2. Auto-generate or reuse the RUN ID.
3. Show an **Execution Plan** with job counts and a DAG diagram.
4. Launch Snakemake in the background.

Progress is shown below as chunk and final-file counts update. You can safely navigate away and come back — the pipeline runs in the background inside the container.

---

### Section 3 — Results

Located in the right main column, labelled **3. Results**.

| Field | Purpose |
|-------|---------|
| **Results RUN ID** (text box) | Filter downloads to one specific run. Leave blank to show all runs. |

When results are available, download buttons appear for each output file:

- **GeoParquet (recommended)** — column-compressed, geometry-preserving, cloud-native format. One file per product per run.
- **Legacy CSV** — flat table without geometry.

File naming convention:

```
<product>_<start_date>_to_<end_date>.parquet
# e.g. CHIRPS_1986-01-01_to_2026-02-28.parquet
```

Click the download button to save the file to your machine.

---

### Section 4 — Partial Checkout

Appears when a Results RUN ID is entered. Useful when a long run is still in progress and you need intermediate results.

1. Click **Prepare/Refresh Partial Checkout Files**.
2. The app merges all completed chunk files (even if the full pipeline hasn't finished) into a single GeoParquet per product.
3. Download buttons appear for the merged partial files.

Re-click the button at any time to include newer completed chunks.

---

## 6. Looking at past downloads with the Data Explorer

Section **5. Data Explorer** (at the bottom of the page, below a divider) lets you browse, filter, and preview any GeoParquet result file without writing a line of code.

### Step 1 — Find your file in the catalog

The catalog table lists every `.parquet` file under `data/results/`, with:

| Column | Meaning |
|--------|---------|
| Run ID | The 6-character run identifier |
| Product | Dataset name (CHIRPS, ERA5_LAND, etc.) |
| File | Filename including date range |
| Size MB | File size on disk |

**Filter the catalog:**

- **Filter by Run ID** — pick a specific run from the dropdown, or leave on "All".
- **Search filename** — type any substring (e.g. `CHIRPS`, `2024`) to narrow the list.

### Step 2 — Select a file

Use the **Select a file to explore** dropdown. It shows `Run / Product / Filename` for easy identification.

### Step 3 — Inspect metadata

After selecting a file, three metric cards appear instantly:

- **Rows** — total row count
- **Columns** — number of columns
- **Geometry columns** — number of geometry (WKB binary) columns

Expand **Schema** to see every column name and its data type.

### Step 4 — Filter without code

| Control | What it does |
|---------|-------------|
| **Date from / Date to** | Text inputs pre-filled with the actual min/max date in the file. Edit to restrict the time window. |
| **Show columns** (multi-select) | Pick which columns appear in the preview. Geometry columns are excluded automatically to keep the table readable. |
| **Preview rows** (slider) | 10 – 2 000 rows shown in the table. |

### Step 5 — Run and download

Click **Run / Refresh** to execute the query and display the results table. A **Download result as CSV** button beneath the table lets you save exactly what you see (filtered, column-selected) to your machine.

### Advanced: Custom SQL

Expand **Custom SQL (advanced)** to see the DuckDB query the filter builder generated. You can edit it freely — `read_parquet('/path/to/file.parquet')` is the table reference. Tick **Use custom SQL instead of filter above** and click **Run / Refresh**.

Example queries:

```sql
-- Monthly mean precipitation for region 42
SELECT "Date", "region_id", "precipitation_sum"
FROM read_parquet('/app/data/results/ABC123/CHIRPS/CHIRPS_2020-01-01_to_2024-12-31.parquet')
WHERE "region_id" = 42
ORDER BY "Date"

-- Hottest months (mean day LST > 305 K)
SELECT "Date", AVG("LST_Day_1km_mean") AS avg_lst
FROM read_parquet('/app/data/results/XYZ/MODIS_LST/MODIS_LST_2000-02-18_to_2026-02-10.parquet')
GROUP BY "Date"
HAVING avg_lst > 305
ORDER BY avg_lst DESC
LIMIT 20
```

---

## 7. Querying with DuckDB directly

For power users who prefer a terminal or Python script over the UI.

### From a terminal inside the container

```bash
docker compose exec app bash

# Open DuckDB CLI
duckdb

# Then inside DuckDB:
LOAD spatial;
SELECT * FROM read_parquet('/app/data/results/<run_id>/<product>/*.parquet') LIMIT 5;
```

### From Python (outside the container)

```python
import duckdb

conn = duckdb.connect()  # in-memory

# Query a result file directly
df = conn.execute("""
    SELECT *
    FROM read_parquet('data/results/<run_id>/<product>/<file>.parquet')
    LIMIT 1000
""").df()

print(df.head())
```

### Querying the run registry database

All run metadata is stored in `data/runs/run_state.duckdb`.

```python
import duckdb

conn = duckdb.connect("data/runs/run_state.duckdb", read_only=True)

# List all runs and their status
conn.execute("SELECT run_id, status, attempts, updated_at FROM run_status ORDER BY updated_at DESC").df()

# Full event history for one run
conn.execute("""
    SELECT event_time, event_type, status, message
    FROM run_events
    WHERE run_id = 'ABC123'
    ORDER BY event_time
""").df()
```

### Useful DuckDB patterns for GeoParquet

```sql
-- Describe the schema of a file
DESCRIBE SELECT * FROM read_parquet('path/to/file.parquet') LIMIT 0;

-- Count rows and date range
SELECT COUNT(*), MIN("Date"), MAX("Date")
FROM read_parquet('path/to/file.parquet');

-- Query multiple runs at once with a glob
SELECT *, filename
FROM read_parquet('data/results/*/CHIRPS/*.parquet', filename=true);

-- Export a filtered subset back to parquet
COPY (
    SELECT * FROM read_parquet('input.parquet') WHERE "Date" >= '2020-01-01'
) TO 'output_filtered.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
```

---

## 8. Stopping, resuming, and housekeeping

### Stopping a running pipeline

- **From the UI:** Enter the RUN ID in Section 0 → click **Stop This RUN ID**.
- **From the terminal:** `docker compose exec app kill -TERM <snakemake_pid>` (PID shown in the sidebar).
- **Nuclear option:** `docker compose down` — stops everything; you can restart and resume.

### Resuming a failed or stopped run

1. Enter the RUN ID in the Section 0 sidebar input.
2. Re-upload the same AOI file (or the app will use the cached copy if the hash matches).
3. Configure the same products and date range.
4. Click **Run Analysis** — Snakemake picks up from where it left off (only missing chunks are re-extracted).

### Unlocking a stale Snakemake lock

If the app crashed mid-run, Snakemake may leave a `.snakemake/locks/` directory. The app automatically attempts an unlock before each new run. If problems persist:

```bash
docker compose exec app snakemake --unlock --snakefile Snakefile_parquet --directory /app
```

### Deleting a run's data

In the sidebar, expand **Local RUN registry** → select the run → click **Delete this saved run**. This removes the YAML registry entry, result files, intermediate chunks, and logs for that RUN ID.

---

## 9. Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| UI never loads after `quickstart.sh` | Port 8501 blocked or image build failed | `docker compose logs -f app` to inspect |
| "GEE authentication error" | `gee-key.json` missing or wrong path | Confirm file is at `config/gee-key.json` |
| Pipeline hangs at 0 chunks | GEE rate limit or network issue | Wait 2 min; check `data/logs/<run_id>/snakemake_run.log` |
| Run shows `failed` immediately | Snakemake lock from a prior crash | Run the unlock command above, then retry |
| Data Explorer shows no files | No completed runs yet, or wrong Results RUN ID | Check `data/results/` directory |
| "Could not read parquet file" in Explorer | Partial/corrupt file from an incomplete run | Use Partial Checkout (§4) for in-progress runs |
| File ownership issues on Linux | UID/GID mismatch | `./quickstart.sh` fixes this automatically by exporting `HOST_UID`/`HOST_GID` |

**Log locations:**

| Log | Path |
|-----|------|
| Snakemake run log | `data/logs/<run_id>/snakemake_run.log` |
| Container stdout | `docker compose logs app` |
| Run event history | `data/runs/run_state.duckdb` → `run_events` table |
