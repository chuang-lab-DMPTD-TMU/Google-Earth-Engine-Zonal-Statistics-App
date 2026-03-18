# GEE Web App

This repository bundles the Streamlit + Snakemake workflow that drives the
Earth Engine batch processor. Everything you need to run it locally is inside the
codebase and the Docker definition, so cloning and running `./quickstart.sh` is
all you need to open the app in your browser.

## Requirements

- **Docker** (Engine + Compose) installed on your machine
- Enough disk space for intermediate chunks and final results (see `data/`,
  `results/`, `chunks/`, `runs/`)

## Quick start

1. Clone this repository:
   ```bash
   git clone https://github.com/<your-org>/gee_web_app.git
   cd gee_web_app
   ```
2. Make sure the entry script is executable:
   ```bash
   chmod +x quickstart.sh
   ```
3. Run the helper:
   ```bash
   ./quickstart.sh
   ```
   The script builds the `app` service, launches it in detached mode, waits until
   Streamlit answers on `localhost:8501`, and prints the URL when it is ready.
4. Open `http://localhost:8501` in your browser and follow the prompts.

## Authentication

The UI contains a dedicated Earth Engine authentication drawer. When the app
starts for the first time it will give you a short-lived code and a link to open
in your browser. Paste that code back into the UI prompt to finish the
handshake. Once the token is cached, it is reused for later runs until the
session expires.

If you prefer to authenticate manually before the UI starts, you can also run
`docker compose exec app ee authenticate` (or activate your virtual environment
and run `earthengine authenticate` before launching the container).

## Data & logs locations

- `data/` holds the uploaded geometries plus chunk storage (`geojson_chunks`,
  `pq_chunks`).
- `results/` hosts the final merged files (`.parquet`/`.csv`).
- `runs/` keeps the YAML registry for each RUN ID.
- `logs/` contains Snakemake logs per run (`data/logs/<run_id>/snakemake_run.log`).
- `config/` stores reusable input configurations.

All of the directories live under `/app/data` inside the container and are
mounted to this repo’s `data/` folder, so everything survives container
restarts.

## Development & troubleshooting

- To watch logs in real time:
  ```bash
  docker compose logs -f app
  ```
- To open a shell inside the container:
  ```bash
  docker compose exec app bash
  ```
- When you are done working, stop the service:
  ```bash
  docker compose down
  ```

## Directory layout (self-contained)

```
gee_web_app/
├── data/              # uploads, chunks, parquet working directories
├── results/           # final merged files grouped by RUN ID
├── runs/              # run registry and YAML payload snapshots
├── scripts/           # Snakemake helper scripts (GEE → GeoJSON → Parquet)
├── Snakefile_parquet  # main orchestrator for the production workflow
├── quickstart.sh      # builds + launches Docker (self-contained entry point)
├── docker-compose.yml # defines the app service
├── Dockerfile         # base image, system dependencies, and pip install
└── README.md          # this file
```

## Customization notes

- `HOST_UID` and `HOST_GID` are exported by the entry script so files created
  by the container match your host UID.
- If you want to inspect the Snakemake graph manually, run:
  ```bash
  docker compose exec app streamlit run main.py
  ```
  (the script already does this for you, so this is only needed for advanced debugging.)
- Results are written in GeoParquet by default; there are download buttons in
  the UI to pull `.parquet` or legacy `.csv` files for each run.

Enjoy the self-contained workflow—just pull the repo, build the image, and the
UI plus Earth Engine auth flow are all inside the container.