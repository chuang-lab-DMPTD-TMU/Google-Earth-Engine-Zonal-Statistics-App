"""
Snakemake --log-handler-script for the GEE batch processor.

Snakemake calls log_handler(log) in this module for every internal event.
We use it to keep the DuckDB jobs table up to date in real time so the
Streamlit UI can show accurate per-job status without relying on file counting.

State machine per job:
    pending  →  running   (job_info fires)
    running  →  done      (info "Finished job N." fires)
    running  →  failed    (job_error fires)

The module maintains a jobid→wildcards mapping in memory because Snakemake's
completion message only carries the numeric jobid, not the wildcards.
"""

import os
import re
import sys
import threading
import time
import duckdb
from datetime import datetime, timezone

# ── context injected by main.py via environment variables ────────────────────
RUN_ID  = os.environ.get("GEE_RUN_ID")
DB_PATH = os.environ.get("GEE_DB_PATH")

# In-memory map: jobid (int) → {"prod": ..., "band": ..., "time_chunk": ...}
# Populated on job_info, consumed on completion/error.
_job_map: dict[int, dict] = {}

# ── per-job log tail threads ──────────────────────────────────────────────────
# jobid (int) → threading.Event  (set to stop the tail thread)
_tail_stop: dict[int, threading.Event] = {}


def _tail_job_log(log_path: str, stop: threading.Event, prefix: str):
    """
    Read new lines from a per-job log file and print them to stdout so they
    appear in snakemake_run.log (Snakemake redirects this process's stdout there).
    Polls every 2 s until stop is set, then drains any remaining lines.
    """
    try:
        with open(log_path, "r", errors="replace") as fh:
            # Seek to end — we only want lines written from now on.
            fh.seek(0, 2)
            while not stop.is_set():
                line = fh.readline()
                if line:
                    print(f"[job:{prefix}] {line}", end="", flush=True)
                else:
                    stop.wait(2)
            # Drain any final lines after the job finishes.
            for line in fh:
                print(f"[job:{prefix}] {line}", end="", flush=True)
    except Exception:
        pass


def _start_tail(jobid: int, log_path: str, prefix: str):
    stop = threading.Event()
    _tail_stop[jobid] = stop
    t = threading.Thread(target=_tail_job_log, args=(log_path, stop, prefix), daemon=True)
    t.start()


def _stop_tail(jobid: int):
    stop = _tail_stop.pop(jobid, None)
    if stop:
        stop.set()


def _wildcards_to_dict(wildcards) -> dict:
    """Normalise Snakemake's Wildcards object or plain dict to a plain dict."""
    if wildcards is None:
        return {}
    if isinstance(wildcards, dict):
        return wildcards
    # Snakemake Wildcards is a namedtuple-like object
    try:
        return wildcards._asdict()
    except AttributeError:
        pass
    try:
        return dict(wildcards)
    except Exception:
        return {}


def _append_run_event(message: str, event_type: str = "info"):
    """Write a single event to run_events. Silently ignores all errors."""
    if not RUN_ID or not DB_PATH:
        return
    for attempt in range(3):
        try:
            with duckdb.connect(DB_PATH) as conn:
                conn.execute(
                    """INSERT INTO run_events
                           (event_time, run_id, event_type, status, message, payload_json)
                       VALUES (CURRENT_TIMESTAMP, ?, ?, ?, ?, '{}')""",
                    [RUN_ID, event_type, event_type, message],
                )
            return
        except Exception:
            if attempt < 2:
                time.sleep(0.05 * (attempt + 1))


def _upsert_job(prod: str, band: str, chunk: str, status: str,
                jobid: int | None = None,
                log_path: str | None = None,
                error: str | None = None):
    """Write a single job status update to DuckDB. Silently ignores all errors."""
    if not RUN_ID or not DB_PATH:
        return
    if not prod or not band or not chunk:
        return

    now = datetime.now(timezone.utc).isoformat()
    started_at  = now if status == "running" else None
    finished_at = now if status in ("done", "failed") else None

    for attempt in range(3):
        try:
            with duckdb.connect(DB_PATH) as conn:
                conn.execute(
                    """
                    INSERT INTO jobs
                        (run_id, product, band, time_chunk, status,
                         jobid, log_path, started_at, finished_at, error)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (run_id, product, band, time_chunk) DO UPDATE SET
                        status      = excluded.status,
                        jobid       = COALESCE(excluded.jobid,      jobs.jobid),
                        log_path    = COALESCE(excluded.log_path,   jobs.log_path),
                        started_at  = COALESCE(excluded.started_at, jobs.started_at),
                        finished_at = excluded.finished_at,
                        error       = excluded.error
                    """,
                    [RUN_ID, prod, band, chunk, status,
                     jobid, log_path, started_at, finished_at, error],
                )
            return  # success
        except Exception:
            if attempt < 2:
                time.sleep(0.05 * (attempt + 1))  # brief back-off on contention


def log_handler(log: dict):
    """Entry point called by Snakemake for every log event."""
    try:
        _dispatch(log)
    except Exception:
        pass  # never let the handler crash the pipeline


def _dispatch(log: dict):
    level = log.get("level", "")

    # ── job dispatched ────────────────────────────────────────────────────────
    if level == "job_info":
        rule  = log.get("name") or ""
        jobid = log.get("jobid")
        wc    = _wildcards_to_dict(log.get("wildcards"))
        prod  = wc.get("prod")

        if rule == "merge_product_parquet":
            if prod:
                _append_run_event(f"Started {prod} merge", event_type="job_start")
            return

        # Only track the GEE extraction step — convert_to_parquet shares the same
        # (prod, band, chunk) key and would regress "done" back to "running".
        if rule not in ("", "extract_geojson_chunk"):
            return

        band      = wc.get("band")
        chunk     = wc.get("time_chunk")
        log_files = log.get("log") or []
        log_path  = log_files[0] if log_files else None

        if jobid is not None and prod and band and chunk:
            jid = int(jobid)
            _job_map[jid] = {"prod": prod, "band": band, "chunk": chunk}
            _upsert_job(prod, band, chunk, "running", jobid=jid, log_path=log_path)
            if log_path:
                # Wait briefly for the worker to create the log file before tailing.
                threading.Thread(
                    target=lambda: (time.sleep(1), _start_tail(jid, log_path, f"{band}/{chunk}")),
                    daemon=True,
                ).start()

    # ── job failed ────────────────────────────────────────────────────────────
    elif level == "job_error":
        rule  = log.get("name") or ""
        jobid = log.get("jobid")
        wc    = _wildcards_to_dict(log.get("wildcards"))
        prod  = wc.get("prod")
        band  = wc.get("band")
        chunk = wc.get("time_chunk")
        log_files = log.get("log") or []
        log_path  = log_files[0] if log_files else None

        exc = log.get("exception")
        error_str = str(exc) if exc else "job failed"
        if len(error_str) > 500:
            error_str = error_str[:500] + "…"

        if rule == "merge_product_parquet":
            if prod:
                _append_run_event(f"Failed {prod} merge: {error_str}", event_type="job_error")
            return

        if jobid is not None:
            _stop_tail(int(jobid))
        if prod and band and chunk:
            _upsert_job(prod, band, chunk, "failed",
                        jobid=int(jobid) if jobid is not None else None,
                        log_path=log_path, error=error_str)
        elif jobid is not None:
            wc_cached = _job_map.get(int(jobid), {})
            if wc_cached:
                _upsert_job(wc_cached["prod"], wc_cached["band"], wc_cached["chunk"],
                            "failed", jobid=int(jobid), error=error_str)

    # ── job finished successfully ─────────────────────────────────────────────
    elif level == "info":
        msg = log.get("msg") or ""
        match = re.search(r"Finished job\s+(\d+)", msg)
        if match:
            jobid = int(match.group(1))
            _stop_tail(jobid)
            wc_cached = _job_map.get(jobid, {})
            if wc_cached:
                _upsert_job(wc_cached["prod"], wc_cached["band"], wc_cached["chunk"],
                            "done", jobid=jobid)
                _job_map.pop(jobid, None)  # free memory
