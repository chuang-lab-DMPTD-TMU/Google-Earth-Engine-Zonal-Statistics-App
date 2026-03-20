#!/usr/bin/env python3
"""
CLI script to build partial checkout files for a given run ID.
Called by main.py via subprocess.Popen so it runs out-of-process
and does not block the Streamlit UI thread.

Usage:
    python scripts/build_partial.py <run_id> <runs_dir>
"""
import sys
import re
import json
import duckdb
from pathlib import Path


def _log_event(runs_dir: Path, run_id: str, message: str):
    db_path = runs_dir / "run_state.duckdb"
    if not db_path.exists():
        return
    try:
        with duckdb.connect(str(db_path)) as conn:
            conn.execute(
                """INSERT INTO run_events (event_time, run_id, event_type, status, message, payload_json)
                   VALUES (CURRENT_TIMESTAMP, ?, 'info', 'info', ?, ?)""",
                [run_id, message, json.dumps({})],
            )
    except Exception:
        pass


def sql_quote_ident(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def merge_parquet_chunks_to_output(chunk_files, output_file: Path):
    """Merge chunk parquet files into one parquet file using key-based joins."""
    if not chunk_files:
        return False

    conn = duckdb.connect(":memory:")
    try:
        conn.execute("CREATE TABLE chunk_0 AS SELECT * FROM read_parquet(?)", [str(chunk_files[0])])
        first_cols = [row[1] for row in conn.execute("PRAGMA table_info('chunk_0')").fetchall()]

        join_candidates = ["region_id", "Date", "geom", "geometry"]
        fallback_union = False

        for idx, chunk_path in enumerate(chunk_files[1:], start=1):
            conn.execute(f"CREATE TABLE chunk_{idx} AS SELECT * FROM read_parquet(?)", [str(chunk_path)])
            chunk_cols = [row[1] for row in conn.execute(f"PRAGMA table_info('chunk_{idx}')").fetchall()]
            common_keys = [k for k in join_candidates if k in first_cols and k in chunk_cols]
            if not common_keys:
                fallback_union = True
                break

        if fallback_union:
            union_query = " UNION ALL ".join(
                [f"SELECT * FROM chunk_{idx}" for idx in range(len(chunk_files))]
            )
            conn.execute(f"CREATE TABLE merged AS {union_query}")
        else:
            select_items = [f"base.{sql_quote_ident(col)}" for col in first_cols]
            join_clauses = []
            seen_cols = set(first_cols)

            for idx in range(1, len(chunk_files)):
                chunk_cols = [row[1] for row in conn.execute(f"PRAGMA table_info('chunk_{idx}')").fetchall()]
                common_keys = [k for k in join_candidates if k in seen_cols and k in chunk_cols]
                new_cols = [c for c in chunk_cols if c not in seen_cols and c not in common_keys]

                if not common_keys:
                    continue

                on_clause = " AND ".join(
                    [f"base.{sql_quote_ident(k)} = c{idx}.{sql_quote_ident(k)}" for k in common_keys]
                )
                join_clauses.append(f"LEFT JOIN chunk_{idx} c{idx} ON {on_clause}")

                for col in new_cols:
                    select_items.append(f"c{idx}.{sql_quote_ident(col)}")
                    seen_cols.add(col)

            merge_query = (
                "CREATE TABLE merged AS SELECT "
                + ", ".join(select_items)
                + " FROM chunk_0 base "
                + " ".join(join_clauses)
            )
            conn.execute(merge_query)

        sort_cols = []
        merged_cols = [row[1] for row in conn.execute("PRAGMA table_info('merged')").fetchall()]
        if "Date" in merged_cols:
            sort_cols.append(sql_quote_ident("Date"))
        if "region_id" in merged_cols:
            sort_cols.append(sql_quote_ident("region_id"))

        if sort_cols:
            conn.execute(f"CREATE TABLE sorted AS SELECT * FROM merged ORDER BY {', '.join(sort_cols)}")
            table_to_export = "sorted"
        else:
            table_to_export = "merged"

        output_file.parent.mkdir(parents=True, exist_ok=True)
        conn.execute(
            f"COPY {table_to_export} TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
            [str(output_file)]
        )
        return output_file.exists()
    finally:
        conn.close()


def build_partial_checkout_files_parquet(run_id: str, runs_dir: Path):
    """Build merged partial checkout GeoParquet files from completed parquet chunks."""
    intermediate = runs_dir / run_id / "intermediate"
    results = runs_dir / run_id / "results"
    run_chunk_root = intermediate / "chunks"
    partial_root = results / "partial_checkout"

    if not run_chunk_root.exists():
        return []

    merged_files = []
    for product_dir in sorted([item for item in run_chunk_root.iterdir() if item.is_dir()]):
        band_chunk_files = []
        discovered_chunks = []

        for chunk_file in sorted(product_dir.glob("*.parquet")):
            match = re.match(
                r"^(?P<band>.+?)_(?P<chunk>\d{4}-\d{2}_\d{4}-\d{2}|\d{4}-\d{2}|\d{4})\.parquet$",
                chunk_file.name,
            )
            if not match:
                continue
            discovered_chunks.append(match.group("chunk"))
            band_chunk_files.append(chunk_file)

        if not band_chunk_files or not discovered_chunks:
            continue

        unique_chunks = sorted(set(discovered_chunks))
        output_dir = partial_root / product_dir.name
        output_file = output_dir / (
            f"{product_dir.name}_partial_{unique_chunks[0]}_to_{unique_chunks[-1]}.parquet"
        )

        latest_chunk_mtime = max(chunk.stat().st_mtime for chunk in band_chunk_files)
        if output_file.exists() and output_file.stat().st_mtime >= latest_chunk_mtime:
            merged_files.append(output_file)
            continue

        output_dir.mkdir(parents=True, exist_ok=True)
        if merge_parquet_chunks_to_output(band_chunk_files, output_file):
            merged_files.append(output_file)
            _log_event(runs_dir, run_id, f"Build partial output to: {output_file.name}")

    return sorted(merged_files)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: build_partial.py <run_id> <runs_dir>", file=sys.stderr)
        sys.exit(1)

    run_id = sys.argv[1]
    runs_dir = Path(sys.argv[2])

    results = build_partial_checkout_files_parquet(run_id, runs_dir)
    print(f"Built {len(results)} partial checkout file(s).")
    for f in results:
        print(f"  {f}")
