"""
Convert GeoJSON to GeoParquet using DuckDB.
Preserves geometry and adds spatial indexing.
"""
import duckdb
import os
import sys
from pathlib import Path
from datetime import datetime

def log_progress(message, log_file=None):
    """Write progress to log file if provided"""
    timestamp = datetime.now().isoformat()
    print(f"[{timestamp}] {message}")
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        with open(log_file, "a") as f:
            f.write(f"[{timestamp}] {message}\n")

def convert_geojson_to_parquet(geojson_path, parquet_path, log_file=None):
    """
    Convert GeoJSON to GeoParquet using DuckDB.
    
    DuckDB's spatial extension provides:
    - Efficient columnar storage
    - Automatic compression
    - Spatial functions for future queries
    - Schema enforcement
    """
    log_progress(f"Converting {geojson_path} → {parquet_path}", log_file)
    
    if not os.path.exists(geojson_path):
        raise FileNotFoundError(f"Input GeoJSON not found: {geojson_path}")
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
    
    # Connect to DuckDB (in-memory)
    conn = duckdb.connect(':memory:')
    
    try:
        # Install and load spatial extension
        conn.execute("INSTALL spatial;")
        conn.execute("LOAD spatial;")
        log_progress("DuckDB spatial extension loaded", log_file)
        
        # Read GeoJSON
        # st_read automatically handles geometry parsing
        log_progress(f"Reading GeoJSON: {geojson_path}", log_file)
        conn.execute(f"""
            CREATE TABLE geojson_data AS 
            SELECT * FROM st_read('{geojson_path}')
        """)
        
        # Get row count
        row_count = conn.execute("SELECT COUNT(*) FROM geojson_data").fetchone()[0]
        log_progress(f"Loaded {row_count} features", log_file)
        
        if row_count == 0:
            log_progress("WARNING: GeoJSON contains no features", log_file)
        
        # Get column info
        columns = conn.execute("PRAGMA table_info(geojson_data)").fetchall()
        column_names = [col[1] for col in columns]
        log_progress(f"Columns: {', '.join(column_names)}", log_file)
        
        # Write to Parquet with compression
        log_progress(f"Writing GeoParquet: {parquet_path}", log_file)
        conn.execute(f"""
            COPY geojson_data 
            TO '{parquet_path}' 
            (FORMAT PARQUET, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000)
        """)
        
        # Verify output
        if not os.path.exists(parquet_path):
            raise RuntimeError(f"Parquet file not created: {parquet_path}")
        
        file_size_mb = os.path.getsize(parquet_path) / (1024*1024)
        geojson_size_mb = os.path.getsize(geojson_path) / (1024*1024)
        compression_ratio = (1 - file_size_mb / geojson_size_mb) * 100 if geojson_size_mb > 0 else 0
        
        log_progress(
            f"✓ Conversion successful: {file_size_mb:.2f} MB "
            f"(compressed {compression_ratio:.1f}% from {geojson_size_mb:.2f} MB GeoJSON)",
            log_file
        )
        
        return True
        
    except Exception as e:
        log_progress(f"ERROR: {str(e)}", log_file)
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    # Support both script and Snakemake usage
    try:
        # Snakemake mode
        geojson_in = snakemake.input.geojson
        parquet_out = snakemake.output.parquet
        log_file = snakemake.log[0] if snakemake.log else None
    except NameError:
        # CLI mode
        if len(sys.argv) < 3:
            print("Usage: python geojson_to_parquet.py <input.geojson> <output.parquet> [logfile]")
            sys.exit(1)
        geojson_in = sys.argv[1]
        parquet_out = sys.argv[2]
        log_file = sys.argv[3] if len(sys.argv) > 3 else None
    
    try:
        convert_geojson_to_parquet(geojson_in, parquet_out, log_file)
        sys.exit(0)
    except Exception as e:
        print(f"FATAL ERROR: {str(e)}", file=sys.stderr)
        sys.exit(1)
