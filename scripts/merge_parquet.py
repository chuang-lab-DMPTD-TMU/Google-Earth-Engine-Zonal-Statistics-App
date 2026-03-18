"""
Merge multiple GeoParquet chunks into a single file.
Uses DuckDB for efficient joining with spatial data preservation.
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

def merge_parquet_chunks(chunk_files, output_path, merge_strategy="wide", log_file=None):
    """
    Merge multiple GeoParquet chunks into a single file.
    
    Args:
        chunk_files: List of parquet file paths to merge
        output_path: Output merged parquet file
        merge_strategy: 
            - "wide": Merge all bands as columns (one row per region/date)
            - "long": Stack all bands vertically (normalized form)
        log_file: Optional log file path
    """
    log_progress(f"Merging {len(chunk_files)} parquet chunks → {output_path}", log_file)
    
    if not chunk_files:
        raise ValueError("No chunk files provided for merging")
    
    # Verify all input files exist
    missing = [f for f in chunk_files if not os.path.exists(f)]
    if missing:
        raise FileNotFoundError(f"Missing chunk files: {missing}")
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    conn = duckdb.connect(':memory:')
    
    try:
        # Install spatial extension
        conn.execute("INSTALL spatial;")
        conn.execute("LOAD spatial;")
        log_progress("DuckDB spatial extension loaded", log_file)
        
        # Read all chunks into a single table
        log_progress(f"Reading {len(chunk_files)} chunk files...", log_file)
        
        # Read first chunk to get schema
        conn.execute(f"CREATE TABLE chunk_0 AS SELECT * FROM read_parquet('{chunk_files[0]}')")
        first_cols = [col[0] for col in conn.execute("PRAGMA table_info(chunk_0)").fetchall()]
        log_progress(f"First chunk columns: {', '.join(first_cols)}", log_file)
        
        # Load remaining chunks
        for idx, chunk_file in enumerate(chunk_files[1:], start=1):
            log_progress(f"  Reading chunk {idx+1}/{len(chunk_files)}: {Path(chunk_file).name}", log_file)
            conn.execute(f"CREATE TABLE chunk_{idx} AS SELECT * FROM read_parquet('{chunk_file}')")
        
        # Determine merge strategy based on schema
        if merge_strategy == "wide":
            # Wide merge: join on region_id + Date to combine bands as columns
            log_progress("Performing WIDE merge (bands as columns)", log_file)
            
            # Build join query dynamically
            base_table = "chunk_0"
            query_parts = [f"SELECT * FROM {base_table}"]
            
            for idx in range(1, len(chunk_files)):
                # Get columns from this chunk
                chunk_cols = [
                    col[0] for col in 
                    conn.execute(f"PRAGMA table_info(chunk_{idx})").fetchall()
                ]
                
                # Identify join keys (common across all chunks)
                join_keys = ['region_id', 'Date', 'geom']
                available_join_keys = [k for k in join_keys if k in chunk_cols and k in first_cols]
                
                if not available_join_keys:
                    log_progress(
                        f"WARNING: No common join keys found between chunks. "
                        f"Falling back to row-wise concatenation.",
                        log_file
                    )
                    # Fallback to UNION ALL (stacking)
                    query_parts = [f"SELECT * FROM chunk_{i}" for i in range(len(chunk_files))]
                    merged_query = " UNION ALL ".join(query_parts)
                    break
                
                # Select only new columns (exclude join keys and already present cols)
                existing_cols = set(first_cols)
                new_cols = [c for c in chunk_cols if c not in existing_cols and c not in join_keys]
                
                if not new_cols:
                    log_progress(f"  No new columns in chunk_{idx}, skipping", log_file)
                    continue
                
                # Build join clause
                join_on = " AND ".join([f"{base_table}.{k} = chunk_{idx}.{k}" for k in available_join_keys])
                select_new = ", ".join([f"chunk_{idx}.{c}" for c in new_cols])
                
                query_parts.append(
                    f"LEFT JOIN chunk_{idx} ON {join_on}"
                )
                
                # Update first_cols to include new columns
                first_cols.extend(new_cols)
            
            # Construct final query
            if len(query_parts) > 1 and "UNION ALL" not in query_parts[0]:
                merged_query = f"SELECT * FROM {query_parts[0]} " + " ".join(query_parts[1:])
            else:
                merged_query = " UNION ALL ".join([f"SELECT * FROM chunk_{i}" for i in range(len(chunk_files))])
        
        else:  # "long" strategy
            # Long merge: stack all rows vertically
            log_progress("Performing LONG merge (stacking rows)", log_file)
            merged_query = " UNION ALL ".join([f"SELECT * FROM chunk_{i}" for i in range(len(chunk_files))])
        
        # Execute merge
        log_progress("Executing merge query...", log_file)
        conn.execute(f"CREATE TABLE merged AS {merged_query}")
        
        # Get result stats
        row_count = conn.execute("SELECT COUNT(*) FROM merged").fetchone()[0]
        log_progress(f"Merged table has {row_count} rows", log_file)
        
        # Sort by Date and region_id if available
        sort_cols = []
        merged_cols = [col[0] for col in conn.execute("PRAGMA table_info(merged)").fetchall()]
        if 'Date' in merged_cols:
            sort_cols.append('Date')
        if 'region_id' in merged_cols:
            sort_cols.append('region_id')
        
        if sort_cols:
            sort_clause = ", ".join(sort_cols)
            log_progress(f"Sorting by: {sort_clause}", log_file)
            conn.execute(f"CREATE TABLE sorted AS SELECT * FROM merged ORDER BY {sort_clause}")
            table_to_export = "sorted"
        else:
            table_to_export = "merged"
        
        # Write to Parquet
        log_progress(f"Writing final GeoParquet: {output_path}", log_file)
        conn.execute(f"""
            COPY {table_to_export} 
            TO '{output_path}' 
            (FORMAT PARQUET, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000)
        """)
        
        # Verify and report
        if not os.path.exists(output_path):
            raise RuntimeError(f"Merged parquet file not created: {output_path}")
        
        file_size_mb = os.path.getsize(output_path) / (1024*1024)
        log_progress(f"✓ Merge successful: {output_path} ({file_size_mb:.2f} MB, {row_count} rows)", log_file)
        
        return True
        
    except Exception as e:
        log_progress(f"ERROR during merge: {str(e)}", log_file)
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    # Support both script and Snakemake usage
    try:
        # Snakemake mode
        chunk_files = snakemake.input.chunks
        output_path = snakemake.output.merged
        log_file = snakemake.log[0] if snakemake.log else None
        merge_strategy = snakemake.params.get("merge_strategy", "wide")
    except NameError:
        # CLI mode
        if len(sys.argv) < 3:
            print("Usage: python merge_parquet.py <output.parquet> <chunk1.parquet> <chunk2.parquet> ...")
            sys.exit(1)
        output_path = sys.argv[1]
        chunk_files = sys.argv[2:]
        log_file = None
        merge_strategy = "wide"
    
    try:
        merge_parquet_chunks(chunk_files, output_path, merge_strategy, log_file)
        sys.exit(0)
    except Exception as e:
        print(f"FATAL ERROR: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
