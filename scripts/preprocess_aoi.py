"""
Pre-process the AOI shapefile once per run.
Normalises CRS, assigns region_id, applies geometry simplification to stay
within the GEE payload budget, and writes to GeoParquet.

Each chunk worker loads this file instead of the raw shapefile, so the
coordinate-counting and simplification ladder runs exactly once per run.
"""
import os
from pathlib import Path
from datetime import datetime

import geopandas as gpd

try:
    LOG_FILE = snakemake.log[0] if snakemake.log else "preprocess_aoi.log"
except NameError:
    LOG_FILE = "preprocess_aoi.log"


def log_progress(message):
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True) if os.path.dirname(LOG_FILE) else None
    with open(LOG_FILE, "a") as f:
        f.write(f"[{datetime.now().isoformat()}] {message}\n")
        f.flush()


def _count_coords(geom):
    if geom is None or geom.is_empty:
        return 0
    if hasattr(geom, 'geoms'):
        return sum(_count_coords(g) for g in geom.geoms)
    if hasattr(geom, 'exterior'):
        return len(geom.exterior.coords) + sum(len(r.coords) for r in geom.interiors)
    if hasattr(geom, 'coords'):
        return len(geom.coords)
    return 0


_COORD_BUDGET = 200_000
_SIMPLIFY_LADDER = [0.001, 0.003, 0.01, 0.02, 0.05]


shp_path = snakemake.input.shp
out_path  = snakemake.output.aoi

log_progress(f"Loading AOI from {shp_path}")
input_path = Path(shp_path)
if input_path.suffix.lower() in {".parquet", ".geoparquet"}:
    gdf = gpd.read_parquet(input_path)
else:
    gdf = gpd.read_file(shp_path)
log_progress(f"Loaded {len(gdf)} features")

# Normalise CRS to EPSG:4326
if gdf.crs is None:
    gdf = gdf.set_crs("EPSG:4326")
else:
    gdf = gdf.to_crs("EPSG:4326")

# Assign and deduplicate region_id
if 'region_id' not in gdf.columns:
    id_candidates = ['ADMIN', 'NAME', 'ISO_A3', 'NAME_LONG', 'id', 'fid']
    region_col = next((c for c in id_candidates if c in gdf.columns), None)
    gdf['region_id'] = gdf[region_col].astype(str) if region_col else gdf.index.astype(str)

if gdf['region_id'].duplicated().any():
    counts = {}
    new_ids = []
    for rid in gdf['region_id']:
        if rid in counts:
            counts[rid] += 1
            new_ids.append(f"{rid}_{counts[rid]}")
        else:
            counts[rid] = 0
            new_ids.append(rid)
    gdf['region_id'] = new_ids

# Apply minimum simplification tolerance needed to stay within GEE payload budget
total = sum(_count_coords(g) for g in gdf.geometry)
log_progress(f"Geometry complexity: {total:,} total coordinates (budget: {_COORD_BUDGET:,})")

if total > _COORD_BUDGET:
    log_progress("Coord count exceeds budget — selecting minimum simplification tolerance")
    simplified = gdf
    for tol in _SIMPLIFY_LADDER:
        candidate = gdf.copy()
        candidate["geometry"] = candidate.geometry.simplify(tol, preserve_topology=True)
        candidate = candidate[~candidate.geometry.is_empty & candidate.geometry.notna()]
        reduced = sum(_count_coords(g) for g in candidate.geometry)
        log_progress(f"  tolerance={tol}: {reduced:,} coords")
        if reduced <= _COORD_BUDGET:
            log_progress(
                f"Selected tolerance={tol} ({total:,} → {reduced:,} coords, "
                f"{100 * (1 - reduced / total):.0f}% reduction)"
            )
            simplified = candidate
            break
    else:
        log_progress(f"WARNING: Still above budget after maximum tolerance={_SIMPLIFY_LADDER[-1]}")
        simplified = candidate
    gdf = simplified
else:
    log_progress("Geometry within budget — no simplification needed")

os.makedirs(os.path.dirname(out_path), exist_ok=True)
gdf.to_parquet(out_path)
log_progress(f"Written prepped AOI: {out_path} ({len(gdf)} features)")
