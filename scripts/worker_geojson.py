"""
Worker script for GEE extraction using GeoJSON format.
Exports zonal statistics as GeoJSON preserving geometry.
"""
import os
import json
import re
import uuid
import tempfile
import ee
import geemap
import geopandas as gpd
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import sys
import traceback

# Prefer Snakemake job log if configured
try:
    LOG_FILE = snakemake.log[0] if snakemake.log else "worker_debug.log"
except NameError:
    LOG_FILE = "worker_debug.log"

def initialize_earth_engine():
    """Initialize Earth Engine with service account or default credentials"""
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    service_account = os.getenv("EE_SERVICE_ACCOUNT")

    if credentials_path and os.path.exists(credentials_path):
        if not service_account:
            try:
                with open(credentials_path, "r", encoding="utf-8") as fp:
                    key_data = json.load(fp)
                service_account = key_data.get("client_email")
            except Exception:
                service_account = None

        if service_account:
            credentials = ee.ServiceAccountCredentials(service_account, credentials_path)
            ee.Initialize(credentials)
            return

    ee.Initialize()

def build_regions(shp_path, simplify_tolerance=None):
    """
    Convert shapefile to Earth Engine FeatureCollection.
    Pre-emptively simplifies geometry if needed to avoid payload limit errors.
    """
    input_path = Path(shp_path)
    if input_path.suffix.lower() in {".parquet", ".geoparquet"}:
        gdf = gpd.read_parquet(input_path)
    else:
        gdf = gpd.read_file(shp_path)
    
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    else:
        gdf = gdf.to_crs("EPSG:4326")
    
    # Add region_id if not present
    if 'region_id' not in gdf.columns:
        # Try to find a suitable ID column
        id_candidates = ['ADMIN', 'NAME', 'ISO_A3', 'NAME_LONG', 'id', 'fid']
        region_col = next((col for col in id_candidates if col in gdf.columns), None)
        if region_col:
            gdf['region_id'] = gdf[region_col].astype(str)
        else:
            gdf['region_id'] = gdf.index.astype(str)
    
    # Simplify if needed
    if simplify_tolerance is not None:
        gdf["geometry"] = gdf.geometry.simplify(simplify_tolerance, preserve_topology=True)
        gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()]
    else:
        # Proactive complexity check
        total_coords = sum(
            len(list(geom.exterior.coords)) if hasattr(geom, 'exterior') else 0 
            for geom in gdf.geometry
        )
        log_progress(f"Geometry complexity: {total_coords} total coordinates")
        
        if total_coords > 100000:
            log_progress(f"WARNING: High complexity, pre-simplifying geometry")
            gdf["geometry"] = gdf.geometry.simplify(0.001, preserve_topology=True)
            gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()]
    
    with tempfile.TemporaryDirectory(prefix="gee_geom_") as tmpdir:
        geom_path = os.path.join(tmpdir, f"geometry_{uuid.uuid4().hex}.shp")
        gdf.to_file(geom_path)
        return geemap.shp_to_ee(geom_path), gdf

def log_progress(message):
    """Write progress message to log file"""
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True) if os.path.dirname(LOG_FILE) else None
    with open(LOG_FILE, "a") as f:
        f.write(f"[{datetime.now().isoformat()}] {message}\n")
        f.flush()

def export_to_geojson(image, regions, scale, out_geojson, max_retries=5):
    """
    Export zonal statistics as GeoJSON with geometry.
    Uses reduceRegions for proper zonal stats computation.
    """
    log_progress(f"Exporting to GeoJSON: {out_geojson}")
    
    # Compute zonal statistics using reduceRegions
    stats = image.reduceRegions(
        collection=regions,
        reducer=ee.Reducer.mean(),  # Already reduced, so use mean
        scale=scale,
        crs='EPSG:4326'
    )
    
    # Export to GeoJSON with retries
    for attempt in range(max_retries):
        try:
            # Convert to GeoJSON dict
            geojson_dict = stats.getInfo()
            
            # Write to file
            os.makedirs(os.path.dirname(out_geojson), exist_ok=True)
            with open(out_geojson, 'w') as f:
                json.dump(geojson_dict, f)
            
            log_progress(f"✓ GeoJSON export successful: {len(geojson_dict.get('features', []))} features")
            return True
            
        except Exception as e:
            error_msg = str(e)
            if "Request payload size exceeds" in error_msg or "Computation timed out" in error_msg:
                if attempt < max_retries - 1:
                    log_progress(f"✗ Export failed (attempt {attempt+1}/{max_retries}): {error_msg}")
                    # On retry, we'll need to simplify geometry
                    return False
                else:
                    raise RuntimeError(
                        f"Failed to export after {max_retries} attempts. "
                        f"Geometry may be too complex. Error: {error_msg}"
                    )
            else:
                raise

try:
    log_progress("Starting GeoJSON worker")
    initialize_earth_engine()
    log_progress("Earth Engine initialized")

    # Access snakemake parameters
    col_id = snakemake.params.ee_collection
    scale  = snakemake.params.scale
    stats_list = snakemake.params.stats  # List of statistics to compute
    stat   = stats_list[0]  # Use first stat for now (can be extended)
    start  = snakemake.params.start_date
    end    = snakemake.params.end_date
    band   = snakemake.wildcards.band
    shp    = snakemake.input.shp
    out    = snakemake.output.geojson
    
    log_progress(f"Parameters: collection={col_id}, band={band}, stat={stat}, dates={start} to {end}")

    # Ensure output directory exists
    os.makedirs(os.path.dirname(out), exist_ok=True)

    # Load regions and get original GeoDataFrame
    log_progress("Building regions from shapefile")
    regions, gdf_original = build_regions(shp)
    log_progress(f"Regions built: {len(gdf_original)} features")
    
    # Filter collection (add 1 day to end for exclusive range)
    end_dt = (datetime.strptime(end, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    
    log_progress("Filtering image collection")
    collection = ee.ImageCollection(col_id).filterDate(start, end_dt).select([band])
    
    collection_count = collection.size().getInfo()
    log_progress(f"Collection has {collection_count} images")
    
    if collection_count == 0:
        log_progress(
            f"WARNING: No images found for {col_id}/{band} between {start} and {end}. "
            "Writing empty GeoJSON to unblock pipeline."
        )
        empty_features = []
        for idx, row in gdf_original.iterrows():
            feature = {
                "type": "Feature",
                "geometry": json.loads(gpd.GeoSeries([row.geometry]).to_json())['features'][0]['geometry'],
                "properties": {
                    "region_id": row.get('region_id', str(idx)),
                    "Date": start,
                    f"{band}_{stat}": None
                }
            }
            empty_features.append(feature)
        
        empty_geojson = {
            "type": "FeatureCollection",
            "features": empty_features
        }
        with open(out, 'w') as f:
            json.dump(empty_geojson, f)
        log_progress(f"Wrote empty GeoJSON to {out}")
        sys.exit(0)
    
    # Reduce collection based on statistic
    log_progress(f"Reducing collection using {stat.upper()}")
    if stat.upper() == "SUM":
        image = collection.reduce(ee.Reducer.sum())
    elif stat.upper() == "MEAN":
        image = collection.reduce(ee.Reducer.mean())
    elif stat.upper() == "MAX":
        image = collection.reduce(ee.Reducer.max())
    elif stat.upper() == "MIN":
        image = collection.reduce(ee.Reducer.min())
    elif stat.upper() == "MEDIAN":
        image = collection.reduce(ee.Reducer.median())
    else:
        image = collection.reduce(ee.Reducer.mean())
    
    # Rename band to include statistic
    band_names = image.bandNames().getInfo()
    if band_names:
        image = image.rename([f"{band}_{stat}"])
    
    # Export to GeoJSON
    log_progress("Starting zonal statistics export to GeoJSON")
    success = export_to_geojson(image, regions, scale, out, max_retries=3)
    
    # If export failed due to complexity, retry with simplified geometry
    if not success:
        simplify_tolerances = [0.001, 0.005, 0.01, 0.02]
        for tolerance in simplify_tolerances:
            log_progress(f"Retrying with simplified geometry (tolerance={tolerance})")
            regions_simplified, _ = build_regions(shp, simplify_tolerance=tolerance)
            success = export_to_geojson(image, regions_simplified, scale, out, max_retries=1)
            if success:
                break
        
        if not success:
            raise RuntimeError(
                "Failed to export GeoJSON even with geometry simplification. "
                "The AOI may be too complex. Consider uploading a simpler shapefile."
            )
    
    # Verify output file was created
    if not os.path.exists(out):
        raise RuntimeError(f"GeoJSON export completed but file not found: {out}")
    
    file_size = os.path.getsize(out) / (1024*1024)  # MB
    log_progress(f"SUCCESS: GeoJSON written to {out} ({file_size:.2f} MB)")

except Exception as e:
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True) if os.path.dirname(LOG_FILE) else None
    with open(LOG_FILE, "a") as f:
        f.write(f"ERROR: {str(e)}\n")
        f.write(traceback.format_exc())
        f.write("\n")
    raise e
