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

def build_reducer(stat_name):
    """Return the EE reducer for a given stat name."""
    return {
        "SUM":    ee.Reducer.sum(),
        "MEAN":   ee.Reducer.mean(),
        "MAX":    ee.Reducer.max(),
        "MIN":    ee.Reducer.min(),
        "MEDIAN": ee.Reducer.median(),
    }.get(stat_name.upper(), ee.Reducer.mean())


def build_compound_reducer(stats_list):
    """
    Build a compound reducer for all configured stats.
    For a single stat returns that reducer directly.
    For multiple stats combines them with sharedInputs=True so all receive
    the same input band and each outputs a separate '{band}_{stat}' property.
    """
    if len(stats_list) == 1:
        return build_reducer(stats_list[0])
    base = build_reducer(stats_list[0])
    for s in stats_list[1:]:
        base = base.combine(build_reducer(s), sharedInputs=True)
    return base


def build_daily_stats(collection, regions, scale, spatial_reducer):
    """Map reduceRegions over each image in the collection, tagging each feature with its Date."""
    def reduce_image(img):
        date_str = img.date().format("YYYY-MM-dd")
        return img.reduceRegions(
            collection=regions,
            reducer=spatial_reducer,
            scale=scale,
            crs='EPSG:4326'
        ).map(lambda f: f.set("Date", date_str))
    return collection.map(reduce_image).flatten()


def export_to_geojson(image, regions, scale, out_geojson, max_retries=5, prop_rename=None,
                      precomputed_stats=None):
    """
    Export zonal statistics as GeoJSON with geometry.
    Uses reduceRegions for proper zonal stats computation.
    Pass precomputed_stats to skip the internal reduceRegions call (e.g. for daily per-image mode).
    """
    log_progress(f"Exporting to GeoJSON: {out_geojson}")

    if precomputed_stats is not None:
        stats = precomputed_stats
    else:
        # Compute zonal statistics using reduceRegions
        stats = image.reduceRegions(
            collection=regions,
            reducer=ee.Reducer.mean(),  # Already temporally reduced, so use mean spatially
            scale=scale,
            crs='EPSG:4326'
        )
    
    # Export to GeoJSON with retries
    for attempt in range(max_retries):
        try:
            # Paginate getInfo() to handle collections with >5000 features
            PAGE_SIZE = 5000
            total = stats.size().getInfo()
            log_progress(f"Collection has {total} features, fetching in pages of {PAGE_SIZE}")
            features = []
            for offset in range(0, total, PAGE_SIZE):
                page = stats.toList(PAGE_SIZE, offset).getInfo()
                features.extend(page)

            # Rename reducer output properties to expected {band}_{stat} convention.
            # GEE reduceRegions names output properties after the reducer (e.g. 'mean'),
            # not the band name, so we rename here before writing.
            if prop_rename:
                for feature in features:
                    props = feature.get("properties", {})
                    for old_key, new_key in prop_rename.items():
                        if old_key in props:
                            props[new_key] = props.pop(old_key)

            geojson_dict = {"type": "FeatureCollection", "features": features}

            # Write to file
            os.makedirs(os.path.dirname(out_geojson), exist_ok=True)
            with open(out_geojson, 'w') as f:
                json.dump(geojson_dict, f)

            log_progress(f"✓ GeoJSON export successful: {len(features)} features")
            return True

        except Exception as e:
            error_msg = str(e)
            if (
                "Request payload size exceeds" in error_msg
                or "Computation timed out" in error_msg
                or "Collection query aborted" in error_msg
            ):
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
    col_id     = snakemake.params.ee_collection
    scale      = snakemake.params.scale
    stats_list = snakemake.params.stats
    start      = snakemake.params.start_date
    end        = snakemake.params.end_date
    cadence    = snakemake.params.cadence
    band       = snakemake.wildcards.band
    shp        = snakemake.input.shp
    out        = snakemake.output.geojson

    log_progress(f"Parameters: collection={col_id}, band={band}, stats={stats_list}, cadence={cadence}, dates={start} to {end}")

    os.makedirs(os.path.dirname(out), exist_ok=True)

    log_progress("Building regions from shapefile")
    regions, gdf_original = build_regions(shp)
    log_progress(f"Regions built: {len(gdf_original)} features")

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
            props = {"region_id": row.get('region_id', str(idx)), "Date": start}
            for s in stats_list:
                props[f"{band}_{s.lower()}"] = None
            empty_features.append({
                "type": "Feature",
                "geometry": json.loads(gpd.GeoSeries([row.geometry]).to_json())['features'][0]['geometry'],
                "properties": props
            })
        with open(out, 'w') as f:
            json.dump({"type": "FeatureCollection", "features": empty_features}, f)
        log_progress(f"Wrote empty GeoJSON to {out}")
        sys.exit(0)

    # GEE property naming:
    # - Single stat + single-output reducer → property named after reducer (e.g. 'mean'), not band.
    #   Use prop_rename to correct this.
    # - Multiple stats via compound reducer → GEE outputs '{band}_{stat}' correctly.
    #   No rename needed.
    if len(stats_list) == 1:
        s = stats_list[0]
        prop_rename = {s.lower(): f"{band}_{s.lower()}"}
    else:
        prop_rename = {}

    def _do_export(regions_fc, max_retries):
        if cadence in ("daily", "composite"):
            compound = build_compound_reducer(stats_list)
            stats_fc = build_daily_stats(collection, regions_fc, scale, compound)
            return export_to_geojson(
                image=None, regions=regions_fc, scale=scale, out_geojson=out,
                max_retries=max_retries, prop_rename=prop_rename, precomputed_stats=stats_fc
            )
        else:
            # Build one temporally-reduced image per stat, rename each band, combine.
            # Spatial mean over pixels within each region is applied inside export_to_geojson.
            stat_images = []
            for s in stats_list:
                img = collection.reduce(build_reducer(s))
                if img.bandNames().getInfo():
                    img = img.rename([f"{band}_{s.lower()}"])
                stat_images.append(img)
            combined = stat_images[0]
            for img in stat_images[1:]:
                combined = combined.addBands(img)
            return export_to_geojson(
                combined, regions_fc, scale, out,
                max_retries=max_retries, prop_rename=prop_rename
            )

    log_progress(f"Extracting {len(stats_list)} stat(s): {stats_list}")
    success = _do_export(regions, max_retries=3)

    if not success:
        for tolerance in [0.001, 0.005, 0.01, 0.02]:
            log_progress(f"Retrying with simplified geometry (tolerance={tolerance})")
            regions_simplified, _ = build_regions(shp, simplify_tolerance=tolerance)
            success = _do_export(regions_simplified, max_retries=1)
            if success:
                break

    if not success:
        raise RuntimeError(
            "Failed to export GeoJSON even with geometry simplification. "
            "The AOI may be too complex. Consider uploading a simpler shapefile."
        )

    if not os.path.exists(out):
        raise RuntimeError(f"GeoJSON export completed but file not found: {out}")

    file_size = os.path.getsize(out) / (1024*1024)
    log_progress(f"SUCCESS: GeoJSON written to {out} ({file_size:.2f} MB)")

except Exception as e:
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True) if os.path.dirname(LOG_FILE) else None
    with open(LOG_FILE, "a") as f:
        f.write(f"ERROR: {str(e)}\n")
        f.write(traceback.format_exc())
        f.write("\n")
    raise e
