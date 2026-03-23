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
import threading
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

def _count_coords(geom):
    """Count all coordinates in a geometry, including holes and multi-part components."""
    if geom is None or geom.is_empty:
        return 0
    if hasattr(geom, 'geoms'):  # Multi* or GeometryCollection
        return sum(_count_coords(g) for g in geom.geoms)
    if hasattr(geom, 'exterior'):  # Polygon
        return len(geom.exterior.coords) + sum(len(r.coords) for r in geom.interiors)
    if hasattr(geom, 'coords'):  # LineString, Point
        return len(geom.coords)
    return 0


def build_regions(shp_path, simplify_tolerance=None):
    """
    Convert shapefile/parquet to Earth Engine FeatureCollection.

    Returns (ee_fc, gdf_slim, attr_lookup):
    - ee_fc:       GEE FeatureCollection with geometry + region_id only (minimal payload)
    - gdf_slim:    GeoDataFrame with geometry + region_id (for empty-feature fallback)
    - attr_lookup: dict mapping region_id -> extra attribute columns (no geometry)
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
        id_candidates = ['ADMIN', 'NAME', 'ISO_A3', 'NAME_LONG', 'id', 'fid']
        region_col = next((col for col in id_candidates if col in gdf.columns), None)
        if region_col:
            gdf['region_id'] = gdf[region_col].astype(str)
        else:
            gdf['region_id'] = gdf.index.astype(str)

    # Ensure region_id is unique — deduplicate by appending _1, _2, ... to repeated values
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

    # Simplify if needed
    if simplify_tolerance is not None:
        gdf["geometry"] = gdf.geometry.simplify(simplify_tolerance, preserve_topology=True)
        gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()]
    else:
        # Proactive complexity check — count all rings including holes and multi-parts
        total_coords = sum(_count_coords(geom) for geom in gdf.geometry)
        log_progress(f"Geometry complexity: {total_coords} total coordinates")

        if total_coords > 100000:
            log_progress("WARNING: High complexity, pre-simplifying geometry")
            gdf["geometry"] = gdf.geometry.simplify(0.001, preserve_topology=True)
            gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()]

    # Build attribute lookup (no geometry) before releasing the full GDF.
    # Downstream code rejoins these into the GeoJSON output after GEE extraction,
    # without ever sending them to GEE (which would inflate the request payload).
    geom_col = gdf.geometry.name
    extra_cols = [c for c in gdf.columns if c not in (geom_col, 'region_id')]
    attr_lookup = (
        gdf[['region_id'] + extra_cols].set_index('region_id').to_dict('index')
        if extra_cols else {}
    )

    # Slim GDF — geometry + region_id only — for the GEE payload and empty-feature fallback.
    gdf_slim = gdf[['region_id', geom_col]].copy()
    del gdf  # release full GDF; attributes are captured in attr_lookup

    with tempfile.TemporaryDirectory(prefix="gee_geom_") as tmpdir:
        geom_path = os.path.join(tmpdir, f"geometry_{uuid.uuid4().hex}.shp")
        gdf_slim.to_file(geom_path)
        return geemap.shp_to_ee(geom_path), gdf_slim, attr_lookup

def log_progress(message):
    """Write progress message to log file"""
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True) if os.path.dirname(LOG_FILE) else None
    with open(LOG_FILE, "a") as f:
        f.write(f"[{datetime.now().isoformat()}] {message}\n")
        f.flush()


def _blocking_getinfo(ee_obj, interval=30):
    """
    Call ee_obj.getInfo() while emitting a heartbeat log every `interval` seconds.
    Gives visible feedback when GEE server-side computation is slow (e.g. high-res
    frequencyHistogram) and no per-page progress is possible.
    """
    result_box = [None]
    exc_box    = [None]

    def _run():
        try:
            result_box[0] = ee_obj.getInfo()
        except Exception as e:
            exc_box[0] = e

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    elapsed = 0
    while t.is_alive():
        t.join(timeout=interval)
        if t.is_alive():
            elapsed += interval
            log_progress(f"Still computing on GEE server... ({elapsed}s elapsed)")

    if exc_box[0] is not None:
        raise exc_box[0]
    return result_box[0]

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


def build_histogram_stats(collection, regions, scale, band):
    """
    Compute per-class pixel counts for a categorical (LULC) band using frequencyHistogram.
    Returns a FeatureCollection where each feature has a '{band}' property containing
    a dict of {class_value: pixel_count, ...}.
    """
    image = collection.mosaic().select([band])
    return image.reduceRegions(
        collection=regions,
        reducer=ee.Reducer.frequencyHistogram(),
        scale=scale,
        crs='EPSG:4326'
    )


def export_to_geojson(image, regions, scale, out_geojson, max_retries=5, prop_rename=None,
                      precomputed_stats=None, categorical=False, attr_lookup=None):
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
            # Paginate getInfo() to handle collections with >5000 features.
            # stats.size().getInfo() forces GEE to fully evaluate the computation
            # graph on the server before returning. For high-resolution categorical
            # products (e.g. frequencyHistogram at 10m) this can take several minutes
            # with no per-page progress possible — _blocking_getinfo emits a heartbeat.
            PAGE_SIZE = 5000
            log_progress("Evaluating collection on GEE server — heartbeat every 30s until done...")
            total = _blocking_getinfo(stats.size())
            log_progress(f"Collection has {total} features, fetching in pages of {PAGE_SIZE}")
            features = []
            for offset in range(0, total, PAGE_SIZE):
                page = _blocking_getinfo(stats.toList(PAGE_SIZE, offset))
                features.extend(page)
                fetched = min(offset + PAGE_SIZE, total)
                pct = int(fetched / total * 100) if total else 100
                log_progress(f"Fetched {fetched}/{total} features ({pct}%)")

            # Rename reducer output properties to expected {band}_{stat} convention.
            # GEE reduceRegions names output properties after the reducer (e.g. 'mean'),
            # not the band name, so we rename here before writing.
            if prop_rename:
                for feature in features:
                    props = feature.get("properties", {})
                    for old_key, new_key in prop_rename.items():
                        if old_key in props:
                            props[new_key] = props.pop(old_key)

            # For categorical products, serialize histogram dicts to JSON strings
            # so downstream parquet storage remains flat/tabular.
            if categorical:
                for feature in features:
                    props = feature.get("properties", {})
                    for key, val in list(props.items()):
                        if isinstance(val, dict):
                            props[key] = json.dumps(val)

            # Rejoin original input attributes (not sent to GEE) using region_id.
            if attr_lookup:
                for feature in features:
                    rid = feature.get("properties", {}).get("region_id")
                    if rid is not None and rid in attr_lookup:
                        props = feature["properties"]
                        for k, v in attr_lookup[rid].items():
                            if k not in props:
                                props[k] = v

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
    col_id      = snakemake.params.ee_collection
    scale       = snakemake.params.scale
    stats_list  = snakemake.params.stats
    start       = snakemake.params.start_date
    end         = snakemake.params.end_date
    cadence     = snakemake.params.cadence
    categorical = snakemake.params.categorical
    band        = snakemake.wildcards.band
    shp        = snakemake.input.shp
    out        = snakemake.output.geojson

    log_progress(f"Parameters: collection={col_id}, band={band}, stats={stats_list}, cadence={cadence}, dates={start} to {end}")

    os.makedirs(os.path.dirname(out), exist_ok=True)

    log_progress("Building regions from shapefile")
    regions, gdf_original, attr_lookup = build_regions(shp)
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
            if categorical:
                props[f"{band}_histogram"] = None
            else:
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
        if categorical:
            stats_fc = build_histogram_stats(collection, regions_fc, scale, band)
            # GEE names the histogram output property after the band; rename to {band}_histogram
            hist_rename = {band: f"{band}_histogram"}
            return export_to_geojson(
                image=None, regions=regions_fc, scale=scale, out_geojson=out,
                max_retries=max_retries, prop_rename=hist_rename,
                precomputed_stats=stats_fc, categorical=True, attr_lookup=attr_lookup
            )
        elif cadence in ("daily", "composite"):
            compound = build_compound_reducer(stats_list)
            stats_fc = build_daily_stats(collection, regions_fc, scale, compound)
            return export_to_geojson(
                image=None, regions=regions_fc, scale=scale, out_geojson=out,
                max_retries=max_retries, prop_rename=prop_rename,
                precomputed_stats=stats_fc, attr_lookup=attr_lookup
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
                max_retries=max_retries, prop_rename=prop_rename, attr_lookup=attr_lookup
            )

    log_progress(f"Extracting {len(stats_list)} stat(s): {stats_list}")
    success = _do_export(regions, max_retries=3)

    if not success:
        for tolerance in [0.001, 0.005, 0.01, 0.02]:
            log_progress(f"Retrying with simplified geometry (tolerance={tolerance})")
            regions_simplified, _, _ = build_regions(shp, simplify_tolerance=tolerance)
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
