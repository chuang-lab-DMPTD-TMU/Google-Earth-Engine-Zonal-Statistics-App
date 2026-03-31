"""
Worker script for GEE extraction using GeoJSON format.
Exports zonal statistics as GeoJSON preserving geometry.
"""
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)



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
from datetime import datetime, timedelta, timezone
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


# Conservative coordinate budget before GEE's request payload limit (~10 MB of serialized geometry).
# ~300 k coord pairs × 30 bytes/pair ≈ 9 MB.
_COORD_BUDGET = 200_000
# Ordered tolerances tried when the raw AOI exceeds the budget.
_SIMPLIFY_LADDER = [0.001, 0.003, 0.01, 0.02, 0.05]


def _load_gdf(shp_path):
    """Read file, normalize CRS to EPSG:4326, assign and deduplicate region_id."""
    input_path = Path(shp_path)
    if input_path.suffix.lower() in {".parquet", ".geoparquet"}:
        gdf = gpd.read_parquet(input_path)
    else:
        gdf = gpd.read_file(shp_path)

    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    else:
        gdf = gdf.to_crs("EPSG:4326")

    if 'region_id' not in gdf.columns:
        id_candidates = ['ADMIN', 'NAME', 'ISO_A3', 'NAME_LONG', 'id', 'fid']
        region_col = next((col for col in id_candidates if col in gdf.columns), None)
        if region_col:
            gdf['region_id'] = gdf[region_col].astype(str)
        else:
            gdf['region_id'] = gdf.index.astype(str)

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

    return gdf


def _split_attrs(gdf):
    """
    Split a GeoDataFrame into (gdf_slim, attr_lookup).
    gdf_slim: geometry + region_id only (minimal GEE payload).
    attr_lookup: dict mapping region_id -> extra attribute columns (rejoined after extraction).
    """
    geom_col = gdf.geometry.name
    extra_cols = [c for c in gdf.columns if c not in (geom_col, 'region_id')]
    attr_lookup = (
        gdf[['region_id'] + extra_cols].set_index('region_id').to_dict('index')
        if extra_cols else {}
    )
    return gdf[['region_id', geom_col]].copy(), attr_lookup


def _simplify_to_budget(gdf_slim, budget=_COORD_BUDGET):
    """
    Measure AOI coordinate count and apply the minimum simplification tolerance
    needed to stay within the GEE payload budget — without any GEE round trips.

    Returns (gdf_ready, tolerance_used):
    - gdf_ready:       GeoDataFrame ready to send to GEE
    - tolerance_used:  the Shapely tolerance applied, or None if no simplification was needed
    """
    total = sum(_count_coords(g) for g in gdf_slim.geometry)
    log_progress(f"Geometry complexity: {total:,} total coordinates (budget: {budget:,})")

    if total <= budget:
        return gdf_slim, None

    log_progress(f"Coord count exceeds budget — selecting minimum simplification tolerance")
    for tol in _SIMPLIFY_LADDER:
        candidate = gdf_slim.copy()
        candidate["geometry"] = candidate.geometry.simplify(tol, preserve_topology=True)
        candidate = candidate[~candidate.geometry.is_empty & candidate.geometry.notna()]
        reduced = sum(_count_coords(g) for g in candidate.geometry)
        log_progress(f"  tolerance={tol}: {reduced:,} coords")
        if reduced <= budget:
            log_progress(f"Selected tolerance={tol} ({total:,} → {reduced:,} coords, "
                         f"{100 * (1 - reduced / total):.0f}% reduction)")
            return candidate, tol

    log_progress(f"WARNING: Still above budget after maximum tolerance={_SIMPLIFY_LADDER[-1]}")
    return candidate, _SIMPLIFY_LADDER[-1]


def _gdf_to_ee(gdf_slim):
    """Convert a slim GeoDataFrame (geometry + region_id) to an EE FeatureCollection."""
    with tempfile.TemporaryDirectory(prefix="gee_geom_") as tmpdir:
        geom_path = os.path.join(tmpdir, f"geometry_{uuid.uuid4().hex}.shp")
        gdf_slim.to_file(geom_path)
        return geemap.shp_to_ee(geom_path)

def log_progress(message):
    """Write progress message to log file"""
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True) if os.path.dirname(LOG_FILE) else None
    with open(LOG_FILE, "a") as f:
        f.write(f"[{datetime.now().isoformat()}] {message}\n")
        f.flush()


GEE_TIMEOUT            = 1800  # seconds before a stalled getInfo() is treated as a hung task
GEE_TIMEOUT_MAX_RETRIES = 3    # after this many timeouts the chunk is shelved (empty GeoJSON written)


def _retry_count_path(out_path):
    return out_path + ".retries"


def _get_retry_count(out_path):
    try:
        return int(Path(_retry_count_path(out_path)).read_text().strip())
    except Exception:
        return 0


def _increment_retry_count(out_path):
    count = _get_retry_count(out_path) + 1
    Path(_retry_count_path(out_path)).write_text(str(count))
    return count


def _write_shelved_event(prod: str, band: str, chunk: str, count: int):
    """Write a job_shelved event to run_events so the UI can surface it."""
    run_id  = os.getenv("GEE_RUN_ID")
    db_path = os.getenv("GEE_DB_PATH")
    if not run_id or not db_path:
        return
    try:
        import duckdb
        payload = json.dumps({"prod": prod, "band": band, "chunk": chunk})
        msg = f"Shelved {prod}/{band} [{chunk}] — timed out {count} times, written as empty chunk"
        now = datetime.now(timezone.utc).isoformat()
        with duckdb.connect(db_path) as conn:
            conn.execute(
                """INSERT INTO run_events
                       (event_time, run_id, event_type, status, message, payload_json)
                   VALUES (?, ?, 'job_shelved', 'job_shelved', ?, ?)""",
                [now, run_id, msg, payload],
            )
    except Exception:
        pass


def _blocking_getinfo(ee_obj, interval=30, label=None):
    """
    Call ee_obj.getInfo() while emitting a heartbeat log every `interval` seconds.
    Raises TimeoutError after GEE_TIMEOUT seconds so Snakemake can reschedule.
    `label` is included in heartbeat messages to clarify which operation is running.
    """
    result_box = [None]
    exc_box    = [None]

    def _run():
        try:
            result_box[0] = ee_obj.getInfo()
        except Exception as e:
            exc_box[0] = e

    prefix = f"[{label}] " if label else ""
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    elapsed = 0
    while t.is_alive():
        t.join(timeout=interval)
        if t.is_alive():
            elapsed += interval
            log_progress(f"{prefix}Still computing on GEE server... ({elapsed}s elapsed)")
            if elapsed >= GEE_TIMEOUT:
                log_progress(
                    f"{prefix}GEE timeout after {elapsed}s — killing job so Snakemake can reschedule"
                )
                raise TimeoutError(
                    f"GEE getInfo() did not return after {elapsed}s"
                )

    if exc_box[0] is not None:
        raise exc_box[0]
    return result_box[0]

# Landsat Collection 2 Level-2 surface reflectance scale factors.
# Applied before computing NDBI so the additive offset doesn't cancel incorrectly.
_LS_SCALE  = 0.0000275
_LS_OFFSET = -0.2


def _build_multi_ndbi_collection(multi_collections, start, end_dt):
    """
    Merge multiple Landsat sensors into a single 'NDBI' ImageCollection covering
    [start, end_dt) (both strings, end_dt exclusive as expected by GEE filterDate).

    Each sensor dict must contain:
        id         – GEE ImageCollection asset ID
        date_start – first usable date for this sensor (inclusive, YYYY-MM-DD)
        date_end   – last usable date for this sensor  (inclusive, YYYY-MM-DD)
        swir_band  – SWIR band name in the raw collection
        nir_band   – NIR  band name in the raw collection

    NDBI = (SWIR_ref − NIR_ref) / (SWIR_ref + NIR_ref)
    where *_ref = DN × 0.0000275 + (−0.2)  (Landsat C02 L2 scale factors).
    """
    merged = None
    for sensor in multi_collections:
        # Convert inclusive sensor end-date to exclusive for GEE filterDate.
        sensor_end_excl = (
            datetime.strptime(sensor["date_end"], "%Y-%m-%d") + timedelta(days=1)
        ).strftime("%Y-%m-%d")

        # Intersect the sensor's operational window with this chunk's window.
        seg_start = max(start, sensor["date_start"])
        seg_end   = min(end_dt, sensor_end_excl)
        if seg_start >= seg_end:
            continue  # Sensor has no imagery in this time chunk.

        swir_name = sensor["swir_band"]
        nir_name  = sensor["nir_band"]

        def _to_ndbi(img, swir=swir_name, nir=nir_name):
            swir_ref = img.select(swir).multiply(_LS_SCALE).add(_LS_OFFSET)
            nir_ref  = img.select(nir).multiply(_LS_SCALE).add(_LS_OFFSET)
            ndbi = (
                swir_ref.subtract(nir_ref)
                .divide(swir_ref.add(nir_ref))
                .rename("NDBI")
            )
            return ndbi.copyProperties(img, ["system:time_start", "system:index"])

        seg_col = (
            ee.ImageCollection(sensor["id"])
            .filterDate(seg_start, seg_end)
            .map(_to_ndbi)
        )
        merged = seg_col if merged is None else merged.merge(seg_col)

    return merged


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
                      precomputed_stats=None, categorical=False, attr_lookup=None, extra_props=None):
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
            total = _blocking_getinfo(stats.size(), label="size()")
            log_progress(f"Collection has {total} features, fetching in pages of {PAGE_SIZE}")
            num_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE if total else 1
            features = []
            for page_idx, offset in enumerate(range(0, total, PAGE_SIZE), start=1):
                page_label = f"page {page_idx}/{num_pages}"
                page = _blocking_getinfo(stats.toList(PAGE_SIZE, offset), label=page_label)
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

            if extra_props:
                for feature in features:
                    feature["properties"].update(extra_props)

            geojson_dict = {"type": "FeatureCollection", "features": features}

            # Write to file
            os.makedirs(os.path.dirname(out_geojson), exist_ok=True)
            with open(out_geojson, 'w') as f:
                json.dump(geojson_dict, f)

            log_progress(f"✓ GeoJSON export successful: {len(features)} features")
            return True

        except Exception as e:
            error_msg = str(e)
            is_rate_limit = (
                "Too many concurrent aggregations" in error_msg
                or "429" in error_msg
            )
            is_retryable = (
                is_rate_limit
                or "Request payload size exceeds" in error_msg
                or "Computation timed out" in error_msg
                or "Collection query aborted" in error_msg
            )
            if is_retryable:
                log_progress(f"✗ Export failed (attempt {attempt+1}/{max_retries}): {error_msg}")
                if attempt < max_retries - 1:
                    if is_rate_limit:
                        # Exponential backoff: 60s, 120s, 240s, … so we don't immediately
                        # hammer GEE again while the quota window is still saturated.
                        import time
                        wait = 60 * (2 ** attempt)
                        log_progress(f"Rate-limited by GEE — waiting {wait}s before retry {attempt+2}/{max_retries}")
                        time.sleep(wait)
                    continue
                return False
            else:
                raise

try:
    log_progress("Starting GeoJSON worker")
    initialize_earth_engine()
    log_progress("Earth Engine initialized")

    # Access snakemake parameters
    col_id            = snakemake.params.ee_collection
    multi_collections = snakemake.params.multi_collections
    scale             = snakemake.params.scale
    stats_list        = snakemake.params.stats
    start             = snakemake.params.start_date
    end               = snakemake.params.end_date
    cadence           = snakemake.params.cadence
    categorical       = snakemake.params.categorical
    band       = snakemake.wildcards.band
    prod       = getattr(snakemake.wildcards, "prod", "")
    time_chunk = getattr(snakemake.wildcards, "time_chunk", "")
    aoi        = snakemake.input.aoi
    out        = snakemake.output.geojson

    # Annual only: stamp the chunk start date as Date (one value per region per year).
    # Daily and composite: GEE sets Date per image via build_daily_stats.
    extra_props = {"Date": start} if cadence == "annual" else None

    log_progress(f"Parameters: collection={col_id}, band={band}, stats={stats_list}, cadence={cadence}, dates={start} to {end}")

    os.makedirs(os.path.dirname(out), exist_ok=True)

    # Load pre-processed AOI (CRS normalised, region_id assigned, geometry already simplified).
    log_progress("Loading pre-processed AOI")
    gdf_full = gpd.read_parquet(aoi)
    gdf_slim, attr_lookup = _split_attrs(gdf_full)
    del gdf_full
    gdf_original = gdf_slim  # keep for empty-feature fallback

    regions = _gdf_to_ee(gdf_slim)
    log_progress(f"Regions built: {len(gdf_slim)} features")

    end_dt = (datetime.strptime(end, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    log_progress("Filtering image collection")
    if multi_collections:
        collection = _build_multi_ndbi_collection(multi_collections, start, end_dt)
        if collection is None:
            collection = ee.ImageCollection([])  # No sensors active in this chunk
        else:
            collection = collection.filterBounds(regions)
    else:
        collection = ee.ImageCollection(col_id).filterDate(start, end_dt).select([band]).filterBounds(regions)

    collection_count = _blocking_getinfo(collection.size(), label="size()")
    log_progress(f"Collection has {collection_count} images")

    if collection_count == 0:
        log_progress(
            f"WARNING: No images found for {col_id}/{band} between {start} and {end}. "
            "Writing empty GeoJSON to unblock pipeline."
        )
        empty_features = []
        for idx, row in gdf_original.iterrows():
            props = {"region_id": row.get('region_id', str(idx)), "Date": start}
            if extra_props:
                props.update(extra_props)
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
                precomputed_stats=stats_fc, categorical=True, attr_lookup=attr_lookup,
                extra_props=extra_props
            )
        elif cadence in ("daily", "composite"):
            # Per-image reduction: one row per region per image date.
            # Composite products (e.g. MODIS 8-day, Landsat 16-day) have their own
            # acquisition date per image, so treat them the same as daily.
            compound = build_compound_reducer(stats_list)
            stats_fc = build_daily_stats(collection, regions_fc, scale, compound)
            return export_to_geojson(
                image=None, regions=regions_fc, scale=scale, out_geojson=out,
                max_retries=max_retries, prop_rename=prop_rename,
                precomputed_stats=stats_fc, attr_lookup=attr_lookup,
                extra_props=extra_props
            )
        else:
            # Annual: collapse the whole year to a single value per region.
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
                max_retries=max_retries, prop_rename=prop_rename, attr_lookup=attr_lookup,
                extra_props=extra_props
            )

    log_progress(f"Extracting {len(stats_list)} stat(s): {stats_list}")
    success = _do_export(regions, max_retries=3)

    if not success:
        # Geometry was already under the coord budget, so failure is likely a transient
        # GEE error or an edge case where our estimate was insufficient.
        # Apply one emergency simplification step at the maximum tolerance and retry once.
        emergency_tol = _SIMPLIFY_LADDER[-1]
        log_progress(f"Export failed — applying emergency simplification (tolerance={emergency_tol})")
        gdf_emergency = gdf_slim.copy()
        gdf_emergency["geometry"] = gdf_slim.geometry.simplify(emergency_tol, preserve_topology=True)
        gdf_emergency = gdf_emergency[~gdf_emergency.geometry.is_empty & gdf_emergency.geometry.notna()]
        regions_emergency = _gdf_to_ee(gdf_emergency)
        success = _do_export(regions_emergency, max_retries=1)

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
    if isinstance(e, TimeoutError):
        try:
            count = _increment_retry_count(out)
            if count >= GEE_TIMEOUT_MAX_RETRIES:
                log_progress(
                    f"GEE timeout on attempt {count}/{GEE_TIMEOUT_MAX_RETRIES} — "
                    f"chunk is persistently slow, shelving with empty GeoJSON"
                )
                os.makedirs(os.path.dirname(out), exist_ok=True)
                with open(out, "w") as f:
                    json.dump({"type": "FeatureCollection", "features": []}, f)
                _write_shelved_event(prod, band, time_chunk, count)
                sys.exit(0)
            else:
                log_progress(
                    f"GEE timeout on attempt {count}/{GEE_TIMEOUT_MAX_RETRIES} — "
                    f"will retry on next Snakemake pass"
                )
        except Exception:
            pass  # fall through to normal error handling

    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True) if os.path.dirname(LOG_FILE) else None
    with open(LOG_FILE, "a") as f:
        f.write(f"ERROR: {str(e)}\n")
        f.write(traceback.format_exc())
        f.write("\n")
    raise e
