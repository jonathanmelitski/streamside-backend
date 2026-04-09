# Welcome to Cloud Functions for Firebase for Python!
# To get started, simply uncomment the below code or create your own.
# Deploy with `firebase deploy`

from firebase_functions import https_fn, scheduler_fn
from firebase_functions.options import set_global_options
from firebase_admin import initialize_app, db, storage
from datetime import datetime
import requests, json, gzip, time, os
import toolz
from concurrent.futures import ThreadPoolExecutor, as_completed

# For cost control, you can set the maximum number of containers that can be
# running at the same time. This helps mitigate the impact of unexpected
# traffic spikes by instead downgrading performance. This limit is a per-function
# limit. You can override the limit for each function using the max_instances
# parameter in the decorator, e.g. @https_fn.on_request(max_instances=5).
set_global_options(max_instances=10)

initialize_app()

def fetch_location(state: str):
    url = f"https://waterservices.usgs.gov/nwis/iv/?format=json&stateCd={state}&siteStatus=all&siteType=ST"
    res = requests.get(url)
    print(f"Fetching state: {state}")
    json_data = res.json()
    iterable = json_data["value"]["timeSeries"]
    return list(map(lambda x: {
        "name": x["sourceInfo"]["siteName"],
        "id": x["sourceInfo"]["siteCode"][0]["value"],
        "state": state.upper(),
        "geo": {
            "latitude": x["sourceInfo"]["geoLocation"]["geogLocation"]["latitude"],
            "longitude": x["sourceInfo"]["geoLocation"]["geogLocation"]["longitude"]
        }
    }, iterable))

@scheduler_fn.on_schedule(schedule="0 0 1 */3 *")
def fetch_usgs_locations(event: scheduler_fn.ScheduledEvent) -> None:
    all_states = [
    'al', 'ak', 'az', 'ar', 'ca', 'co', 'ct', 'de', 'fl', 'ga', 
    'hi', 'id', 'il', 'in', 'ia', 'ks', 'ky', 'la', 'me', 'md', 
    'ma', 'mi', 'mn', 'ms', 'mo', 'mt', 'ne', 'nv', 'nh', 'nj', 
    'nm', 'ny', 'nc', 'nd', 'oh', 'ok', 'or', 'pa', 'ri', 'sc', 
    'sd', 'tn', 'tx', 'ut', 'vt', 'va', 'wa', 'wv', 'wi', 'wy'
    ]

    storage_ref = db.reference(path="/all_usgs_locations", url="https://streamside-2b8f1-default-rtdb.firebaseio.com/")
    
    res = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = (executor.submit(fetch_location, state) for state in all_states)

        for future in as_completed(futures):
            res = res + future.result()

    unique_locs = toolz.unique(res, key=lambda x: x["id"])
    storage_ref.set(list(unique_locs))


class Profile:
     uid: str
     last_updated: int
     first_name: str | None = None
     last_name: str | None = None
     gauges: list = []
     markers: list = []
     fish: list = []

     def __init__(self, uid, **kwargs):
          self.uid = uid
          self.first_name = kwargs.get("first_name")
          self.last_name = kwargs.get("last_name")
          self.gauges = kwargs.get("gauges", [])
          self.last_updated = kwargs.get("last_updated", int(datetime.now().timestamp()))
          self.markers = kwargs.get("markers", [])
          self.fish = kwargs.get("fish", [])

          

@https_fn.on_call()
def get_or_create_profile(req: https_fn.CallableRequest):
    if req.app is None:
            # Throwing an HttpsError so that the client gets the error details.
            raise https_fn.HttpsError(code=https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
                                    message="The function must be called from a verified environment.")

    if req.auth is None:
        # Throwing an HttpsError so that the client gets the error details.
        raise https_fn.HttpsError(code=https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
                                message="The function must be called while authenticated.")

    storage_ref = db.reference(path="/users", url="https://streamside-2b8f1-default-rtdb.firebaseio.com/").child(req.auth.uid)

    res = storage_ref.get()
    if not res:
         profile = Profile(req.auth.uid)
         storage_ref.set(profile.__dict__)
         return profile.__dict__
    else:
        if res["uid"] != req.auth.uid:
            raise https_fn.HttpsError(code=https_fn.FunctionsErrorCode.ABORTED,
                                        message="Invalid State, ID on profile did not match with auth user id.")
        else:
            res.pop("uid")
        profile = Profile(req.auth.uid, **res)
        return profile.__dict__

DEC_BASE_URL = "https://gisservices.dec.ny.gov/arcgis/rest/services/dil/dil_water_activities/MapServer"
NHD_BASE_URL = "https://hydro.nationalmap.gov/arcgis/rest/services/NHDPlus_HR/MapServer"
NY_BOUNDS = (-79.76, 40.50, -71.86, 45.01)    # min_lon, min_lat, max_lon, max_lat
CONUS_BOUNDS = (-125.0, 24.0, -66.0, 50.0)    # continental US

def _bbox_geometry(min_lon, min_lat, max_lon, max_lat) -> str:
    return json.dumps({
        "xmin": min_lon, "ymin": min_lat,
        "xmax": max_lon, "ymax": max_lat,
        "spatialReference": {"wkid": 4326}
    })

def _query_dec_layer(layer_id: int, min_lon: float, min_lat: float, max_lon: float, max_lat: float) -> dict:
    res = requests.get(
        f"{DEC_BASE_URL}/{layer_id}/query",
        params={
            "f": "json",
            "returnGeometry": "true",
            "spatialRel": "esriSpatialRelIntersects",
            "geometry": _bbox_geometry(min_lon, min_lat, max_lon, max_lat),
            "geometryType": "esriGeometryEnvelope",
            "inSR": "4326",
            "outFields": "*",
            "outSR": "4326",
        }
    )
    res.raise_for_status()
    return res.json()

def _query_nhd_layer(layer_id: int, min_lon: float, min_lat: float, max_lon: float, max_lat: float, where: str = "1=1", offset: int = 0) -> dict:
    res = requests.get(
        f"{NHD_BASE_URL}/{layer_id}/query",
        params={
            "f": "json",
            "returnGeometry": "true",
            "spatialRel": "esriSpatialRelIntersects",
            "geometry": _bbox_geometry(min_lon, min_lat, max_lon, max_lat),
            "geometryType": "esriGeometryEnvelope",
            "inSR": "4326",
            "outFields": "*",
            "outSR": "4326",
            "where": where,
            "resultOffset": offset,
        }
    )
    res.raise_for_status()
    return res.json()

def _intersects_ny(min_lon: float, min_lat: float, max_lon: float, max_lat: float) -> bool:
    return (min_lon < NY_BOUNDS[2] and max_lon > NY_BOUNDS[0]
            and min_lat < NY_BOUNDS[3] and max_lat > NY_BOUNDS[1])

def _to_geojson_feature(esri_feature: dict, geometry_type: str, layer: str, props: dict = None) -> dict:
    geom = esri_feature["geometry"]

    if geometry_type == "esriGeometryPoint":
        geometry = {"type": "Point", "coordinates": [geom["x"], geom["y"]]}
    elif geometry_type == "esriGeometryPolygon":
        geometry = {"type": "Polygon", "coordinates": geom["rings"]}
    elif geometry_type == "esriGeometryPolyline":
        geometry = {"type": "MultiLineString", "coordinates": geom["paths"]}
    else:
        geometry = None

    properties = props if props is not None else esri_feature["attributes"]
    return {"type": "Feature", "geometry": geometry, "properties": {**properties, "layer": layer}}

def _nhd_stream_props(attrs: dict) -> dict:
    return {
        "name": attrs.get("gnis_name"),
        "length_km": attrs.get("lengthkm"),
        "stream_order": attrs.get("streamorder"),
        "slope": attrs.get("slope"),
        "drainage_area_sqkm": attrs.get("totdasqkm"),
        "max_elevation_m": attrs.get("maxelevation"),
        "min_elevation_m": attrs.get("minelevation"),
        "mean_annual_flow": attrs.get("qama"),
        "source": "nhd_plus",
    }

def _dec_stream_props(attrs: dict) -> dict:
    length_mi = attrs.get("LENGTH_MILES")
    return {
        "name": attrs.get("WATER"),
        "length_km": length_mi * 1.60934 if length_mi else None,
        "dec_mgmt_cat": attrs.get("MGMTCAT"),
        "dec_description": attrs.get("DESCRIPTION"),
        "dec_harvest_reg": attrs.get("HARVESTREG"),
        "dec_cr_reg": attrs.get("CRREG"),
        "dec_stocked_species": attrs.get("STOCKEDSPECIES"),
        "dec_total_trout": attrs.get("TOTALTROUT"),
        "dec_schedule": attrs.get("SCHEDULE"),
        "source": "dec",
    }

def _merge_streams(nhd_features: list, dec_features: list) -> list:
    nhd_by_name: dict[str, list] = {}
    for f in nhd_features:
        name = (f["properties"].get("name") or "").lower().strip()
        if name:
            nhd_by_name.setdefault(name, []).append(f)

    unmatched = []
    for dec_f in dec_features:
        name = (dec_f["properties"].get("name") or "").lower().strip()
        if name in nhd_by_name:
            dec_props = {k: v for k, v in dec_f["properties"].items() if k.startswith("dec_")}
            for nhd_f in nhd_by_name[name]:
                nhd_f["properties"].update(dec_props)
                nhd_f["properties"]["source"] = "nhd_plus+dec"
        else:
            unmatched.append(dec_f)

    return nhd_features + unmatched

LAYER_CATALOG = [
    {
        "id": "streams",
        "name": "Streams & Rivers",
        "geometry": "MultiLineString",
        "description": "All named rivers and streams. In New York, NHD data is enriched with NYS DEC regulation and stocking information where available.",
        "ny_only": False,
    },
    {
        "id": "water_bodies",
        "name": "Lakes & Ponds",
        "geometry": "Polygon",
        "description": "Named lakes, ponds, and reservoirs.",
        "ny_only": False,
    },
    {
        "id": "stream_access",
        "name": "Stream Access Areas",
        "geometry": "Polygon",
        "description": "NYS DEC public fishing rights easements — sections of private bank where the public has legal access to fish.",
        "ny_only": True,
    },
    {
        "id": "parking_areas",
        "name": "Parking Areas",
        "geometry": "Point",
        "description": "Public fishing access parking areas managed by NYS DEC.",
        "ny_only": True,
    },
    {
        "id": "fishing_piers",
        "name": "Fishing Piers & Platforms",
        "geometry": "Point",
        "description": "Public fishing piers and platforms managed by NYS DEC.",
        "ny_only": True,
    },
    {
        "id": "boat_launches",
        "name": "Boat Launches",
        "geometry": "Point",
        "description": "Public boat launch sites managed by NYS DEC.",
        "ny_only": True,
    },
]

_VALID_LAYER_IDS = {layer["id"] for layer in LAYER_CATALOG}
_DEC_AMENITY_LAYER_IDS = {
    "stream_access": 2,
    "parking_areas": 3,
    "fishing_piers": 4,
    "boat_launches": 5,
}

def _fetch_layer_features(layer_id: str, min_lon: float, min_lat: float, max_lon: float, max_lat: float) -> list:
    in_ny = _intersects_ny(min_lon, min_lat, max_lon, max_lat)

    if layer_id == "streams":
        workers = 2 if in_ny else 1
        with ThreadPoolExecutor(max_workers=workers) as executor:
            f_nhd = executor.submit(_query_nhd_layer, 3, min_lon, min_lat, max_lon, max_lat, "gnis_name IS NOT NULL")
            f_dec = executor.submit(_query_dec_layer, 0, min_lon, min_lat, max_lon, max_lat) if in_ny else None
            nhd_data = f_nhd.result()
            dec_data = f_dec.result() if f_dec else None

        nhd_features = [
            _to_geojson_feature(f, nhd_data["geometryType"], "streams", _nhd_stream_props(f["attributes"]))
            for f in nhd_data.get("features", [])
        ]
        if not in_ny:
            return nhd_features

        dec_features = [
            _to_geojson_feature(f, dec_data["geometryType"], "streams", _dec_stream_props(f["attributes"]))
            for f in dec_data.get("features", [])
        ]
        return _merge_streams(nhd_features, dec_features)

    if layer_id == "water_bodies":
        data = _query_nhd_layer(9, min_lon, min_lat, max_lon, max_lat, "gnis_name IS NOT NULL")
        return [
            _to_geojson_feature(f, data["geometryType"], "water_bodies", {
                "name": f["attributes"].get("gnis_name"),
                "area_sqkm": f["attributes"].get("areasqkm"),
                "ftype": f["attributes"].get("ftype"),
                "source": "nhd_plus",
            })
            for f in data.get("features", [])
        ]

    # NY-only DEC layers
    if not in_ny:
        return []
    data = _query_dec_layer(_DEC_AMENITY_LAYER_IDS[layer_id], min_lon, min_lat, max_lon, max_lat)
    return [
        _to_geojson_feature(f, data["geometryType"], layer_id)
        for f in data.get("features", [])
    ]

@https_fn.on_call()
def get_layers(req: https_fn.CallableRequest):
    if req.app is None:
        raise https_fn.HttpsError(
            code=https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            message="The function must be called from a verified environment."
        )
    return LAYER_CATALOG

@https_fn.on_call()
def get_layer(req: https_fn.CallableRequest):
    if req.app is None:
        raise https_fn.HttpsError(
            code=https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            message="The function must be called from a verified environment."
        )

    layer_id = req.data.get("layer")
    if layer_id not in _VALID_LAYER_IDS:
        raise https_fn.HttpsError(
            code=https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            message=f"Unknown layer '{layer_id}'. Valid layers: {sorted(_VALID_LAYER_IDS)}"
        )

    bbox = req.data.get("bbox")
    if not bbox or len(bbox) != 4:
        raise https_fn.HttpsError(
            code=https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            message="bbox must be [min_lon, min_lat, max_lon, max_lat]"
        )

    min_lon, min_lat, max_lon, max_lat = bbox
    features = _fetch_layer_features(layer_id, min_lon, min_lat, max_lon, max_lat)
    return {"type": "FeatureCollection", "features": features}


def _conus_tiles(tile_size: float):
    lon = CONUS_BOUNDS[0]
    while lon < CONUS_BOUNDS[2]:
        lat = CONUS_BOUNDS[1]
        while lat < CONUS_BOUNDS[3]:
            yield (lon, lat,
                   min(lon + tile_size, CONUS_BOUNDS[2]),
                   min(lat + tile_size, CONUS_BOUNDS[3]))
            lat += tile_size
        lon += tile_size

def _fetch_nhd_tile(layer_id: int, tile: tuple, where: str = "1=1", max_retries: int = 3) -> list:
    a, b, c, d = tile
    features, offset = [], 0
    while True:
        for attempt in range(max_retries):
            try:
                data = _query_nhd_layer(layer_id, a, b, c, d, where, offset=offset)
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)  # 1s, 2s backoff
        page = data.get("features", [])
        features.extend(page)
        if not data.get("exceededTransferLimit") or not page:
            break
        offset += len(page)
    return features

def _upload_layer(layer_id: str, features: list) -> None:
    """For small layers (DEC amenities) that fit comfortably in memory."""
    payload = json.dumps({"type": "FeatureCollection", "features": features}).encode()
    compressed = gzip.compress(payload)
    blob = storage.bucket().blob(f"layers/{layer_id}.geojson.gz")
    blob.content_encoding = "gzip"
    blob.upload_from_string(compressed, content_type="application/json")
    print(f"Uploaded layers/{layer_id}.geojson.gz ({len(features)} features, {len(compressed)//1024}KB compressed)")

def _upload_layer_from_file(layer_id: str, tmp_path: str, feature_count: int) -> None:
    """For large layers written incrementally to /tmp."""
    blob = storage.bucket().blob(f"layers/{layer_id}.geojson.gz")
    blob.content_encoding = "gzip"
    blob.upload_from_filename(tmp_path, content_type="application/json")
    file_size = os.path.getsize(tmp_path) // 1024
    print(f"Uploaded layers/{layer_id}.geojson.gz ({feature_count} features, {file_size}KB compressed)")
    os.unlink(tmp_path)

def _load_resume_state(layer_id: str) -> tuple:
    """
    Returns (seen_ids, done_tile_indices, feature_count).
    If both checkpoint files exist, resumes from them.
    Otherwise cleans up any partial files and starts fresh.
    """
    pid_path = f"/tmp/{layer_id}_pids.txt"
    done_path = f"/tmp/{layer_id}_done_tiles.txt"
    jsonl_path = f"/tmp/{layer_id}_features.jsonl"
    dec_path = f"/tmp/{layer_id}_matched_dec.txt"

    if not (os.path.exists(pid_path) and os.path.exists(done_path)):
        for path in [pid_path, done_path, jsonl_path, dec_path]:
            if os.path.exists(path):
                os.unlink(path)
        return set(), set(), 0

    with open(pid_path) as f:
        seen_ids = {line.strip() for line in f if line.strip()}
    with open(done_path) as f:
        done_tiles = {int(line.strip()) for line in f if line.strip()}
    print(f"  Resuming {layer_id}: {len(seen_ids)} features, {len(done_tiles)} tiles already done")
    return seen_ids, done_tiles, len(seen_ids)

def _finalize_layer(layer_id: str, feature_count: int) -> None:
    """Stream JSONL → GeoJSON.gz, upload, then delete all /tmp checkpoint files."""
    jsonl_path = f"/tmp/{layer_id}_features.jsonl"
    gz_path = f"/tmp/{layer_id}.geojson.gz"
    print(f"Compressing {layer_id} ({feature_count} features)...")
    with gzip.open(gz_path, "wt", encoding="utf-8") as gz:
        gz.write('{"type":"FeatureCollection","features":[')
        first = True
        with open(jsonl_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if not first:
                    gz.write(",")
                gz.write(line)
                first = False
        gz.write("]}")
    _upload_layer_from_file(layer_id, gz_path, feature_count)
    for suffix in ["_features.jsonl", "_pids.txt", "_done_tiles.txt", "_matched_dec.txt"]:
        path = f"/tmp/{layer_id}{suffix}"
        if os.path.exists(path):
            os.unlink(path)

@https_fn.on_request(timeout_sec=3600, memory=1024)
def cache_water_layers(req: https_fn.Request) -> https_fn.Response:
    _run_cache_water_layers()
    return https_fn.Response("Done")

def _run_cache_water_layers() -> None:
    # --- DEC layers (NY only, small — single full-state query each) ---
    dec_layers = [
        (2, "stream_access"),
        (3, "parking_areas"),
        (4, "fishing_piers"),
        (5, "boat_launches"),
    ]
    for dec_id, layer_id in dec_layers:
        data = _query_dec_layer(dec_id, *NY_BOUNDS[:2], *NY_BOUNDS[2:])
        features = [_to_geojson_feature(f, data["geometryType"], layer_id)
                    for f in data.get("features", [])]
        _upload_layer(layer_id, features)

    # --- DEC streams — fetch NY-wide, keep in memory for NHD merge ---
    dec_stream_data = _query_dec_layer(0, *NY_BOUNDS[:2], *NY_BOUNDS[2:])
    dec_by_name: dict = {}
    for f in dec_stream_data.get("features", []):
        props = _dec_stream_props(f["attributes"])
        name = (props.get("name") or "").lower().strip()
        if name:
            dec_by_name[name] = _to_geojson_feature(f, dec_stream_data["geometryType"], "streams", props)

    # --- NHD streams — tile CONUS at 0.5°, checkpoint to /tmp JSONL ---
    seen_stream_ids, done_stream_tiles, stream_count = _load_resume_state("streams")

    matched_dec_path = "/tmp/streams_matched_dec.txt"
    matched_dec: set = set()
    if os.path.exists(matched_dec_path):
        with open(matched_dec_path) as mdf:
            matched_dec = {line.strip() for line in mdf if line.strip()}

    stream_tiles = list(_conus_tiles(0.5))
    remaining_stream = [(i, t) for i, t in enumerate(stream_tiles) if i not in done_stream_tiles]
    print(f"Fetching NHD streams: {len(remaining_stream)}/{len(stream_tiles)} tiles remaining (workers=30)...")

    with open("/tmp/streams_features.jsonl", "a") as feat_f, \
         open("/tmp/streams_pids.txt", "a") as pid_f, \
         open("/tmp/streams_done_tiles.txt", "a") as done_f, \
         open(matched_dec_path, "a") as dec_ckpt_f:
        completed = 0
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=30) as executor:
            futures = {
                executor.submit(_fetch_nhd_tile, 3, tile, "gnis_name IS NOT NULL"): (i, tile)
                for i, tile in remaining_stream
            }
            for future in as_completed(futures):
                tile_idx, tile = futures.pop(future)
                try:
                    for f in future.result():
                        pid = f["attributes"].get("permanent_identifier")
                        if pid and pid in seen_stream_ids:
                            continue
                        if pid:
                            seen_stream_ids.add(pid)
                        props = _nhd_stream_props(f["attributes"])
                        name = (props.get("name") or "").lower().strip()
                        if name in dec_by_name and name not in matched_dec:
                            dec_props = {k: v for k, v in dec_by_name[name]["properties"].items() if k.startswith("dec_")}
                            props.update(dec_props)
                            props["source"] = "nhd_plus+dec"
                            matched_dec.add(name)
                            dec_ckpt_f.write(name + "\n")
                        feature = _to_geojson_feature(f, "esriGeometryPolyline", "streams", props)
                        feat_f.write(json.dumps(feature) + "\n")
                        if pid:
                            pid_f.write(pid + "\n")
                        stream_count += 1
                    done_f.write(str(tile_idx) + "\n")
                    feat_f.flush()
                    pid_f.flush()
                    done_f.flush()
                    dec_ckpt_f.flush()
                except Exception as e:
                    print(f"Stream tile error (tile {tile_idx} {tile}): {e}")
                completed += 1
                interval = max(1, len(remaining_stream) // 20)
                if completed % interval == 0 or completed == len(remaining_stream):
                    elapsed = time.time() - t0
                    rate = completed / elapsed if elapsed > 0 else 1
                    eta = int((len(remaining_stream) - completed) / rate)
                    print(f"  Streams: {completed}/{len(remaining_stream)} tiles ({100*completed//len(remaining_stream)}%) | {stream_count} features | ETA {eta}s")

    # Append unmatched DEC features
    with open("/tmp/streams_features.jsonl", "a") as feat_f:
        for name, dec_feat in dec_by_name.items():
            if name not in matched_dec:
                feat_f.write(json.dumps(dec_feat) + "\n")
                stream_count += 1

    _finalize_layer("streams", stream_count)

    # --- NHD waterbodies — tile CONUS at 2.0°, checkpoint to /tmp JSONL ---
    seen_water_ids, done_water_tiles, water_count = _load_resume_state("water_bodies")

    water_tiles = list(_conus_tiles(2.0))
    remaining_water = [(i, t) for i, t in enumerate(water_tiles) if i not in done_water_tiles]
    print(f"Fetching NHD waterbodies: {len(remaining_water)}/{len(water_tiles)} tiles remaining (workers=30)...")

    with open("/tmp/water_bodies_features.jsonl", "a") as feat_f, \
         open("/tmp/water_bodies_pids.txt", "a") as pid_f, \
         open("/tmp/water_bodies_done_tiles.txt", "a") as done_f:
        completed = 0
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=30) as executor:
            futures = {
                executor.submit(_fetch_nhd_tile, 9, tile, "gnis_name IS NOT NULL"): (i, tile)
                for i, tile in remaining_water
            }
            for future in as_completed(futures):
                tile_idx, tile = futures.pop(future)
                try:
                    for f in future.result():
                        pid = f["attributes"].get("permanent_identifier")
                        if pid and pid in seen_water_ids:
                            continue
                        if pid:
                            seen_water_ids.add(pid)
                        feature = _to_geojson_feature(f, "esriGeometryPolygon", "water_bodies", {
                            "name": f["attributes"].get("gnis_name"),
                            "area_sqkm": f["attributes"].get("areasqkm"),
                            "ftype": f["attributes"].get("ftype"),
                            "source": "nhd_plus",
                        })
                        feat_f.write(json.dumps(feature) + "\n")
                        if pid:
                            pid_f.write(pid + "\n")
                        water_count += 1
                    done_f.write(str(tile_idx) + "\n")
                    feat_f.flush()
                    pid_f.flush()
                    done_f.flush()
                except Exception as e:
                    print(f"Waterbody tile error (tile {tile_idx} {tile}): {e}")
                completed += 1
                interval = max(1, len(remaining_water) // 20)
                if completed % interval == 0 or completed == len(remaining_water):
                    elapsed = time.time() - t0
                    rate = completed / elapsed if elapsed > 0 else 1
                    eta = int((len(remaining_water) - completed) / rate)
                    print(f"  Waterbodies: {completed}/{len(remaining_water)} tiles ({100*completed//len(remaining_water)}%) | {water_count} features | ETA {eta}s")

    _finalize_layer("water_bodies", water_count)

@https_fn.on_call()
def get_layer_url(req: https_fn.CallableRequest):
    if req.app is None:
        raise https_fn.HttpsError(
            code=https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            message="The function must be called from a verified environment."
        )

    layer_id = req.data.get("layer")
    if layer_id not in _VALID_LAYER_IDS:
        raise https_fn.HttpsError(
            code=https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            message=f"Unknown layer '{layer_id}'. Valid layers: {sorted(_VALID_LAYER_IDS)}"
        )

    blob = storage.bucket().blob(f"layers/{layer_id}.geojson.gz")
    if not blob.exists():
        raise https_fn.HttpsError(
            code=https_fn.FunctionsErrorCode.NOT_FOUND,
            message="Layer not yet cached. The cache refreshes quarterly."
        )

    return {"url": blob.public_url}


@https_fn.on_call()
def update_profile(req: https_fn.CallableRequest):
    if req.app is None:
        # Throwing an HttpsError so that the client gets the error details.
        raise https_fn.HttpsError(code=https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
                                    message="The function must be called from a verified environment.")

    if req.auth is None:
        # Throwing an HttpsError so that the client gets the error details.
        raise https_fn.HttpsError(code=https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
                                message="The function must be called while authenticated.")

    storage_ref = db.reference(path="/users", url="https://streamside-2b8f1-default-rtdb.firebaseio.com/").child(req.auth.uid)
    if req.data["uid"] != req.auth.uid:
        raise https_fn.HttpsError(code=https_fn.FunctionsErrorCode.ABORTED,
                                        message="Invalid State, ID on profile did not match with auth user id.")
    else:
        req.data.pop("uid")
    profile = Profile(req.auth.uid, **req.data)
    storage_ref.set(profile.__dict__)
    return profile.__dict__
    
         