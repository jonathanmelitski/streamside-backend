from firebase_functions import https_fn, scheduler_fn
from firebase_functions.options import set_global_options
from firebase_admin import initialize_app, db
from datetime import datetime
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

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

    unique_locs = list({x["id"]: x for x in res}.values())
    storage_ref.set(unique_locs)


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
            raise https_fn.HttpsError(code=https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
                                    message="The function must be called from a verified environment.")

    if req.auth is None:
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


@https_fn.on_call()
def update_profile(req: https_fn.CallableRequest):
    if req.app is None:
        raise https_fn.HttpsError(code=https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
                                    message="The function must be called from a verified environment.")

    if req.auth is None:
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


DEC_BASE_URL = "https://gisservices.dec.ny.gov/arcgis/rest/services/dil/dil_water_activities/MapServer"
NHD_BASE_URL = "https://hydro.nationalmap.gov/arcgis/rest/services/NHDPlus_HR/MapServer"

LAYER_CATALOG = [
    {
        "id": "streams",
        "name": "Streams & Rivers",
        "url": f"{NHD_BASE_URL}/3",
        "description": "All named rivers and streams (NHD national dataset).",
        "states": [],
    },
    {
        "id": "dec_streams",
        "name": "NYS DEC Streams",
        "url": f"{DEC_BASE_URL}/0",
        "description": "NYS DEC stream regulation and stocking data.",
        "states": ["NY"],
    },
    {
        "id": "water_bodies",
        "name": "Lakes & Ponds",
        "url": f"{NHD_BASE_URL}/9",
        "description": "Named lakes, ponds, and reservoirs.",
        "states": [],
    },
    {
        "id": "stream_access",
        "name": "Stream Access Areas",
        "url": f"{DEC_BASE_URL}/2",
        "description": "NYS DEC public fishing rights easements — sections of private bank where the public has legal access to fish.",
        "states": ["NY"],
    },
    {
        "id": "parking_areas",
        "name": "Parking Areas",
        "url": f"{DEC_BASE_URL}/3",
        "description": "Public fishing access parking areas managed by NYS DEC.",
        "states": ["NY"],
    },
    {
        "id": "fishing_piers",
        "name": "Fishing Piers & Platforms",
        "url": f"{DEC_BASE_URL}/4",
        "description": "Public fishing piers and platforms managed by NYS DEC.",
        "states": ["NY"],
    },
    {
        "id": "boat_launches",
        "name": "Boat Launches",
        "url": f"{DEC_BASE_URL}/5",
        "description": "Public boat launch sites managed by NYS DEC.",
        "states": ["NY"],
    },
]

@https_fn.on_call()
def get_layers(req: https_fn.CallableRequest):
    if req.app is None:
        raise https_fn.HttpsError(
            code=https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            message="The function must be called from a verified environment."
        )
    return LAYER_CATALOG
