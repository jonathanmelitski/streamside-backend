# Welcome to Cloud Functions for Firebase for Python!
# To get started, simply uncomment the below code or create your own.
# Deploy with `firebase deploy`

from firebase_functions import https_fn, scheduler_fn
from firebase_functions.options import set_global_options
from firebase_admin import initialize_app, db
import requests, json
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
    json_data = json.loads(res.json())
    iterable = json_data["value"]["timeSeries"]
    return map(lambda x: {
        "name": x["sourceInfo"]["siteName"],
        "id": x["sourceInfo"]["siteCode"][0]["value"],
        "geo": {
            "latitude": x["sourceInfo"]["geoLocation"]["geogLocation"]["latitude"],
            "longitude": x["sourceInfo"]["geoLocation"]["geogLocation"]["longitude"]
        }
    }, iterable)

@scheduler_fn.on_schedule(schedule="0 0 1 */3 *")
def fetch_usgs_locations(event: scheduler_fn.ScheduledEvent) -> None:
    all_states = [
    'al', 'ak', 'az', 'ar', 'ca', 'co', 'ct', 'de', 'fl', 'ga', 
    'hi', 'id', 'il', 'in', 'ia', 'ks', 'ky', 'la', 'me', 'md', 
    'ma', 'mi', 'mn', 'ms', 'mo', 'mt', 'ne', 'nv', 'nh', 'nj', 
    'nm', 'ny', 'nc', 'nd', 'oh', 'ok', 'or', 'pa', 'ri', 'sc', 
    'sd', 'tn', 'tx', 'ut', 'vt', 'va', 'wa', 'wv', 'wi', 'wy'
    ]

    storage_ref = db.reference("/all_usgs_locations")
    
    res = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        
        futures = (executor.submit(fetch_location, state) for state in all_states)

        for future in as_completed(futures):
            res = res + future.result()

    storage_ref.set(res)



@https_fn.on_request()
def on_request_example(req: https_fn.Request) -> https_fn.Response:
    return https_fn.Response("Hello world!")