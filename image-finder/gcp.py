#!/usr/bin/env python3
import os
import sys
import json
import tempfile
import re
import atexit
from google.oauth2 import service_account
from google.cloud import compute_v1
from label_mapper import map_label 

# -------- Exit codes --------
EXIT_SUCCESS = 0
EXIT_USAGE = 2
EXIT_ZONE_NOT_IN_REGION = 3
EXIT_NO_IMAGES_MATCHED = 4
EXIT_AUTH_FAILURE = 5
EXIT_PROJECT_FAILED = 8
EXIT_UNKNOWN_ERROR = 9

def err(*args):
    print("ERROR:", *args, file=sys.stderr)

def info(*args):
    print("INFO:", *args)

# -------- Parse environment --------
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")

try:
    input_json = json.loads(os.environ["INPUT_JSON"])
except KeyError:
    err("INPUT_JSON environment variable is required")
    sys.exit(EXIT_USAGE)

TOP_REGION = os.getenv("REGION")
output_path = os.environ.get("OUTPUT_PATH")
ZONES = input_json.get("images", [])
ARCH = "X86_64"

if not TOP_REGION:
    err("Region missing from environment")
    sys.exit(EXIT_USAGE)

info(f"Using region: {TOP_REGION}")
info(f"Zones to search: {len(ZONES)}")

# -------- Auth --------
credentials = None
tmpkey = None

def cleanup_tmp():
    if tmpkey and os.path.exists(tmpkey):
        os.remove(tmpkey)

atexit.register(cleanup_tmp)

try:
    if SERVICE_ACCOUNT_JSON:
        tmpkey = tempfile.mktemp(suffix=".json")
        with open(tmpkey, "w") as f:
            f.write(SERVICE_ACCOUNT_JSON)
        credentials = service_account.Credentials.from_service_account_file(tmpkey)
    else:
        err("No valid service account credentials provided")
        sys.exit(EXIT_AUTH_FAILURE)
except Exception as e:
    err(f"Service account activation failed: {e}")
    sys.exit(EXIT_AUTH_FAILURE)

# -------- Project check --------
if not GOOGLE_CLOUD_PROJECT:
    err("No project is set. Set GOOGLE_CLOUD_PROJECT.")
    sys.exit(EXIT_PROJECT_FAILED)

# -------- GCP client --------
image_client = compute_v1.ImagesClient(credentials=credentials)

OUT = []



for z in ZONES:
    name_label = z.get("nameLabel")
    zone = z.get("zone")

    mapped_label = map_label(name_label)["gcp"]
    pattern = re.compile(mapped_label)

    filter_expr = (
        '(status eq "READY") '
        '(architecture eq "X86_64") '
        f'(name eq ".*{mapped_label}.*")'
        f'(family eq ".*{mapped_label}.*")'
    )
    req = compute_v1.ListImagesRequest(
            project="ubuntu-os-cloud",
            filter=filter_expr,            
            max_results=1,
        )
    images = list(image_client.list(request=req))
    print(f"Searching with filter: {req.filter}, found {len(images)} images", flush=True)

    # Iterate until we find the newest image whose name OR family matches in Python
    latest = None
    for img in images:
        print(f".. Checking image: {img.name} (family: {img.family})", flush=True)
        name = getattr(img, "name", "") or ""
        family = getattr(img, "family", "") or ""
        if pattern.search(name) or pattern.search(family):
            latest = img
            break

    print(f".. Selected image: {latest.name if latest else 'None'}", flush=True)
    if latest:
        uri = latest.self_link.replace("https://www.googleapis.com/compute/v1/", "")
        out_zone = {"nameLabel": name_label, "zone": zone, "name": uri}
    else:
        out_zone = {"nameLabel": name_label, "zone": zone, "name": None}
    
    OUT.append(out_zone)

OUTPUT = {
    "region": TOP_REGION,
    "images": OUT
}

OUTPUT = json.dumps(OUTPUT)
print(OUTPUT, flush=True)
#  print into /dev/termination-log
with open(output_path, "w") as f:
    f.write(OUTPUT + "\n")

sys.exit(EXIT_SUCCESS)
