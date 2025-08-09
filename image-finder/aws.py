#!/usr/bin/env python3
import json
import os
import sys
import subprocess
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from label_mapper import map_label

OWNER = "amazon"
ARCH = "x86_64"
TERMINATION_LOG = "/dev/termination-log"


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        eprint(f"Need {name} env var")
        sys.exit(2)
    return val


def find_latest_ami(region: str, pattern: str) -> Optional[str]:
    """
    Query EC2 for images owned by 'amazon' with the given name pattern and architecture.
    Returns the newest ImageId or None.
    """
    session = boto3.session.Session(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
        region_name=region,
    )
    ec2 = session.client("ec2")

    name_value = f"*{pattern}*" if pattern else "*"
    filters = [
        {"Name": "name", "Values": [name_value]},
        {"Name": "architecture", "Values": [ARCH]},
        {"Name": "state", "Values": ["available"]},
    ]

    try:
        resp = ec2.describe_images(Owners=[OWNER], Filters=filters)
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(str(exc)) from exc

    images = resp.get("Images", [])
    if not images:
        return None

    # Sort by CreationDate ascending, pick the last (newest)
    images.sort(key=lambda im: im.get("CreationDate", ""))
    return images[-1].get("ImageId")


def main():
    # Required env
    input_json = require_env("INPUT_JSON")
    require_env("AWS_ACCESS_KEY_ID")
    require_env("AWS_SECRET_ACCESS_KEY")
    # AWS_SESSION_TOKEN is optional

    try:
        payload = json.loads(input_json)
    except json.JSONDecodeError as exc:
        eprint(f"INPUT_JSON is not valid JSON: {exc}")
        sys.exit(2)

    top_region = payload.get("region")
    zones: List[Dict[str, Any]] = payload.get("zones", [])

    if not top_region or not isinstance(zones, list):
        eprint("INPUT_JSON must include 'region' and 'zones' array.")
        sys.exit(2)

    print(f"Using AWS region: {top_region}")
    print(f"Zones to search: {len(zones)}")

    out_zones: List[Dict[str, Any]] = []

    for z in zones:
        name_label = z.get("nameLabel", "")
        zone = z.get("zone", "")
        region = top_region

        mapped_label = map_label(name_label)["aws"]
        pattern = mapped_label or ""
        print(
            f"Searching for AMI with pattern '*{pattern}*' in region '{region}' zone '{zone}' (nameLabel='{name_label}')"
        )

        try:
            ami = find_latest_ami(region=region, pattern=pattern)
        except Exception as exc:
            eprint(
                f"EC2 describe-images failed for region '{region}' zone '{zone}' (nameLabel='{name_label}')"
            )
            eprint(str(exc))
            # Match bash behavior: exit with non-zero on AWS call failure
            sys.exit(1)

        # Normalize: if None-like -> empty / null in output
        ami_str = (ami or "").strip()
        if not ami_str or ami_str.lower() in {"none", "null"}:
            ami_str = ""

        print(
            f"Found AMI '{ami_str}' for region '{region}' zone '{zone}' (nameLabel='{name_label}')"
        )

        out_zones.append(
            {
                "nameLabel": name_label,
                "zone": zone,
                "name": ami_str if ami_str else None,
            }
        )

    output = {"Region": top_region, "zones": out_zones}
    out_text = json.dumps(output, separators=(",", ":"))

    print(json.dumps(output, separators=(",", ":"), indent=2))
    try:
        with open(TERMINATION_LOG, "w") as f:
            f.write(out_text + "\n")
    except Exception:
        # Non-fatal if not running in an environment with a termination log.
        pass

    sys.exit(0)


if __name__ == "__main__":
    main()
