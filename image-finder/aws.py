#!/usr/bin/env python3
import json
import os
import sys
import subprocess
from typing import Any, Dict, List, Optional
import re
import boto3
from botocore.exceptions import BotoCoreError, ClientError

from label_mapper import map_label

# Canonical owner for official Ubuntu images
CANONICAL_OWNER = "099720109477"
# Placeholder owner to be replaced by you for non-official images (skypilot, custom, etc)
OTHER_OWNERS = "195275664570"

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


def choose_owner(pattern_or_label: str) -> str:
  """
  If the pattern/nameLabel indicates an Ubuntu 24.04, 22.04, or 20.04 image,
  return the Canonical owner ID. Otherwise return a placeholder owner ID
  that you can adjust to the real owner.
  """
  if not pattern_or_label:
    return OTHER_OWNERS
  # match ubuntu-24.04, ubuntu-22.04, ubuntu-20.04 (case-insensitive)
  if re.search(r"^ubuntu-(?:24\.04|22\.04|20\.04)$", pattern_or_label, re.IGNORECASE):
    return CANONICAL_OWNER
  return OTHER_OWNERS

def find_latest_ami(region: str, pattern: str, owner: str) -> Optional[str]:
  """
  Query EC2 for images owned by `owner` with the given name pattern and architecture.
  Returns the newest ImageId or None.
  """
  session = boto3.session.Session(
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
    region_name=region,
  )
  ec2 = session.client("ec2")

  name_value = f"{pattern}" if pattern else "*"
  filters = [
    {"Name": "name", "Values": [name_value]},
    {"Name": "architecture", "Values": [ARCH]},
    {"Name": "state", "Values": ["available"]},
  ]

  try:
    resp = ec2.describe_images(Owners=[owner], Filters=filters)
  except (BotoCoreError, ClientError) as exc:
    raise RuntimeError(str(exc)) from exc

  images = resp.get("Images", [])
  if not images:
    return None

  # regex to match 'pro' as a standalone token or preceded/followed by non-alphanumeric
  pro_regex = re.compile(r"(?<![A-Za-z0-9])pro(?![A-Za-z0-9])", re.IGNORECASE)

  def looks_like_pro(img: Dict[str, Any]) -> bool:
    # Marketplace / paid images (ProductCodes present)
    if img.get("ProductCodes"):
      return True
    # Check common textual fields for 'pro' (name, description, image location/source)
    for fld in ("Name", "Description", "ImageLocation"):
      val = img.get(fld)
      if isinstance(val, str) and pro_regex.search(val):
        return True
    return False

  print("Filter images to find free images")
  # Exclude images that look like Pro/paid
  free_images = [im for im in images if not looks_like_pro(im)]

  if not free_images:
    return None

  # Sort by CreationDate ascending, pick the last (newest)
  free_images.sort(key=lambda im: im.get("CreationDate", ""))
  return free_images[-1].get("ImageId")


def main():
  # Required env
  input_json = require_env("INPUT_JSON")
  output_path = require_env("OUTPUT_PATH")
  require_env("AWS_ACCESS_KEY_ID")
  require_env("AWS_SECRET_ACCESS_KEY")
  # AWS_SESSION_TOKEN is optional

  try:
    payload = json.loads(input_json)
  except json.JSONDecodeError as exc:
    eprint(f"INPUT_JSON is not valid JSON: {exc}")
    sys.exit(2)

  top_region = require_env("REGION")
  zones: List[Dict[str, Any]] = payload.get("images", [])

  if not top_region or not isinstance(zones, list):
    eprint("INPUT_JSON must include 'region' and 'zones' array.")
    sys.exit(2)

  print(f"Using AWS region: {top_region}")
  print(f"Zones to search: {len(zones)}")

  out_zones: List[Dict[str, Any]] = []

  for z in zones:
    name_label = z.get("nameLabel", "")
    pattern = z.get("pattern", "")
    zone = z.get("zone", "")
    region = top_region

    pattern = pattern or name_label
    owner = choose_owner(name_label)
    if owner == CANONICAL_OWNER:
      print(f"Pattern indicates official Ubuntu LTS; using Canonical owner {CANONICAL_OWNER}")
    else:
      print(f"Using owner '{OTHER_OWNERS}' for pattern '{pattern}'")

    print(
      f"Searching for AMI with pattern '{pattern}' in region '{region}' zone '{zone}' (nameLabel='{name_label}')"
    )

    try:
      ami = find_latest_ami(region=region, pattern=pattern, owner=owner)
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

  output = {"region": top_region, "images": out_zones}
  OUTPUT = json.dumps(output)
  print(OUTPUT, flush=True)
  # write into /dev/termination-log (or provided output path)
  with open(output_path, "w") as f:
    f.write(OUTPUT + "\n")

  sys.exit(0)


if __name__ == "__main__":
  main()