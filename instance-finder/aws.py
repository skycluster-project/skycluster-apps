#!/usr/bin/env python3
"""
Finds EC2 instance types from a given family that are *actually offered* in a specific
AWS region + availability zone, and prints a JSON document with details, including
vCPUs, RAM, GPU info, On-Demand price, and recent Spot price.

Requirements:
  - Python 3.8+
  - boto3 (`pip install boto3`)
  - Valid AWS credentials in your environment/profile
Environment variables:
  REGION               AWS region code, e.g., us-west-1
  ZONE                 Availability zone, e.g., us-west-1a
  FAMILY               Instance family prefix, e.g., m7i, c7g, g5
  AWS_PROFILE          AWS CLI profile name (optional)
  SPOT_LOOKBACK_HOURS  Hours to look back for spot price (default: 24)
"""

import os
import boto3
import botocore
import json
import datetime
from decimal import Decimal, ROUND_HALF_UP

# ---------- helpers ----------

def dec_to_str_money(x):
    if x is None:
        return ""
    try:
        d = Decimal(str(x))
        return str(d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    except Exception:
        return ""

def mib_to_gib_str(mib):
    if mib is None:
        return ""
    gib = Decimal(mib) / Decimal(1024)
    q = gib.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if q == q.to_integral():
        return f"{int(q)}Gi"
    return f"{q}Gi"

def to_title_label(instance_type: str) -> str:
    parts = instance_type.split(".")
    if len(parts) == 2:
        return f"{parts[0].upper()} {parts[1].capitalize()}"
    return instance_type

def paginate(client, method_name, result_key, **kwargs):
    paginator = client.get_paginator(method_name)
    for page in paginator.paginate(**kwargs):
        for item in page.get(result_key, []):
            yield item

def get_offered_instance_types_in_az(ec2, az: str):
    offered = set()
    params = {
        "LocationType": "availability-zone",
        "Filters": [
            {"Name": "location", "Values": [az]},
        ]
    }
    for it in paginate(ec2, "describe_instance_type_offerings", "InstanceTypeOfferings", **params):
        t = it.get("InstanceType")
        if t:
            offered.add(t)
    return offered

def filter_by_family(instance_types, family_prefix: list[str]):
    if not family_prefix:
        return instance_types
    print(f"Filtering by family prefix: '{family_prefix}'")
    return {t for t in instance_types if any(t.lower().startswith(pref.lower()) for pref in family_prefix)}

def describe_types(ec2, types):
    out = {}
    types = list(types)
    for i in range(0, len(types), 100):
        chunk = types[i:i+100]
        resp = ec2.describe_instance_types(InstanceTypes=chunk)
        for it in resp.get("InstanceTypes", []):
            out[it["InstanceType"]] = it
    return out

def extract_gpu_info(it_desc):
    g = it_desc.get("GpuInfo")
    if not g:
        return {
            "enabled": False,
            "manufacturer": "",
            "count": 0,
            "model": "",
            "memory": "",
        }
    gpus = g.get("Gpus", [])
    total_count = sum(x.get("Count", 0) for x in gpus) if gpus else g.get("TotalGpuCount") or 0
    model = ""
    manufacturer = ""
    memory_str = ""
    if gpus:
        first = gpus[0]
        model = first.get("Name") or ""
        manufacturer = first.get("Manufacturer") or ""
        mem_mib = first.get("MemoryInfo", {}).get("SizeInMiB")
        if mem_mib:
            memory_str = mib_to_gib_str(mem_mib)
    return {
        "enabled": True,
        "manufacturer": manufacturer,
        "count": total_count,
        "model": model,
        "memory": memory_str,
    }

def on_demand_price_usd_per_hour(pricing, region_code: str, instance_type: str):
    try:
        flt = [
            {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
            {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
            {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
            {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
            {"Type": "TERM_MATCH", "Field": "capacitystatus", "Value": "Used"},
            {"Type": "TERM_MATCH", "Field": "regionCode", "Value": region_code},
        ]
        for attempt in (flt, flt[:-1]):
            pages = paginate(
                pricing,
                "get_products",
                "PriceList",
                ServiceCode="AmazonEC2",
                Filters=attempt,
                MaxResults=100
            )
            for raw in pages:
                prod = json.loads(raw)
                terms = prod.get("terms", {}).get("OnDemand", {})
                for term in terms.values():
                    price_dims = term.get("priceDimensions", {})
                    for dim in price_dims.values():
                        if dim.get("unit") == "Hrs":
                            price_str = dim.get("pricePerUnit", {}).get("USD")
                            if price_str is not None:
                                try:
                                    return Decimal(price_str)
                                except Exception:
                                    pass
        return None
    except botocore.exceptions.BotoCoreError:
        return None

def recent_spot_price_usd_per_hour(ec2, az: str, instance_type: str, lookback_hours=24):
    end = datetime.datetime.utcnow()
    start = end - datetime.timedelta(hours=lookback_hours)
    try:
        resp = ec2.describe_spot_price_history(
            InstanceTypes=[instance_type],
            ProductDescriptions=["Linux/UNIX"],
            AvailabilityZone=az,
            StartTime=start,
            EndTime=end,
            MaxResults=1000
        )
        hist = resp.get("SpotPriceHistory", [])
        if not hist:
            return None
        latest = max(hist, key=lambda x: x["Timestamp"])
        return Decimal(latest["SpotPrice"])
    except botocore.exceptions.BotoCoreError:
        return None

# ---------- main ----------

def main():
    data_str = os.environ.get("INPUT_JSON")
    data = json.loads(data_str)
    if not data:
        raise SystemExit("INPUT_JSON env var is missing or empty")

    # Extract list of zone names
    zones = [z["name"] for z in data["zones"]]
    if not zones:
        raise SystemExit("No zones provided in INPUT_JSON")

    # Optionally set environment variables
    region = data["region"]
    if not region:
        raise SystemExit("No region provided in INPUT_JSON")
        
    family = os.environ.get("FAMILY")
    families = family.split(",") if family else None
    ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
    SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
    spot_hours = int(os.environ.get("SPOT_LOOKBACK_HOURS", "24"))

    if not region or not zones or not family:
        raise SystemExit("Missing required env vars: REGION, ZONE, FAMILY")

    print(f"Finding offered instance types in {zones} ({region}), family '{family}'...", flush=True)

    session = boto3.Session(aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=SECRET_ACCESS_KEY)

    ec2 = session.client("ec2", region_name=region)
    ec2_pricing = session.client("ec2", region_name="us-east-1")
    pricing = session.client("pricing", region_name="us-east-1")
    
    zones_out = []
    for zone in zones:
        print(f"Processing zone: {zone}", flush=True)
        offered = get_offered_instance_types_in_az(ec2, zone)
        print(f"  Found {len(offered)} offered instance types in {zone}", flush=True)
        candidates = filter_by_family(offered, families)
        print(f"  {len(candidates)} match family '{families}'", flush=True)
        described = describe_types(ec2_pricing, sorted(candidates))
        print(f"  Retrieved descriptions for {len(described)} instance types", flush=True)

        for it_name, it_desc in sorted(described.items(), key=lambda kv: kv[0]):
            vcpus = it_desc.get("VCpuInfo", {}).get("DefaultVCpus", 0)
            ram_gib_str = mib_to_gib_str(it_desc.get("MemoryInfo", {}).get("SizeInMiB"))
            gpu_info = extract_gpu_info(it_desc)

            ond = on_demand_price_usd_per_hour(pricing, region, it_name)
            spot = recent_spot_price_usd_per_hour(ec2, zone, it_name, lookback_hours=spot_hours)

            zones_out.append({
                "name": it_name,
                "nameLabel": to_title_label(it_name),
                "zoneName": zone,
                "vcpus": vcpus,
                "ram": ram_gib_str,
                "price": dec_to_str_money(ond),
                "gpu": {
                    "enabled": gpu_info["enabled"],
                    "manufacturer": gpu_info["manufacturer"],
                    "count": gpu_info["count"],
                    "model": gpu_info["model"],
                    "memory": gpu_info["memory"],
                },
                "spot": {
                    "price": dec_to_str_money(spot),
                    "enabled": spot is not None
                }
            })

    output = {
        "region": region,
        "zones": zones_out
    }
    OUTPUT = json.dumps(output)
    print(OUTPUT, flush=True)
    #  print into /dev/termination-log
    with open("/dev/termination-log", "w") as f:
        f.write(OUTPUT + "\n")

if __name__ == "__main__":
    main()
