#!/usr/bin/env python3
"""
GCP equivalent of the provided AWS script.

- Reads INPUT_JSON from env with the same structure as your AWS script expects:
  {
    "Region": "us-central1",
    "zones": [{"name": "us-central1-a"}, {"name": "us-central1-b"}]
  }

- Filters machine types by FAMILY env var (comma-separated prefixes, e.g. "n2,e2,c3").
- Outputs, per zone, machine types with vCPUs, RAM, GPU info (best-effort), and prices:
  * On-demand price computed from Cloud Billing Catalog API (core + RAM SKUs).
  * Spot price computed from "Preemptible/Spot" core + RAM SKUs (GCP Spot/Preemptible).
    (GCP Spot pricing is fixed-discount vs on-demand, not market-based.)

Environment variables:
  INPUT_JSON            (required) JSON as above
  FAMILY                (required) comma-separated machine family prefixes (e.g. "n2,e2,c3,a2")
  GCP_PROJECT           (optional) used for quota context (not strictly required here)
  SPOT_LOOKBACK_HOURS   (optional) accepted for parity; unused on GCP (kept for interface compat)
  GOOGLE_APPLICATION_CREDENTIALS (optional) path to a service account key.json, or use ADC.

Auth:
  Uses Application Default Credentials (ADC). You can `gcloud auth application-default login`
  or set GOOGLE_APPLICATION_CREDENTIALS to a service account JSON.

Note:
  - GPU info on GCP is generally attached as an accelerator, not part of machine type.
    As a best-effort, we detect A2 family machine types (e.g., a2-highgpu-4g) and infer GPU count.
  - Pricing mapping relies on best-effort matching of Cloud Billing SKUs to machine families and region.
    If a SKU cannot be found, price fields will be null.
"""

import json
import os
import re
from decimal import Decimal, ROUND_HALF_UP, ROUND_HALF_DOWN
from typing import Dict, List, Optional, Tuple

# ---- GCP SDKs ----
# pip install google-cloud-compute google-api-python-client google-auth
from google.cloud import compute_v1
from google.auth import default as google_auth_default
from google.oauth2 import service_account
from googleapiclient.discovery import build as google_api_build

# --------------- Helpers ---------------

def dec_to_str_money(d: Optional[Decimal]) -> Optional[str]:
    if d is None:
        return None
    return f"${d.quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)}"

# def mib_to_gib_str(mib: Optional[int]) -> Optional[str]:
#     if mib is None:
#         return None
#     gib = Decimal(mib) / Decimal(1024)
#     return f"{gib.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)}GiB"

# def mb_to_gib_str(mb: Optional[int]) -> Optional[str]:
#     if mb is None:
#         return None
#     gib = Decimal(mb) / Decimal(1024)
#     return f"{gib.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)}GiB"

# def mib_to_gb_str(mib: Optional[int]) -> Optional[str]:
#     if mib is None:
#         return None
#     gb = Decimal(mib) / Decimal(1000)  # MiB to GB (decimal)
#     return f"{gb.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)}GB"

def mb_to_gb_str(mb: Optional[int]) -> Optional[str]:
    if mb is None:
        return None
    gb = Decimal(mb) / Decimal(1000)  # MB to GB (decimal)
    gb_rounded = gb.quantize(Decimal('1'), rounding=ROUND_HALF_DOWN)  # nearest whole GB
    return f"{gb_rounded}GB"

# def to_title_label(machine_type_name: str) -> str:
#     return machine_type_name.replace("-", " ").title()

# def parse_input() -> Tuple[str, List[str]]:
#     data_str = os.environ.get("INPUT_JSON")
#     if not data_str:
#         raise SystemExit("INPUT_JSON env var is missing or empty")
#     data = json.loads(data_str)

#     region = os.environ.get("REGION")
#     if not region:
#         raise SystemExit("No region provided in env var REGION")

#     zones = [z["name"] for z in data.get("zones", []) if z.get("name")]
#     if not zones:
#         raise SystemExit("No zones provided in INPUT_JSON")

#     return region, zones

# def get_families() -> List[str]:
#     family = os.environ.get("FAMILY")
#     if not family:
#         raise SystemExit("Missing required env var: FAMILY")
#     return [f.strip().lower() for f in family.split(",") if f.strip()]

def filter_by_family(machine_type_name: str, families: List[str]) -> bool:
    # Accept if the machine type name starts with any of the given family prefixes.
    # Examples: n2-standard-4, e2-standard-2, c3-highcpu-8, a2-highgpu-4g
    lower_name = machine_type_name.lower()
    return any(lower_name.startswith(fam + "-") or lower_name.startswith(fam) for fam in families)

# ---- GPU heuristics for A2 ----

# Regex to extract GPU count for A2-like names (e.g. "a2-highgpu-1g", "a2-megagpu-16g")
_A2_GPU_SUFFIX_RE = re.compile(r'-(\d+)g\b', re.IGNORECASE)

# Map known accelerator type strings (or their last path segment) to short model names.
_ACC_TYPE_TO_MODEL = {
    # A2/A3/A4 families
    "nvidia-a100-80gb": "A100",
    "nvidia-a100-40gb": "A100",
    "nvidia-h100-80gb": "H100",
    "nvidia-h100-mega-80gb": "H100",
    "nvidia-h100-mega": "H100",
    "nvidia-h200-141gb": "H200",
    "nvidia-b200": "B200",
    "nvidia-gb200": "GB200",  # GB200 Superchip

    # G4 / G2 families
    "nvidia-rtx-pro-6000": "RTX PRO 6000",
    "nvidia-rtx-pro-6000-vws": "RTX PRO 6000",
    "nvidia-l4": "L4",
    "nvidia-l4-vws": "L4",

    # N1 attachable models (and common legacy names)
    "nvidia-tesla-t4": "T4",
    "nvidia-tesla-t4-vws": "T4",
    "nvidia-tesla-p4": "P4",
    "nvidia-tesla-p4-vws": "P4",
    "nvidia-tesla-v100": "V100",
    "nvidia-tesla-p100": "P100",
    "nvidia-tesla-p100-vws": "P100",
}


def _normalize_accelerator_type(acc_type: Optional[str]) -> Optional[str]:
    """Return the last path segment of an accelerator type (lowercased), or None."""
    if not acc_type:
        return None
    # Some APIs return a full URL like ".../guestAcceleratorTypes/nvidia-tesla-t4"
    token = acc_type.split("/")[-1].lower()
    return token


def _model_from_acc_type_token(token: str) -> str:
    """Map accelerator type token to the short model. Fallback by stripping common
    prefixes/suffixes and uppercasing the core identifier."""
    if not token:
        return None

    if token in _ACC_TYPE_TO_MODEL:
        return _ACC_TYPE_TO_MODEL[token]

    # Fallback heuristics: remove common prefixes and suffixes
    core = token
    # remove nvidia / nvidia-tesla prefix
    core = re.sub(r'^(nvidia-|nvidia-tesla-)', '', core)
    # remove common suffixes
    core = re.sub(r'(-vws|-80gb|-141gb|-mega|-megas|-superchips)$', '', core)
    # Normalize separators to space for readability (e.g., rtx-pro-6000 -> RTX PRO 6000)
    core = core.replace('-', ' ').strip()
    return core.upper() if core else None


def extract_gpu_info(machine_desc: Dict) -> Dict:
    """
    Extract GPU info from a GCP machine type description dict.

    Logic:
    1. If 'accelerators' field exists, use it to get GPU model and count (preferred).
    2. Otherwise, infer GPU model/count from machine type name for families we recognize
       (A2, A3, A4, G4, G2).
    3. If neither, assume no GPU.
    """
    name = machine_desc.get("name", "") or machine_desc.get("machineType", "")
    name = name.lower()

    # Step 1: Check accelerators field
    accelerators = machine_desc.get("accelerators", []) or machine_desc.get("guestAccelerators", [])
    if accelerators:
        # Use the first accelerator entry (GCP machine types typically have one accelerator type)
        acc = accelerators[0]
        # Accelerator type may appear under different keys depending on the API
        acc_type_raw = acc.get("guestAcceleratorType") or acc.get("acceleratorType") or acc.get("type")
        acc_type_token = _normalize_accelerator_type(acc_type_raw)

        model = _model_from_acc_type_token(acc_type_token) if acc_type_token else None
        count = acc.get("guestAcceleratorCount", acc.get("count", 0)) or 0

        return {
            "enabled": count > 0,
            "manufacturer": "NVIDIA" if count > 0 else None,
            "count": int(count),
            "model": model,
            "memory": None,  # GPU memory is not always provided in the API accelerator object
        }

    # Step 2: Try to infer from machine family/name when accelerators field is not present
    # A2 family -> A100 GPUs; count inferred from suffix like "-1g", "-16g"
    if name.startswith("a2-"):
        m = _A2_GPU_SUFFIX_RE.search(name)
        count = int(m.group(1)) if m else 0
        enabled = count > 0
        return {
            "enabled": enabled,
            "manufacturer": "NVIDIA" if enabled else None,
            "count": count if enabled else 0,
            "model": "A100" if enabled else None,
            "memory": None,
        }

    # A3 family: distinguish Ultra (H200) vs Mega/High/Edge (H100)
    if name.startswith("a3-"):
        # look for keywords in the machine name
        if "ultra" in name:
            model = "H200"
        else:
            # mega, high, edge -> H100
            model = "H100" if any(k in name for k in ("mega", "high", "edge")) else "H100"
        # try to extract a GPU count suffix similarly to A2 if present
        m = re.search(r'-(\d+)g\b', name)
        count = int(m.group(1)) if m else 0
        enabled = count > 0
        return {
            "enabled": enabled,
            "manufacturer": "NVIDIA" if enabled else None,
            "count": count if enabled else 0,
            "model": model if enabled else None,
            "memory": None,
        }

    # A4 family: A4X -> GB200 Superchips, A4 -> B200
    if name.startswith("a4-"):
        # heuristics: 'x' or 'xlarge' or 'x' token in name -> treat as A4X => GB200
        if re.search(r'\b(x|xlarge|x\w*)\b', name):
            model = "GB200"
        else:
            model = "B200"
        m = re.search(r'-(\d+)g\b', name)
        count = int(m.group(1)) if m else 0
        enabled = count > 0
        return {
            "enabled": enabled,
            "manufacturer": "NVIDIA" if enabled else None,
            "count": count if enabled else 0,
            "model": model if enabled else None,
            "memory": None,
        }

    # G4 family -> RTX PRO 6000
    if name.startswith("g4-"):
        m = re.search(r'-(\d+)g\b', name)
        count = int(m.group(1)) if m else 0
        enabled = count > 0
        return {
            "enabled": enabled,
            "manufacturer": "NVIDIA" if enabled else None,
            "count": count if enabled else 0,
            "model": "RTX PRO 6000" if enabled else None,
            "memory": None,
        }

    # G2 family -> L4
    if name.startswith("g2-"):
        m = re.search(r'-(\d+)g\b', name)
        count = int(m.group(1)) if m else 0
        enabled = count > 0
        return {
            "enabled": enabled,
            "manufacturer": "NVIDIA" if enabled else None,
            "count": count if enabled else 0,
            "model": "L4" if enabled else None,
            "memory": None,
        }

    # If nothing matched, assume no GPU information available
    return {
        "enabled": False,
        "manufacturer": None,
        "count": 0,
        "model": None,
        "memory": None,
    }
# ---- Pricing via Cloud Billing Catalog API ----

# Family labels we try to match against SKU descriptions.
# Maps family prefix -> tokens to look for in SKU description strings.
FAMILY_SKU_HINTS = {
    "e2":  ("E2",),
    "n1":  ("N1",),
    "n2":  ("N2",),
    "n2d": ("N2D",),
    "c2":  ("C2",),
    "c2d": ("C2D",),
    "c3":  ("C3",),
    "c3d": ("C3D",),
    "t2d": ("Tau T2D", "T2D"),
    "a2":  ("A2",),  # A2 CPU/RAM still charged, plus separate GPU SKUs if you attach more
}

def get_compute_service_name(billing_service) -> str:
    """
    Find the Cloud Billing 'serviceName' for Compute Engine.
    We discover it at runtime to avoid hardcoding the ID.
    """
    req = billing_service.services().list()
    while req is not None:
        resp = req.execute()
        for svc in resp.get("services", []):
            if svc.get("displayName") == "Compute Engine":
                return svc["name"]  # e.g., 'services/6F81-5844-456A'
        req = billing_service.services().list_next(previous_request=req, previous_response=resp)
    raise RuntimeError("Could not find Cloud Billing service for Compute Engine")

def _unit_price_to_decimal(pricing_info: dict) -> Optional[Decimal]:
    tiers = pricing_info.get("pricingExpression", {}).get("tieredRates", [])
    if not tiers:
        return None
    # Take the first tier per hour USD.
    unit_amount = tiers[0].get("unitPrice", {})
    nanos = unit_amount.get("nanos", 0)
    units = int(unit_amount.get("units", 0))
    return Decimal(units) + (Decimal(nanos) / Decimal(1_000_000_000))

def _region_in_desc(desc: str, region: str) -> bool:
    # SKU descriptions often say "... running in Iowa" or "... in us-central1".
    # Try to match exact region or region pretty-name is not trivial; prefer exact region code.
    r = region.lower()
    d = desc.lower()
    return r in d

def _matches_family(desc: str, fam: str) -> bool:
    fam = fam.lower()
    desc_l = desc.lower()
    hints = FAMILY_SKU_HINTS.get(fam, (fam.upper(),))
    return any(h.lower() in desc_l for h in hints)

def _usage_type_ok(sku: dict, want_spot: bool) -> bool:
    # Cloud Billing uses "usageType" values like "OnDemand", "Preemptible".
    # Spot is the new name; catalog may still say "Preemptible".
    usage = sku.get("category", {}).get("usageType", "")
    if want_spot:
        return usage.lower() in ("preemptible", "spot")
    return usage.lower() == "ondemand"

def _is_core_or_ram(sku: dict) -> Optional[str]:
    """
    Return "core" or "ram" if this SKU is a vCPU/RAM meter; None otherwise.
    """
    desc = sku.get("description", "")
    desc_l = desc.lower()
    if "instance core" in desc_l or "vcpu" in desc_l or "core running" in desc_l:
        return "core"
    if "ram" in desc_l or "memory" in desc_l:
        return "ram"
    return None

def fetch_family_core_ram_prices(
    billing_service,
    compute_service_name: str,
    region: str,
    family_prefix: str,
    want_spot: bool,
) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    """
    Return (core_price_per_hour, ram_price_per_hour) for the given family & region & usage type.
    Prices are hourly per vCPU and per GiB RAM.
    """
    core_price = None
    ram_price = None

    req = billing_service.services().skus().list(
        parent=compute_service_name,
        pageSize=5000,  # big page to reduce pagination churn
    )
    while req is not None and (core_price is None or ram_price is None):
        resp = req.execute()
        for sku in resp.get("skus", []):
            if not _usage_type_ok(sku, want_spot):
                continue
            desc = sku.get("description", "")
            kind = _is_core_or_ram(sku)
            if not kind:
                continue
            if not _region_in_desc(desc, region):
                # Some SKUs are region-agnostic or use location names; also check regions in pricingInfo
                # Try pricingInfo regions
                regions = []
                for pi in sku.get("pricingInfo", []):
                    regions.extend(pi.get("pricingExpression", {}).get("usageUnitDescription", "").lower().split())
                    regions.extend([r.lower() for r in pi.get("aggregationInfo", {}).get("aggregations", [])])
                # (Above is a weak fallback; main path is desc match.)
                # We still allow if desc didn't include region but category/resourceGroup hints match.
                pass

            if not _matches_family(desc, family_prefix):
                continue

            # Extract unit price from pricingInfo
            for pi in sku.get("pricingInfo", []):
                # Some SKUs have region-specific segments; prefer ones whose summary mentions the region
                if region.lower() not in json.dumps(pi).lower() and not _region_in_desc(desc, region):
                    # Keep going; we still may take it if no region-specific match is found.
                    pass
                price = _unit_price_to_decimal(pi)
                if price is None:
                    continue
                if kind == "core" and core_price is None:
                    core_price = price
                if kind == "ram" and ram_price is None:
                    ram_price = price
                if core_price is not None and ram_price is not None:
                    break

            if core_price is not None and ram_price is not None:
                break

        req = billing_service.services().skus().list_next(previous_request=req, previous_response=resp)

    return core_price, ram_price

def estimate_machine_price(
    billing_service,
    compute_service_name: str,
    region: str,
    machine_type_name: str,
    vcpus: int,
    mem_mb: int,
) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    """
    Returns (on_demand_hourly, spot_hourly) as Decimals or None if not found.
    Approximates price = (vCPU_count * per-vCPU) + (GiB_RAM * per-GiB) using SKUs that match the family.
    """
    # Deduce family prefix (segment before first '-'), e.g., 'n2', 'e2', 'c3', 'a2'...
    fam = machine_type_name.split("-", 1)[0].lower()

    # Convert MB to GiB
    ram_gib = Decimal(mem_mb) / Decimal(1024)

    # On-demand
    print(f"    Estimating prices for {machine_type_name} (family {fam})...", flush=True)
    core_price_od, ram_price_od = fetch_family_core_ram_prices(
        billing_service, compute_service_name, region, fam, want_spot=False
    )
    on_demand = None
    if core_price_od is not None and ram_price_od is not None:
        on_demand = (Decimal(vcpus) * core_price_od) + (ram_gib * ram_price_od)

    # Spot / Preemptible
    print(f"    Estimating spot prices for {machine_type_name} (family {fam})...", flush=True)
    core_price_spot, ram_price_spot = fetch_family_core_ram_prices(
        billing_service, compute_service_name, region, fam, want_spot=True
    )
    spot = None
    if core_price_spot is not None and ram_price_spot is not None:
        spot = (Decimal(vcpus) * core_price_spot) + (ram_gib * ram_price_spot)

    return on_demand, spot

# --------------- Main ---------------

def main():
    
    region = os.environ.get("REGION")
    output_path = os.environ.get("OUTPUT_PATH")
    data = json.loads(os.environ.get("INPUT_JSON", "{}"))
    zones = [z["zone"] for z in data["offerings"]]
    families = [f.get("nameLabel") for z in data.get("offerings", []) for f in z.get("zoneOfferings", [])]
    
    spot_hours = int(os.environ.get("SPOT_LOOKBACK_HOURS", "24"))  # parity only; unused

    print(f"Finding offered machine types in {zones} ({region}), family '{','.join(families)}'...", flush=True)

    # Auth
    sa_info = json.loads(os.environ['GCP_SA_JSON'])
    project = sa_info.get('project_id')

    creds = service_account.Credentials.from_service_account_info(sa_info)

    # Clients
    mt_client = compute_v1.MachineTypesClient(credentials=creds)
    billing_service = google_api_build("cloudbilling", "v1", credentials=creds, cache_discovery=False)
    compute_service_name = get_compute_service_name(billing_service)

    zones_out = []

    for zone in zones:
        print(f"Processing zone: {zone}", flush=True)

        # List machine types in zone
        machine_types = list(mt_client.list(project=project, zone=zone))  # project '-' works for public types
        print(f"  Found {len(machine_types)} machine types in {zone}", flush=True)

        # Filter by family prefixes
        candidates = [mt for mt in machine_types if filter_by_family(mt.name, families)]
        print(f"  {len(candidates)} match family '{families}'", flush=True)

        flavors = []
        for mt in sorted(candidates, key=lambda m: m.name):
            vcpus = mt.guest_cpus or 0
            ram_gb_str = mb_to_gb_str(mt.memory_mb)  # memory_mb is in MB
            gpu_info = extract_gpu_info(mt)

            ond_price, spot_price = estimate_machine_price(
                billing_service, compute_service_name, region, mt.name, vcpus, mt.memory_mb or 0
            )

            gpu_enabled = gpu_info.get("enabled", False)
            if gpu_enabled:
                nameLabel = f"{vcpus}vCPU-{ram_gb_str}-{gpu_info['count']}x{gpu_info['model']}-{gpu_info['memory']}"
            else:
                nameLabel = f"{vcpus}vCPU-{ram_gb_str}"

            flavors.append({
                "name": mt.name,
                "nameLabel": nameLabel,
                "vcpus": vcpus,
                "ram": ram_gb_str,
                "price": dec_to_str_money(ond_price),
                "gpu": {
                    "enabled": gpu_info["enabled"],
                    "manufacturer": gpu_info["manufacturer"],
                    "count": gpu_info["count"],
                    "model": gpu_info["model"],
                    "memory": gpu_info["memory"],
                },
                "spot": {
                    "price": dec_to_str_money(spot_price),
                    "enabled": spot_price is not None
                }
            })
        
        zones_out.append({
            "zone": zone,
            "zoneOfferings": flavors
        })

    output = {
        "region": region,
        "offerings": zones_out
    }
    OUTPUT = json.dumps(output)
    print(OUTPUT, flush=True)
    #  print into /dev/termination-log
    with open(output_path, "w") as f:
        f.write(OUTPUT + "\n")

if __name__ == "__main__":
    main()
