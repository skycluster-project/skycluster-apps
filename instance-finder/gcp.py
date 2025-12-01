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
from typing import Dict, Any, Optional, Tuple, List

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

def to_title_label(machine_type_name: str) -> str:
  return machine_type_name.replace("-", " ").title()

def parse_input() -> Tuple[str, List[str]]:
  data_str = os.environ.get("INPUT_JSON")
  if not data_str:
    raise SystemExit("INPUT_JSON env var is missing or empty")
  data = json.loads(data_str)

  region = os.environ.get("REGION")
  if not region:
    raise SystemExit("No region provided in env var REGION")

  zones = [z["name"] for z in data.get("zones", []) if z.get("name")]
  if not zones:
    raise SystemExit("No zones provided in INPUT_JSON")

  return region, zones

def get_families() -> List[str]:
  family = os.environ.get("FAMILY")
  if not family:
    raise SystemExit("Missing required env var: FAMILY")
  return [f.strip().lower() for f in family.split(",") if f.strip()]

def filter_by_family(machine_type_name: str, families: List[str]) -> bool:
  # Accept if the machine type name starts with any of the given family prefixes.
  # Examples: n2-standard-4, e2-standard-2, c3-highcpu-8, a2-highgpu-4g
  lower_name = machine_type_name.lower()
  return any(lower_name.startswith(fam + "-") or lower_name.startswith(fam) for fam in families)

# ---- GPU heuristics for A2 ----
def extract_gpu_info(mt) -> Dict[str, Any]:
    """
    mt: compute_v1.MachineType object (or similar)
    Returns:
      - enabled: bool
      - manufacturer: str | None
      - count: int
      - model: str | None (canonical: A100, V100, L4, T4, P100, P4, K80, A10G, A30, etc.)
      - memory: int | None (GPU memory in GB)
    Notes:
      - Memory is returned in GB (integer). If source is in MiB/MB it will be converted to GB (rounded).
      - Tries to recognize many common GCP/NVIDIA GPU types and use explicit GPU memory fields when present.
      - For a2 machine types (A100), if GPU memory info exists on the accelerator or machine type it's used.
    """

    def _get_attr_one_of(obj: Any, names):
        for n in names:
            val = getattr(obj, n, None)
            if val is not None:
                return val
        return None

    def _extract_type_str(raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        s = str(raw).lower()
        if "/" in s:
            s = s.rsplit("/", 1)[-1]
        return s

    def _parse_explicit_gb_from_str(s: str) -> Optional[int]:
        # look for explicit "48gb", "40g", "80gb", or just numbers followed by gb/g
        if not s:
            return None
        s = s.lower()
        m = re.search(r"(\d+)\s*(gb|g)\b", s)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
        # sometimes encoded like "a100-80" or "a100_80" - catch trailing numbers
        m2 = re.search(r"(\d{1,3})\b", s)
        if m2:
            # But only accept if we also see 'gb' or a gpu model that commonly has that variant.
            # We'll return None here to avoid false positives unless paired with gb above.
            return None
        return None

    def _model_and_default_memory_from_type_str(ts: str) -> Tuple[Optional[str], Optional[int]]:
        """
        Return (model, default_memory_gb) based on substrings in type_str.
        """
        if not ts:
            return None, None
        ts_low = ts.lower()

        # If explicit GB specified in the type string (like a100-80gb), prefer that.
        explicit_gb = _parse_explicit_gb_from_str(ts_low)
        # mapping of substring -> (MODEL, typical GB)
        mapping = [
            ("a100", ("A100", 40)),   # A100 commonly 40GB on GCP A2; 80GB variant may be present and caught above
            ("a100-80", ("A100", 80)),
            ("a10", ("A10G", 24)),    # A10G ~24GB
            ("a10g", ("A10G", 24)),
            ("a30", ("A30", 24)),     # A30 24GB
            ("l4", ("L4", 24)),       # L4 24GB
            ("v100", ("V100", 16)),   # V100 commonly 16GB (there are 32GB variants)
            ("v100-32", ("V100", 32)),
            ("t4", ("T4", 16)),       # T4 16GB on GCP
            ("p100", ("P100", 16)),   # P100 16GB
            ("p4", ("P4", 8)),        # P4 8GB
            ("k80", ("K80", 12)),     # K80 ~12GB
            ("p40", ("P40", 24)),     # P40 24GB
            ("a2", ("A100", 40)),     # a2-* machines => A100 family (default 40GB)
        ]

        # If explicit GB was found, try to detect model substring anyway
        for key, (model, gb) in mapping:
            if key in ts_low:
                if explicit_gb is not None:
                    return model, explicit_gb
                return model, gb

        # fallback: if explicit GB exists but no known model substring, still return memory but model unknown
        if explicit_gb is not None:
            return None, explicit_gb

        return None, None

    def _extract_memory_gb_from_obj(obj: Any) -> Optional[int]:
        """
        Look for common attribute names containing GPU memory and return GB (int).
        Accepts attributes in GB or MB/MiB forms.
        """
        if obj is None:
            return None

        candidates = [
            ("guest_accelerator_memory_gb", "gb"),
            ("guest_accelerator_memory_mb", "mb"),
            ("guest_accelerator_memory_mib", "mb"),
            ("accelerator_memory_gb", "gb"),
            ("accelerator_memory_mb", "mb"),
            ("accelerator_memory_mib", "mb"),
            ("memory_gb", "gb"),
            ("memory_mb", "mb"),
            ("memory_mib", "mb"),
            ("gpu_memory_gb", "gb"),
            ("gpu_memory_mb", "mb"),
            ("gpu_memory_mib", "mb"),
        ]
        # Try direct attributes first
        for attr, kind in candidates:
            val = getattr(obj, attr, None)
            if val is None:
                continue
            try:
                if isinstance(val, (int, float)):
                    v = float(val)
                else:
                    vs = str(val).strip().lower()
                    # strip units if present
                    vs = re.sub(r"[^\d\.]", "", vs)
                    if not vs:
                        continue
                    v = float(vs)
                if kind == "gb":
                    return int(round(v))
                else:
                    # MB -> convert to GB
                    return int(round(v / 1024.0))
            except Exception:
                continue

        # Try parsing any string-like attributes that might contain "...GB"
        # for convenience, check a few generic attribute names
        generic_attrs = ["description", "display", "type", "name"]
        for attr in generic_attrs:
            val = getattr(obj, attr, None)
            if not val:
                continue
            s = str(val).lower()
            m = re.search(r"(\d+)\s*(gb|g)\b", s)
            if m:
                try:
                    return int(m.group(1))
                except Exception:
                    continue

        return None

    name = getattr(mt, "name", "") or ""
    name_l = str(name).lower()

    # Step 1: prefer explicit accelerators field (list)
    accelerators = getattr(mt, "accelerators", None)
    if accelerators:
        acc = accelerators[0]
        raw_type = _get_attr_one_of(acc, ["guest_accelerator_type", "accelerator_type", "type"])
        accel_type = _extract_type_str(raw_type)
        count_attr = _get_attr_one_of(acc, ["guest_accelerator_count", "accelerator_count", "count"])
        try:
            count = int(count_attr) if count_attr is not None else 0
        except Exception:
            count = 0

        model, default_gb = (None, None)
        if accel_type:
            model, default_gb = _model_and_default_memory_from_type_str(accel_type)

        # prefer explicit memory on accelerator
        mem_gb = _extract_memory_gb_from_obj(acc)
        if mem_gb is None and default_gb is not None:
            mem_gb = default_gb

        enabled = count > 0
        return {
            "enabled": enabled,
            "manufacturer": "NVIDIA" if enabled else None,
            "count": count if enabled else 0,
            "model": model,
            "memory": mem_gb,
        }

    # Step 2: special-case A2 (a2-*) which are A100 machines on GCP
    # pattern commonly like a2-highgpu-1g, a2-highgpu-2g, a2-megagpu-16g
    _A2_GPU_SUFFIX_RE = re.compile(r"a2-(?:.+)-(\d+)g$")
    if name_l.startswith("a2-") or ("a2-" in name_l):
        m = _A2_GPU_SUFFIX_RE.search(name_l)
        count = int(m.group(1)) if m else 0
        enabled = count > 0

        # prefer explicit memory fields on mt (or accelerator if found earlier)
        mem_gb = _extract_memory_gb_from_obj(mt)
        # fallback default for A100 on a2 is commonly 40GB
        if mem_gb is None and enabled:
            mem_gb = 40

        return {
            "enabled": enabled,
            "manufacturer": "NVIDIA" if enabled else None,
            "count": count if enabled else 0,
            "model": "A100" if enabled else None,
            "memory": mem_gb,
        }

    # Step 3: try to infer model and memory from machine-type name (other GPUs encoded)
    model, default_gb = _model_and_default_memory_from_type_str(name_l)
    if model:
        # try to find an explicit count on the machine type object
        count_attr = _get_attr_one_of(mt, ["guest_accelerator_count", "accelerator_count", "count"])
        try:
            count = int(count_attr) if count_attr is not None else 1
        except Exception:
            count = 1

        mem_gb = _extract_memory_gb_from_obj(mt)
        if mem_gb is None and default_gb is not None:
            mem_gb = default_gb

        enabled = count > 0
        return {
            "enabled": enabled,
            "manufacturer": "NVIDIA" if enabled else None,
            "count": count if enabled else 0,
            "model": model,
            "memory": mem_gb,
        }

    # Step 4: no GPU detected
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
