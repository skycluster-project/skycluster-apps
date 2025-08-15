#!/usr/bin/env python3
"""
Azure equivalent of the provided AWS script.

Reads INPUT_JSON from env with the same structure you used before, e.g.:

  {
    "Region": "eastus",
    "zones": [{"name": "1"}, {"name": "2"}, {"name": "3"}]
  }

Environment variables (parity with your AWS script):
  INPUT_JSON              (required) JSON as above
  FAMILY                  (required) comma-separated family prefixes, e.g. "Dv5,Ev5,Fs_v2,NVadsA10v5"
  AZURE_SUBSCRIPTION_ID   (required) your subscription id (for listing VM sizes/SKUs)
  SPOT_LOOKBACK_HOURS     (optional) ignored (kept for interface parity)

Auth:
  Management-plane listing uses Azure credentials. Supported by DefaultAzureCredential.
  Typical local setup:
    az login
  or set a Service Principal with env vars:
    AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_CLIENT_SECRET

What it does (mirrors AWS behavior):
  1) Lists VM sizes (Resource SKUs) available in the specified region.
  2) Filters them by FAMILY prefixes.
  3) Determines which Availability Zones (1/2/3) each size is offered in, and emits an entry per requested zone.
  4) Gathers vCPUs, RAM, and best-effort GPU info from SKU capabilities.
  5) Retrieves on-demand and Spot retail prices via Azure Retail Prices API for Linux (pay-as-you-go).
     (Spot price is not market-based like AWS Spot; it's a discounted retail rate, but can vary over time.)
  6) Prints a JSON payload with the same shape your AWS tool emitted.

Notes:
  * Input zone names should be "1", "2", "3" (or "eastus-1" / "eastus-2" / "eastus-3"); we'll normalize to the digit.
  * Pricing lookup is best-effort. Azure's Retail Prices API has many SKUs; we try to find the most specific Linux item.
    If we cannot match, price fields will be null.
  * GPU model/memory are not consistently present in SKU capabilities. We include what’s available.
"""

import json
import os
import re
import sys
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Iterable, List, Optional, Tuple

# ----- Dependencies -----
# pip install azure-identity azure-mgmt-compute requests

import requests
from azure.identity import ClientSecretCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.compute.models import ResourceSku

# ----------------- Helpers -----------------

def die(msg: str) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(msg)

def dec_to_str_money(d: Optional[Decimal]) -> Optional[str]:
    if d is None:
        return None
    return f"${d.quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)}"

def gib_to_str(gib: Optional[Decimal]) -> Optional[str]:
    if gib is None:
        return None
    return f"{gib.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)} GiB"

def to_title_label(name: str) -> str:
    # "Standard_D4s_v5" -> "Standard D4S V5"
    return re.sub(r"[_\-]", " ", name).replace("  ", " ").title()

def read_families() -> List[str]:
    fam = os.environ.get("FAMILY")
    if not fam:
        die("Missing required env var: FAMILY")
    return [f.strip() for f in fam.split(",") if f.strip()]

def normalize_family_match_tokens(family: str) -> List[str]:
    """
    Azure VM size names look like: Standard_D4s_v5, Standard_E16ads_v5, Standard_NCads_A10_v5, etc.
    We'll consider matches on:
      - raw family (e.g., "Dv5", "Ev5", "Fsv2", "NCads_A10_v5", "NVadsA10v5")
      - case-insensitive
      - optionally prefixed by 'Standard_'
    """
    toks = set()
    f = family.strip()
    toks.add(f)
    toks.add(f"Standard_{f}")
    # Also allow variants without underscores for convenience
    toks.add(f.replace("_", ""))
    toks.add(f"Standard_{f.replace('_', '')}")
    return list(toks)

def size_matches_family(size_name: str, families: List[str]) -> bool:
    s_lower = size_name.lower().replace("_", "")
    for fam in families:
        for tok in normalize_family_match_tokens(fam):
            if s_lower.startswith(tok.lower().replace("_", "")):
                return True
    return False

def get_capability(capabilities: Iterable, key: str) -> Optional[str]:
    for c in capabilities or []:
        if (getattr(c, "name", "") or "").lower() == key.lower():
            return getattr(c, "value", None)
    return None

def parse_gpu_info_from_sku(sku: ResourceSku) -> Dict:
    # Common capability names: "GPUs", sometimes a "GpuName" / "GpuModel"
    gpus = get_capability(sku.capabilities, "GPUs")
    model = get_capability(sku.capabilities, "GpuName") or get_capability(sku.capabilities, "GpuModel")
    mem = get_capability(sku.capabilities, "GpuMemoryGb") or get_capability(sku.capabilities, "GPU_Memory_GB")
    try:
        count = int(gpus) if gpus is not None else 0
    except Exception:
        count = 0
    return {
        "enabled": count > 0,
        "manufacturer": None if count == 0 else "NVIDIA",  # Azure current GPU SKUs are NVIDIA-based
        "count": count,
        "model": model,
        "memory": (f"{mem} GiB" if mem else None),
    }

def sku_supported_zones(sku: ResourceSku, region: str) -> List[str]:
    zones: List[str] = []
    for li in sku.location_info or []:
        if (li.location or "").lower() != region.lower():
            continue
        for z in (li.zones or []):
            zones.append(str(z))
    # Deduplicate, keep as strings "1","2","3"
    return sorted({z for z in zones})

# ----------------- Pricing (Azure Retail Prices API) -----------------

RETAIL_API = "https://prices.azure.com/api/retail/prices"

def _retail_iter(params: Dict[str, str]) -> Iterable[dict]:
    """
    Iterate the Azure Retail Prices API with simple query params.
    The API uses OData-style $filter inside the querystring.
    """
    url = RETAIL_API
    while url:
        # print(f"Params: {params}", flush=True)
        resp = requests.get(url, params=params if url == RETAIL_API else None, timeout=10,verify=False)
        # print(f"  Status: {resp.status_code}", flush=True)
        resp.raise_for_status()
        data = resp.json()
        for item in data.get("Items", []):
            yield item
        url = data.get("NextPageLink")

def short_size_name(size_name: str) -> str:
    """
    Azure retail API often uses 'skuName' like 'D4s v5', 'E16ads v5', etc.
    Convert 'Standard_D4s_v5' -> 'D4s v5'
    """
    s = size_name
    s = s.replace("Standard_", "")
    s = s.replace("_", " ")
    # ensure " v" spacing exists (already achieved by replacing "_")
    return s

def retail_price_for_size(region: str, size_name: str, spot: bool) -> Optional[Decimal]:
    """
    Best-effort retail price lookup for Linux VM in a region.
    We:
      - filter by serviceName eq 'Virtual Machines'
      - armRegionName eq <region>
      - priceType eq 'Consumption'
      - productName contains 'Spot' iff spot=True
      - skuName contains the normalized size (e.g. 'D4s v5')
      - operatingSystem is 'Linux' (when present)
    Returns the *lowest* unitPrice among matching meters.
    """
    sshort = short_size_name(size_name)
    print(f"Looking up retail price for size '{size_name}' (as '{sshort}') in region '{region}', spot={spot}", flush=True)

    # Build $filter
    bits = [
        "serviceName eq 'Virtual Machines'",
        f"armRegionName eq '{region.lower()}'",
        "priceType eq 'Consumption'",
        f"contains(skuName, '{sshort}')",
    ]
    if spot:
        bits.append("contains(meterName, 'Spot')")
    
    # OS filter is sometimes in 'operatingSystem'
    # # We include it but don't rely solely on it.
    # # (If it filters out too much, we still have size/region/productName gates.)
    # bits.append("(operatingSystem eq 'Linux' or operatingSystem eq null)")

    q = " and ".join(bits)
    params = {"$filter": q}
    
    best: Optional[Decimal] = None
    item_iter = _retail_iter(params)
    for item in item_iter:
        print(f"  Found item: [{item.get('skuName')}], meterName: [{item.get('meterName')}] with price {item.get('unitPrice')}", flush=True)
        # Extra defensive checks:
        if item.get("serviceName") != "Virtual Machines":
            print(f"  Skipping item with unexpected serviceName: {item.get('serviceName')}", flush=True)
            continue
        # Skip if windows OS
        if "windows" in (item.get("productName") or "").lower():
            print(f"  Skipping Windows item: {item.get('productName')}", flush=True)
            continue
        if (item.get("armRegionName") or "").lower() != region.lower():
            print(f"  Skipping item in different region: {item.get('armRegionName')}", flush=True)
            continue
        if spot and "spot" not in (item.get("meterName") or "").lower():
            print(f"  Skipping non-spot item: {item.get('meterName')}", flush=True)
            continue
        if (not spot) and any(x in (item.get("meterName") or "").lower() for x in ("spot", "low priority")):
            print(f"  Skipping spot/low priority item: {item.get('meterName')}", flush=True)
            continue
        if sshort.lower() not in (item.get("skuName") or "").lower():
            print(f"  Skipping item with different SKU: {item.get('skuName')}", flush=True)
            continue
        if item.get("unitOfMeasure") not in ("1 Hour", "Hour", "hours", "1 hour"):
            print(f"  Skipping item with unexpected unitOfMeasure: {item.get('unitOfMeasure')}", flush=True)
            continue

        price = item.get("unitPrice")
        if price is None:
            print(f"  Skipping item with no unitPrice: {item}", flush=True)
            continue
        try:
            dec = Decimal(str(price))
        except Exception:
            print(f"  Skipping item with invalid unitPrice: {item}", flush=True)
            continue
        if best is None or dec < best:
            best = dec

    return best

# ----------------- Main -----------------

def main():
    region = os.environ.get("REGION")
    output_path = os.environ.get("OUTPUT_PATH")

    data = json.loads(os.environ.get("INPUT_JSON", "{}"))
    zones = [z["zone"] for z in data["offerings"]]
    families = [f.get("nameLabel") for z in data.get("offerings", []) for f in z.get("zoneOfferings", [])]
    
    azure_cfg = os.environ.get("AZ_CONFIG_JSON")
    azure_cred = json.loads(azure_cfg) if azure_cfg else None
    subscription_id = azure_cred.get("subscriptionId") if azure_cred else None
    if not subscription_id:
        die("Missing required env var: AZURE_SUBSCRIPTION_ID")

    # Auth for management plane
    tenant = azure_cred.get("tenant_id") or azure_cred.get("tenantId")
    client = azure_cred.get("client_id") or azure_cred.get("clientId")
    secret = azure_cred.get("client_secret") or azure_cred.get("clientSecret")

    cred = ClientSecretCredential(tenant_id=tenant, client_id=client, client_secret=secret)
    compute_client = ComputeManagementClient(cred, subscription_id)

    print(f"Finding offered VM sizes in zones {zones} ({region}), family '{','.join(families)}'...", flush=True)
    
    # List all Resource SKUs for this region
    print("Querying Azure Resource SKUs (this can take ~10-30s)...", flush=True)
    skus: List[ResourceSku] = [
        s for s in compute_client.resource_skus.list(filter=f"location eq '{region}'")
        if (s.resource_type or "").lower() == "virtualmachines"
    ]
    print(f"  Found {len(skus)} VM SKUs in {region}", flush=True)

    # Filter by family
    skus = [s for s in skus if size_matches_family(s.name or "", families)]
    print(f"  {len(skus)} match family '{families}'", flush=True)

    # zones_out: List[dict] = []
    zone_flavors: Dict[str, List[dict]] = {}

    for sku in sorted(skus, key=lambda s: s.name or ""):
        size_name = sku.name or ""
        print(f"Processing size: {size_name}", flush=True)

        # Availability Zones this size supports in the region
        supported_zones = sku_supported_zones(sku, region)
        print(f"  Size '{size_name}' supports zones: {supported_zones}", flush=True)
        if not supported_zones:
            # Some sizes don't advertise zones or not available in zones. Skip.
            continue

        # Capabilities
        vcpus_str = get_capability(sku.capabilities, "vCPUs") or get_capability(sku.capabilities, "vCPUS")
        mem_gb = get_capability(sku.capabilities, "MemoryGB") or get_capability(sku.capabilities, "MemoryGb")
        mem_gb_str = f"{int(round(float(mem_gb)))}GB" if mem_gb is not None else None
        generation = get_capability(sku.capabilities, "HyperVGenerations") 
        try:
            vcpus = int(float(vcpus_str)) if vcpus_str else 0
        except Exception:
            vcpus = 0
        # mem_gib = Decimal(mem_gb_str) if mem_gb_str is not None else None

        gpu_info = parse_gpu_info_from_sku(sku)
        print(f"  Size '{size_name}' has {vcpus} vCPUs, {mem_gb_str} RAM, GPU: {gpu_info}", flush=True)

        # # Prices (once per size; reused across zones)
        try:
            print(f"  Looking up prices for size '{size_name}' in region '{region}'...", flush=True)
            ond = retail_price_for_size(region, size_name, spot=False)
        except Exception:
            ond = None
        try:
            print(f"  Looking up Spot price for size '{size_name}' in region '{region}'...", flush=True)
            spot = retail_price_for_size(region, size_name, spot=True)
        except Exception:
            spot = None

        # Emit an entry for each requested zone that is supported by this size
        for z_req in zones:
            if z_req not in zone_flavors:
                zone_flavors[z_req] = []
            if z_req not in supported_zones:
                continue
            zone_flavors[z_req].append({
                "name": size_name,
                "nameLabel": f"{vcpus}vCPU-{mem_gb_str}",
                "vcpus": vcpus,
                "ram": f"{mem_gb_str}",
                "price": dec_to_str_money(ond),
                "generation": generation,
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
        "offerings": [{"zone": z, "zoneOfferings": zv} for z, zv in zone_flavors.items()]
    }
    # print(json.dumps(output, indent=2))
    OUTPUT = json.dumps(output)
    print(OUTPUT, flush=True)
    #  print into /dev/termination-log
    with open(output_path, "w") as f:
        f.write(OUTPUT + "\n")

if __name__ == "__main__":
    main()
