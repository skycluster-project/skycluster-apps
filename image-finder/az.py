#!/usr/bin/env python3
"""
Exit codes:
0 success
2 usage / missing env / bad args
4 no images matched
5 authentication failure
6 Azure SDK failure
8 subscription not set / invalid
"""
import json
import os
import re
import sys
from typing import Any, Dict, List, Optional
from label_mapper import map_label 

from azure.identity import ClientSecretCredential
from azure.mgmt.compute import ComputeManagementClient

TERMINATION_LOG = "/dev/termination-log"

def info(msg: str) -> None:
    print(f"INFO: {msg}")

def err(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)

def require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        err(f"Missing required env var {name}")
        sys.exit(2)
    return val

def parse_int_parts(version: str) -> List[Any]:
    parts: List[Any] = []
    for p in (version or "").split("."):
        try:
            parts.append(int(p))
        except ValueError:
            parts.append(p)
    return parts

def choose_best_image(images: List[Dict[str, Any]], pattern: str, arch_hint: str = "x86") -> Optional[Dict[str, Any]]:
    rx = re.compile(pattern, re.IGNORECASE)
    arch = (arch_hint or "").lower()

    def arch_ok(img: Dict[str, Any]) -> bool:
        txt = ":".join([
            img.get("urn", "") or "",
            img.get("offer", "") or "",
            img.get("sku", "") or "",
        ]).lower()
        if arch in ("", "any", "auto"):
            return True
        if arch == "arm64":
            return ("arm" in txt) or ("aarch64" in txt)
        if arch in ("x64", "amd64", "x86", "x86_64"):
            return ("arm" not in txt) and ("aarch64" not in txt)
        return True

    candidates = []
    for img in images:
        hay = f"{img.get('urn','')} {img.get('offer','')} {img.get('sku','')}"
        if rx.search(hay) and arch_ok(img):
            candidates.append(img)

    if not candidates:
        return None

    candidates.sort(key=lambda i: parse_int_parts(i.get("version", "0")), reverse=True)
    return candidates[0]

def list_images_via_sdk(subscription_id: str, location: str) -> List[Dict[str, Any]]:
    """
    Replaces `az vm image list --location <location>`.
    Traverses publishers -> offers -> skus -> versions and returns a flat list
    with fields roughly matching the CLI output we used before.
    """

    # --- auth from AZ_CONFIG_JSON env (already validated by caller) ---
    az_cfg = os.environ.get("AZ_CONFIG_JSON", "{}")
    conf = json.loads(az_cfg)
    client_id = (conf.get("clientId") or "").strip()
    tenant_id = (conf.get("tenantId") or "").strip()
    client_secret = (conf.get("clientSecret") or "").strip()

    try:
        cred = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
    except Exception as e:
        err(f"Failed to create Azure credential: {e}")
        sys.exit(5)

    # --- enumerate images ---
    try:
        compute = ComputeManagementClient(credential=cred, subscription_id=subscription_id)
        
        publisher = "Canonical"

        # Helper: keep only Ubuntu-related offers (classic + modern naming + Pro)
        def is_ubuntu_offer(name: Optional[str]) -> bool:
            if not name:
                return False
            n = name.lower()
            if "pro" in n:
                return False
            if "ubuntu" in n:
                return True
            # Some Canonical offers historically used these exact names
            return n in {
                "ubuntu",
                "ubuntuserver",
            }

        out: List[Dict[str, Any]] = []

        # Fetch Canonical offers and filter to Ubuntu
        offers = compute.virtual_machine_images.list_offers(location, publisher)
        ubuntu_offers = [o.name for o in offers if is_ubuntu_offer(getattr(o, "name", None))]

        for offer in ubuntu_offers:
            skus = compute.virtual_machine_images.list_skus(location, publisher, offer)
            for sk in skus:
                sku = getattr(sk, "name", None)
                if not sku:
                    continue

                # versions: we fetch a reasonable number for recency; adjust if needed
                versions = compute.virtual_machine_images.list(location, publisher, offer, sku, top=2)
                for v in versions:
                    version = getattr(v, "name", None)  # version string like "2024.05.10"
                    if not version:
                        continue
                    urn = f"{publisher}:{offer}:{sku}:{version}"
                    print(f" -- Found image: {urn}")
                    out.append({
                        "publisher": publisher,
                        "offer": offer,
                        "sku": sku,
                        "version": version,
                        "urn": urn,
                    })
        return out
    except Exception as e:
        err(f"Azure SDK failure while listing images: {e}")
        sys.exit(6)

def main() -> None:
    # --- inputs ---
    input_json = require_env("INPUT_JSON")
    try:
        payload = json.loads(input_json)
    except json.JSONDecodeError as e:
        err(f"INPUT_JSON is not valid JSON: {e}")
        sys.exit(2)

    location = payload.get("region")
    zones = payload.get("zones", [])
    if not location or not isinstance(zones, list):
        err("INPUT_JSON must include 'region' and an array 'zones'")
        sys.exit(2)

    az_cfg = os.environ.get("AZ_CONFIG_JSON", "{}")
    try:
        az_conf = json.loads(az_cfg)
    except json.JSONDecodeError as e:
        err(f"AZ_CONFIG_JSON is not valid JSON: {e}")
        sys.exit(2)

    client_id = (az_conf.get("clientId") or "").strip()
    tenant_id = (az_conf.get("tenantId") or "").strip()
    client_secret = (az_conf.get("clientSecret") or "").strip()
    subscription_id = (az_conf.get("subscriptionId") or "").strip()

    print(f"Using region: {location}")
    print(f"Zones to search: {len(zones)}")

    # --- auth presence check (creation happens inside list_images_via_sdk) ---
    if not (client_id and tenant_id and client_secret):
        err("Azure authentication failed (need clientId, tenantId, clientSecret in AZ_CONFIG_JSON)")
        sys.exit(5)
    if not subscription_id:
        err("No active subscription. Set AZURE_SUBSCRIPTION_ID (subscriptionId in AZ_CONFIG_JSON).")
        sys.exit(8)

    # --- list images once (SDK) ---
    images = list_images_via_sdk(subscription_id=subscription_id, location=location)

    any_match = False
    out_zones: List[Dict[str, Any]] = []

    for z in zones:
        name_label = (z.get("nameLabel") or "").strip()
        zone = (z.get("zone") or "").strip()

        mapped_label = map_label(name_label)["azure"]
        pattern = mapped_label or ""

        print(f"Searching for image pattern '{pattern}' in region '{location}' zone '{zone}' (nameLabel='{name_label}')")

        best = choose_best_image(images, pattern=pattern, arch_hint="x86")
        if best is None:
            err(f"No images matched for pattern '{pattern}' in zone '{zone}'.")
            out_zones.append({"nameLabel": name_label, "zone": zone, "name": None, "generation": None})
            continue

        any_match = True
        urn = best.get("urn", "")
        sku = (best.get("sku") or "").lower()
        generation = "V2" if "gen2" in sku else "V1"

        print(f"Found image '{urn}' (generation {generation}) for zone '{zone}' (nameLabel='{name_label}')")

        out_zones.append({
            "nameLabel": name_label,
            "zone": zone,
            "name": urn if urn else None,
            "generation": generation,
        })

    output = {"Region": location, "zones": out_zones}
    out_text = json.dumps(output, separators=(",", ":"))
    print(json.dumps(output, separators=(",", ":"), indent=2))

    # termination log
    try:
        with open(TERMINATION_LOG, "w") as f:
            f.write(out_text + "\n")
    except Exception:
        pass

    if not any_match:
        sys.exit(4)
    sys.exit(0)

if __name__ == "__main__":
    main()
