#!/usr/bin/env bash
set -euo pipefail

# Exit codes:
# 0 success
# 2 usage / missing env / bad args
# 4 no images matched
# 5 authentication failure
# 6 az cli failure / not installed
# 8 subscription not set / set failed

err() { echo "ERROR: $*" >&2; }
info() { echo "INFO: $*" >&2; }

# --- tool check ---
command -v az >/dev/null 2>&1 || { err "az (Azure CLI) not found"; exit 6; }
command -v python3 >/dev/null 2>&1 || { err "python3 not found (needed for filtering)"; exit 6; }

LOCATION=$(jq -r '.Region' <<<"$INPUT_JSON")
mapfile -t ZONES < <(jq -c '.zones[]' <<<"$INPUT_JSON")

echo "Using region: $LOCATION"
echo "Zones to search: ${#ZONES[@]}"

AZURE_CLIENT_ID=$(jq -r '.clientId // empty' <<<"$AZ_CONFIG_JSON")
AZURE_TENANT_ID=$(jq -r '.tenantId // empty' <<<"$AZ_CONFIG_JSON")
AZURE_CLIENT_SECRET=$(jq -r '.clientSecret // empty' <<<"$AZ_CONFIG_JSON")
AZURE_SUBSCRIPTION_ID=$(jq -r '.subscriptionId // empty' <<<"$AZ_CONFIG_JSON")
# MANAGED_IDENTITY_CLIENT_ID=$(jq -r '.managedIdentityClientId // empty' <<<"$AZ_CONFIG_JSON")

# --- auth ---
set +e
if [[ -n "$AZURE_CLIENT_ID" && -n "$AZURE_TENANT_ID" && -n "$AZURE_CLIENT_SECRET" ]]; then
  az login --service-principal \
    -u "$AZURE_CLIENT_ID" -p "$AZURE_CLIENT_SECRET" --tenant "$AZURE_TENANT_ID" \
    --only-show-errors >/dev/null
  rc=$?
else
  err "Azure authentication failed"
  exit 5
fi
set -e
if [[ $rc -ne 0 ]]; then err "Azure authentication failed"; exit 5; fi

# --- subscription ---
if [[ -n "$AZURE_SUBSCRIPTION_ID" ]]; then
  az account set --subscription "$AZURE_SUBSCRIPTION_ID" --only-show-errors || { err "Failed to set subscription"; exit 8; }
fi
# ensure we have a subscription
az account show --only-show-errors >/dev/null 2>&1 || { err "No active subscription. Set AZURE_SUBSCRIPTION_ID."; exit 8; }


OUT=()
for z in "${ZONES[@]}"; do
  nameLabel=$(jq -r '.nameLabel' <<<"$z")
  mappedLabel=$(python3 label-mapper.py "$nameLabel" --provider azure)
  PATTERN=${mappedLabel:-}
  zone=$(jq -r '.zone' <<<"$z")
  set +e
  JSON=("$(az vm image list --location "$LOCATION" --only-show-errors -o json)")
  set -e
  if [[ $rc -ne 0 ]]; then err "az vm image list failed"; exit 6; fi

  # Fields present: publisher, offer, sku, version, urn
  # We'll match regex across urn/offer/sku.
  # ARCH best-effort: prefer entries whose sku/urn hints arm64/x64.
  IMG_DATA="$(python3 - "$PATTERN" "x86" "$JSON" <<'PY' 
import json, os, re, sys
pattern = sys.argv[1]
arch = (sys.argv[2] or "").lower()
data = json.loads(sys.argv[3])

rx = re.compile(pattern, re.IGNORECASE)
def arch_ok(img):
    text = ":".join([img.get("urn",""), img.get("offer",""), img.get("sku","")]).lower()
    if arch in ("", "any", "auto"): return True
    if arch == "arm64":
        return ("arm" in text) or ("aarch64" in text)
    if arch in ("x64", "amd64"):
        # treat entries mentioning arm as NOT ok
        return ("arm" not in text) and ("aarch64" not in text)
    return True

def version_key(v):
    # Azure image versions are like "24.04.202408010"
    # Split by '.' and compare numerically where possible
    parts = []
    for p in (v or "").split("."):
        try: parts.append(int(p))
        except: parts.append(p)
    return parts

candidates = []
for img in data:
    urn = img.get("urn","")
    offer = img.get("offer","")
    sku = img.get("sku","")
    version = img.get("version","")
    hay = f"{urn} {offer} {sku}"
    if rx.search(hay) and arch_ok(img):
        candidates.append(img)

if not candidates:
    print("", end="")
    sys.exit(0)

# Pick newest by version (descending)
candidates.sort(key=lambda i: version_key(i.get("version","0")), reverse=True)
best = candidates[0]
urn = best.get("urn", "")
sku = best.get("sku", "").lower()
if "gen2" in sku:
    generation = "V2"
else:
    generation = "V1"
print(json.dumps({"urn": urn, "generation": generation}))
PY
)"
  
  URI=$(jq -r '.urn // empty' <<<"$IMG_DATA")
  if [[ -z "$URI" ]]; then
    err "No images matched for pattern '$PATTERN' in zone '$zone'."
    continue
  fi
  generation=$(jq -r '.generation // empty' <<<"$IMG_DATA")

  out_zone=$(jq -n --arg nl "$nameLabel" --arg zn "$zone" --arg URI "$URI" --arg gen "$generation" \
    '{nameLabel:$nl, zone:$zn, name: ($URI//null), generation: ($gen//null)}')
  OUT+=("$out_zone")
done

OUTPUT=$(printf '%s\n' "${OUT[@]}" | jq -s --arg region "$LOCATION" '{Region:$region, zones: .}')

printf '%s\n' "$OUTPUT" 
printf '%s\n' "$OUTPUT" > /dev/termination-log

exit 0
