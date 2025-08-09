#!/usr/bin/env bash
set -euo pipefail

# -------- Exit codes --------
# 0  success
# 2  usage / missing required env
# 3  zone not in region
# 4  no images matched
# 5  auth failure
# 6  gcloud not available/failed
# 7  jq not available
# 8  project not set
# 9  unknown error

err() { echo "ERROR: $*" >&2; }
info() { echo "INFO: $*" >&2; }

# -------- Required tools --------
command -v gcloud >/dev/null 2>&1 || { err "gcloud not found"; exit 6; }
command -v jq >/dev/null 2>&1 || { err "jq not found"; exit 7; }

# Required:
GOOGLE_CLOUD_PROJECT="${GOOGLE_CLOUD_PROJECT:-}" 
TOP_REGION=$(jq -r '.Region' <<<"$INPUT_JSON")
mapfile -t ZONES < <(jq -c '.zones[]' <<<"$INPUT_JSON")

echo "Using region: $TOP_REGION"
echo "Zones to search: ${#ZONES[@]}"

ARCH="amd64"

# Auth options:
# 1) SERVICE_ACCOUNT_JSON contains the JSON key inline
SERVICE_ACCOUNT_JSON="${SERVICE_ACCOUNT_JSON:-}"
# 2) SERVICE_ACCOUNT_KEY_FILE points to a mounted JSON key file
SERVICE_ACCOUNT_KEY_FILE="${SERVICE_ACCOUNT_KEY_FILE:-}"


# -------- Auth --------
activate_sa() {
  local keyfile="$1"
  # info "Activating service account with key file: $keyfile"
  if ! gcloud auth activate-service-account --key-file="$keyfile" 1>/dev/null; then
    err "Service account activation failed."
    exit 5
  fi
}

did_auth="false"
tmpkey=""
cleanup() { [[ -n "$tmpkey" && -f "$tmpkey" ]] && rm -f "$tmpkey"; }
trap cleanup EXIT

if [[ -n "$SERVICE_ACCOUNT_JSON" ]]; then
  tmpkey="$(mktemp /tmp/gcp-key.XXXXXX.json)"
  printf '%s' "$SERVICE_ACCOUNT_JSON" > "$tmpkey"
  activate_sa "$tmpkey"
  did_auth="true"
elif [[ -n "$SERVICE_ACCOUNT_KEY_FILE" && -f "$SERVICE_ACCOUNT_KEY_FILE" ]]; then
  activate_sa "$SERVICE_ACCOUNT_KEY_FILE"
  did_auth="true"
else
  exit 5
fi

# -------- Project --------
if [[ -n "$GOOGLE_CLOUD_PROJECT" ]]; then
  gcloud config set project "$GOOGLE_CLOUD_PROJECT" 1>/dev/null || { err "Failed to set project"; exit 8; }
fi

# Verify we have an effective project (needed for 'gcloud compute images list' scope)
if ! gcloud config get-value project --quiet >/dev/null 2>&1; then
  err "No project is set. Set GOOGLE_CLOUD_PROJECT."
  exit 8
fi

OUT=()
for z in "${ZONES[@]}"; do
  nameLabel=$(jq -r '.nameLabel' <<<"$z")
  mappedLabel=$(python3 label-mapper.py "$nameLabel" --provider gcp)
  PATTERN="$mappedLabel"
  zone=$(jq -r '.zone' <<<"$z")
  region=${TOP_REGION}

  FILTER="(name~'${PATTERN}' OR family~'${PATTERN}') AND status=READY AND architecture=X86_64"

  set +e
  JSON=$(gcloud compute images list \
      --filter="${FILTER}" \
      --sort-by="~creationTimestamp" \
      --limit=1 \
      --format=json 2>&1)
  gcloud_rc=$?
  set -e
  if [[ $gcloud_rc -ne 0 ]]; then
    err "gcloud failed while listing images."
    exit 6
  fi

  echo "$JSON"
  
  URI="$(jq -r 'sort_by(.creationTimestamp) | reverse | .[0] | "\(.selfLink)"' <<<"$JSON" | sed -E 's#^https?://www.googleapis.com/compute/v1/##')"
  out_zone=$(jq -n --arg nl "$nameLabel" --arg zn "$zone" --arg URI "$URI" \
    '{nameLabel:$nl, zone:$zn, name: ($URI//null)}')
  OUT+=("$out_zone")

done

OUTPUT=$(printf '%s\n' "${OUT[@]}" | jq -s --arg region "$TOP_REGION" '{Region:$region, zones: .}')

printf '%s\n' "$OUTPUT" 
printf '%s\n' "$OUTPUT" > /dev/termination-log

exit 0
