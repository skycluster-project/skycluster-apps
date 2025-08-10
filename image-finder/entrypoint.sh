#!/usr/bin/env bash
set -euo pipefail

: "${PROVIDER:=}"
: "${INPUT_JSON:?INPUT_JSON must be set for provider aws}"
: "${REGION:?REGION must be set for provider aws}"

echo "Starting image-finder for provider='${PROVIDER}'"
echo "Input JSON:"
echo "${INPUT_JSON}"

if [ "${PROVIDER,,}" = "aws" ]; then
  : "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID must be set for provider aws}"
  : "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY must be set for provider aws}"
  # AWS_SESSION_TOKEN is optional and will be forwarded if present
  exec python /app/aws.py
elif [ "${PROVIDER,,}" = "gcp" ]; then
  : "${GOOGLE_CLOUD_PROJECT:?GOOGLE_CLOUD_PROJECT must be set for provider gcp}"
  : "${SERVICE_ACCOUNT_JSON:?SERVICE_ACCOUNT_JSON must be set for provider gcp}"
  exec python /app/gcp.py
elif [ "${PROVIDER,,}" = "azure" ]; then
  : "${AZ_CONFIG_JSON:?AZ_CONFIG_JSON must be set for provider azure}"
  exec python /app/az.py
else
  echo "PROVIDER is not set (PROVIDER='${PROVIDER:-<unset>}'). Nothing to do."
  exit 0
fi