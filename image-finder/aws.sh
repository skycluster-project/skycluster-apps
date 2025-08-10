#!/usr/bin/env bash
set -uo pipefail

: "${INPUT_JSON:?Need INPUT_JSON env var with the input JSON}"
: "${AWS_ACCESS_KEY_ID:?Need AWS_ACCESS_KEY_ID}"
: "${AWS_SECRET_ACCESS_KEY:?Need AWS_SECRET_ACCESS_KEY}"

command -v aws >/dev/null 2>&1 || { echo "aws CLI not found" >&2; exit 2; }
command -v jq >/dev/null 2>&1 || { echo "jq not found" >&2; exit 2; }

OWNER=amazon
ARCH='x86_64'

TOP_REGION="${REGION:-}"
mapfile -t ZONES < <(jq -c '.zones[]' <<<"$INPUT_JSON")

echo "Using AWS region: $TOP_REGION"
echo "Zones to search: ${#ZONES[@]}"

OUT=()
for z in "${ZONES[@]}"; do
  nameLabel=$(jq -r '.nameLabel' <<<"$z")
  mappedLabel=$(python3 label-mapper.py "$nameLabel" --provider aws)
  PATTERN=${mappedLabel:-}
  zone=$(jq -r '.zone' <<<"$z")
  region=${TOP_REGION}

  echo "Searching for AMI with pattern '*${PATTERN}*' in region '$region' zone '$zone' (nameLabel='$nameLabel')"

  # run aws and capture stdout+stderr and exit code
  aws_out=$(AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
            AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
            AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}" \
            aws ec2 describe-images \
              --region "$region" \
              --owners "$OWNER" \
              --filters "Name=name,Values=*${PATTERN}*" "Name=architecture,Values=${ARCH}" "Name=state,Values=available" \
              --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' --output text 2>&1)
  rc=$?

  if [ $rc -ne 0 ]; then
    echo "aws CLI failed for region '$region' zone '$zone' (nameLabel='$nameLabel')" >&2
    echo "$aws_out" >&2
    exit $rc
  fi

  # normalize aws text output -> empty if "None" or "null"
  ami=$(printf '%s' "$aws_out" | tr -d '\r\n')
  if [ -z "$ami" ] || [ "$ami" = "None" ] || [ "$ami" = "null" ]; then
    ami=""
  fi

  echo "Found AMI '$ami' for region '$region' zone '$zone' (nameLabel='$nameLabel')"
  
  out_zone=$(jq -n --arg nl "$nameLabel" --arg zn "$zone" --arg ami "$ami" \
    '{nameLabel:$nl, zone:$zn, name: ($ami//null)}')
  OUT+=("$out_zone")
done

OUTPUT=$(printf '%s\n' "${OUT[@]}" | jq -s --arg region "$TOP_REGION" '{region:$region, zones: .}')

printf '%s\n' "$OUTPUT" 
printf '%s\n' "$OUTPUT" > /dev/termination-log

exit 0