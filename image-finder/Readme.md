# Image Finder

Only LTS versions are supported.

```bash
export INPUT_JSON='{
  "Region":"TO_BE_FILLED",
  "zones":[
    {"nameLabel":"ubuntu-24.04","zone":"TO_BE_FILLED"},
    {"nameLabel":"ubuntu-22.04","zone":"TO_BE_FILLED"},
    {"nameLabel":"ubuntu-20.04","zone":"TO_BE_FILLED"}
  ]
}'
```

## AWS 

```bash

export INPUT_JSON=$(echo "$INPUT_JSON" | jq '.Region = "us-west-2" | .zones[].zone = "us-west-2a"')

sudo docker run --rm \
  -e PROVIDER=aws \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  -e INPUT_JSON="$INPUT_JSON" image-finder:latest

```

## GCP

```bash
export INPUT_JSON=$(echo "$INPUT_JSON" | jq '.Region = "us-east1" | .zones[].zone = "us-east-1a"')
export GOOGLE_CLOUD_PROJECT="PROJECT_ID"
export SERVICE_ACCOUNT_KEY_FILE="/path.json"
export SERVICE_ACCOUNT_JSON=$(cat $SERVICE_ACCOUNT_KEY_FILE)

sudo docker run --rm \
  -e PROVIDER=gcp \
  -e GOOGLE_CLOUD_PROJECT=$GOOGLE_CLOUD_PROJECT \
  -e SERVICE_ACCOUNT_JSON="$SERVICE_ACCOUNT_JSON" \
  -e INPUT_JSON="$INPUT_JSON" image-finder:latest

```

## Azure

```bash

export INPUT_JSON=$(echo "$INPUT_JSON" | jq '.Region = "centralus" | .zones[].zone = "1"')

export AZ_CONFIG_PATH="/config.json"
export AZ_CONFIG_JSON=$(cat $AZ_CONFIG_PATH)

sudo docker run --rm -v /tmp/folder:/root/folder \
  -e PROVIDER=azure \
  -e AZ_CONFIG_JSON="$AZ_CONFIG_JSON" \
  -e INPUT_JSON="$INPUT_JSON" image-finder:latest
```