#!/bin/sh
set -euo pipefail

echo "[INFO] Starting headscale service in background..."
/headscale serve &
HEADSCALE_PID=$!

# Step 0: Wait for headscale to become responsive
echo "[INFO] Waiting for headscale service to be ready..."
until nc -z localhost 8080; do
  sleep 1
done

echo "[INFO] Headscale service is ready."

sleep 3  # Additional wait to ensure headscale is fully initialized

USER_OUT="/tmp/user.json"
PREAUTH_OUT="/tmp/preauth.json"

# Step 1: Create user
echo "[INFO] Creating user..."
USER_LIST_JSON=$(/headscale users list -o json)

if [ -n "$USER_LIST_JSON" ] && [ "$USER_LIST_JSON" != "null" ]; then
  EXISTING_USER=$(echo "$USER_LIST_JSON" | jq -c '.[] | select(.name=="skycluster")')
else
  EXISTING_USER=""
fi

if [ -n "$EXISTING_USER" ]; then
  echo "[INFO] User 'skycluster' already exists."
  echo "$EXISTING_USER" > "$USER_OUT"
else
  echo "[INFO] Creating user 'skycluster'..."
  /headscale users create skycluster -o json-line > "$USER_OUT"
fi

USER_ID=$(jq -r '.id' "$USER_OUT")

if [ -z "$USER_ID" ]; then
  echo "[ERROR] Failed to extract user ID"
  exit 1
fi
echo "[INFO] Created user with ID: $USER_ID"

# Step 2: Create preauth key
echo "[INFO] Creating preauth key..."
/headscale preauthkeys create --reusable --ephemeral -u "$USER_ID" -e 365d -o json-line > "$PREAUTH_OUT"

# Step 3: Validate output
USER_COUNT=$(jq '.user.id | length' "$PREAUTH_OUT")
if [ "$USER_COUNT" -ne 1 ]; then
  echo "[ERROR] Invalid preauth key output"
  exit 1
fi

API_SERVER="${HEADSCALE_SERVER_URL}"
if [ -z "$API_SERVER" ]; then
  echo "[ERROR] HEADSCALE_SERVER_URL environment variable is not set"
  exit 1
fi

SECRET_NAME="${HEADSCALE_SECRET_NAME}"
if [ -z "$SECRET_NAME" ]; then
  echo "[ERROR] HEADSCALE_SECRET_NAME environment variable is not set"
  exit 1
fi

# Step 4: Create secret
echo "[INFO] Creating Kubernetes secret..."
kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: "$SECRET_NAME"
  namespace: skycluster-system
  labels:
    skycluster.io/managed-by: skycluster
    skycluster.io/secret-type: headscale-connection
type: Opaque
data:
  user.json: $(cat "$USER_OUT" | base64 -w0)
  preauth.json: $(cat "$PREAUTH_OUT" | base64 -w0)
stringData:
  api_server: "$API_SERVER"
  auth_key: "$(jq -r '.key' "$PREAUTH_OUT")"
EOF

echo "[SUCCESS] Secret created successfully."
