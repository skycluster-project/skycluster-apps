#!/bin/sh
set -euo pipefail

echo "[INFO] Waiting for headscale service..."

# Wait until TCP port is ready
until nc -z headscale-server 8080; do
  echo "Headscale not ready, retrying..."
  sleep 3
done

sleep 5  # Additional wait to ensure headscale is fully initialized

USER_OUT="/tmp/user.json"
PREAUTH_OUT="/tmp/preauth.json"

# Step 1: Create user
echo "[INFO] Creating user..."
/headscale users create skycluster -o json-line > "$USER_OUT"

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
USER_COUNT=$(jq '.user | length' "$PREAUTH_OUT")
if [ "$USER_COUNT" -ne 1 ]; then
  echo "[ERROR] Invalid preauth key output"
  exit 1
fi

# Step 4: Create secret
echo "[INFO] Creating Kubernetes secret..."
kubectl create secret generic headscale-init-output -n skycluster-system \
  --from-file=user.json="$USER_OUT" \
  --from-file=preauth.json="$PREAUTH_OUT" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "[SUCCESS] Secret created successfully."
