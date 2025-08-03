#!/bin/sh

# If the CA cert exists, install it
if [ -f /etc/headscale/ca.crt ]; then
    cp /etc/headscale/ca.crt /usr/local/share/ca-certificates/my-root-ca.crt
    update-ca-certificates
fi

# Debug purpose: Uncomment the next line to keep the container running for debugging
# tail -f /dev/null

# Run headscale
exec /headscale "$@"
