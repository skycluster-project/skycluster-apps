#!/bin/sh

# If the CA cert exists, install it
if [ -f /etc/headscale/ca.crt ]; then
    cp /etc/headscale/ca.crt /usr/local/share/ca-certificates/my-root-ca.crt
    update-ca-certificates
fi

# Run headscale
exec /headscale "$@"
