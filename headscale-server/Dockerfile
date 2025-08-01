FROM nixos/nix:2.30.2 AS builder1
WORKDIR /src
RUN git clone https://github.com/juanfont/headscale.git .
ENV NIX_CONFIG='experimental-features = nix-command flakes'
# RUN nix develop --command make generate
# RUN nix develop --command go build -o "headscale" ./cmd/headscale
RUN nix develop --command bash -c '\
    export CGO_ENABLED=0 && \
    export GOOS=linux && \
    export GOARCH=amd64 && \
    make generate && \
    go build -trimpath -ldflags="-s -w -extldflags=-static" -o headscale ./cmd/headscale \
'

FROM alpine:3.22.1
COPY --from=builder1 /src/headscale /headscale
RUN apk add --no-cache ca-certificates

# Add CA cert install script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
