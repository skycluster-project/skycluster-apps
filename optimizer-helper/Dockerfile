# syntax=docker/dockerfile:1
FROM golang:1.24-alpine AS builder
WORKDIR /src/vservices

# Copy module files first to leverage layer caching (add go.mod/go.sum to repo for best caching)
COPY vservices/go.mod vservices/go.sum ./
RUN apk add --no-cache git ca-certificates && \
    update-ca-certificates && \
    go env -w GOPROXY=https://proxy.golang.org,direct && \
    go mod download

# Copy source and build a static, stripped binary
COPY vservices/vservices.go ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /vservices ./vservices.go

# Final tiny image
FROM busybox:1.36
# copy certs in case TLS/networking is used by the binary
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /vservices /vservices

# run as non-root user (UID 65532 = nobody on many distros)
#USER 65532
#ENTRYPOINT ["/bin/sh"]


#CMD mkdir -p /optimizer-helper/vservices
#COPY vservices.go /optimizer-helper/vservices/vservices.go

#WORKDIR /optimizer-helper/vservices
#RUN go mod init main && go mod tidy && go build -o vservices . 
