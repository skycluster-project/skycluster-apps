FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY . .
RUN go build -o ca-signer .
RUN chmod +x ca-signer

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/ca-signer .
CMD ["./ca-signer"]