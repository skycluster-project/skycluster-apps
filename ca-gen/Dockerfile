FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY . .
RUN go build -o ca-gen .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/ca-gen .
CMD ["./ca-gen"]