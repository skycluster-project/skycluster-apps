FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY . .
RUN go build -o ca-gen .
RUN chmod +x ca-gen

FROM alpine:3.22.1
WORKDIR /app
COPY --from=builder /app/ca-gen .
CMD ["./ca-gen"]