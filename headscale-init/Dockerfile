FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY go.mod go.sum main.go ./
RUN go build -o main .
RUN chmod +x main

FROM alpine:3.22.1
WORKDIR /app
COPY --from=builder /app/main .
COPY config.yml  .
COPY acl.json .
CMD ["./main"]