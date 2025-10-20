FROM golang:1.24-alpine


CMD mkdir -p /optimizer-helper/vservices
COPY vservices.go /optimizer-helper/vservices/vservices.go

WORKDIR /optimizer-helper/vservices
RUN go mod init main && go mod tidy && go build -o vservices . 