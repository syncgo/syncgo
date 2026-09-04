ARG GO_VERSION=1.27

FROM golang:${GO_VERSION}-alpine AS builder

RUN apk --no-cache add git ca-certificates

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -v -o syncgo cmd/syncgo/main.go

FROM alpine AS runner

WORKDIR /

RUN mkdir /etc/syncgo

COPY --from=builder /app/syncgo /usr/bin/syncgo
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENTRYPOINT [ "/usr/bin/syncgo" ]
CMD [""]
