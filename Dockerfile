ARG GO_VERSION=1.27

FROM golang:${GO_VERSION}-alpine AS builder

ARG VERSION=dev

RUN apk --no-cache add git ca-certificates

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w -X github.com/syncgo/syncgo/pkg/cli.version=${VERSION}" -v -o syncgo ./cmd/syncgo

FROM alpine AS runner

WORKDIR /

RUN mkdir /etc/syncgo

COPY --from=builder /app/syncgo /usr/bin/syncgo
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENTRYPOINT ["/usr/bin/syncgo"]
CMD ["run"]
