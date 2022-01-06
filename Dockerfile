FROM registry-mirror.pingcap.net/library/golang:1.17.5-bullseye as builder

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . /build
RUN go test -c ./...


FROM registry-mirror.pingcap.net/library/debian:bullseye

RUN apt -y update && apt -y install wget curl \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/shard-index.test /usr/local/bin/shard-index.test

ENTRYPOINT ["/usr/local/bin/shard-index.test"]

# hub.pingcap.net/test-store/shard-index-test
