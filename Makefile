VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS  = -s -w -X main.version=$(VERSION)
IMAGE   ?= pg-feed

.PHONY: build test vet run check docker up down logs clean

build:            ## static binary at ./pg-feed
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o pg-feed ./cmd/pg-feed

test:
	go test ./...

vet:
	go vet ./... && gofmt -l . | (! grep .)

run: build        ## run against config.yaml in this folder
	./pg-feed -config config.yaml

check: build      ## check Postgres prerequisites from config.yaml
	./pg-feed -config config.yaml -check

docker:           ## build the image
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) -t $(IMAGE):latest .

up:               ## local Postgres 18 (wal_level=logical) + the feed, data in ./data
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f pg-feed

clean:
	rm -f pg-feed; rm -rf dist
