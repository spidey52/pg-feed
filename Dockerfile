# Cross-compiles on the build host: the Go stage runs on the builder's own
# platform and targets $TARGETARCH, so `docker buildx build --platform
# linux/amd64,linux/arm64` needs no emulation.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w -X main.version=$VERSION" -o /pg-feed ./cmd/pg-feed \
 && mkdir -p /data

# distroless: no shell, runs as uid 65532 (nonroot). /data is copied with that
# owner so a fresh named volume mounted there is writable.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /pg-feed /pg-feed
COPY --from=build --chown=nonroot:nonroot /data /data
VOLUME /data
EXPOSE 9040
ENTRYPOINT ["/pg-feed", "-config", "/etc/pg-feed/config.yaml"]
