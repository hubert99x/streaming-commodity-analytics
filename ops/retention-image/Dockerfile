# Stage 1: build patched gosu with a newer Go toolchain
FROM golang:1.25.8-bookworm AS gosu-builder

WORKDIR /src

RUN apt-get update \
 && apt-get install -y --no-install-recommends git ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Build gosu from source using a Go version that contains the fix
RUN git clone https://github.com/tianon/gosu.git . \
 && (git checkout 1.19 || git checkout v1.19 || git checkout refs/tags/1.19 || git checkout refs/tags/v1.19) \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/gosu .

# Stage 2: final retention image
FROM postgres:16.6

RUN apt-get update \
 && apt-get upgrade -y \
 && rm -rf /var/lib/apt/lists/*

# Replace gosu binary with a rebuild compiled on a patched Go version
COPY --from=gosu-builder /out/gosu /usr/local/bin/gosu

RUN chmod 0755 /usr/local/bin/gosu \
 && gosu nobody true

WORKDIR /ops

COPY run_retention.sh /ops/run_retention.sh

RUN chmod 0755 /ops/run_retention.sh

ENTRYPOINT ["/bin/sh", "-c"]
CMD ["/ops/run_retention.sh"]