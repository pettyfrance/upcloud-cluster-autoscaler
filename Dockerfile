# --- Build ---
# Debian 13 (trixie), to match the runtime image's glibc.
FROM rust:1.100-trixie AS build

# protoc compiles proto/externalgrpc.proto in build.rs; libprotobuf-dev has the
# google/protobuf/*.proto files it imports.
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN cargo build --release --locked

# --- Runtime ---
# distroless/cc: glibc and CA certificates (for HTTPS to api.upcloud.com),
# no shell. :nonroot runs as an unprivileged user.
FROM gcr.io/distroless/cc-debian13:nonroot

COPY --from=build /app/target/release/upcloud-cluster-autoscaler /upcloud-cluster-autoscaler

ENTRYPOINT ["/upcloud-cluster-autoscaler"]
