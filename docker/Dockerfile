# Multi-stage build for all gitstratum binaries
FROM rust:1.75-bookworm AS builder

ARG BINARY=gitstratum-coordinator

WORKDIR /build

RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    libclang-dev \
    cmake \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY bins/ bins/
COPY vendor/ vendor/

RUN mkdir -p .cargo && \
    echo '[source.crates-io]' >> .cargo/config.toml && \
    echo 'replace-with = "vendored-sources"' >> .cargo/config.toml && \
    echo '[source.vendored-sources]' >> .cargo/config.toml && \
    echo 'directory = "vendor"' >> .cargo/config.toml

RUN cargo build --release --bin ${BINARY}

# Runtime stage
FROM gcr.io/distroless/cc-debian12:nonroot

ARG BINARY=gitstratum-coordinator

COPY --from=builder /build/target/release/${BINARY} /usr/local/bin/app

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/app"]
