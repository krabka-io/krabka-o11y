# syntax=docker/dockerfile:1.7
FROM rust:1.97.1-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --locked --release \
    --bin krabka-metrics \
    --bin krabka-metrics-service \
    --bin krabka-traces \
    --bin krabka-profiles \
    --bin krabka-observability \
    --bin krabka-o11y-bootstrap

FROM debian:bookworm-slim
ARG REVISION=unknown
LABEL org.opencontainers.image.source="https://github.com/krabka-io/krabka-o11y" \
      org.opencontainers.image.revision="$REVISION"
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/krabka-metrics \
    /src/target/release/krabka-metrics-service \
    /src/target/release/krabka-traces \
    /src/target/release/krabka-profiles \
    /src/target/release/krabka-observability \
    /src/target/release/krabka-o11y-bootstrap \
    /usr/local/bin/
USER 65532:65532
WORKDIR /data
