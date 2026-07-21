FROM rust:1.86-bookworm@sha256:300ec56abce8cc9448ddea2172747d048ed902a3090e6b57babb2bf19f754081 AS rust-builder

WORKDIR /src
COPY rust/scan/Cargo.toml rust/scan/Cargo.lock ./rust/scan/
COPY rust/scan/src ./rust/scan/src
COPY rust/scan/include ./rust/scan/include
RUN cargo build --locked --release --manifest-path rust/scan/Cargo.toml

FROM golang:1.24-bookworm@sha256:1a6d4452c65dea36aac2e2d606b01b4a029ec90cc1ae53890540ce6173ea77ac AS go-builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=rust-builder /src/rust/scan/target/release/libjuno_scan.so ./rust/scan/target/release/libjuno_scan.so
RUN CGO_ENABLED=1 go build -trimpath -ldflags="-s -w" -o /out/juno-scan ./cmd/juno-scan

FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home --home-dir /var/lib/juno-scan juno-scan \
    && chown -R 10001:10001 /var/lib/juno-scan

COPY --from=go-builder /out/juno-scan /usr/local/bin/juno-scan
COPY --from=rust-builder /src/rust/scan/target/release/libjuno_scan.so /usr/local/lib/libjuno_scan.so

ENV LD_LIBRARY_PATH=/usr/local/lib
USER 10001:10001
EXPOSE 8080
VOLUME ["/var/lib/juno-scan"]
ENTRYPOINT ["/usr/local/bin/juno-scan"]
