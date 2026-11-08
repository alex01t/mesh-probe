FROM rust:1-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
# Build a dummy binary first so dependency compilation is cached separately
# from application source changes.
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs
RUN cargo build --release
RUN rm -rf src
COPY src ./src
RUN cargo build --release
FROM debian:bookworm-slim
RUN apt-get update -qqq && apt-get install -qqq procps net-tools curl vim
COPY --from=builder /app/target/release/k8s-tcp-mesh-probe /usr/local/bin/network-probe
ENTRYPOINT ["/usr/local/bin/network-probe"]
