# Shopping Sessions: a single native Rust binary.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Scorers are Intel Xeon Cascade Lake or newer: AVX2 is always there.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /src/target/release/sessions /app/sessions
# Runs as uid 1000 with a read-only root; only /output and /scratch are written.
CMD ["/app/sessions"]
