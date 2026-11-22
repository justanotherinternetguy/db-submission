# Shopping Sessions: one static native Rust binary in an empty image.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Scorers are Intel Xeon Cascade Lake or newer: AVX2 is always there.
# Static, non-PIE: no dynamic loader and no self-relocation at startup.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static -C relocation-model=static"
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/sessions /sessions

FROM scratch
COPY --from=build /sessions /sessions
# Runs as uid 1000 with a read-only root; only /output and /scratch are written.
CMD ["/sessions"]
