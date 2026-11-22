# Shopping Sessions: one static native Rust binary in an empty image,
# optimized with a profile from a generated training set (PGO).
FROM rust:1.100.0-bookworm AS build
RUN rustup component add llvm-tools
WORKDIR /src
# The published dataset generator (vendored), for the training run only.
COPY tools/generator ./tools/generator
RUN cargo build --release --locked --manifest-path tools/generator/Cargo.toml --target-dir /gen
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Scorers are Intel Xeon Cascade Lake or newer: AVX2 is always there.
# Static, non-PIE: no dynamic loader and no self-relocation at startup.
ENV FLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static -C relocation-model=static"
ENV TARGET=x86_64-unknown-linux-gnu
# 1. instrumented build, 2. run it on a fresh 3M-event dataset, 3. rebuild with the profile.
RUN RUSTFLAGS="$FLAGS -Cprofile-generate=/pgo" cargo build --release --locked --target $TARGET --target-dir /instr \
 && /gen/release/generate --seed 20261122 --events 3000000 --clicks 750000 --output /train \
 && DATA_DIR=/train OUTPUT_DIR=/train-out NCPU=4 /instr/$TARGET/release/sessions \
 && "$(rustc --print sysroot)/lib/rustlib/$TARGET/bin/llvm-profdata" merge -o /pgo.profdata /pgo \
 && rm -rf /train /train-out /instr \
 && RUSTFLAGS="$FLAGS -Cprofile-use=/pgo.profdata" cargo build --release --locked --target $TARGET \
 && cp target/$TARGET/release/sessions /sessions

FROM scratch
COPY --from=build /sessions /sessions
# Runs as uid 1000 with a read-only root; only /output and /scratch are written.
CMD ["/sessions"]
