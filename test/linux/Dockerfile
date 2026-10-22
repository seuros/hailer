# Local OrbStack verification uses the same Ruby/Trixie baseline as Linux CI.
# Build separately with --platform linux/arm64 and --platform linux/amd64.
FROM rust:1.99-trixie AS toolchain

FROM ruby:4.0-trixie
COPY --from=toolchain /usr/local/cargo /usr/local/cargo
COPY --from=toolchain /usr/local/rustup /usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    PATH=/usr/local/cargo/bin:$PATH \
    CARGO_BUILD_JOBS=1 \
    CARGO_INCREMENTAL=0
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /work
