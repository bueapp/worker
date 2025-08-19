# Builder stage
FROM rust:1.85-slim as builder

# Install musl cross toolchain and build deps
RUN apt-get update && apt-get install -y \
  musl-tools \
  build-essential \
  pkg-config \
  libssl-dev \
  && rm -rf /var/lib/apt/lists/*

# Add musl target
RUN rustup target add x86_64-unknown-linux-musl

WORKDIR /usr/src/app
COPY . .

# Build statically linked binary
RUN cargo +nightly build -Z build-std=std,panic_abort --target x86_64-unknown-linux-musl --release

# Final minimal runtime image
FROM debian:bullseye-slim
WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/app/target/x86_64-unknown-linux-musl/release/bue-worker .

CMD ["./bue-worker"]
