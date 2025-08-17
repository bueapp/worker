FROM rust:1.85-slim as builder

RUN apt-get update && apt-get install -y \
  build-essential \
  pkg-config \
  libssl-dev \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app
COPY . .
RUN cargo build --release

FROM debian:bullseye-slim
WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/app/target/release/bue-worker .

CMD ["./bue-worker"]
