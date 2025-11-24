# Build stage
FROM rustlang/rust:nightly-bookworm AS builder

WORKDIR /app

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Build the event-router
RUN cargo build --release -p event-router

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/event-router /usr/local/bin/

ENV RUST_LOG=info

CMD ["event-router"]
