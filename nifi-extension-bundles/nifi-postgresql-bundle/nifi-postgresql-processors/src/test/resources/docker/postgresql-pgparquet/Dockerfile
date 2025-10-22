# Multi-stage Dockerfile for PostgreSQL 17.2 with pg_parquet extension
# Stage 1: Builder - Install dependencies and compile pg_parquet
FROM postgres:17.2-bookworm AS builder

# Set version for maintainability
ARG PG_PARQUET_VERSION=main

# Avoid prompts from apt
ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies (let Cargo handle Arrow dependencies)
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Build toolchain
    build-essential \
    cmake \
    git \
    pkg-config \
    curl \
    ca-certificates \
    # PostgreSQL development files
    postgresql-server-dev-17 \
    # System libraries that may be needed for compilation
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Rust and Cargo (required for pg_parquet)
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /build

# --- Build and Install pg_parquet ---
# Clone from the official CrunchyData repository
RUN git clone --branch ${PG_PARQUET_VERSION} --depth 1 https://github.com/CrunchyData/pg_parquet.git
WORKDIR /build/pg_parquet

# Follow official installation instructions from https://github.com/CrunchyData/pg_parquet
# Set environment variables as per their script
ENV CARGO_PGRX_VERSION=0.16.0
ENV PG_MAJOR=17

# Install cargo-pgrx with exact version and flags from their script
RUN cargo install --force --locked cargo-pgrx@"${CARGO_PGRX_VERSION}"

# Configure pgrx with PostgreSQL 17
RUN cargo pgrx init --pg"${PG_MAJOR}" $(which pg_config)

# Build and install the extension with release flag and feature flag
RUN cargo pgrx install --release --features pg"${PG_MAJOR}"

# pg_parquet extension is now installed and ready

# ------------------------------------------------------------------------------

# Stage 2: Final Image - Clean PostgreSQL image with only runtime dependencies
FROM postgres:17.2-bookworm

# Copy the compiled pg_parquet extension from the builder stage
COPY --from=builder /usr/lib/postgresql/17/lib/pg_parquet.so /usr/lib/postgresql/17/lib/
COPY --from=builder /usr/share/postgresql/17/extension/pg_parquet* /usr/share/postgresql/17/extension/

# Copy initialization script to enable pg_parquet extension
COPY init-pg-parquet.sql /docker-entrypoint-initdb.d/

# Configure PostgreSQL to load pg_parquet extension
# Use postgres command line option to set shared_preload_libraries
CMD ["postgres", "-c", "shared_preload_libraries=pg_parquet"]