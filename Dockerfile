ARG RUST_VER=1.84.1-bookworm
ARG FEATURES="database,ui"

# Step 0: Install cargo-chef
FROM rust:${RUST_VER} AS chef

RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash
RUN cargo binstall trunk

# We only pay the installation cost once,
# it will be cached from the second build onwards
RUN cargo binstall cargo-chef
# install cmake for snmalloc
RUN apt-get update \
    && apt-get install --no-install-recommends -y cmake

# Step 1: Compute a recipe file
FROM chef AS planner
WORKDIR /roapi_src
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Step 2: Cache project dependencies
FROM chef AS cacher
ARG FEATURES
WORKDIR /roapi_src
COPY --from=planner /roapi_src/recipe.json recipe.json
RUN cargo chef cook --features ${FEATURES} --release --recipe-path recipe.json

# Step 3: Build the release binary
FROM chef AS builder
ARG FEATURES
WORKDIR /roapi_src
COPY ./ /roapi_src
COPY --from=cacher /roapi_src/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
RUN cargo build --release --locked --bin roapi --features ${FEATURES}

# Step 4: Assemble the final image
FROM debian:bookworm-slim
LABEL org.opencontainers.image.source https://github.com/roapi/roapi

RUN apt-get update \
    && apt-get install -y libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY test_data /test_data
COPY --from=builder /roapi_src/target/release/roapi /usr/local/bin/roapi

EXPOSE 8080
ENTRYPOINT ["roapi"]
