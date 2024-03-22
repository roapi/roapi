ARG RUST_VER=nightly-bullseye-2024-03-01
ARG RUSTFLAGS='-C target-cpu=skylake'
ARG FEATURES="database"

# Step 0: Install cargo-chef
FROM instrumentisto/rust:${RUST_VER} AS chef
# We only pay the installation cost once,
# it will be cached from the second build onwards
RUN cargo +nightly install cargo-chef
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
ARG RUSTFLAGS
ARG FEATURES
WORKDIR /roapi_src
COPY --from=planner /roapi_src/recipe.json recipe.json
RUN RUSTFLAGS=${RUSTFLAGS} \
    cargo +nightly chef cook --features ${FEATURES} --release --recipe-path recipe.json

# Step 3: Build the release binary
FROM chef AS builder
ARG RUSTFLAGS
ARG FEATURES
WORKDIR /roapi_src
COPY ./ /roapi_src
COPY --from=cacher /roapi_src/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
RUN RUSTFLAGS=${RUSTFLAGS} \
    cargo +nightly build --release --locked --bin roapi --features ${FEATURES}

# Step 4: Assemble the final image
FROM debian:bullseye-slim
LABEL org.opencontainers.image.source https://github.com/roapi/roapi

RUN apt-get update \
    && apt-get install -y libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY test_data /test_data
COPY --from=builder /roapi_src/target/release/roapi /usr/local/bin/roapi

EXPOSE 8080
ENTRYPOINT ["roapi"]
