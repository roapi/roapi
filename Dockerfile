FROM instrumentisto/rust:nightly-bullseye-2022-09-24 AS builder
WORKDIR /roapi_src
COPY ./ /roapi_src
RUN apt-get update \
    && apt-get install --no-install-recommends -y cmake

RUN RUSTFLAGS='-C target-cpu=skylake' \
    cargo +nightly install --locked --git https://github.com/roapi/roapi --branch main --bins roapi --features "simd database"

FROM debian:bullseye-slim
LABEL org.opencontainers.image.source https://github.com/roapi/roapi

RUN apt-get update && apt-get install -y libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*
COPY test_data /test_data
COPY --from=builder /usr/local/cargo/bin/roapi /usr/local/bin/roapi

EXPOSE 8080
ENTRYPOINT ["roapi"]
