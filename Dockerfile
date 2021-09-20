FROM rust:1.55 as builder
WORKDIR /roapi_src
COPY ./ /roapi_src
RUN cargo install --locked --path ./roapi-http --bin roapi-http

FROM debian:buster-slim
LABEL org.opencontainers.image.source https://github.com/roapi/roapi

RUN apt-get update && apt-get install -y libssl-dev && rm -rf /var/lib/apt/lists/*
COPY test_data /test_data
COPY --from=builder /usr/local/cargo/bin/roapi-http /usr/local/bin/roapi-http

EXPOSE 8080
ENTRYPOINT ["roapi-http"]
