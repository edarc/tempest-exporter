FROM debian:bullseye AS compile-image

RUN apt update
RUN apt install -y --no-install-recommends build-essential libssl-dev curl ca-certificates pkg-config
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

WORKDIR /root
RUN ln -s /root/.cargo/bin/cargo /usr/local/bin/cargo && \
    ln -s /root/.cargo/bin/rustc /usr/local/bin/rustc && \
    mkdir /root/tempest-exporter

COPY src tempest-exporter/src
COPY Cargo.toml tempest-exporter/Cargo.toml
COPY Cargo.lock tempest-exporter/Cargo.lock
RUN cd tempest-exporter && cargo build --release && cd ..


FROM debian:bullseye AS runtime-image
LABEL image.name="tempest-exporter"

RUN apt update
RUN apt install -y --no-install-recommends libssl1.1

WORKDIR /root
COPY --from=compile-image /root/tempest-exporter/target/release/tempest-exporter .

USER root
EXPOSE 8080/tcp
ENTRYPOINT [ "/root/tempest-exporter" ]
