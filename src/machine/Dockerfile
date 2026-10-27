FROM debian:12.11-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential pkg-config libsqlite3-dev libpaho-mqtt-dev librdkafka-dev libcjson-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY src/machine/ ./
RUN make check && make all

FROM debian:12.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsqlite3-0 libpaho-mqtt1.3 librdkafka1 libcjson1 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 machine && useradd --uid 10001 --gid machine --no-create-home machine \
    && mkdir /data && chown machine:machine /data
COPY --from=build /build/build/beer-machine /usr/local/bin/beer-machine
COPY --from=build /build/build/machinectl /usr/local/bin/machinectl
USER 10001:10001
ENV STATE_DB=/data/machine.db SOCKET_PATH=/tmp/machine.sock TIME_SCALE=2 FLUSH_H=0.1
HEALTHCHECK --interval=10s --timeout=4s --start-period=10s CMD ["/usr/local/bin/machinectl", "snapshot"]
ENTRYPOINT ["/usr/local/bin/beer-machine"]
