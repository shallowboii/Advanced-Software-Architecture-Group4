# Build with src/beer_factory as the context:
# docker build -f fermentation_maturation/Dockerfile -t fermentation_maturation .
ARG MACHINE=fermentation_maturation
ARG BINARY=tank-mqtt

FROM gcc:14-bookworm AS build

RUN apt-get update \
 && apt-get install -y --no-install-recommends librdkafka-dev python3 \
 && rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 --branch v1.1.7 https://github.com/LiamBindle/MQTT-C.git /mqtt-c

WORKDIR /app
COPY machine_commons/ machine_commons/
ARG MACHINE
ARG BINARY
ARG TIME_DIVISOR=1
COPY ${MACHINE}/ ${MACHINE}/
WORKDIR /app/${MACHINE}

# The legacy tank tests use an older API; run the current reuse regression.
RUN make test-reuse TIME_DIVISOR=1 \
 && make all MQTT_C_DIR=/mqtt-c TIME_DIVISOR=${TIME_DIVISOR} \
 && cp build/${BINARY} /machine

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends librdkafka1 \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build /machine /usr/local/bin/machine
ENTRYPOINT ["/usr/local/bin/machine"]
CMD ["0"]
