# Build from src/beer_factory (the heartbeat library lives next to this folder):
#   cd src/beer_factory
#   docker build -f lautering/Dockerfile -t lautering .
#   docker run --rm lautering 0        # argument = machine number

ARG MACHINE=lautering
ARG BINARY=lautering


# ---- Stage 1: build and test ------------------------------------------------
FROM gcc:14 AS build
ARG MACHINE
ARG BINARY

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends librdkafka-dev \
 && rm -rf /var/lib/apt/lists/*

# MQTT-C sources; the Makefile compiles them in via MQTT_C_DIR.
RUN git clone --depth 1 https://github.com/LiamBindle/MQTT-C.git /mqtt-c

COPY machine_commons/machine_mqtt/ machine_commons/machine_mqtt/
COPY machine_commons/machine_kafka/ machine_commons/machine_kafka/
COPY ${MACHINE}/ ${MACHINE}/

WORKDIR /app/${MACHINE}

# Tests first: a failing test stops the image build.
RUN make clean 2>/dev/null || true \
 && make test \
 && make all MQTT_C_DIR=/mqtt-c

RUN cp build/${BINARY} /machine


# ---- Stage 2: runtime -------------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends librdkafka1 \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build /machine /usr/local/bin/machine

ENTRYPOINT ["machine"]
CMD ["0"]
