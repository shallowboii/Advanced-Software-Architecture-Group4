# Build/test image for the C machines (Windows, or any host without the
# libraries). Used by run.sh; built once, then cached by Docker.
FROM gcc:14

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      librdkafka-dev libcjson-dev pkg-config python3 \
 && rm -rf /var/lib/apt/lists/*

# MQTT-C: the headers in machine_mqtt/ are this version (v1.1.7).
RUN git clone --quiet --depth 1 --branch v1.1.7 \
      https://github.com/LiamBindle/MQTT-C /opt/MQTT-C
ENV MQTT_C_DIR=/opt/MQTT-C
