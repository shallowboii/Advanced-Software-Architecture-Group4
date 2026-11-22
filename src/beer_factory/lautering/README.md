# Lautering

The lautering machine in C, made from the Lautering template in the UPPAAL
model (`src/docs/Beer_Factory.xml`), boiling style.

`src/lautering.c` / `src/lautering.h`: a state machine in seconds with result
codes. It waits for wort, takes one batch, lauters it for 2 hours, then makes
one sweet-wort batch and goes back to waiting. `src/main.c` runs one batch and
publishes heartbeats over MQTT (`machine_commons/machine_mqtt`).

```sh
make test                         # state-machine tests, no broker needed
make all                          # build/lautering, needs libmqttc installed
make all MQTT_C_DIR=/path/MQTT-C  # or compile MQTT-C from a checkout
./build/lautering 0               # argument = machine number
```

Docker, from `src/beer_factory`:

```sh
docker build -f lautering/Dockerfile -t lautering .
docker run --rm lautering 0
```
