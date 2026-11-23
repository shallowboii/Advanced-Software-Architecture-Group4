# Mashing

The C state machine follows the Mashing template in UPPAAL. It starts in
`Wait`, enters `Mash` when started, and produces one wort batch after two hours
(7,200 seconds). It then returns to `Wait` and reports completion once.
[UPPAAL model](../../docs/Beer_Factory.xml)

## Run the unit tests

Run from `src/beer_factory/mashing`.

```sh
make test
```

The tests pass timestamps directly to the state machine. They cover the two-hour
boundary, busy starts, single completion, a second batch, and invalid timing and
start arguments. They run with a C compiler and Make, independently of MQTT.
[Test cases](tests/test_mashing.c)

## Build and run the controller

With librdkafka and MQTT-C installed as `libmqttc`, build with `make all`.
If you have a local MQTT-C source checkout, supply its path instead.

```sh
make all MQTT_C_DIR=/path/to/MQTT-C
```

Run the executable with a nonnegative machine number.

```sh
KAFKA_BROKERS=localhost:9092 ./build/mashing 0
```

The controller waits for `KAFKA_START` on `asaat67.machine.mashing` for its machine
number, using a separate consumer group (`mashing-<machine-number>`). It replies
with `KAFKA_RUNNING`, then `KAFKA_IDLE` after two hours, preserving the batch ID.
Messages for other machines, statuses, and starts received while busy are ignored.
Stop, flush, and start parameters are not handled, as in the other controllers.
It prints `Mashing started`, then `done_mashing` at completion and stays running
in `Wait`. With a broker available at
`localhost:1883`, machine `0` connects as `MASHING-0` and sends heartbeats on the
`mashing` topic every five seconds, including after completion.
[Controller](src/main.c) · [Shared heartbeat code](../machine_commons/machine_mqtt/heartbeat.c)
