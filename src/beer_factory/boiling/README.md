# Boiling controller tests

Run the state-machine and controller tests:

```sh
make test
```

The state-machine tests cover initialization, start guards, the one-hour
boundary, single completion, subsequent batches, backward time, and large
timestamps. Calls require valid pointers and batch counters.

The controller tests compile the real controller and state machine against
heartbeat and clock doubles. They advance simulated time past one hour without
sleeping or connecting to a broker. They check the numeric machine argument,
usage errors, five-second heartbeat scheduling on `boiling`, continued boiling
after heartbeat failures, and cleanup after a clock failure. Run them separately
with `make test-controller`. Python 3 is required.

The test build suppresses strict-prototype warnings from the shared heartbeat
header, whose no-argument declarations currently use `()` rather than `(void)`.
Production build flags remain unchanged.

Build the MQTT controller with an installed MQTT-C library:

```sh
make mqtt
```

Or supply a local MQTT-C checkout containing `src/mqtt.c` and `src/mqtt_pal.c`:

```sh
make mqtt MQTT_C_DIR=/path/to/MQTT-C
```

Run with a machine number:

```sh
./build/boiling-mqtt 7
```

The controller passes this number to the shared heartbeat service in
`../machine_commons/machine_mqtt`. The broker is fixed to `localhost:1883`.
The controller uses `atoi`; it currently does not reject malformed numeric
arguments. SIGTERM and Ctrl-C use the default signal handlers.

Run the real MQTT protocol tests with port 1883 free (stop any local broker):

```sh
make test-mqtt MQTT_C_DIR=/path/to/MQTT-C
```

These tests start a local protocol peer and check the expected `BOILING-7`
client identity and heartbeat payload, the `boiling` topic, QoS 1, five-second
spacing, and continued operation after broker loss. They terminate the
controller with SIGTERM; graceful MQTT disconnect is not expected.

The real MQTT build is currently blocked: the shared header declares
`heartbeat_init(int)` but its implementation defines `heartbeat_init(char *)`.
The shared code also triggers strict-prototype warnings with the default
compiler flags. The controller doubles do not exercise that implementation
or verify its machine-name formatting.
