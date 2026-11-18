# Boiling with MQTT heartbeats

The MQTT controller runs one normal one-hour batch, then waits. It publishes
the machine name and current Unix timestamp on `heartbeat` at QoS 1 every two
real seconds, during both boiling and waiting. Boiling uses monotonic time;
the existing state-machine API is unchanged.

Build with an installed MQTT-C library:

```sh
make mqtt
```

Or supply a local MQTT-C checkout (containing `src/mqtt.c` and `src/mqtt_pal.c`):

```sh
make mqtt MQTT_C_DIR=/path/to/MQTT-C
```

Start your broker, then run:

```sh
./build/boiling-mqtt                         # localhost:1883, boiling-1
./build/boiling-mqtt 127.0.0.1 1883 kettle-2 # custom broker and machine name
```

Each machine needs a unique name of 1–31 bytes. The binary heartbeat payload
comes from `../machine_mqtt`; this connection uses no authentication or TLS.

Press Ctrl-C to disconnect and exit. A failed initial broker connection stops
startup; a connection lost after startup is reported and heartbeat publishing
stops, while boiling continues. Automatic reconnection is not implemented.

Run the protocol integration tests without installing a broker:

```sh
make test-mqtt MQTT_C_DIR=/path/to/MQTT-C
```

Tests use a local MQTT test peer to check CONNECT, heartbeat payloads, QoS 1
acknowledgements, two-second timing, clean shutdown, and continued operation
after broker loss. Python 3 is required.

The pre-existing `make all`/`make demo` targets reference a missing
`examples/demo.c`, and `make test` expects an older boiling API. The MQTT
targets build independently of those targets.
