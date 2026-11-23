# Machine MQTT heartbeats

All six C machines use this shared library to publish a heartbeat every five
seconds on their machine-type topic (for example, `boiling`). The payload is
`machine_heartbeat_t`: a Unix timestamp and machine index, both native C integers.

## Run with the Compose services

From `src/beer_factory`, start the messaging services:

```sh
docker compose up -d rabbitmq kafka
```

The RabbitMQ service enables its MQTT plugin and exposes port 1883. Machines
running on the host use the published ports by default:

| Setting | Default |
| --- | --- |
| `KAFKA_BROKERS` | `localhost:9092` |
| `MQTT_HOST` | `localhost` |
| `MQTT_PORT` | `1883` |
| `MQTT_USERNAME` | `admin` |
| `MQTT_PASSWORD` | `admin123` |

These credentials match `docker-compose.yml`. Override the environment variables
if you change the RabbitMQ account. Set both MQTT credential variables to empty
strings to connect without authentication to a different broker.

For a machine running on the Compose `messaging` network, use the internal
service addresses instead:

```sh
KAFKA_BROKERS=kafka:9093 MQTT_HOST=rabbitmq ./build/mashing 0
```

Kafka topics remain `asaat67.machine.<machine-type>`. MQTT heartbeat topics are
unchanged.

## Build and test

```sh
make       # build/libheartbeat.a
make test  # wrapper tests; no broker needed
```

Link your application with `build/libheartbeat.a`, a compatible MQTT-C library,
and the platform threading library. Only MQTT-C headers are included here.

## API

```c
int mqtt_on = *heartbeat_init("BOILING", 7) == MQTT_OK;
/* Call regularly with monotonic seconds: */
heartbeat_update("boiling", now_seconds);
heartbeat_destroy();
```

Initialization opens the TCP connection and queues MQTT CONNECT with credentials,
a clean session, a 60-second keep-alive, and no Last Will. Updates publish with
QoS 1 and service MQTT I/O. If initialization or runtime I/O fails, the library
reports the error; the machine can continue processing without heartbeats.

The library owns the socket and client mutex. `heartbeat_destroy()` is safe
before initialization, after a failed initialization, and on repeated calls.
Reinitialization closes the previous connection first.
