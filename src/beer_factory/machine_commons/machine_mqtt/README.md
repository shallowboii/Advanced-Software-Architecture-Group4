# Machine MQTT heartbeats

Publish machine names and timestamps to the `heartbeat` topic using MQTT QoS 1.

## Build and test

```sh
make       # build/libheartbeat.a
make test  # wrapper tests; no broker needed
```

Link your application with `build/libheartbeat.a`, a compatible MQTT-C library,
and the platform threading library. Only MQTT-C headers are included here.
For example, with MQTT-C installed as `libmqttc`:

```sh
cc -std=c11 -I. publisher.c build/libheartbeat.a -lmqttc -pthread -o publisher
```

## Initialize and publish

First open a TCP connection to your broker and make the socket nonblocking.
The following code belongs inside your publisher function; `broker_socket` is
that connected socket:

```c
#include "heartbeat.h"
#include <stdio.h>
#include <unistd.h>

/* Keep these alive for the entire session; never copy an initialized client. */
struct mqtt_client client;
uint8_t sendbuf[1024], recvbuf[1024];

enum MQTTErrors result = heartbeat_init(
    &client, broker_socket,
    sendbuf, sizeof(sendbuf), recvbuf, sizeof(recvbuf),
    "boiling-1");  /* Use a unique client ID for each machine. */

if (result != MQTT_OK) {
    fprintf(stderr, "Initialization failed: %s\n", mqtt_error_str(result));
    /* Handle failure and release resources before leaving this scope. */
} else {
    /* Publish every two seconds and service MQTT once per second. */
    for (unsigned int tick = 0; ; ++tick) {
        if (tick % 2 == 0) {
            machine_heartbeat_t hb = heartbeat_make("boiling-1");
            result = heartbeat_send(&client, hb);
            if (result != MQTT_OK) break;
        }

        result = mqtt_sync(&client);
        if (result != MQTT_OK) break;
        sleep(1);
    }
    fprintf(stderr, "MQTT stopped: %s\n", mqtt_error_str(result));
}
```

`heartbeat_init` queues CONNECT with a clean session, a 60-second keep-alive,
and no credentials or Last Will. `heartbeat_send` queues a message;
`mqtt_sync` sends packets and processes acknowledgements.

Names must be non-NULL, null-terminated strings of at most 31 bytes; invalid
names abort. The payload is the native `machine_heartbeat_t` struct (`int`
timestamp and `char machine_name[32]`), so receivers must use a compatible
layout and byte order.

The caller owns socket and mutex cleanup. Once `mqtt_init` succeeds, its mutex
needs cleanup even if queuing CONNECT subsequently fails. On normal shutdown,
queue `mqtt_disconnect`, service it with `mqtt_sync`, then release resources.
