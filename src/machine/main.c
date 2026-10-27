#define _POSIX_C_SOURCE 200809L
#include "machine.h"
#include "wire.h"
#include <MQTTAsync.h>
#include <librdkafka/rdkafka.h>
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#define QUEUE_SIZE 256
#define MAX_INPUT 65536
#define LOCAL_CLIENTS 16

typedef struct { char *payload; int ack; } Incoming;
typedef struct { int fd; size_t used; double accepted; char buf[MAX_INPUT]; } LocalClient;
typedef struct {
    MQTTAsync mqtt;
    rd_kafka_t *kafka;
    BFMachine machine;
    pthread_mutex_t lock;
    Incoming queue[QUEUE_SIZE];
    size_t head, tail, count;
    atomic_int connected, replay_reset;
    char cmd_topic[256], ack_topic[256], heartbeat_topic[256], events_topic[256];
} App;
static volatile sig_atomic_t running = 1;
static void signal_stop(int sig) { (void)sig; running = 0; }
static double clock_seconds(clockid_t id) { struct timespec t; clock_gettime(id, &t); return t.tv_sec + t.tv_nsec / 1e9; }
static const char *env(const char *key, const char *fallback) { const char *v = getenv(key); return v && *v ? v : fallback; }
static double env_num(const char *key, double fallback) {
    const char *v = getenv(key); if (!v || !*v) return fallback;
    char *end; errno = 0; double n = strtod(v, &end);
    if (errno || *end || !isfinite(n) || n <= 0) { fprintf(stderr, "Invalid %s\n", key); exit(2); }
    return n;
}
static int publish(App *a, const char *topic, const char *payload, int qos) {
    if (!atomic_load(&a->connected)) return -1;
    MQTTAsync_message message = MQTTAsync_message_initializer;
    message.payload = (void *)payload; message.payloadlen = (int)strlen(payload); message.qos = qos; message.retained = 0;
    int rc = MQTTAsync_sendMessage(a->mqtt, topic, &message, NULL);
    return rc == MQTTASYNC_SUCCESS ? 0 : -1;
}
static int publish_event(const char *payload, void *ctx) { App *a = ctx; return publish(a, a->events_topic, payload, 1); }
static void connection_lost(void *ctx, char *cause) {
    App *a = ctx; atomic_store(&a->connected, 0); atomic_store(&a->replay_reset, 1);
    fprintf(stderr, "MQTT disconnected: %s\n", cause ? cause : "unknown");
}
static void subscribed(void *ctx, MQTTAsync_successData *data) {
    (void)data; App *a = ctx;
    /* A connection may have died before queued events reached the broker. Replay
       its unknown history in order before publishing newer material events. */
    atomic_store(&a->replay_reset, 1); atomic_store(&a->connected, 1);
}
static void subscribe_failed(void *ctx, MQTTAsync_failureData *data) {
    App *a = ctx; atomic_store(&a->connected, 0);
    fprintf(stderr, "MQTT subscribe failed: %d\n", data ? data->code : -1);
}
static void subscribe_all(App *a) {
    char *topics[] = {a->cmd_topic, a->ack_topic, "factory/line/control"}; int qos[] = {1, 1, 1};
    MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
    opts.context = a; opts.onSuccess = subscribed; opts.onFailure = subscribe_failed;
    int rc = MQTTAsync_subscribeMany(a->mqtt, 3, topics, qos, &opts);
    if (rc != MQTTASYNC_SUCCESS) fprintf(stderr, "MQTT subscribe queue failed: %d\n", rc);
}
static void connected(void *ctx, char *cause) { (void)cause; subscribe_all(ctx); }
static void connect_success(void *ctx, MQTTAsync_successData *data) { (void)data; subscribe_all(ctx); }
static void connect_failure(void *ctx, MQTTAsync_failureData *data) {
    App *a = ctx; atomic_store(&a->connected, 0);
    fprintf(stderr, "MQTT connect failed: %d\n", data ? data->code : -1);
}
static int arrived(void *ctx, char *topic, int topic_len, MQTTAsync_message *message) {
    App *a = ctx;
    if (message->retained || message->payloadlen <= 0 || message->payloadlen >= MAX_INPUT) {
        MQTTAsync_freeMessage(&message); MQTTAsync_free(topic); return 1;
    }
    char *payload = malloc((size_t)message->payloadlen + 1);
    if (!payload) return 0;
    memcpy(payload, message->payload, (size_t)message->payloadlen); payload[message->payloadlen] = 0;
    if (strlen(payload) != (size_t)message->payloadlen) { free(payload); MQTTAsync_freeMessage(&message); MQTTAsync_free(topic); return 1; }
    size_t length = topic_len > 0 ? (size_t)topic_len : strlen(topic);
    int ack = length == strlen(a->ack_topic) && !strncmp(topic, a->ack_topic, length);
    pthread_mutex_lock(&a->lock);
    if (a->count == QUEUE_SIZE) { pthread_mutex_unlock(&a->lock); free(payload); return 0; }
    a->queue[a->tail] = (Incoming){payload, ack}; a->tail = (a->tail + 1) % QUEUE_SIZE; a->count++;
    pthread_mutex_unlock(&a->lock);
    MQTTAsync_freeMessage(&message); MQTTAsync_free(topic); return 1;
}
static int pop(App *a, Incoming *in) {
    pthread_mutex_lock(&a->lock);
    if (!a->count) { pthread_mutex_unlock(&a->lock); return 0; }
    *in = a->queue[a->head]; a->head = (a->head + 1) % QUEUE_SIZE; a->count--;
    pthread_mutex_unlock(&a->lock); return 1;
}
static int socket_open(const char *path) {
    struct sockaddr_un addr; memset(&addr, 0, sizeof addr); addr.sun_family = AF_UNIX;
    if (strlen(path) >= sizeof addr.sun_path) { fprintf(stderr, "SOCKET_PATH is too long\n"); return -1; }
    snprintf(addr.sun_path, sizeof addr.sun_path, "%s", path);
    int fd = socket(AF_UNIX, SOCK_STREAM, 0); if (fd < 0) return -1;
    /* A second controller must not unlink the socket of a running instance. */
    int probe = socket(AF_UNIX, SOCK_STREAM, 0);
    if (probe >= 0 && connect(probe, (struct sockaddr *)&addr, sizeof addr) == 0) {
        close(probe); close(fd); errno = EADDRINUSE; return -1;
    }
    if (probe >= 0) close(probe);
    unlink(path);
    if (bind(fd, (struct sockaddr *)&addr, sizeof addr) || chmod(path, 0600) || listen(fd, LOCAL_CLIENTS) || fcntl(fd, F_SETFL, O_NONBLOCK)) {
        close(fd); return -1;
    }
    return fd;
}
static void send_local(int fd, const char *payload) {
    /* Small local replies normally fit; polling bounds a client that stops reading. */
    size_t n = strlen(payload), sent = 0; double deadline = clock_seconds(CLOCK_MONOTONIC) + 0.05;
    while (sent < n && clock_seconds(CLOCK_MONOTONIC) < deadline) {
        ssize_t rc = send(fd, payload + sent, n - sent, 0);
        if (rc > 0) sent += (size_t)rc;
        else if (rc < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            struct pollfd p = {fd, POLLOUT, 0}; if (poll(&p, 1, 10) <= 0) break;
        } else break;
    }
    (void)send(fd, "\n", 1, 0);
}
static int handle_command(App *a, const char *payload, int local, char *response) {
    BFCommand cmd; char error[128];
    if (bf_parse_command(payload, a->machine.s.machine_id, a->machine.s.epoch, local, &cmd, error, sizeof error)) {
        snprintf(response, BF_JSON, "{\"error\":\"%s\"}", error); return 1;
    }
    if (local && !strcmp(cmd.cmd, "SNAPSHOT"))
        return bf_snapshot(&a->machine, clock_seconds(CLOCK_MONOTONIC), response, BF_JSON);
    int rc = bf_command(&a->machine, &cmd, clock_seconds(CLOCK_MONOTONIC), clock_seconds(CLOCK_REALTIME), response, BF_JSON);
    if (rc < 0) fprintf(stderr, "Durable command failed: %s\n", a->machine.error);
    return rc;
}
static int service_local(App *a, int listener, LocalClient *clients) {
    for (int i = 0; i < LOCAL_CLIENTS; i++) {
        if (clients[i].fd >= 0) continue;
        int fd = accept(listener, NULL, NULL); if (fd < 0) break;
        if (fcntl(fd, F_SETFL, O_NONBLOCK)) { close(fd); continue; }
        clients[i].fd = fd; clients[i].used = 0; clients[i].accepted = clock_seconds(CLOCK_MONOTONIC);
    }
    for (int i = 0; i < LOCAL_CLIENTS; i++) {
        LocalClient *c = &clients[i]; if (c->fd < 0) continue;
        if (clock_seconds(CLOCK_MONOTONIC) - c->accepted > 2) { close(c->fd); c->fd = -1; continue; }
        ssize_t n = recv(c->fd, c->buf + c->used, sizeof c->buf - c->used - 1, 0);
        if (n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) { close(c->fd); c->fd = -1; continue; }
        if (n <= 0) continue;
        c->used += (size_t)n; c->buf[c->used] = 0;
        if (strlen(c->buf) != c->used) { send_local(c->fd, "{\"error\":\"embedded_nul\"}"); close(c->fd); c->fd = -1; continue; }
        char *end = strchr(c->buf, '\n');
        if (!end && c->used == sizeof c->buf - 1) { send_local(c->fd, "{\"error\":\"input_too_large\"}"); close(c->fd); c->fd = -1; continue; }
        if (!end) continue;
        *end = 0; char response[BF_JSON]; int rc = handle_command(a, c->buf, 1, response);
        send_local(c->fd, response); close(c->fd); c->fd = -1;
        if (rc < 0) return -1;
    }
    return 0;
}
static void kafka_delivery(rd_kafka_t *rk, const rd_kafka_message_t *msg, void *opaque) {
    (void)rk; (void)opaque;
    if (msg->err) fprintf(stderr, "Telemetry delivery failed: %s\n", rd_kafka_err2str(msg->err));
}
static rd_kafka_t *kafka_open(const char *brokers) {
    rd_kafka_conf_t *conf = rd_kafka_conf_new(); char error[512];
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, error, sizeof error) != RD_KAFKA_CONF_OK ||
        rd_kafka_conf_set(conf, "message.timeout.ms", "10000", error, sizeof error) != RD_KAFKA_CONF_OK ||
        rd_kafka_conf_set(conf, "queue.buffering.max.messages", "1000", error, sizeof error) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Kafka configuration: %s\n", error); rd_kafka_conf_destroy(conf); return NULL;
    }
    rd_kafka_conf_set_dr_msg_cb(conf, kafka_delivery);
    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error, sizeof error);
    if (!rk) { fprintf(stderr, "Kafka create: %s\n", error); rd_kafka_conf_destroy(conf); }
    return rk;
}
static void sensor(App *a, const char *name, const char *unit, double value, double wall) {
    char payload[BF_JSON], execution[BF_ID*6], id[64], utc[40], machine[BF_ID*6], batch[BF_ID*6], recipe[BF_ID*6], run[BF_ID*6], phase[BF_NAME*6];
    bf_uuid(id, sizeof id); bf_utc(wall, utc, sizeof utc);
    BFState *s = &a->machine.s;
    bf_json_string(s->execution_id, execution, sizeof execution);
    bf_json_string(s->machine_id, machine, sizeof machine); bf_json_string(s->batch_id, batch, sizeof batch);
    bf_json_string(s->recipe_version_id, recipe, sizeof recipe); bf_json_string(s->run_id, run, sizeof run);
    bf_json_string(s->phases[s->phase_index].name, phase, sizeof phase);
    snprintf(payload, sizeof payload, "{\"schema_version\":1,\"event_id\":\"%s\",\"message_id\":\"%s\",\"machine_id\":%s,\"type\":\"%s\",\"batch_id\":%s,\"execution_id\":%s,\"transfer_id\":null,\"recipe_version_id\":%s,\"run_id\":%s,\"phase\":%s,\"sensor\":\"%s\",\"unit\":\"%s\",\"value\":%.6f,\"ts_utc\":\"%s\",\"sim_time_h\":%.9f}",
        id, id, machine, s->type, batch, execution, recipe, *s->run_id ? run : "null", phase, name, unit, value, utc, s->sim_time_h);
    rd_kafka_resp_err_t err = rd_kafka_producev(a->kafka, RD_KAFKA_V_TOPIC("telemetry"),
        RD_KAFKA_V_KEY(s->machine_id, strlen(s->machine_id)), RD_KAFKA_V_VALUE(payload, strlen(payload)),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY), RD_KAFKA_V_END);
    if (err) fprintf(stderr, "Telemetry enqueue failed: %s\n", rd_kafka_err2str(err));
}
static void telemetry(App *a, double wall) {
    BFState *s = &a->machine.s;
    if (strcmp(s->state, "PROCESSING") || s->phase_index >= s->phase_count) return;
    BFPhase *p = &s->phases[s->phase_index]; double fraction = fmin(1, s->phase_elapsed_h / p->duration_h);
    double noise = 0.15 * sin(s->sim_time_h * 7.31);
    double temp = 20 + (p->target_temp_c - 20) * fmin(1, fraction * 5) + noise;
    sensor(a, "temp_c", "C", temp, wall);
    if (!strcmp(s->type, "unitank")) sensor(a, "density", "SG", 1.050 - 0.040 * fraction, wall);
    else if (!strcmp(s->type, "filter")) sensor(a, "co2", "vol", 2.5 * fraction, wall);
    else sensor(a, "ph", "pH", 5.2 + noise / 4, wall);
}
int main(void) {
    signal(SIGINT, signal_stop); signal(SIGTERM, signal_stop); signal(SIGPIPE, SIG_IGN);
    App a; memset(&a, 0, sizeof a); pthread_mutex_init(&a.lock, NULL);
    const char *type = env("MACHINE_TYPE", "mash"), *id = env("MACHINE_ID", env("HOSTNAME", "mash-0"));
    for (const unsigned char *p = (const unsigned char *)id; *p; p++)
        if (!((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') || (*p >= '0' && *p <= '9') || *p == '-' || *p == '_')) { fprintf(stderr, "Invalid MACHINE_ID\n"); return 2; }
    const char *db = env("STATE_DB", "/data/machine.db"), *sock = env("SOCKET_PATH", "/tmp/machine.sock");
    char lock_path[1024]; snprintf(lock_path, sizeof lock_path, "%s.lock", db);
    int lockfd = open(lock_path, O_CREAT | O_RDWR, 0600);
    struct flock file_lock; memset(&file_lock, 0, sizeof file_lock); file_lock.l_type = F_WRLCK; file_lock.l_whence = SEEK_SET;
    if (lockfd < 0 || fcntl(lockfd, F_SETLK, &file_lock)) { perror("machine journal lock"); return 2; }
    if (bf_open(&a.machine, db, id, type, env_num("TIME_SCALE", 2), env_num("FLUSH_H", 0.1), clock_seconds(CLOCK_MONOTONIC), clock_seconds(CLOCK_REALTIME))) {
        fprintf(stderr, "Journal init: %s\n", a.machine.error); return 2;
    }
    int listener = socket_open(sock); if (listener < 0) { perror("local operator socket"); return 2; }
    LocalClient *clients = calloc(LOCAL_CLIENTS, sizeof *clients); if (!clients) return 2;
    for (int i = 0; i < LOCAL_CLIENTS; i++) clients[i].fd = -1;
    snprintf(a.cmd_topic, sizeof a.cmd_topic, "factory/%s/%s/cmd", type, id);
    snprintf(a.ack_topic, sizeof a.ack_topic, "factory/%s/%s/ack", type, id);
    snprintf(a.heartbeat_topic, sizeof a.heartbeat_topic, "factory/%s/%s/heartbeat", type, id);
    snprintf(a.events_topic, sizeof a.events_topic, "factory/%s/%s/events", type, id);
    char client_id[BF_ID + 16]; snprintf(client_id, sizeof client_id, "machine-%s", id);
    int rc = MQTTAsync_create(&a.mqtt, env("MQTT_URL", "tcp://rabbitmq:1883"), client_id, MQTTCLIENT_PERSISTENCE_NONE, NULL);
    if (rc != MQTTASYNC_SUCCESS) { fprintf(stderr, "MQTT create: %d\n", rc); return 2; }
    MQTTAsync_setCallbacks(a.mqtt, &a, connection_lost, arrived, NULL); MQTTAsync_setConnected(a.mqtt, &a, connected);
    a.kafka = kafka_open(env("KAFKA_BROKERS", "kafka:9092")); if (!a.kafka) return 2;
    double next_connect = 0, heartbeat_at = 0, telemetry_at = 0, replay_at = 0;
    uint64_t published_sequence = 0;
    double heartbeat_s = env_num("HEARTBEAT_MS", 2000) / 1000.;
    fprintf(stderr, "Controller %s type=%s ready; local socket=%s\n", id, type, sock);
    int failed = 0;
    while (running) {
        double mono = clock_seconds(CLOCK_MONOTONIC), wall = clock_seconds(CLOCK_REALTIME);
        if (service_local(&a, listener, clients)) { failed = 1; break; }
        Incoming in;
        for (int i = 0; i < 32 && pop(&a, &in); i++) {
            if (in.ack) {
                char event_id[BF_ID]; if (!bf_parse_ack(in.payload, event_id, sizeof event_id) && bf_ack(&a.machine, event_id)) failed = 1;
            } else {
                char response[BF_JSON]; if (handle_command(&a, in.payload, 0, response) < 0) failed = 1;
            }
            free(in.payload);
        }
        if (failed || bf_tick(&a.machine, clock_seconds(CLOCK_MONOTONIC), wall)) { failed = 1; break; }
        if (MQTTAsync_isConnected(a.mqtt) && !atomic_load(&a.connected) && mono >= next_connect) {
            subscribe_all(&a); next_connect = mono + 5;
        }
        if (!MQTTAsync_isConnected(a.mqtt) && mono >= next_connect) {
            MQTTAsync_connectOptions opts = MQTTAsync_connectOptions_initializer;
            opts.keepAliveInterval = 5; opts.cleansession = 0; opts.automaticReconnect = 1;
            opts.minRetryInterval = 1; opts.maxRetryInterval = 5; opts.connectTimeout = 3;
            opts.username = env("MQTT_USERNAME", "beerfactory"); opts.password = env("MQTT_PASSWORD", "dev-only-password");
            opts.context = &a; opts.onSuccess = connect_success; opts.onFailure = connect_failure;
            rc = MQTTAsync_connect(a.mqtt, &opts);
            if (rc != MQTTASYNC_SUCCESS) fprintf(stderr, "MQTT connect queue: %d\n", rc);
            next_connect = mono + 5;
        }
        if (mono >= heartbeat_at) { char snapshot[BF_JSON]; if (!bf_snapshot(&a.machine, mono, snapshot, sizeof snapshot)) publish(&a, a.heartbeat_topic, snapshot, 0); heartbeat_at = mono + heartbeat_s; }
        if (atomic_exchange(&a.replay_reset, 0)) published_sequence = 0;
        if (atomic_load(&a.connected) && published_sequence < a.machine.s.machine_sequence) {
            if (bf_outbox_new(&a.machine, &published_sequence, 64, publish_event, &a) < 0) { failed = 1; break; }
        }
        if (mono >= replay_at) {
            if (bf_outbox(&a.machine, 64, publish_event, &a) < 0) { failed = 1; break; }
            replay_at = mono + 0.5;
        }
        if (mono >= telemetry_at) { telemetry(&a, wall); telemetry_at = mono + 1; }
        rd_kafka_poll(a.kafka, 0);
        struct timespec nap = {0, 10000000}; nanosleep(&nap, NULL);
    }
    if (failed) fprintf(stderr, "Controller stopped after persistence failure: %s\n", a.machine.error);
    else if (bf_tick(&a.machine, clock_seconds(CLOCK_MONOTONIC), clock_seconds(CLOCK_REALTIME)) ||
             bf_checkpoint(&a.machine, clock_seconds(CLOCK_MONOTONIC))) failed = 1;
    close(listener); unlink(sock);
    for (int i = 0; i < LOCAL_CLIENTS; i++) if (clients[i].fd >= 0) close(clients[i].fd);
    free(clients);
    MQTTAsync_disconnectOptions disc = MQTTAsync_disconnectOptions_initializer; disc.timeout = 500;
    MQTTAsync_disconnect(a.mqtt, &disc); MQTTAsync_destroy(&a.mqtt);
    rd_kafka_flush(a.kafka, 1000); rd_kafka_destroy(a.kafka);
    Incoming in; while (pop(&a, &in)) free(in.payload);
    bf_close(&a.machine); pthread_mutex_destroy(&a.lock); close(lockfd);
    return failed ? 1 : 0;
}
