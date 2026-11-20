#include "heartbeat.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

/* Only the test build of heartbeat.c redirects network calls to these doubles. */
extern struct mqtt_client client;
extern uint64_t last_heartbeat_s;
static struct sockaddr address;
static struct addrinfo broker_address;
static enum MQTTErrors init_result, connect_result, publish_result;
static int lookup_result, tcp_result, poll_result, socket_error;
static unsigned int init_calls, connect_calls, publish_calls, close_calls;
static unsigned int destroy_calls, free_calls;
static machine_heartbeat_t published;
static const char *expected_name;
static const char *expected_topic;
static enum MQTTErrors sync_result;
static unsigned int sync_calls, disconnect_calls;

int test_getaddrinfo(const char *host, const char *port,
                     const struct addrinfo *hints, struct addrinfo **result) {
  assert(strcmp(host, "localhost") == 0);
  assert(strcmp(port, "1883") == 0);
  assert(hints->ai_family == AF_UNSPEC);
  assert(hints->ai_socktype == SOCK_STREAM);
  if (lookup_result != 0) return lookup_result;
  broker_address = (struct addrinfo){.ai_family = AF_INET,
                                    .ai_socktype = SOCK_STREAM,
                                    .ai_addr = &address,
                                    .ai_addrlen = sizeof(address)};
  *result = &broker_address;
  return 0;
}

void test_freeaddrinfo(struct addrinfo *addresses) {
  assert(addresses == &broker_address);
  ++free_calls;
}

int test_socket(int domain, int type, int protocol) {
  assert(domain == AF_INET && type == SOCK_STREAM && protocol == 0);
  return 42;
}

int test_fcntl(int fd, int command, ...) {
  assert(fd == 42);
  assert(command == F_GETFL || command == F_SETFL);
  return 0;
}

int test_connect(int fd, const struct sockaddr *addr, socklen_t length) {
  assert(fd == 42 && addr == &address && length == sizeof(address));
  errno = EINPROGRESS;
  return tcp_result;
}

int test_poll(struct pollfd *fds, nfds_t count, int timeout) {
  assert(count == 1 && fds->fd == 42 && fds->events == POLLOUT);
  assert(timeout == 5000);
  fds->revents = POLLOUT;
  return poll_result;
}

int test_getsockopt(int fd, int level, int option, void *value,
                    socklen_t *length) {
  assert(fd == 42 && level == SOL_SOCKET && option == SO_ERROR);
  assert(*length == sizeof(int));
  *(int *)value = socket_error;
  return 0;
}

int test_close(int fd) {
  assert(fd == 42);
  ++close_calls;
  return 0;
}

int test_pthread_mutex_destroy(pthread_mutex_t *mutex) {
  assert(mutex == &client.mutex);
  ++destroy_calls;
  return pthread_mutex_destroy(mutex);
}

enum MQTTErrors mqtt_init(
    struct mqtt_client *actual, mqtt_pal_socket_handle fd,
    uint8_t *send, size_t send_size, uint8_t *recv, size_t recv_size,
    void (*callback)(void **, struct mqtt_response_publish *)) {
  ++init_calls;
  assert(actual == &client && fd == 42);
  assert(send != NULL && send_size > 0);
  assert(recv != NULL && recv_size > 0 && recv != send);
  assert(callback == NULL);
  if (init_result == MQTT_OK) {
    assert(pthread_mutex_init(&actual->mutex, NULL) == 0);
    actual->socketfd = fd;
  }
  return init_result;
}

enum MQTTErrors mqtt_connect(
    struct mqtt_client *actual, const char *id, const char *will_topic,
    const void *will_message, size_t will_size, const char *username,
    const char *password, uint8_t flags, uint16_t keep_alive) {
  ++connect_calls;
  assert(actual == &client);
  assert(strcmp(id, expected_name) == 0);
  assert(will_topic == NULL && will_message == NULL && will_size == 0);
  assert(username == NULL && password == NULL);
  assert(flags == MQTT_CONNECT_CLEAN_SESSION && keep_alive == 60);
  return connect_result;
}

const char *mqtt_error_str(enum MQTTErrors error) {
  (void)error;
  return "simulated MQTT error";
}

enum MQTTErrors mqtt_publish(struct mqtt_client *actual, const char *topic,
                            const void *payload, size_t size, uint8_t flags) {
  ++publish_calls;
  assert(actual == &client);
  assert(strcmp(topic, expected_topic) == 0);
  assert(size == sizeof(published) && flags == MQTT_PUBLISH_QOS_1);
  memcpy(&published, payload, size);
  return publish_result;
}


enum MQTTErrors mqtt_sync(struct mqtt_client *actual) {
  assert(actual == &client);
  ++sync_calls;
  return sync_result;
}

enum MQTTErrors mqtt_disconnect(struct mqtt_client *actual) {
  assert(actual == &client);
  ++disconnect_calls;
  return MQTT_OK;
}

static void reset(void) {
  init_result = connect_result = publish_result = sync_result = MQTT_OK;
  lookup_result = tcp_result = socket_error = 0;
  poll_result = 1;
  init_calls = connect_calls = publish_calls = close_calls = 0;
  destroy_calls = free_calls = sync_calls = disconnect_calls = 0;
  expected_name = "BOILING-7";
  expected_topic = "boiling";
  /* Isolate scheduling tests without relying on reinitialization semantics. */
  last_heartbeat_s = 0;
}

static void test_initialization_failures(void) {
  reset();
  lookup_result = EAI_NONAME;
  assert(*heartbeat_init("BOILING", 7) == MQTT_ERROR_SOCKET_ERROR);
  assert(init_calls == 0 && close_calls == 0 && free_calls == 0);
  assert(*heartbeat_status() == MQTT_ERROR_SOCKET_ERROR);
  heartbeat_update("boiling", 10);
  assert(publish_calls == 0 && sync_calls == 0);

  reset();
  tcp_result = -1;
  poll_result = 0;
  assert(*heartbeat_init("BOILING", 7) == MQTT_ERROR_SOCKET_ERROR);
  assert(init_calls == 0 && close_calls == 1 && free_calls == 1);

  reset();
  tcp_result = -1;
  socket_error = ECONNREFUSED;
  assert(*heartbeat_init("BOILING", 7) == MQTT_ERROR_SOCKET_ERROR);
  assert(init_calls == 0 && close_calls == 1);

  reset();
  init_result = MQTT_ERROR_RECV_BUFFER_TOO_SMALL;
  assert(*heartbeat_init("BOILING", 7) == init_result);
  assert(init_calls == 1 && connect_calls == 0 && close_calls == 1);
  assert(destroy_calls == 0);

  reset();
  connect_result = MQTT_ERROR_SEND_BUFFER_IS_FULL;
  assert(*heartbeat_init("BOILING", 7) == connect_result);
  assert(connect_calls == 1 && close_calls == 1 && destroy_calls == 1);
}

static void test_identity_and_timing(char *prefix, const char *name,
                                     const char *topic) {
  reset();
  expected_name = name;
  expected_topic = topic;
  enum MQTTErrors *status = heartbeat_init(prefix, 7);
  assert(*status == MQTT_OK && status == heartbeat_status());
  assert(init_calls == 1 && connect_calls == 1 && free_calls == 1);
  heartbeat_update(topic, 0);
  heartbeat_update(topic, 4);
  assert(publish_calls == 0 && sync_calls == 2);
  time_t before = time(NULL);
  assert(heartbeat_update(topic, 5) == status);
  assert(publish_calls == 1 && sync_calls == 3);
  assert(published.machine_idx == 7);
  assert(published.timestamp >= before && published.timestamp <= time(NULL));
  heartbeat_update(topic, 5);
  heartbeat_update(topic, 9);
  assert(publish_calls == 1);
  heartbeat_update(topic, 10);
  assert(publish_calls == 2);
  heartbeat_destroy();
  assert(disconnect_calls == 1 && destroy_calls == 1 && close_calls == 1);
}

static void test_runtime_failures(void) {
  reset();
  enum MQTTErrors *status = heartbeat_init("BOILING", 7);
  publish_result = MQTT_ERROR_SEND_BUFFER_IS_FULL;
  heartbeat_update("boiling", 5);
  assert(*status == publish_result && sync_calls == 0 && publish_calls == 1);
  heartbeat_update("boiling", 10);
  assert(sync_calls == 0 && publish_calls == 1);
  heartbeat_destroy();

  reset();
  status = heartbeat_init("BOILING", 7);
  sync_result = MQTT_ERROR_SOCKET_ERROR;
  heartbeat_update("boiling", 1);
  assert(*status == sync_result && sync_calls == 1 && publish_calls == 0);
  heartbeat_update("boiling", 10);
  assert(sync_calls == 1 && publish_calls == 0);
  heartbeat_destroy();
}

int main(void) {
  test_initialization_failures();
  test_identity_and_timing("BOILING", "BOILING-7", "boiling");
  test_identity_and_timing("FERMENTATION_MATURATION",
                           "FERMENTATION_MATURATION-7", "fermentation_maturation");
  test_runtime_failures();
  puts("Heartbeat tests passed.");
  return 0;
}
