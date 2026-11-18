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
static struct mqtt_client test_client;
static struct sockaddr address;
static struct addrinfo broker_address;
static enum MQTTErrors init_result, connect_result, publish_result;
static int lookup_result, tcp_result, poll_result, socket_error;
static unsigned int init_calls, connect_calls, publish_calls, close_calls;
static unsigned int destroy_calls, free_calls;
static machine_heartbeat_t published;
static const char *expected_name;

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
  assert(mutex == &test_client.mutex);
  ++destroy_calls;
  return pthread_mutex_destroy(mutex);
}

enum MQTTErrors mqtt_init(
    struct mqtt_client *actual, mqtt_pal_socket_handle fd,
    uint8_t *send, size_t send_size, uint8_t *recv, size_t recv_size,
    void (*callback)(void **, struct mqtt_response_publish *)) {
  ++init_calls;
  assert(actual == &test_client && fd == 42);
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
  assert(actual == &test_client);
  assert(strcmp(id, expected_name) == 0);
  assert(will_topic == NULL && will_message == NULL && will_size == 0);
  assert(username == NULL && password == NULL);
  assert(flags == MQTT_CONNECT_CLEAN_SESSION && keep_alive == 60);
  return connect_result;
}

const char *mqtt_error_str(enum MQTTErrors error) {
  assert(error == connect_result);
  return "simulated MQTT error";
}

enum MQTTErrors mqtt_publish(struct mqtt_client *actual, const char *topic,
                            const void *payload, size_t size, uint8_t flags) {
  ++publish_calls;
  assert(actual == &test_client);
  assert(strcmp(topic, "heartbeat") == 0);
  assert(size == sizeof(published) && flags == MQTT_PUBLISH_QOS_1);
  memcpy(&published, payload, size);
  return publish_result;
}

static void reset(void) {
  init_result = connect_result = publish_result = MQTT_OK;
  lookup_result = tcp_result = socket_error = 0;
  poll_result = 1;
  init_calls = connect_calls = publish_calls = close_calls = 0;
  destroy_calls = free_calls = 0;
  expected_name = "machine-1";
}

static enum MQTTErrors initialize(void) {
  return heartbeat_init(&test_client, "machine-1");
}

static void cleanup(void) {
  assert(pthread_mutex_destroy(&test_client.mutex) == 0);
  assert(test_close(test_client.socketfd) == 0);
}

static void test_initialization(void) {
  reset();
  lookup_result = EAI_NONAME;
  assert(initialize() == MQTT_ERROR_SOCKET_ERROR);
  assert(init_calls == 0 && connect_calls == 0 && close_calls == 0);
  assert(free_calls == 0);

  reset();
  tcp_result = -1;
  poll_result = 0;
  assert(initialize() == MQTT_ERROR_SOCKET_ERROR);
  assert(init_calls == 0 && connect_calls == 0 && close_calls == 1);
  assert(free_calls == 1);

  reset();
  tcp_result = -1;
  socket_error = ECONNREFUSED;
  assert(initialize() == MQTT_ERROR_SOCKET_ERROR);
  assert(init_calls == 0 && close_calls == 1 && free_calls == 1);

  reset();
  init_result = MQTT_ERROR_RECV_BUFFER_TOO_SMALL;
  assert(initialize() == MQTT_ERROR_RECV_BUFFER_TOO_SMALL);
  assert(init_calls == 1 && connect_calls == 0 && close_calls == 1);
  assert(destroy_calls == 0 && free_calls == 1);

  reset();
  connect_result = MQTT_ERROR_SEND_BUFFER_IS_FULL;
  assert(initialize() == MQTT_ERROR_SEND_BUFFER_IS_FULL);
  assert(init_calls == 1 && connect_calls == 1 && close_calls == 1);
  assert(destroy_calls == 1);

  reset();
  assert(initialize() == MQTT_OK);
  assert(init_calls == 1 && connect_calls == 1 && close_calls == 0);
  assert(destroy_calls == 0 && free_calls == 1);
  cleanup();

  reset();
  tcp_result = -1;
  assert(initialize() == MQTT_OK);
  assert(init_calls == 1 && connect_calls == 1 && close_calls == 0);
  cleanup();
}

static void expect_invalid_name(char *name) {
  pid_t child = fork();
  assert(child >= 0);
  if (child == 0) {
    const struct rlimit no_core = {0, 0};
    (void)setrlimit(RLIMIT_CORE, &no_core);
    (void)heartbeat_init(&test_client, name);
    _exit(0);
  }
  int status;
  assert(waitpid(child, &status, 0) == child);
  assert(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT);
}

static void check_payload(char *name) {
  reset();
  expected_name = name;
  assert(heartbeat_init(&test_client, name) == MQTT_OK);
  time_t before = time(NULL);
  assert(heartbeat_send(&test_client) == MQTT_OK);
  time_t after = time(NULL);
  assert(published.timestamp >= before && published.timestamp <= after);
  assert(strcmp(published.machine_name, name) == 0);
  for (size_t i = strlen(name); i < sizeof(published.machine_name); ++i)
    assert(published.machine_name[i] == '\0');
  assert(publish_calls == 1);

  publish_result = MQTT_ERROR_SEND_BUFFER_IS_FULL;
  assert(heartbeat_send(&test_client) == MQTT_ERROR_SEND_BUFFER_IS_FULL);
  assert(publish_calls == 2);
  assert(heartbeat_send(NULL) == MQTT_ERROR_NULLPTR);
  assert(publish_calls == 2);
  cleanup();
}

int main(void) {
  test_initialization();
  check_payload("boiling");
  check_payload("");
  char name[MAX_MACHINE_NAME_LEN + 1];
  memset(name, 'x', sizeof(name));
  name[MAX_MACHINE_NAME_LEN - 1] = '\0';
  check_payload(name);
  name[MAX_MACHINE_NAME_LEN - 1] = 'x';
  name[MAX_MACHINE_NAME_LEN] = '\0';
  expect_invalid_name(name);
  expect_invalid_name(NULL);
  puts("Heartbeat tests passed.");
  return 0;
}
