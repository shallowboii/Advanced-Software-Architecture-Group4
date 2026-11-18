#include "heartbeat.h"

#include <assert.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

/* MQTT-C doubles check the wrapper's contract without a running broker. */
static struct mqtt_client client;
static uint8_t sendbuf[1024], recvbuf[1024];
static enum MQTTErrors init_result = MQTT_OK;
static enum MQTTErrors connect_result = MQTT_OK;
static enum MQTTErrors publish_result = MQTT_OK;
static unsigned int init_calls, connect_calls, publish_calls;
static machine_heartbeat_t expected;

enum MQTTErrors mqtt_init(
    struct mqtt_client *actual, mqtt_pal_socket_handle socketfd,
    uint8_t *send, size_t send_size, uint8_t *recv, size_t recv_size,
    void (*callback)(void **, struct mqtt_response_publish *)) {
  ++init_calls;
  assert(actual == &client);
  assert(socketfd == 42);
  assert(send == sendbuf && send_size == sizeof(sendbuf));
  assert(recv == recvbuf && recv_size == sizeof(recvbuf));
  assert(callback == NULL);
  return init_result;
}

enum MQTTErrors mqtt_connect(
    struct mqtt_client *actual, const char *id, const char *will_topic,
    const void *will_message, size_t will_size, const char *username,
    const char *password, uint8_t flags, uint16_t keep_alive) {
  ++connect_calls;
  assert(actual == &client);
  assert(strcmp(id, "machine-1") == 0);
  assert(will_topic == NULL && will_message == NULL && will_size == 0);
  assert(username == NULL && password == NULL);
  assert(flags == MQTT_CONNECT_CLEAN_SESSION && keep_alive == 60);
  return connect_result;
}

enum MQTTErrors mqtt_publish(struct mqtt_client *actual, const char *topic,
                              const void *payload, size_t size, uint8_t flags) {
  ++publish_calls;
  assert(actual == &client);
  assert(strcmp(topic, "heartbeat") == 0);
  assert(size == sizeof(machine_heartbeat_t));
  assert(flags == MQTT_PUBLISH_QOS_1);
  const machine_heartbeat_t *hb = payload;
  assert(hb->timestamp == expected.timestamp);
  assert(memcmp(hb->machine_name, expected.machine_name,
                sizeof(hb->machine_name)) == 0);
  return publish_result;
}

static enum MQTTErrors initialize(void) {
  return heartbeat_init(&client, 42, sendbuf, sizeof(sendbuf),
                        recvbuf, sizeof(recvbuf), "machine-1");
}

static void test_initialization(void) {
  assert(heartbeat_init(NULL, 42, sendbuf, sizeof(sendbuf), recvbuf,
                        sizeof(recvbuf), "machine-1") == MQTT_ERROR_NULLPTR);
  assert(heartbeat_init(&client, 42, NULL, sizeof(sendbuf), recvbuf,
                        sizeof(recvbuf), "machine-1") == MQTT_ERROR_NULLPTR);
  assert(heartbeat_init(&client, 42, sendbuf, sizeof(sendbuf), NULL,
                        sizeof(recvbuf), "machine-1") == MQTT_ERROR_NULLPTR);
  assert(heartbeat_init(&client, 42, sendbuf, 0, recvbuf,
                        sizeof(recvbuf), "machine-1") ==
         MQTT_ERROR_MALFORMED_REQUEST);
  assert(heartbeat_init(&client, 42, sendbuf, sizeof(sendbuf), recvbuf,
                        0, "machine-1") == MQTT_ERROR_MALFORMED_REQUEST);
  assert(init_calls == 0 && connect_calls == 0);

  init_result = MQTT_ERROR_SOCKET_ERROR;
  assert(initialize() == MQTT_ERROR_SOCKET_ERROR);
  assert(init_calls == 1 && connect_calls == 0);

  init_result = MQTT_OK;
  connect_result = MQTT_ERROR_SEND_BUFFER_IS_FULL;
  assert(initialize() == MQTT_ERROR_SEND_BUFFER_IS_FULL);
  assert(init_calls == 2 && connect_calls == 1);

  connect_result = MQTT_OK;
  assert(initialize() == MQTT_OK);
  assert(init_calls == 3 && connect_calls == 2);
}

static void expect_invalid_name(const char *name) {
  pid_t child = fork();
  assert(child >= 0);
  if (child == 0) {
    const struct rlimit no_core = {0, 0};
    (void)setrlimit(RLIMIT_CORE, &no_core);
    (void)heartbeat_make(name);
    _exit(0);
  }
  int status;
  assert(waitpid(child, &status, 0) == child);
  assert(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT);
}

static void test_payload(void) {
  time_t before = time(NULL);
  expected = heartbeat_make("boiling");
  time_t after = time(NULL);
  assert(expected.timestamp >= before && expected.timestamp <= after);
  assert(strcmp(expected.machine_name, "boiling") == 0);
  for (size_t i = strlen("boiling") + 1; i < sizeof(expected.machine_name); ++i) {
    assert(expected.machine_name[i] == '\0');
  }
  assert(heartbeat_send(&client, expected) == MQTT_OK);
  assert(publish_calls == 1);
  publish_result = MQTT_ERROR_SEND_BUFFER_IS_FULL;
  assert(heartbeat_send(&client, expected) == MQTT_ERROR_SEND_BUFFER_IS_FULL);
  assert(publish_calls == 2);
  assert(heartbeat_send(NULL, expected) == MQTT_ERROR_NULLPTR);
  assert(publish_calls == 2);

  machine_heartbeat_t empty = heartbeat_make("");
  assert(empty.machine_name[0] == '\0');
  char name[MAX_MACHINE_NAME_LEN + 1];
  memset(name, 'x', sizeof(name));
  name[MAX_MACHINE_NAME_LEN - 1] = '\0';
  machine_heartbeat_t longest = heartbeat_make(name);
  assert(strcmp(longest.machine_name, name) == 0);
  name[MAX_MACHINE_NAME_LEN - 1] = 'x';
  name[MAX_MACHINE_NAME_LEN] = '\0';
  expect_invalid_name(name);
  expect_invalid_name(NULL);
}

int main(void) {
  test_initialization();
  test_payload();
  puts("Heartbeat tests passed.");
  return 0;
}
