#include "machine_kafka.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

/* Replace just the I/O calls; the wrapper runs without a Kafka broker. */
static int producer_marker, consumer_marker, context_marker;
static rd_kafka_t *const producer = (rd_kafka_t *)&producer_marker;
static rd_kafka_t *const consumer = (rd_kafka_t *)&consumer_marker;
static rd_kafka_message_t received;
static rd_kafka_message_t *next_message;
static rd_kafka_resp_err_t produce_error;
static unsigned int handled, committed, destroyed, producer_polls;
static kafka_message_t handled_message;

rd_kafka_message_t *rd_kafka_consumer_poll(rd_kafka_t *handle, int timeout_ms) {
  assert(handle == consumer && timeout_ms == KAFKA_POLL_TIMEOUT_MS);
  return next_message;
}

rd_kafka_resp_err_t rd_kafka_commit_message(rd_kafka_t *handle,
    const rd_kafka_message_t *message, int async) {
  assert(handle == consumer && message == &received && async == 0);
  assert(handled == 1); /* The handler must finish before committing. */
  ++committed;
  return RD_KAFKA_RESP_ERR_NO_ERROR;
}

void rd_kafka_message_destroy(rd_kafka_message_t *message) {
  assert(message == &received);
  ++destroyed;
}

rd_kafka_resp_err_t rd_kafka_producev(rd_kafka_t *handle, ...) {
  assert(handle == producer);
  return produce_error;
}

int rd_kafka_poll(rd_kafka_t *handle, int timeout_ms) {
  assert(handle == producer && timeout_ms == 0);
  ++producer_polls;
  return 0;
}

static void on_message(const kafka_message_t *message, void *context) {
  assert(context == &context_marker);
  handled_message = *message;
  ++handled;
}

static void reset(void) {
  memset(&received, 0, sizeof(received));
  next_message = &received;
  handled = committed = destroyed = producer_polls = 0;
  produce_error = RD_KAFKA_RESP_ERR_NO_ERROR;
}

static void check_rejected(void) {
  assert(kafka_poll(consumer, on_message, &context_marker) == -1);
  assert(handled == 0 && committed == 0 && destroyed == 1);
}

static void test_receive_errors(void) {
  reset();
  next_message = NULL;
  assert(kafka_poll(consumer, on_message, &context_marker) == 0);
  assert(handled == 0 && committed == 0 && destroyed == 0);

  reset();
  received.err = RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART;
  check_rejected();

  reset();
  received.len = sizeof(kafka_message_t); /* Missing payload despite valid size. */
  check_rejected();

  char short_payload[1] = {0};
  reset();
  received.payload = short_payload;
  received.len = sizeof(short_payload);
  check_rejected();

  reset();
  received.payload = short_payload;
  received.len = 0;
  check_rejected();

  reset();
  received.payload = short_payload;
  received.len = sizeof(kafka_message_t) + 1;
  check_rejected();
}

static void test_receive_valid_message(void) {
  reset();
  kafka_message_t message = {0};
  message.type = KAFKA_START;
  message.machine_id = 7;
  strcpy(message.machine_type, "boiling");
  strcpy(message.batch_id, "B-1");
  received.payload = &message;
  received.len = sizeof(message);
  assert(kafka_poll(consumer, on_message, &context_marker) == 1);
  assert(handled == 1 && committed == 1 && destroyed == 1);
  assert(handled_message.type == KAFKA_START && handled_message.machine_id == 7);
  assert(strcmp(handled_message.batch_id, "B-1") == 0);
}

static void test_send_results(void) {
  reset();
  kafka_message_t message = {.type = KAFKA_RUNNING, .machine_id = 7};
  assert(kafka_send(producer, "asaat67.machine.boiling", &message) == 0);
  assert(producer_polls == 1);

  produce_error = RD_KAFKA_RESP_ERR__QUEUE_FULL;
  assert(kafka_send(producer, "asaat67.machine.boiling", &message) == -1);
  assert(producer_polls == 2);

  produce_error = RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE;
  assert(kafka_send(producer, "asaat67.machine.boiling", &message) == -1);
  assert(producer_polls == 3);
}

int main(void) {
  test_receive_errors();
  test_receive_valid_message();
  test_send_results();
  puts("Kafka wrapper tests passed.");
  return 0;
}
