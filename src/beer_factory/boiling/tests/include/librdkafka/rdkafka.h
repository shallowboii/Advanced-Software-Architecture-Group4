/* Only the librdkafka declarations used by the controller test doubles. */
#pragma once

typedef struct rd_kafka_s rd_kafka_t;

int rd_kafka_poll(rd_kafka_t *producer, int timeout_ms);
