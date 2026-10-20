# Source Code

# Implementation Decisions

## Message Buses

**What do we want?** Two separate message buses:

1. **Production bus** – for the actual production messages (commands to the machines, scheduling, inventory).
2. **Monitoring bus** – for quality control and monitoring of the machines (heartbeats, sensor readings).

We considered 4 types of message buses: **MQTT**, **Kafka**, **RabbitMQ** and **ZeroMQ**.

### MQTT

- MQTT is a lightweight publish/subscribe **protocol**, not a product. To use it you need to run a **broker** (e.g. Eclipse Mosquitto), which acts as a router.
- Clients publish and subscribe to **hierarchical topics**, e.g. `brewery/mashing/heartbeat`.
- The broker delivers each message to the clients that are subscribed to that topic. Messages are not stored for later.
- Docs: https://mqtt.org/

### Kafka

- Kafka is a distributed **event streaming platform**. Messages are stored in a durable log and can be replayed.

> Event streaming is the practice of capturing data in real-time from event sources like databases, sensors, mobile devices, cloud services, and software applications in the form of streams of events; storing these event streams durably for later retrieval; manipulating, processing, and reacting to the event streams in real-time as well as retrospectively; and routing the event streams to different destination technologies as needed.
>
> — [Apache Kafka, Introduction](https://kafka.apache.org/43/getting-started/introduction/)

### RabbitMQ

- RabbitMQ is a traditional **message broker** (AMQP). Producers send messages to an exchange, which routes them into **queues**.
- Consumers acknowledge messages, and a message is removed from the queue once acknowledged. Retries and dead-letter queues are built in.
- Docs: https://www.rabbitmq.com/docs

### ZeroMQ

- ZeroMQ is a messaging **library** with **no broker**. It is as bare-bones as it gets: there is no persistence and no built-in acknowledgement.
- It connects processes directly to each other (peer-to-peer) over TCP or IPC, so each side has to know the other's address.
- Docs: https://zguide.zeromq.org/

## Selected Message Bus: MQTT

| | |
|---|---|
| **Message bus** | MQTT |
| **Integration points** | Between the **per-machine controllers** (publishers) and the **production monitoring subsystem** (machine monitoring and beer monitoring, subscribers), for real-time heartbeats. |
| **Justification of MQTT** | Chosen for its efficiency and fast data exchange. It is optimal for quick processing and delivery of frequent heartbeats from the machines. |
| **Justification of QA** | MQTT promotes **efficiency** (QA-2) by delivering small messages with low bandwidth. Fast heartbeat delivery lets the system detect a failed machine quickly, which supports **availability** (QA-8). |
