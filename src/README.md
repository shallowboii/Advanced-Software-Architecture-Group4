# Source Code

# Implementational decisions

# Message Buses

What do we want? We want two different message buses, one for the actual production messages, and one for the quality control part (monitoring the machines as well?)


We are considered 4 types of message buses: Kafka, MQTT, RabbitMQ, ZeroMQ

MQTT is a protocol, you need to run a broker (which is typically used as a router) to implement it.
Clients subscribe to hierarchical topics
The router delivers to clients who are subscribed to a topic


Kafka
 event streaming is the practice of capturing data in real-time from event sources like databases, sensors, mobile devices, cloud services, and software applications in the form of streams of events; storing these event streams durably for later retrieval; manipulating, processing, and reacting to the event streams in real-time as well as retrospectively; and routing the event streams to different destination technologies as needed
 Source: https://kafka.apache.org/43/getting-started/introduction/




RabbitMQ


ZeroMQ
This is a messaging library, with no brokers. Essentially this is as bare bones as it gets, there is no persistancy. It connects with 

