# Kafka Streams Demo App

This project contains 3 example apps that showcases interesting use-cases of Kafka Streams framework.

1. [Stateless](https://github.com/matvey-mtn/kafka-streams-demo/tree/master/src/main/java/com/example/kafkastreams/stateless) enrichment of the Json documents   
2. [Stateful](https://github.com/matvey-mtn/kafka-streams-demo/tree/master/src/main/java/com/example/kafkastreams/stateful) counting of the user requests
3. [Real-Time Alerts](https://github.com/matvey-mtn/kafka-streams-demo/tree/master/src/main/java/com/example/kafkastreams/alerting) demo using Kafka Streams

All examples are written in Java. I didn't use other programming languages deliberately to keep code as simple as possible.

## Prerequisites
1. JDK 21
2. Kafka Producer that will produce a json records to the input topic
   3. here is an example of the json record:
   ```
   {
       "timestamp": "1696606746",
       "method": "GET",
       "user": "John Doe",
       "permission": "Allow",
       "requestedResourceId": "42",
       "type": "demo"
   }
   ```

## Running demo apps

1. compile the code using `mvnw clean install` or `mnvw.cmd clean install` for windows
2. run the docker-compose: `docker-compose up -d`
3. open kafka-ui http://localhost:18080/ and create a input topic called `json-enrichment`
4. start producing json records to the `json-enrichment` using producer mentioned in the prerequisites
3. go to one of the following classes: `com.example.kafkastreams.stateless.EnrichmentStreamsApp`, `com.example.kafkastreams.stateful.UserRequestsCounterApp`, or `com.example.kafkastreams.alerting.SuspiciousActivityMonitorApp` and run the main function.
4. :exclamation: **note that EnrichmentStreamsApp is always required to run for other Apps to work** :exclamation:. UserRequestsCounterApp and SuspiciousActivityMonitorApp use `json-enrichment-output` topic as their input topic.

