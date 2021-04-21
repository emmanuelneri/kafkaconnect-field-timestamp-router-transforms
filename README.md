# kafkaconnect-field-timestamp-router-transforms

----- 

A [custom transformations](https://docs.confluent.io/platform/current/connect/transforms/custom.html) implementation based on [TimestampRouter](https://docs.confluent.io/platform/current/connect/transforms/timestamprouter.html) to use with sink connector to replace destination topics with `${topic}-${field}-${timestamp}` format. Should be used to segregate records by a field value and message timestamp. 

#### installation

To install this custom transforms, this maven project compile a JAR file to  that should be copied into `plugin.path` directory used by Kafka Connect.

With Kafka Connect from Confluent docker image the target directory is `/usr/share/java/`, for example in this Dockerfile configuration. 

Dockerfile
```
FROM confluentinc/cp-kafka-connect-base

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-elasticsearch:11.0.3
RUN mkdir /usr/share/java/kafkaconnect-field-timestamp-router-transforms
COPY kafkaconnect-field-timestamp-router-transforms-1.0-SNAPSHOT.jar /usr/share/java/kafkaconnect-field-timestamp-router-transforms/kafkaconnect-field-timestamp-router-transforms-1.0-SNAPSHOT.jar
```

#### example
For example, with `elasticsearchSinkConnector` the target index will be according to the value of `type` field from each reacord. 

```
{
  "name": "elasticsearch-sink",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "tasks.max": "1",
    "topics": "orders",
    "key.ignore": "true",
    "type.name": "order",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "schema.ignore": "true",
    "connection.url": "http://elasticsearch:9200",
    "name": "elasticsearch-sink",
    "transforms":"fieldTimestampRouter",
    "transforms.fieldTimestampRouter.type":"br.com.emmanuelneri.kafka.connect.smt.FieldTimestampRouter",
    "transforms.fieldTimestampRouter.topic.format":"${topic}-${field}-${timestamp}",
    "transforms.fieldTimestampRouter.timestamp.format":"YYYYMM",
    "transforms.fieldTimestampRouter.field.name":"type"
  }
}
```
The result from `http://localhost:9200/_cat/indices` are indices by record topic name + type record field value + year month from message timestamp. 
```
yellow open orders-purchase-202104 -vwFSkr1TI-A5ssid9WU3g 1 1 500 0 55.1kb 55.1kb
yellow open orders-sale-202104     MHSG0sW5T_Oa8Db9oq6Qug 1 1 100 0   37kb   37kb
```