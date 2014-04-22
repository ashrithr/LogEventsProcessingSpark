Start zookeeper server:

```
${KAFKA_HOME}/bin/zookeeper-server-start.sh config/zookeeper.properties
```

Start kafka broker:

```
${KAFKA_HOME}/bin/kafka-server-start.sh config/server.properties
```

Create a kafka topic (with retention period of 60 minutes):

```
${KAFKA_HOME}/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic apache \
    --partitions 3 --replication-factor 3 \
    --config flush.message=1 --config retention.ms=60
```
more options: [here](http://kafka.apache.org/documentation.html#topic-config)

Start spark server:

```
${SPARK_HOME}/sbin/start-master.sh
```

Start a worker locally:

```
${SPARK_HOME}/sbin/start-slave.sh Worker ${SPARK_URL}
```
where, SPARK_URL="spark://Ashriths-MacBook-Pro.local:7077"

Start a cassandra instance:

```
${CASSANDRA_HOME}/bin/cassandra -f
```

Start the generator:

```
${GENERATOR_HOME}/bin/generator log --eventsPerSec 1 --outputFormat text --destination kafka --kafkaBrokerList "localhost:9092" --flushBatch 1 --kafkaTopicName apache
```

Start Spark streaming application:

```
sbt package
```

Edit the parameters in `run.sh` as required and then:

```
./run.sh
```