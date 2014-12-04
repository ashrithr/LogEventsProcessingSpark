## Sample workflow illustrating spark streaming, kafka and cassandra integration.

**Pre-Requisites**:

 * git
 * sbt
 * scala
 * [kafka](https://kafka.apache.org)
 * [spark](https://spark.apache.org)
 * [cassandra](https://cassandra.apache.org)

1. Get the source and build a package for spark streaming job

    ```
    cd /opt/
    git clone https://github.com/ashrithr/LogEventsProcessingSpark.git
    cd LogEventsProcessingSpark
    sbt package
    ```

2. To emulate real time log events, we are going to use an application called as [generator](https://github.com/cloudwicklabs/generator)

    ```
    cd /opt/
    git clone https://github.com/cloudwicklabs/generator.git
    cd generator
    sbt assembly
    ```

3. Start an instance of zookeeper server, which is required by kafka:

    ```
    cd ${KAFKA_HOME}
    bin/zookeeper-server-start.sh config/zookeeper.properties
    ```

    > Replace ${KAFKA_HOME} with path where you have installed kafka

4. Start an instance of kafka broker:

    ```
    cd ${KAFKA_HOME}
    bin/kafka-server-start.sh config/server.properties
    ```

    > Replace ${KAFKA_HOME} with path where you have installed kafka

5. Create a kafka topic (with retention period of 60 minutes):

    ```
    cd ${KAFKA_HOME}
    bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic apache \
        --partitions 2 --replication-factor 1 \
        --config flush.messages=1 --config retention.ms=60
    ```
    more options: [here](http://kafka.apache.org/documentation.html#topic-config)

    > Replace ${KAFKA_HOME} with path where you have installed kafka. Also, change the replication factor
    > and number of partitions based on how many kafka brokers you have

6. Start an instance of spark server:

    ```
    ${SPARK_HOME}/sbin/start-master.sh
    ```

    > Replace ${SPARK_HOME} with path where you have installed spark

7. Start an instance of worker locally:

    ```
    ${SPARK_HOME}/sbin/start-slave.sh Worker ${SPARK_URL}
    ```

    > where, `SPARK_URL="spark://Ashriths-MacBook-Pro.local:7077"`, replace that with your instance of
    > spark server

8. Start an instance of cassandra in foreground, if you already have a cassandra instance ignore this step:

    ```
    ${CASSANDRA_HOME}/bin/cassandra -f
    ```

9. Start the generator, which will simulate log events and writes it to kafka directly:

    ```
    cd /opt/generator
    bin/generator log --eventsPerSec 1 --outputFormat text --destination kafka \
        --kafkaBrokerList "localhost:9092" --flushBatch 1 --kafkaTopicName apache
    ```

Finally, start Spark streaming application:

> Make sure to edit the parameters in `run.sh` as required to match your environment

```
SCALA_HOME="/opt/scala" ./run.sh
```

You could check the spark consumption from kafka to make sure spark is reading events from kafka:

```
cd ${KAFKA_HOME}
bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker \
   --zkconnect localhost:2181 \
   --group sparkFetcher
```
