#!/bin/bash

SCALA_VERSION=2.10
APPLICATION_NAME="SparkLogEventsProcessing"
APPLICATION_VERSION=1.0
MAIN_CLASS="com.cloudwick.spark.streaming.KafkaWordCount"
SPARK_HOME="/Users/ashrith/BigData/Spark/spark"

SPARK_MASTER="spark://Ashriths-MacBook-Pro.local:7077"
KAFKA_TOPICS="apache"
NUMBER_OF_THREADS=2
SPARK_EXECUTOR_MEMORY="1g"
ZK_QUORUM="localhost:2181"
CASSANDRA_NODES="127.0.0.1"
CASSANDRA_REP_FACTOR=1
CASSANDRA_KEYSPACE="log_events"
KAFKA_CLIENT_GROUP="sparkFetcher"

FWDIR="$(cd `dirname $0`; pwd)"

if [ "$LAUNCH_WITH_SCALA" == "1" ]; then
  if [ `command -v scala` ]; then
    RUNNER="scala"
  else
    if [ -z "$SCALA_HOME" ]; then
      echo "SCALA_HOME is not set" >&2
      exit 1
    fi
    RUNNER="${SCALA_HOME}/bin/scala"
  fi
else
  if [ `command -v java` ]; then
    RUNNER="java"
  else
    if [ -z "$JAVA_HOME" ]; then
      echo "JAVA_HOME is not set" >&2
      exit 1
    fi
    RUNNER="${JAVA_HOME}/bin/java"
  fi
  if [ -z "$SCALA_LIBRARY_PATH" ]; then
    if [ -z "$SCALA_HOME" ]; then
      echo "SCALA_HOME is not set" >&2
      exit 1
    fi
    if [ -d "$SCALA_HOME/lib" ]; then
      SCALA_LIBRARY_PATH="$SCALA_HOME/lib"
    else
      SCALA_LIBRARY_PATH="$SCALA_HOME/libexec/lib"
    fi
  fi
fi

# Build up classpath
# CLASSPATH+="$FWDIR/target/scala-${SCALA_VERSION}/${APPLICATION_NAME}_${SCALA_VERSION}-${APPLICATION_VERSION}.jar"
CLASSPATH+="$FWDIR/target/scala-${SCALA_VERSION}/${APPLICATION_NAME}-assembly-${APPLICATION_VERSION}.jar"

if [ -e "$FWDIR/lib_managed" ]; then
  for jar in `find $FWDIR/lib_managed -name "*.jar"`
  do
      CLASSPATH+=":$jar"
  done
fi

if [ -e "$FWDIR/lib" ]; then
  for jar in `find $FWDIR/lib -name "*.jar"`
  do
      CLASSPATH+=":$jar"
  done
fi

if [ -e "$SPARK_HOME/lib_managed" ]; then
  for jar in `find $SPARK_HOME/lib_managed -name "*.jar"`
  do
      CLASSPATH+=":$jar"
  done
fi

# Figure out whether to run our class with java or with the scala launcher.
# In most cases, we'd prefer to execute our process with java because scala
# creates a shell script as the parent of its Java process, which makes it
# hard to kill the child with stuff like Process.destroy(). However, for
# the Spark shell, the wrapper is necessary to properly reset the terminal
# when we exit, so we allow it to set a variable to launch with scala.
if [ "$SPARK_LAUNCH_WITH_SCALA" == "1" ]; then
  EXTRA_ARGS=""     # Java options will be passed to scala as JAVA_OPTS
else
  CLASSPATH+=":$SCALA_LIBRARY_PATH/scala-library.jar"
  CLASSPATH+=":$SCALA_LIBRARY_PATH/scala-compiler.jar"
  CLASSPATH+=":$SCALA_LIBRARY_PATH/jline.jar"
  # The JVM doesn't read JAVA_OPTS by default so we need to pass it in
  EXTRA_ARGS="$JAVA_OPTS"
fi

if [ -f "$FWDIR/conf/streaming-conf.sh" ]; then
    . $FWDIR/conf/streaming-conf.sh
fi

echo "==================================="
echo "Intializing spark streaming job ..."
echo "==================================="

echo "CLASSPATH: ${CLASSPATH} \n"

exec "$RUNNER" -cp "$CLASSPATH" $MAIN_CLASS $EXTRA_ARGS $SPARK_MASTER $KAFKA_TOPICS \
  $NUMBER_OF_THREADS $ZK_QUORUM $CASSANDRA_NODES $CASSANDRA_KEYSPACE \
  $CASSANDRA_REP_FACTOR $KAFKA_CLIENT_GROUP $SPARK_HOME $CLASSPATH $SPARK_EXECUTOR_MEMORY