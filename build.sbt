name := "SparkLogEventsProcessing"

version := "1.0"

organization := "com.cloudwick"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "0.9.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "0.9.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "0.9.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.3.0"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.0.1"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "1.0.0"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
