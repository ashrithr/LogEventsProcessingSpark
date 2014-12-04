import AssemblyKeys._

assemblySettings

name := "SparkLogEventsProcessing"

version := "1.0"

organization := "com.cloudwick"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "0.9.1" exclude("org.apache.hadoop", "hadoop-client")

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "0.9.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "0.9.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.3.0" exclude("com.google.guava", "guava")

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.0.1" exclude("com.google.guava", "guava") exclude("io.netty", "netty")

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "1.0.0"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
    case PathList("org", "apache", xs @ _*) => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
    case PathList("org", "objectweb", xs @ _*) => MergeStrategy.last
    case PathList("org", "slf4j", xs @ _*) => MergeStrategy.last
    case "about.html" => MergeStrategy.rename
    case x => old(x)
  }
}