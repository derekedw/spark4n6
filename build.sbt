name := """spark4n6"""

version := "1.0"

scalaVersion := "2.11.1"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-streaming_2.10" % "1.2.0",
  "org.apache.pig" % "pig" % "0.14.0",
  "org.apache.hadoop" % "hadoop-common" % "2.4.1",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.4.1",
  // Uncomment to use Akka
  //"com.typesafe.akka" % "akka-actor_2.11" % "2.3.6",
  "junit"             % "junit"           % "4.11"  % "test",
  "com.novocode"      % "junit-interface" % "0.10"  % "test"
)
