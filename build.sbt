name := """spark4n6"""

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core"     % "1.4.1",
  "org.apache.pig"    % "pig"             % "0.14.0",
  "org.apache.pig"    % "piggybank"       % "0.14.0",
  "org.apache.hadoop" % "hadoop-common"   % "2.6.0",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.6.0",
  // Uncomment to use Akka
  //"com.typesafe.akka" % "akka-actor_2.11" % "2.3.6",
  "junit"             % "junit"           % "4.11"  % "test",
  "com.novocode"      % "junit-interface" % "0.10"  % "test",
  "org.specs2"        %% "specs2-core"    % "3.6.4" % "test"
)
