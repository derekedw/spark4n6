name := """spark4n6"""

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  // Amazon EMR 3.11.0 has HBase
  "com.amazonaws"     %  "aws-java-sdk-s3" % "1.10.41",
  "org.apache.spark"  %% "spark-core"      % "1.3.1",
  "org.apache.hbase"  %  "hbase"           % "0.94.18",
  "org.apache.pig"    %  "pig"             % "0.12.0",
  "org.apache.pig"    %  "piggybank"       % "0.12.0",
  "org.apache.hadoop" %  "hadoop-common"   % "2.4.0",
  "org.apache.hadoop" %  "hadoop-mapreduce-client-core" % "2.4.0",
  /* Amazon EMR 4.0.0 is missing HBase */
  /* "org.apache.spark"  %% "spark-core"     % "1.4.1",
   * "org.apache.pig"    % "pig"             % "0.14.0",
   * "org.apache.pig"    % "piggybank"       % "0.14.0",
   * "org.apache.hadoop" % "hadoop-common"   % "2.6.0",
   * "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.6.0",
   */
  // Uncomment to use Akka
  //"com.typesafe.akka" % "akka-actor_2.11" % "2.3.6",
  "junit"             % "junit"           % "4.11"  % "test",
  "com.novocode"      % "junit-interface" % "0.10"  % "test",
  "org.specs2"        %% "specs2-core"    % "3.6.4" % "test"
)
