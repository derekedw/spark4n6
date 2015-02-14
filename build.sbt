//
// http://spark.apache.org/docs/latest/quick-start.html#a-standalone-app-in-scala
//
name         := "spark4n6"
organization := "com.edwardsit"
version      := "1.0"

// lazy val root = (project in file(".")).enablePlugins(PlayJava)

releaseSettings

scalariformSettings

scalaVersion := "2.11.1"

libraryDependencies ++= Seq(
  "org.apache.pig" % "pig" % "0.14.0",
  "org.apache.hadoop" % "hadoop-common" % "2.4.1",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.4.1",
  javaJdbc,
  javaEbean,
  cache,
  javaWs
)

// initialCommands in console := """
//   |import org.apache.spark._
//   |import org.apache.spark.streaming._
//   |import org.apache.spark.streaming.StreamingContext._
//   |import org.apache.spark.streaming.dstream._
//   |import akka.actor.{ActorSystem, Props}
//   |import com.typesafe.config.ConfigFactory
//   |""".stripMargin
