package com.edwardsit.spark4n6

import java.nio.ByteBuffer
import java.util.regex.{Pattern}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.mapreduce.{InputFormat, OutputFormat}
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.JavaConversions._

/**
  * Created by Derek on 2/28/2016.
  */
object Strings {
  def main(args: Array[String]): Unit = {
    Strings.strings.foreach(s => {
	println(s._1)
	s._2.map(x => println("\t" + x))
	})
  }
  def strings : Array[Tuple2[String,Iterable[Long]]] = {
	  val conf = new SparkConf()
	  val sc = new SparkContext("yarn-client", "Strings", conf)
	  val hConf = HBaseConfiguration.create(sc.hadoopConfiguration)
	  hConf.set(TableInputFormat.INPUT_TABLE, EWFImage.tableNameDefault)
	  hConf.setClass("mapreduce.inputformat.class",
	    classOf[TableInputFormat], classOf[InputFormat[ImmutableBytesWritable, Result]])
	  val imageRows = sc.newAPIHadoopRDD(hConf, classOf[TableInputFormat],
	    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
	    classOf[org.apache.hadoop.hbase.client.Result])
	  val imageResults = imageRows.flatMap(x => x._2.list.toList.map(kv => {
	    val keyBuf = ByteBuffer.wrap(kv.getQualifier)
	    (keyBuf.getLong(),kv.getValue)
	  })).repartition(1000)
	val pattern = Pattern.compile("[\\p{Print}\\p{Blank}]{4,}")
	val matches = imageResults.flatMap(kv => {
		var i = 0
		var result = new Array[Tuple2[String,Long]](0)
		val matcher = pattern.matcher(new String(kv._2))
		while(matcher.find(i)) {
			result :+ (new String(kv._2.slice(matcher.start,matcher.end - 1)),
			kv._1 + i)
			i = matcher.end
		}
		result
	})
	matches.groupByKey.collect
  }
}

