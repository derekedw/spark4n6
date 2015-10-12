package com.edwardsit.spark4n6

import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Derek on 10/11/2015.
 */
object HBaseSHA1 {
  def main(args: Array[String]) {
    val sha1 = new HBaseSHA1(args(0))
    sha1.calculate
    println(args(0) + " = " + sha1.toHexString)
  }
}

class HBaseSHA1 (image: String, sparkMaster: String = "yarn-client", table: String = "images") {
  val md = java.security.MessageDigest.getInstance("SHA1")
  def toHexString : String = {
    Hex.encodeHexString(md.digest())
  }
  def calculate {
    val sparkConf = new SparkConf().setAppName("HBaseTest")
    val sc = new SparkContext(sparkMaster,"HBaseSHA1",sparkConf)

    // please ensure HBASE_CONF_DIR is on classpath of spark driver
    // e.g: set it through spark.driver.extraClassPath property
    // in spark-defaults.conf or through --driver-class-path
    // command line option of spark-submit

    val conf = HBaseConfiguration.create()

    // Other options for configuring scan behavior are available. More information available at
    // http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/mapreduce/TableInputFormat.html
    conf.set(TableInputFormat.INPUT_TABLE, table)

    // Initialize hBase table if necessary
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(table)) {
      val tableDesc = new HTableDescriptor(table)
      admin.createTable(tableDesc)
    }

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    hBaseRDD.count()

    sc.stop()
    admin.close()
  }
}