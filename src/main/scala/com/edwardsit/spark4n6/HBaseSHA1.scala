package com.edwardsit.spark4n6

import collection.JavaConversions._
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.{HColumnDescriptor, HBaseConfiguration, HTableDescriptor}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Derek on 10/11/2015.
 */
object HBaseSHA1 {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    // conf.set("spark.executor.extraClassPath","/user/hadoop/spark4n6_2.10-1.0.jar")
    val sc = new SparkContext("yarn-client","SHA1", conf)
    val img = new EWFImage(sc,args(0))
    val sha1 = new HBaseSHA1(img.canonicalNameOf(args(0)),EWFImage.rowKeysOf(args(0)),EWFImage.familyNameDefault)
    sha1.calculate
    println(args(0) + " = " + sha1.toHexString)
  }
}

class HBaseSHA1 (tableName: String,rowKeys: Iterable[String],familyName: String) {
  val md = java.security.MessageDigest.getInstance("SHA1")
  var calculated = false
  def toHexString : String = {
    if (! calculated)
      calculate()
    Hex.encodeHexString(md.digest())
  }
  def calculate() {
    val connection = HConnectionManager.createConnection(new Configuration)
    val table = connection.getTable(tableName.getBytes)
    for (key <- rowKeys) {
      val get = new Get(key.getBytes())
      get.addFamily(familyName.getBytes)
      val r = table.get(get)
      val families = r.getNoVersionMap()
      for (columns <- families.values()) {
        for (data <- columns.values()) {
          md.update(data)
        }
      }
    }
    table.close()
    calculated = true
  }
}
