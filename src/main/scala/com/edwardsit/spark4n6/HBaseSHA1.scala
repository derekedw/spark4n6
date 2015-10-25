package com.edwardsit.spark4n6

import collection.JavaConversions._
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HColumnDescriptor, HBaseConfiguration, HTableDescriptor}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Derek on 10/11/2015.
 */
object HBaseSHA1 {
  def main(args: Array[String]): Unit = {
    val img = new EWFImage(args(0))
    val sha1 = new HBaseSHA1(img)
    sha1.calculate
    println(args(0) + " = " + sha1.toHexString)
  }
}

class HBaseSHA1 (img: EWFImage) {
  val md = java.security.MessageDigest.getInstance("SHA1")
  var calculated = false
  def toHexString : String = {
    if (! calculated)
      calculate()
    Hex.encodeHexString(md.digest())
  }
  def calculate() {
    val it = img.rowKeys
    val conf = HBaseConfiguration.create()
    // Initialize hBase table if necessary
    val connection = HConnectionManager.createConnection(new Configuration)
    val table = connection.getTable(EWFImage.tableNameDefault.getBytes)
    for (row <- it) {
      val get = new Get(row)
      val scan = new Scan(row,row)
      val rs = table.getScanner(scan)
      for (result <- rs) {
        for (col <- result.getFamilyMap(EWFImage.familyNameDefault.getBytes))
          md.update(col._2)
      }
    }
    if (connection != null)
      connection.close()
    calculated = true
  }
}
