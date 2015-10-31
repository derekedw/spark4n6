package com.edwardsit.spark4n6

import collection.JavaConversions._
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HColumnDescriptor, HBaseConfiguration, HTableDescriptor}
import org.apache.log4j.Logger
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
  val log = Logger.getLogger(getClass.getName)
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
    var bytesRead = 0L
    for (row <- it) {
      val scan = new Scan(row,row)
      val rs = table.getScanner(scan)
      for (result <- rs) {
        for (col <- result.getFamilyMap(EWFImage.familyNameDefault.getBytes)) {
          md.update(col._2)
          bytesRead += col._2.length
          log.info(bytesRead + " bytes read")
        }
      }
      if (rs != null)
        rs.close()
    }
    if (connection != null)
      connection.close()
    calculated = true
  }
}
