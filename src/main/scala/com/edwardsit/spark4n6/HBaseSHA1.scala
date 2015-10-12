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
    val sha1 = new HBaseSHA1(args(0))
    sha1.calculate
    println(args(0) + " = " + sha1.toHexString)
  }
  val tableNameDefault: String = "images"
  val familyNameDefault : String = "images"
}

class HBaseSHA1 (image: java.lang.String, tableName: String = HBaseSHA1.tableNameDefault, familyName : String = HBaseSHA1.familyNameDefault) {
  val md = java.security.MessageDigest.getInstance("SHA1")
  var calculated = false
  def this(image: String) { this(image,HBaseSHA1.tableNameDefault,HBaseSHA1.familyNameDefault) }
  def toHexString : String = {
    if (! calculated)
      calculate()
    Hex.encodeHexString(md.digest())
  }
  def calculate() {
    val conf = HBaseConfiguration.create()
    // Initialize hBase table if necessary
    val admin = new HBaseAdmin(conf)
    val connection = HConnectionManager.createConnection(new Configuration)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor("test")
      admin.createTable(tableDesc)
      tableDesc.addFamily(new HColumnDescriptor(familyName.getBytes))
    }
    val table = connection.getTable(tableName.getBytes)
    val scan = new Scan(image.getBytes,new PrefixFilter(image.getBytes()))
    scan.addFamily(familyName.getBytes)
    val rs = table.getScanner(scan)
    for (r : Result <- rs) {
      val families = r.getNoVersionMap()
      for (columns <- families.values()) {
        for (data <- columns.values()) {
          md.update(data)
        }
      }
    }
    rs.close()  // always close the ResultScanner!
    admin.close()
    calculated = true
  }
}