/*
 * Copyright 2015-2016 Derek Edwards
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.edwardsit.spark4n6

import collection.JavaConversions._
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HColumnDescriptor, HBaseConfiguration, HTableDescriptor}
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import java.io._

/**
 * Created by Derek on 10/11/2015.
 */
object HBaseSHA1 {
  def main(args: Array[String]): Unit = {
    val img = new EWFImage(args(0))
    val output = {
      try {
        val f = new File("/mnt",args(1))
        if (f.exists()) throw new IOException("File exists!")
        Some(new FileOutputStream(f))
      } catch {
        case e: ArrayIndexOutOfBoundsException => None
        case e: IOException => System.exit(1); None
      }
    }
    val sha1 = new HBaseSHA1(img,output)
    sha1.calculate
    println(args(0) + " = " + sha1.toHexString)
  }
}

class HBaseSHA1 (img: EWFImage, output: Option[FileOutputStream]) {
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
    val (it1, it2) = it.duplicate
    val imgSize = it1.size
    val conf = HBaseConfiguration.create()
    // Initialize hBase table if necessary
    val connection = HConnectionManager.createConnection(new Configuration)
    val table = connection.getTable(EWFImage.tableNameDefault.getBytes)
    var bytesRead = 0L
    var i = 0L
    for (row <- it2) {
      val scan = new Scan(row,row)
      if (output.isDefined)
        scan.setBatch(1024)
      val rs = table.getScanner(scan)
      for (result <- rs) {
        for (col <- result.getFamilyMap(EWFImage.familyNameDefault.getBytes)) {
          md.update(col._2)
          // if output is defined, write to the output file
          output.foreach(_.write(col._2))
          bytesRead += col._2.length
        }
      }
      i = i + 1
      log.info(f"${bytesRead.toFloat / 1024.0 / 1024.0 / 1024.0}%,5.2f GiB read, " +
        f"${i.toFloat * 100.0 / imgSize.toFloat}%3.2f%% complete")
      if (rs != null)
        rs.close()
    }
    if (connection != null)
      connection.close()
    calculated = true
  }
}
