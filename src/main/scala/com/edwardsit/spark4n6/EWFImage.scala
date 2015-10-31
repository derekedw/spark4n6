package com.edwardsit.spark4n6

import java.net.URI
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.security.MessageDigest
import java.util.NavigableMap
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.io.{Writable, BytesWritable, LongWritable}
import org.apache.hadoop.mapreduce.{OutputFormat}
import org.apache.log4j.{Level,Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

import scala.collection.{immutable, mutable}
import collection.JavaConversions._

/**
 * Created by Derek on 10/13/2015.
 */
object EWFImage {
  val log = Logger.getLogger(getClass.getName)
  log.setLevel(Level.DEBUG)
  val tableNameDefault: String = "images"
  val familyNameDefault : String = "images"
  val rowKeyTableName: String = "row-keys"
  var rowKeyTable = new immutable.TreeMap[String,NavigableMap[Array[Byte],Array[Byte]]]
  def list(tableName: String = EWFImage.rowKeyTableName, familyName : String = EWFImage.familyNameDefault) {
    val conf = HBaseConfiguration.create()
    val connection = HConnectionManager.createConnection(new Configuration)
    val table = connection.getTable(tableName.getBytes)
    val scan = new Scan()
    val rs = table.getScanner(scan)
    for (r <- rs) {
      rowKeyTable = rowKeyTable + (new String(r.getRow) -> r.getFamilyMap(familyName.getBytes))
    }
    if (rs != null)
      rs.close()
    if (connection != null)
      connection.close()
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    createTableIfNecessary
    if ("list".equals(args(0))) {
      list()
      for (image <- rowKeyTable) {
        for (gb <- image._2)
          printf(image._1 + ":" + gb._1 + " is under " + gb._2)
      }
    }
    else if ("load".equals(args{0})) {
      val sc = new SparkContext("yarn-client", "EWFImageLoad", conf)
      val image = new EWFImage(args(1))
      image.load(sc)
    }
  }
  def createTableIfNecessary() {
    // Initialize hBase table if necessary
    val hConf = HBaseConfiguration.create(new Configuration)
    val admin = new HBaseAdmin(hConf)
    val connection = HConnectionManager.createConnection(hConf)
    if (!admin.isTableAvailable(tableNameDefault)) {
      val tableDesc = new HTableDescriptor(tableNameDefault)
      val fam = new HColumnDescriptor(familyNameDefault.getBytes)
      tableDesc.addFamily(fam)
      admin.createTable(tableDesc, Array(
        "2000000000000000000000000000000000000000".getBytes,
	      "4000000000000000000000000000000000000000".getBytes,
        "6000000000000000000000000000000000000000".getBytes,
	      "8000000000000000000000000000000000000000".getBytes,
        "a000000000000000000000000000000000000000".getBytes,
	      "c000000000000000000000000000000000000000".getBytes,
        "e000000000000000000000000000000000000000".getBytes
      ))
    }
    if (!admin.isTableAvailable(rowKeyTableName)) {
      val tableDesc2 = new HTableDescriptor(rowKeyTableName)
      val fam2 = new HColumnDescriptor(familyNameDefault.getBytes)
      tableDesc2.addFamily(fam2)
      admin.createTable(tableDesc2)
    }
    if (connection != null)
      connection.close()
  }
  def toHBasePrep(b: Tuple2[Array[Byte], Array[Byte]]): Tuple5[String,Long,Array[Byte],Path,Array[Byte]] = {
    val keyBuf = ByteBuffer.wrap(b._1)
    val index = keyBuf.getLong
    val len = keyBuf.getInt()
    val pathBuf = new Array[Byte](len)
    keyBuf.get(pathBuf)
    val pathname = new String(pathBuf)
    val path = new Path(pathname)
    val blkId = ByteBuffer.allocate(java.lang.Long.SIZE/8).putLong(index / 128L / 1024L / 1024L)
    blkId.flip()
    val md = MessageDigest.getInstance("SHA1")
    md.update(pathname.getBytes)
    md.update(blkId)
    (Hex.encodeHexString(md.digest),index,b._2,path,blkId.array())
  }
  def toImageColumn(b: Tuple5[String,Long,Array[Byte],Path,Array[Byte]]): Tuple2[ImmutableBytesWritable,Put] = {
    val index = ByteBuffer.allocate(java.lang.Long.SIZE/8).putLong(b._2).array()
    val put = new Put(b._1.getBytes).add(familyNameDefault.getBytes,index,b._3)
    (new ImmutableBytesWritable(b._1.getBytes),put)
  }
  def toRowKeyTuple(b: Tuple5[String,Long,Array[Byte],Path,Array[Byte]]): Tuple3[String,Array[Byte],String] = {
    (b._4.getName,b._5,b._1)
  }
  def toRowKeyColumn(b: Tuple3[String,Array[Byte],String]): Tuple2[ImmutableBytesWritable,Put] = {
    val put = new Put(b._1.getBytes).add(familyNameDefault.getBytes,b._2,b._3.getBytes)
    (new ImmutableBytesWritable(b._1.getBytes),put)
  }
}

class EWFImage(image: String, backupPath: Path = null, verificationHashes: Array[Array[Byte]] = null, metadata: URI = null) {
  /* def verify() {
    if (verificationHashes == null) {
     throw new IllegalArgumentException ("No verification hashes specified");
    } else {

    }

  }
  def backup {
  } */
  def rowKeys: Iterator[Array[Byte]] = {
    if (! EWFImage.rowKeyTable.contains(image)) {
      EWFImage.list()
    }
    val item = EWFImage.rowKeyTable(image)
    item.valuesIterator
  }
  def load(sc: SparkContext, tableName: String = EWFImage.tableNameDefault, familyName : String = EWFImage.familyNameDefault): Unit = {
    // delegate image reading and parsing to concrete image class
    val rawBlocks = sc.newAPIHadoopFile[BytesWritable, BytesWritable, EWFImageInputFormat](image)
    val blocks = rawBlocks.map(b => (b._1.copyBytes(), b._2.copyBytes)).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val hbasePrep = blocks.map(b => EWFImage.toHBasePrep(b))
    val hConf = HBaseConfiguration.create(sc.hadoopConfiguration)
    hConf.setClass("mapreduce.outputformat.class",
      classOf[TableOutputFormat[Object]], classOf[OutputFormat[Object, Writable]])
    hConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    hConf.set("hbase.client.keyvalue.maxsize","0")
    hbasePrep.map(b => EWFImage.toImageColumn(b)).saveAsNewAPIHadoopDataset(hConf)
    val hConf2 = HBaseConfiguration.create(sc.hadoopConfiguration)
    hConf2.setClass("mapreduce.outputformat.class",
      classOf[TableOutputFormat[Object]], classOf[OutputFormat[Object, Writable]])
    hConf2.set(TableOutputFormat.OUTPUT_TABLE, EWFImage.rowKeyTableName)
    hbasePrep.map(b => EWFImage.toRowKeyTuple(b))
      .distinct()
      .map(b => EWFImage.toRowKeyColumn(b))
      .saveAsNewAPIHadoopDataset(hConf2)
  }
  /* def restore {

  } */
}
