package com.edwardsit.spark4n6

import java.net.URI
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.security.MessageDigest
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.io.{Writable, BytesWritable, LongWritable}
import org.apache.hadoop.mapreduce.{OutputFormat}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

import scala.collection.{immutable, mutable}
import collection.JavaConversions._

/**
 * Created by Derek on 10/13/2015.
 */
object EWFImage {
  val tableNameDefault: String = "images"
  val familyNameDefault : String = "images"
  val rowKeyTableName: String = "row-keys"
  val rowKeyTable = new immutable.TreeMap[String,immutable.TreeMap[Array[Byte],Array[Byte]]]
  def list(tableName: String = EWFImage.rowKeyTableName, familyName : String = EWFImage.familyNameDefault) {
    val conf = HBaseConfiguration.create()
    // Initialize hBase table if necessary
    val connection = HConnectionManager.createConnection(new Configuration)
    val table = connection.getTable(tableName.getBytes)
    val scan = new Scan()
    scan.addFamily(familyName.getBytes)
    val rs = table.getScanner(scan)
    for (r <- rs) {
      rowKeyTable + (new String(r.getRow) -> r.getFamilyMap(familyName.getBytes))
    }
    if (rs != null)
      rs.close()
    if (connection != null)
      connection.close()
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    // conf.set("spark.executor.extraClassPath","/user/hadoop/spark4n6_2.10-1.0.jar")
    val sc = new SparkContext("yarn-client", "EWFImageLoad", conf)
    createTableIfNecessary
    if ("list".equals(args(0))) {
      list()
      for (image <- rowKeyTable) {
        for (gb <- image._2)
          printf(image._1 + ":" + gb._1 + " is under " + gb._2)
      }
    }
    else if ("load".equals(args{0})) {
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
      val imagePath = new Path("/")
      val fs = imagePath.getFileSystem(hConf)
      val blockSize = fs.getFileStatus(imagePath).getBlockSize
      tableDesc.addFamily(new HColumnDescriptor(familyNameDefault.getBytes).setBlocksize(blockSize.toInt))
      admin.createTable(tableDesc, Array(
	"4000000000000000000000000000000000000000".getBytes,
	"8000000000000000000000000000000000000000".getBytes,
	"c000000000000000000000000000000000000000".getBytes
      ))
    }
    if (!admin.isTableAvailable(rowKeyTableName)) {
      val tableDesc2 = new HTableDescriptor(rowKeyTableName)
      tableDesc2.addFamily(new HColumnDescriptor(familyNameDefault.getBytes))
      admin.createTable(tableDesc2)
    }
    if (connection != null)
      connection.close()
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
    val item = EWFImage.rowKeyTable(image)
    item.valuesIterator
  }
  def load(sc: SparkContext, tableName: String = EWFImage.tableNameDefault, familyName : String = EWFImage.familyNameDefault): Unit = {
    // delegate image reading and parsing to concrete image class
    val rawBlocks = sc.newAPIHadoopFile[BytesWritable, BytesWritable, EWFImageInputFormat](image)
    val blocks = rawBlocks.map(b => (b._1.copyBytes(), b._2.copyBytes)).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val hbasePrep = blocks.map(b => {
      val keyBuf = ByteBuffer.wrap(b._1)
      val index = keyBuf.getLong
      val pathBuf = new Array[Byte](keyBuf.remaining())
      keyBuf.get(pathBuf)
      val pathname = new String(pathBuf,Charset.forName("US-ASCII"))
      val path = new Path(pathname)
      val gb = ByteBuffer.allocate(java.lang.Long.SIZE).putLong(index / 1024L / 1024L / 1024L).array()
      val md = MessageDigest.getInstance("SHA1")
      md.update(pathname.getBytes)
      md.update(gb)
      (Hex.encodeHexString(md.digest),index,b._2,path,gb)
    })
    val hConf = HBaseConfiguration.create(sc.hadoopConfiguration)
    hConf.setClass("mapreduce.outputformat.class",
      classOf[TableOutputFormat[Object]], classOf[OutputFormat[Object, Writable]])
    hConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    hConf.set("hbase.client.keyvalue.maxsize","0")
    hbasePrep.map(b => {
      val index = ByteBuffer.allocate(java.lang.Long.SIZE).putLong(b._2).array()
      val put = new Put(b._1.getBytes).add(EWFImage.familyNameDefault.getBytes,index,b._3)
      (new ImmutableBytesWritable(b._1.getBytes),put)
    }).saveAsNewAPIHadoopDataset(hConf)
    val hConf2 = HBaseConfiguration.create(sc.hadoopConfiguration)
    hConf2.setClass("mapreduce.outputformat.class",
      classOf[TableOutputFormat[Object]], classOf[OutputFormat[Object, Writable]])
    hConf2.set(TableOutputFormat.OUTPUT_TABLE, EWFImage.rowKeyTableName)
    hbasePrep.map(b => {
      val put = new Put(b._4.getName.getBytes).add(EWFImage.familyNameDefault.getBytes,b._5,b._1.getBytes)
      (new ImmutableBytesWritable(b._4.getName.getBytes),put)
    }).saveAsNewAPIHadoopDataset(hConf2)
  }
  /* def restore {

  } */
}
