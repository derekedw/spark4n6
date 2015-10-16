package com.edwardsit.spark4n6

import java.net.URI
import java.nio.ByteBuffer
import java.security.MessageDigest
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, HConnectionManager, HBaseAdmin}
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
  val rowKeyTable = new immutable.TreeMap[String,String]
  def canonicalNameOf(filename: String): String = {
    val path = new Path(filename)
    path.toUri.getRawPath
  }
  def rowKey(filename: String,key: Long) : String = {
    val gb = key / 1024L / 1024L / 1024L
    val rkCanonical = canonicalNameOf(filename).concat(gb.toHexString)
    if (rowKeyTable contains(rkCanonical))
      rowKeyTable(rkCanonical)
    else {
      val md = MessageDigest.getInstance("SHA1")
      val partitionBytes = ByteBuffer.allocate(java.lang.Long.SIZE).putLong(gb).array()
      md.update(partitionBytes)
      md.update(canonicalNameOf(filename).getBytes())
      val hash = Hex.encodeHexString(md.digest())
      rowKeyTable + (rkCanonical -> hash)
      hash
    }
  }
  def rowKeysOf(s: String): Iterable[String] = {
    rowKeyTable.filter(e => s.equals(e._1.substring(0,e._1.length-8))) values
  }
  def list(): Unit = {
    for (rk <- rowKeyTable) {
      println(rk._1 + " with row key " + rk._2)
    }
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext("yarn-client","EWFImageLoad", conf)
    if ("list".equals(args(0))) list()
    else if ("load".equals(args{0})) {
      val image = new EWFImage(sc, args(1))
      image.load
    }
  }
}

class EWFImage(sc: SparkContext, image: String, tableName: String = EWFImage.tableNameDefault, familyName : String = EWFImage.familyNameDefault, backupPath: Path = null, verificationHashes: Array[Array[Byte]] = null, metadata: URI = null) {
  val hConf = HBaseConfiguration.create(sc.hadoopConfiguration)
  /* def verify() {
    if (verificationHashes == null) {
     throw new IllegalArgumentException ("No verification hashes specified");
    } else {

    }

  }
  def backup {
  } */
  def load {
    // delegate image reading and parsing to concrete image class
    // using EWFRecordReader, parse verificationHashes
    val rawBlocks = sc.newAPIHadoopFile[LongWritable, BytesWritable, EWFImageInputFormat](image)
    val blocks = rawBlocks.map(b => (b._1.get, b._2.copyBytes)).persist(StorageLevel.MEMORY_AND_DISK_SER)
    createTableIfNecessary
    val hbasePrep = blocks.map(b => (EWFImage.rowKey(image,b._1),familyName,b._1,b._2))
    val hbaseStore = hbasePrep.map(b => {
      val columnBytes = ByteBuffer.allocate(java.lang.Long.SIZE).putLong(b._3).array()
      (b._1, new Put(Hex.decodeHex(b._1.toCharArray)).add(b._2.getBytes, columnBytes, b._4))
    })
    // using EWFRecordReader, parse verificationHashes
    hConf.setClass("mapreduce.outputformat.class",
      classOf[TableOutputFormat[Object]], classOf[OutputFormat[Object, Writable]])
    hConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val blockTable = blocks.map(b =>
      (EWFImage.rowKey(image,b._1),b._1,b._2)
    )
    val blockStore = blockTable.map(b => (new ImmutableBytesWritable(Hex.decodeHex(b._1.toCharArray)),{
      val columnName = ByteBuffer.allocate(java.lang.Long.SIZE).putLong(b._2).array()
      new Put(Hex.decodeHex(b._1.toCharArray)).add(familyName.getBytes(),columnName,b._3)
    }))
    blockStore.foldByKey(null) ((p1,p2) => {
      if (p1 == null)
        p2
      else {
        for (familyData <- p2.getFamilyMap) {
          for (kv <- familyData._2) {
            p1.add(kv)
          }
        }
        p1
      }
    }).saveAsNewAPIHadoopDataset(hConf)
  }
  private def createTableIfNecessary {
    // Initialize hBase table if necessary
    val admin = new HBaseAdmin(hConf)
    val connection = HConnectionManager.createConnection(new Configuration)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor("test")
      admin.createTable(tableDesc, Array(
        Array[Byte](0x40,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
                    0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
                    0x00,0x00,0x00,0x00),
        Array[Byte](0x80.toByte,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
                    0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
                    0x00,0x00,0x00,0x00),
        Array[Byte](0xc0.toByte,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
                    0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
                    0x00,0x00,0x00,0x00)
      ))
      val imagePath = new Path(image)
      val hConf = sc.hadoopConfiguration
      val fs = imagePath.getFileSystem(hConf)
      val blockSize = fs.getFileStatus(imagePath).getBlockSize
      tableDesc.addFamily(new HColumnDescriptor(familyName.getBytes).setBlocksize(blockSize.toInt))
    }
  }
  /* def restore {

  } */
}
