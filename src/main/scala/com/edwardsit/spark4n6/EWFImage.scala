package com.edwardsit.spark4n6

import java.net.URI
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.security.MessageDigest
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
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
  def rowKeysOf(s: String): Iterable[String] = {
    rowKeyTable.filter(e => s.equals(e._1.substring(0,e._1.length-8))) values
  }
  def list(): Unit = {
    /* for (rk <- rowKeyTable) {
      println(rk._1 + " with row key " + rk._2)
    } */
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    // conf.set("spark.executor.extraClassPath","/user/hadoop/spark4n6_2.10-1.0.jar")
    val sc = new SparkContext("yarn-client","EWFImageLoad", conf)
    createTableIfNecessary
    if ("list".equals(args(0))) list()
    else if ("load".equals(args{0})) {
      val image = new EWFImage(sc, args(1))
      image.load
    }
  }
  def createTableIfNecessary() {
    // Initialize hBase table if necessary
    val hConf = HBaseConfiguration.create(new Configuration)
    val admin = new HBaseAdmin(hConf)
    val connection = HConnectionManager.createConnection(hConf)
    if (!admin.isTableAvailable(tableNameDefault)) {
      val tableDesc = new HTableDescriptor(tableNameDefault)
      admin.createTable(tableDesc, Array(
	"4000000000000000000000000000000000000000".getBytes,
	"8000000000000000000000000000000000000000".getBytes,
	"c000000000000000000000000000000000000000".getBytes
      ))
      val imagePath = new Path("/")
      val fs = imagePath.getFileSystem(hConf)
      val blockSize = fs.getFileStatus(imagePath).getBlockSize
      tableDesc.addFamily(new HColumnDescriptor(familyNameDefault.getBytes).setBlocksize(blockSize.toInt))
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
  def canonicalNameOf(filename: String): String = {
    val path = new Path(filename)
    path.toUri.getRawPath
  }
  /* def rowKey(key: Array[Byte]) : String = {
    val keyBuf = ByteBuffer.wrap(key)
    val index = keyBuf.getLong()
    val pathBuf = new Array[Byte](keyBuf.remaining())
    keyBuf.get(pathBuf)
    val pathname = new String(pathBuf)
    val gb = index / 1024L / 1024L / 1024L
    val rkCanonical = pathname.concat(gb.toHexString)
    /* if (rowKeyTable contains(rkCanonical))
      rowKeyTable(rkCanonical)
    else { */
      val md = MessageDigest.getInstance("SHA1")
      val partitionBytes = ByteBuffer.allocate(java.lang.Long.SIZE).putLong(gb).array()
      md.update(partitionBytes)
      md.update(canonicalNameOf(filename).getBytes)
      val hash = Hex.encodeHexString(md.digest())
      // rowKeyTable + (rkCanonical -> hash)
      hash
    // }
  }*/
  def load {
    // delegate image reading and parsing to concrete image class
    val rawBlocks = sc.newAPIHadoopFile[BytesWritable, BytesWritable, EWFImageInputFormat](image)
    val blocks = rawBlocks.map(b => (b._1.copyBytes(), b._2.copyBytes)).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val hbasePrep = blocks.map(b => {
      val keyBuf = ByteBuffer.wrap(b._1)
      val index = keyBuf.getLong()
      val pathBuf = new Array[Byte](keyBuf.remaining())
      keyBuf.get(pathBuf)
      val pathname = new String(pathBuf)
      val gb = ByteBuffer.allocate(java.lang.Long.SIZE).putLong(index / 1024L / 1024L / 1024L).array()
      val md = MessageDigest.getInstance("SHA1")
      md.update(pathname.getBytes)
      md.update(gb)
      (md.digest,index,b._2)
    })
    val hbaseStore = hbasePrep.map(b => {
      val columnBytes = ByteBuffer.allocate(java.lang.Long.SIZE).putLong(b._2).array()
      (Hex.encodeHexString(b._1), new Put(b._1).add(EWFImage.familyNameDefault.getBytes, columnBytes, b._3))
    })
    hConf.setClass("mapreduce.outputformat.class",
      classOf[TableOutputFormat[Object]], classOf[OutputFormat[Object, Writable]])
    hConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    hbaseStore.foldByKey(null) ((p1,p2) => {
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
  /* def restore {

  } */
}
