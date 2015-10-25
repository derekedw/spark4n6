package com.edwardsit.spark4n6

import java.nio.ByteBuffer

import org.apache.hadoop.fs.Path
import org.specs2.mutable.Specification

import scala.reflect.ClassTag

/**
 * Created by Derek on 10/20/2015.
 */
class EWFImageSpec  extends Specification {
  val path = new Path("../macwd.E01").toUri.toString.getBytes
  val offset = 12L * 1024L * 1024L * 1024L
  val key = ByteBuffer.allocate(java.lang.Long.SIZE / 8 + java.lang.Integer.SIZE / 8 + path.length).putLong(offset).putInt(path.length).put(path)
  key.flip()
  val value = (0x00 to 0xff).map(_.toByte) toArray
  val result = EWFImage.toHBasePrep(new Tuple2(key.array(), value))
  val result2 = EWFImage.toRowKeyTuple(result)

  "toHBasePrep specification" >> {
    "where the data offset is element #2" >> {
      result._2 must_== offset
    }
    "where the path is element #4" >> {
      result._4.getName must_== "macwd.E01"
    }
    "where the offset number in GiB is element #5" >> {
      result._5 must_== Array[Byte](0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x0c)
    }
  }
  "toRowKeyTuple specification" >> {
    "where the path is element #2" >> {
      result2._1 must_== "macwd.E01"
    }
  }
}
