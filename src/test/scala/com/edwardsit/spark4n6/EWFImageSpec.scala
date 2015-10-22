package com.edwardsit.spark4n6

import java.nio.ByteBuffer

import org.apache.hadoop.fs.Path
import org.specs2.mutable.Specification

import scala.reflect.ClassTag

/**
 * Created by Derek on 10/20/2015.
 */
class EWFImageSpec  extends Specification{

  "this is my specification" >> {
    val path = new Path("../macwd.E01").toUri.toString.getBytes
    val offset = 12L * 1024L * 1024L * 1024L
    val key = ByteBuffer.allocate(java.lang.Long.SIZE + path.length).putLong(offset).put(path)
    key.flip()
    val value = (0x00 to 0xff).map(_.toByte) toArray
    val result = EWFImage.toHBasePrep(new Tuple2(key.array(),value))

    "where the data offset of toHBasePrep is element #2" >> {
      result._2 must_== offset
    }
    "where the path is element #4" >> {
      result._4.getName must_== "macwd.E01"
    }
  }
}
