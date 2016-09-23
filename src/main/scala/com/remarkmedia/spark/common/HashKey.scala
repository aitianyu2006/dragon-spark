package com.remarkmedia.spark.common

import java.util

import com.google.common.hash.Hashing
import com.remarkmedia.spark.common.Hash.ReginInfo
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ArrayBuffer

/**
  * Created by hz on 16/7/12.
  */
object HashKey {

  val regions = new ArrayBuffer[ReginInfo]()

  val second_info = new util.TreeMap[Long, ReginInfo]()

  val hash = Hashing.murmur3_128()

  def init() = {
    var i = 1
    while (i < 128) {
      regions.+=(new ReginInfo(i, "regions:" + i))
      i = i + 1
    }

    regions.foreach(
      shardInfo => {
        second_info.put(hash.hashBytes(("regions:" + shardInfo.id).getBytes()).asLong(), shardInfo)
      }
    )
  }

  def getRegionInfo(oid: String): Long = {
    val hashKey = hash.hashBytes(oid.getBytes()).asLong()
    val tail = second_info.tailMap(hashKey)
    var starkey: Long = second_info.firstKey()
    if (!tail.isEmpty) {
      starkey = tail.firstKey()
    }
    starkey
  }


//  def main(args: Array[String]): Unit = {
//    HashKey.init()
//    val it = second_info.entrySet().iterator()
//
//    while (it.hasNext) {
//      val kv = it.next()
//      println(kv.getKey)
//
//      println(kv.getValue)
//    }
//
//  }

  def getSplits() = {
    val splitKeys = Array.ofDim[Array[Byte]](127)
    val rows = new util.TreeSet[Array[Byte]](Bytes.BYTES_COMPARATOR)
    val it = second_info.entrySet().iterator()
    while (it.hasNext) {
      val kv = it.next()
      rows.add(Bytes.toBytes(kv.getKey))

    }
    val rowKeyIter = rows.iterator()
    var index = 0
    while (rowKeyIter.hasNext()) {
      val tempRow = rowKeyIter.next()

      if (index < 128) {
        splitKeys(index) = tempRow
        index = index + 1

      }

    }

    splitKeys

  }
}


object Hash {

  case class ReginInfo(val id: Int, val name: String)

}
