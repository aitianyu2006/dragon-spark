package com.remarkmedia.spark.task

import java.nio.ByteBuffer

import com.remarkmedia.spark.common.HbaseColumn
import com.remarkmedia.spark.common.HbaseColumn.Post_mate
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by hz on 16/6/22.
  */
object HashTag extends App with Post_mate {

  val sparkConf = new SparkConf().setAppName("HashTag").setMaster("yarn-cluster").set("spark.kryoserializer.buffer", "128")
  val sc = new SparkContext(sparkConf)

  //sc.defaultMinPartitions

  val conf = HBaseConfiguration.create()

  conf.set(TableInputFormat.INPUT_TABLE, POST_META_TABLE_NAME)
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  conf.set("hbase.zookeeper.quorum", "phb01,phb02,phb02")
  conf.setLong("hbase.rpc.timeout", 24 * 60 * 60 * 1000)

  val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    classOf[org.apache.hadoop.hbase.client.Result])


  val tag_map = hBaseRDD.flatMap(
    tagsFlatMap).map(tag => (tag, 1l))
  val tag_reduce = tag_map.reduceByKey(_ + _).sortBy(x => x._2, false).filter(x => x._2 >= 10000)
  tag_reduce.saveAsTextFile("/user/hdfs/hashtag/" + System.currentTimeMillis().toString + "/")

  def tagsFlatMap(kv: (ImmutableBytesWritable, Result)): Array[String] = {
    val arrayBuffer = new ArrayBuffer[String]()
    val tag = kv._2.getValue(HbaseColumn.CF, POST_META_TAG)
    if (tag != null && tag.length >= 4) {
      val buffer = ByteBuffer.wrap(tag)
      val strings = ArrayBuffer[String]()
      while (buffer.remaining() > 0) {
        val size = buffer.getInt()

        val stringBytes = new Array[Byte](size)
        buffer.get(stringBytes)
        strings += Bytes.toString(stringBytes)
      }
      val tags = strings.toArray

      tags.foreach(
        tag_str => arrayBuffer.+=(tag_str)
      )
    }
    arrayBuffer.toArray
  }


}



