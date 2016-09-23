package com.remarkmedia.spark.task

import com.remarkmedia.spark.util.Logs
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hz on 16/6/6.
  */
object OutPutTable extends App with BytesSupport with Logs {
  val sparkConf = new SparkConf().setAppName("OutPutTable").setMaster("yarn-cluster")
  val sc = new SparkContext(sparkConf)
  val conf = HBaseConfiguration.create()
  conf.set(TableInputFormat.INPUT_TABLE, "rd_al_temp")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  conf.set("hbase.zookeeper.quorum", "phb01,phb02,phb03")
  conf.setLong("hbase.rpc.timeout", 24 * 60 * 60 * 1000)

  val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    classOf[org.apache.hadoop.hbase.client.Result])
  val time = System.currentTimeMillis().toString

  hBaseRDD.map(
    data => {
      val id = new String(data._1.get())
      val follow = new String(data._2.getValue("f1".getBytes(), "q1".getBytes()))
      val gender: Int = data._2.getValue("f1".getBytes(), "q2".getBytes())
      val lon: Float = data._2.getValue("f1".getBytes(), "q3".getBytes())
      val lat: Float = data._2.getValue("f1".getBytes(), "q4".getBytes())
      val info = "[" + follow + "|" + gender + "|" + lon + "|" + lat + "]"
      (id, info)
    }
  ).saveAsTextFile("/user/hdfs/rd_al_tmp/" + time)

  logger.info("OutPutTable rd_al_temp path:" + "/user/hdfs/rd_al_tmp/" + time)

}

trait BytesSupport {

  implicit def bytes2int(bytes: Array[Byte]): Int = {
    var i = -1
    if (bytes != null && bytes.length > 0) {
      i = Bytes.toInt(bytes)
    }
    i
  }

  implicit def bytes2Float(bytes: Array[Byte]): Float = {
    var f: Float = 0.0F
    if (bytes != null && bytes.length > 0) {
      f = Bytes.toFloat(bytes)
    }
    f
  }


}
