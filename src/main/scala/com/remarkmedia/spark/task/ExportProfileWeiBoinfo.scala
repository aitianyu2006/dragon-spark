package com.remarkmedia.spark.task

import com.remarkmedia.spark.examples.HtableCount._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
  * Created by hz on 15/12/8.
  */
object ExportProfileWeiBoinfo {
  def main(args: Array[String]) {

    info("begin Export Profile WeiBo info ,fron table name:post")

    val sparkConf = new SparkConf().setAppName("ExportProfileWeiBoinfo").setMaster("yarn-cluster")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()

    conf.set(TableInputFormat.INPUT_TABLE, "post")

    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "phb02,phb03,phb04")
    conf.setLong("hbase.rpc.timeout", 24 * 60 * 60 * 1000)


    val q = Array[Byte](0x00.toByte)
    val f = Array[Byte](0x41.toByte)
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val result = hBaseRDD.map(data => {
      val line = data._2.getValue(f, q)
      val result = new String(line)
      implicit val formats = DefaultFormats
      if (data._2 == null || data._2.isEmpty || data._2.getValue(f, q) == null || data._2.getValue(f, q).isEmpty) {
        ("error", "")
      } else {
        val result = new String(data._2.getValue(f, q))
        val json = parse(result)
        val resourceBean = json.extract[Resource]
        if (resourceBean.sourceUserId.endsWith("weibo")) {
          (resourceBean.id, resourceBean.textContent)
        } else {
          ("error", "")
        }
      }
    }).reduceByKey(rkv).map(data => {
      data._2
    })

    val array = result.collect()

    array.foreach(data => {

    })
    sc.stop()
    info("ending Export Profile WeiBo info ,from table name:post")
  }

  def rkv(v1: String, v2: String): String = {
    if (v1.equals("") && v2.equals("")) {
      v1 + "|" + v2
    } else {
      ""
    }

  }

}

case class Resource(id: String, textContent: String, sourceUserId: String) extends Serializable

