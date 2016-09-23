package com.remarkmedia.spark.task

import com.remarkmedia.spark.common.Profile
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hz on 16/6/7.
  */
object ProFileCount extends App with Profile {

  val sparkConf = new SparkConf().setAppName("FemaleTask").setMaster("yarn-cluster")

  val sc = new SparkContext(sparkConf)


  val conf = HBaseConfiguration.create()
  conf.set(TableInputFormat.INPUT_TABLE, "profile")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  conf.set("hbase.zookeeper.quorum", "phb01,phb02,phb03")
  conf.setLong("hbase.rpc.timeout", 24 * 60 * 60 * 1000)


  val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    classOf[org.apache.hadoop.hbase.client.Result])

  val kv = hBaseRDD.map(
    data => {

      if (data._2.getValue(f, target) == null) {
        ("error", 1l)
      } else {
        val target_val = new String(data._2.getValue(f, target))
        (target_val, 1l)
      }

    }

  ).countByKey()

  kv.foreach(
    data => {
      println(data._1 + ":" + data._2)
    }

  )


}
