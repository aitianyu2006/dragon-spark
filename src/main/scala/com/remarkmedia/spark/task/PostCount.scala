package com.remarkmedia.spark.task

import com.remarkmedia.spark.common.Profile
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hz on 16/6/8.
  */
object PostCount extends App with Profile {

  val mrType = args(0).toInt

  val sparkConf = new SparkConf().setAppName("PostCount").setMaster("yarn-cluster")

  val sc = new SparkContext(sparkConf)

  val conf = HBaseConfiguration.create()
  conf.set(TableInputFormat.INPUT_TABLE, "post_meta")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  conf.set("hbase.zookeeper.quorum", "phb01,phb02,phb03")
  conf.setLong("hbase.rpc.timeout", 24 * 60 * 60 * 1000)


  val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    classOf[org.apache.hadoop.hbase.client.Result])

  mrType match {
    case 0 => {
      val count = hBaseRDD.count()
      printf("post---count:" + count)

    }
    case 1 => {
      val target_count = hBaseRDD.map(
        data => {
          val target_val = new String(data._2.getValue(f, url))
          (target_val.trim.toLowerCase, 1l)
        }
      ).countByKey()

      target_count.foreach(
        data => {
          println(data._1 + ":" + data._2)
        }
      )

    }
    case 2 => {
      val typecount = hBaseRDD.map(
        data => {
          val postType_val = new String(data._2.getValue(f, postCount))
          (postType_val, 1l)
        }

      ).countByKey()

      typecount.foreach(
        data => {
          println(data._1 + ":" + data._2)
        }
      )

    }
  }

  sc.stop()
}


