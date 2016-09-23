package com.remarkmedia.spark.task

import com.remarkmedia.spark.common.HbaseColumn
import com.remarkmedia.spark.common.HbaseColumn.Post_mate
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hz on 16/6/14.
  */
object InsdisRemove extends Post_mate {
  val sparkConf = new SparkConf().setAppName("FemaleTask").setMaster("yarn-cluster")

  val sc = new SparkContext(sparkConf)

  //    val scan = new Scan()
  //    scan.setCacheBlocks(false)
  //    scan.setBatch(10000)

  //    val proto = ProtobufUtil.toScan(scan)
  //    val ScanToString = Base64.encodeBytes(proto.toByteArray())

  val conf = HBaseConfiguration.create()
  conf.set(TableInputFormat.INPUT_TABLE, POST_META_TABLE_NAME)
  // conf.set(TableInputFormat.SCAN, scan.toString)
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  conf.set("hbase.zookeeper.quorum", "phb01,phb02,phb03")
  conf.setLong("hbase.rpc.timeout", 24 * 60 * 60 * 1000)


  val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    classOf[org.apache.hadoop.hbase.client.Result])


  hBaseRDD.foreachPartition(
    parttiton => {
      if (parttiton != null) {
        parttiton.foreach(
          post => {
            val result = post._2
            val row = post._1.get()

            val target = new String(result.getValue(HbaseColumn.CF, POST_META_TARGET))
            target match {
              case "instagram" => {

              }
              case _ =>
            }


          }
        )
      }

    }

  )
}
