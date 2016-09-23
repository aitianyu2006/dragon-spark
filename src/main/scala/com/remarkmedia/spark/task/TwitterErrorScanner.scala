package com.remarkmedia.spark.task

import java.util.regex.Pattern

import com.remarkmedia.spark.common.Hbase
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hz on 16/4/12.
  */
object TwitterErrorScanner {
  def main(args: Array[String]) {
    /**
      * config
      */
    val sparkConf = new SparkConf().setAppName("TwitterErrorScanner").setMaster("yarn-cluster")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    /**
      * scan
      */
    val scan = new Scan()
    scan.setCacheBlocks(false)
    scan.setBatch(10000)

    val proto = ProtobufUtil.toScan(scan)
    val ScanToString = Base64.encodeBytes(proto.toByteArray())


    /**
      * setconf
      */
    conf.set(TableInputFormat.INPUT_TABLE, "profile")
    conf.set(TableInputFormat.SCAN, ScanToString)
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "phb02,phb03,phb04")
    conf.setLong("hbase.rpc.timeout", 24 * 60 * 60 * 1000)
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val cf = Array(0x41.toByte)
    val id = Array(0x00.toByte)
    val dataType = Array(0x01.toByte)

    val pattern = Pattern.compile("[0-9]+:twitter", Pattern.CASE_INSENSITIVE)

    val cache = hBaseRDD.map(
      data => {
        val result = data._2

        if (result != null && result.getValue(cf, id) != null && result.getValue(cf, dataType) != null) {
          val id_1 = new String(result.getValue(cf, id))
          val type_1 = new String(result.getValue(cf, dataType))

          val id_2 = id_1 + ":" + type_1

          val matcher = pattern.matcher(id_2)
          if (matcher.matches()) {
            (result.getRow, id_1, type_1)
          } else {
            ("ohter".getBytes(), "1", "1")
          }
        } else {
          ("ohter".getBytes(), "1", "1")
        }

      }
    ).foreachPartition(
      partition => {
        val conn = Hbase.createNewConnection()
        val table = conn.getTable(TableName.valueOf("twitter_error"))

        partition.foreach(
          data => {
            val key = new String(data._1)
            if (!key.equals("other")) {
              val put = new Put(data._1)
              put.addColumn("t_id".getBytes(), "error_id".getBytes(), data._2.getBytes())
              put.addColumn("t_id".getBytes(), "error_tpye".getBytes(), data._3.getBytes())
              table.put(put)
            }
          }
        )
        table.close()
        conn.close()
      }

    )
  }
}
