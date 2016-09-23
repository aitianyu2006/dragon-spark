package com.remarkmedia.spark.task

import java.nio.ByteBuffer

import com.remarkmedia.spark.common.HbaseColumn.Post_mate
import com.remarkmedia.spark.common.{Hbase, HbaseColumn}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hz on 16/6/16.
  */
object TimerError extends App with Post_mate {

  val sparkConf = new SparkConf().setAppName("TimerError").setMaster("yarn-cluster")
  val sc = new SparkContext(sparkConf)

  val conf = HBaseConfiguration.create()
  conf.set(TableInputFormat.INPUT_TABLE, POST_META_TABLE_NAME)
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  conf.set("hbase.zookeeper.quorum", "phb01,phb02,phb03")
  conf.setLong("hbase.rpc.timeout", 24 * 60 * 60 * 1000)
  conf.set(TableInputFormat.SCAN_CACHEBLOCKS, "false")

  val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    classOf[org.apache.hadoop.hbase.client.Result])


  hBaseRDD.foreachPartition(
    partition => {
      if (partition != null && !partition.isEmpty) {
        val conn = Hbase.createNewConnection()
        val table = conn.getTable(TableName.valueOf(POST_META_TABLE_NAME))
        val error_post_table = conn.getTable(TableName.valueOf("error_post"))
        table.setWriteBufferSize(5 * 1024 * 1024)
        error_post_table.setWriteBufferSize(5 * 1024 * 1024)
        partition.foreach(
          results => {
            val result = results._2
            val rowKey = results._1.get()

            val post_mate_id = result.getValue(HbaseColumn.CF, POST_META_ID)
            val post_mate_targetId = result.getValue(HbaseColumn.CF, POST_META_TARGET_ID)
            val post_mate_target = result.getValue(HbaseColumn.CF, POST_META_TARGET)
            val identity = result.getValue(HbaseColumn.CF, POST_META_IDENTITY)

            if ("weibo" == Bytes.toString(post_mate_target) && (identity != null && identity.length > 0 && 2 == Bytes.toInt(identity))) {
              val bbf = ByteBuffer.wrap(rowKey)
              val profileKey: Array[Byte] = new Array[Byte](16)
              bbf.get(profileKey)
              val timestamp_max_arry: Array[Byte] = new Array[Byte](8)
              bbf.get(timestamp_max_arry)
              val timestamp_max = Bytes.toLong(timestamp_max_arry)
              val timestamp = Long.MaxValue - timestamp_max

              val put = new Put(result.getRow)
              put.addColumn(HbaseColumn.CF, POST_META_ID, post_mate_id)
              put.addColumn(HbaseColumn.CF, POST_META_TARGET_ID, post_mate_targetId)
              put.addColumn(HbaseColumn.CF, POST_META_TARGET, post_mate_target)
              put.addColumn(HbaseColumn.CF, POST_META_TIMESTAMP, Bytes.toBytes(timestamp))
              table.put(put)

              val put1 = new Put(result.getRow)
              put1.addColumn("f1".getBytes(), "q1".getBytes(), Bytes.toBytes(timestamp))
              error_post_table.put(put1)
            }


          }
        )
        error_post_table.close()
        table.close()
        conn.close()
      }
    }
  )


}
