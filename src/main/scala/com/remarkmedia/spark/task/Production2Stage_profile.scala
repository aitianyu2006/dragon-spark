package com.remarkmedia.spark.task

import com.remarkmedia.spark.common.HbaseColumn.Profile
import com.remarkmedia.spark.common.{Hbase, HbaseColumn, Hbase_stage}
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hz on 16/9/1.
  */
object Production2Stage_profile extends Profile {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("ExportProfileWeiBoinfo").setMaster("yarn-cluster")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "profile")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "phb02,phb03,phb04")
    conf.setLong("hbase.rpc.timeout", 24 * 60 * 60 * 1000)

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    hBaseRDD.foreachPartition(
      p => {
        val post = Hbase_stage.createNewConnection().getTable(TableName.valueOf("profile"))
        val profile_friend = Hbase.createNewConnection().getTable(TableName.valueOf("profile_friend"))
        val profile_friend_stage = Hbase_stage.createNewConnection().getTable(TableName.valueOf("profile_friend"))
        p.foreach(
          result => {
            val put = new Put(result._1.get())

            val time = result._2.getValue(HbaseColumn.CF, PROFILE_CREATE_DATA)
            if (time != null && time.length > 0) {
              val time_long = Bytes.toLong(time)
              if (time_long > 61432012800000l) {
                val it = result._2.listCells().iterator()
                while (it.hasNext) {
                  put.add(it.next())
                }
                post.put(put)

                val scan = new Scan()
                scan.setBatch(1000)
                scan.setCacheBlocks(false)
                scan.setStartRow(result._1.get())
                scan.setStopRow(result._1.get() ++ HbaseColumn.STOP)

                val scanner = profile_friend.getScanner(scan)
                val itscan = scanner.iterator()

                while (itscan.hasNext) {
                  val result_friend = itscan.next()
                  val put__friend = new Put(result_friend.getRow)
                  val it__friend = result_friend.listCells().iterator()
                  while (it__friend.hasNext) {
                    put__friend.add(it.next())
                  }
                  profile_friend_stage.put(put)
                }

                scanner.close()
              }
            }

          }
        )
        post.close()
        profile_friend.close()
        profile_friend_stage.close()

      }
    )
  }
}
