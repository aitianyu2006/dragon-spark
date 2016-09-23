package com.remarkmedia.spark.task

import com.remarkmedia.spark.common.{Hbase, HbaseColumn, Hbase_stage}
import com.remarkmedia.spark.common.HbaseColumn.{Post_mate, Profile}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Get, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by hz on 16/9/21.
  */
object ProfilePostcount extends Post_mate with Profile {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("FemaleTask").setMaster("yarn-cluster")

    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, POST_META_TABLE_NAME)
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "shb01.hzvb.kankanapp.com.cn,shb02.hzvb.kankanapp.com.cn,shb03.hzvb.kankanapp.com.cn")
    conf.setLong("hbase.rpc.timeout", 24 * 60 * 60 * 1000)


    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    hBaseRDD.mapPartitions(mapPartitions).reduceByKey(_ + _).foreachPartition(
      p => {
        val conn = Hbase_stage.createNewConnection()
        val profle = conn.getTable(TableName.valueOf("profile"))
        p.foreach(
          data => {
            val tagets = data._1.split(":")
            if (tagets.length == 2) {
              val taget = tagets(1)
              val id = tagets(0)
              val rowkey = Hbase.profileRowKey(id, taget)
              //val extis = profle.exists(new Get(rowkey))
              val result = profle.get(new Get(rowkey))

              //result.
              if (result != null && result.getExists) {
                val put = new Put(rowkey)
                result.rawCells().foreach(
                  cell => {
                    put.add(cell)
                  }
                )
                put.addColumn(HbaseColumn.CF, RM_POST_COUNT, Bytes.toBytes(data._2))
                profle.put(put)
              }

            }

          }
        )
      }
    )

    def mapPartitions(iterator: Iterator[(ImmutableBytesWritable, Result)]) = {
      val profilesPostCount = mutable.Map[String, Long]()
      iterator.foreach(
        data => {
          val taget: String = data._2.getValue(HbaseColumn.CF, POST_META_TARGET)
          val tagetid: String = data._2.getValue(HbaseColumn.CF, POST_META_TARGET_ID)

          if (tagetid != "" && taget != "") {
            val profileId = tagetid + ":" + taget
            profilesPostCount.get(profileId) match {
              case Some(v) => {
                profilesPostCount.+=(profileId -> (v + 1))
              }
              case None => {
                profilesPostCount.+=(profileId -> 1)
              }
            }
          }
        }
      )

      profilesPostCount.iterator
    }

    implicit def bytes2String(bytes: Array[Byte]): String = {
      var f: String = ""
      if (bytes != null && bytes.length > 0) {
        f = Bytes.toString(bytes)
      }
      f
    }


  }
}
