package com.remarkmedia.spark.task

import java.net.URI

import com.remarkmedia.spark.common.HbaseColumn.Profile
import com.remarkmedia.spark.common.{Hbase, HbaseColumn}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{Get, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by hz on 16/7/6.
  */
object NinjaTurtle extends App with Profile {

  val sparkConf = new SparkConf().setAppName("NinjaTurtle_Task").setMaster("yarn-cluster")
  val sc = new SparkContext(sparkConf)
  val conf = HBaseConfiguration.create()
  conf.set(TableInputFormat.INPUT_TABLE, "profile")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  conf.set("hbase.zookeeper.quorum", "phb01,phb02,phb03")
  conf.setLong("hbase.rpc.timeout", 24 * 60 * 60 * 1000)


  val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    classOf[org.apache.hadoop.hbase.client.Result])

  val collect_local = hBaseRDD.mapPartitions(mapPartitions).map(data => (data, 1l)).reduceByKey(_ + _)
    .sortBy(x => x._2, false).collect()

  collect_local.foreach(
    data => println(data._1 + ":" + data._2)
  )

  def mapPartitions(iterator: Iterator[(ImmutableBytesWritable, Result)]) = {
    val list_all = new ArrayBuffer[String]()
    val conn = Hbase.createNewConnection()
    val profile_friend = conn.getTable(TableName.valueOf("profile_friend"))
    val conf: Configuration = new Configuration()
    var profiles = mutable.Map[String, String]()

    val fs = FileSystem.get(URI.create("hdfs://phb01-vb.hz.kankanapp.com.cn:8020/user/hdfs/NinjaTurtle"), conf)
    val in = fs.open(new Path("hdfs://phb01-vb.hz.kankanapp.com.cn:8020/user/hdfs/NinjaTurtle"))
    val it = Source.fromInputStream(in).getLines()
    it.foreach(
      line => {
        val calls = line.split(":")
        profiles += calls(0) -> calls(1)
      }
    )
    iterator.foreach(
      line => {
        if (line._2.getValue(HbaseColumn.CF, PROFILE_TARGET_ID) != null && line._2.getValue(HbaseColumn.CF, PROFILE_TARGET) != null) {
          val targetId = new String(line._2.getValue(HbaseColumn.CF, PROFILE_TARGET_ID))
          val target = new String(line._2.getValue(HbaseColumn.CF, PROFILE_TARGET))
          target match {
            case "weibo" => {
              val list = new ArrayBuffer[String]()
              profiles.foreach(
                kv => {
                  val get = new Get(Hbase.profileFriendRowKey(targetId, target, kv._1))
                  val get_trulte = new Get(Hbase.profileFriendRowKey(targetId, target, "5064224820"))
                  if (profile_friend.exists(get) && profile_friend.exists(get_trulte)) {
                    list.+=(kv._1 + ":" + kv._2)
                  }
                }
              )

              if (!list.isEmpty) {
                list_all.++=(list)
              }

            }
            case _ =>
          }
        }


      }
    )
    profile_friend.close()
    list_all.iterator
  }

}
