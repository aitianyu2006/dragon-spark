package com.remarkmedia.spark.task

import java.net.URI

import com.remarkmedia.spark.common.HbaseColumn.Profile
import com.remarkmedia.spark.common.{Hbase, HbaseColumn}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import org.apache.log4j.Logger

/**
  * Created by hz on 16/8/1.
  */
object RD2Al extends Profile {

  def main(args: Array[String]) {
    val logger = Logger.getLogger(RD2Al.getClass)
    val conn = Hbase.createNewConnection()
    if (conn.getAdmin.tableExists(TableName.valueOf("rd_al_temp"))) {
      conn.getAdmin.disableTable(TableName.valueOf("rd_al_temp"))
      conn.getAdmin.deleteTable(TableName.valueOf("rd_al_temp"))
    }

    val ht = new HTableDescriptor(TableName.valueOf("rd_al_temp"))
    ht.addFamily(new HColumnDescriptor("f1"))
    conn.getAdmin.createTable(ht)
    conn.close()


    val conf1: Configuration = new Configuration()
    val fs = FileSystem.get(URI.create("hdfs://phb01-vb.hz.kankanapp.com.cn:8020/user/hdfs/weibouser.txt"), conf1)
    val in = fs.open(new Path("hdfs://phb01-vb.hz.kankanapp.com.cn:8020/user/hdfs/weibouser.txt"))
    var profiles = mutable.Map[String, String]()
    val it = Source.fromInputStream(in).getLines()

    it.foreach(
      line => {
        val calls = line.split("  ")
        profiles += calls(1).trim -> calls(0).trim
      }
    )
    in.close()


    val sparkConf = new SparkConf().setAppName("rd_profile_al_temp").setMaster("yarn-cluster")

    sparkConf.registerKryoClasses(Array(classOf[BrcRD2Al]))

    //sparkConf.registerKryoClasses(Array(classOf[BrcRD2Al]))

    val sc = new SparkContext(sparkConf)
    val brc = new BrcRD2Al(profiles)
    val broadcastVar = sc.broadcast(brc)



    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "profile")
    conf.set(TableInputFormat.SCAN_BATCHSIZE, "1000")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "phb01,phb02,phb03")
    conf.setLong("hbase.rpc.timeout", 24 * 60 * 60 * 1000)
    val ending = Array[Byte](0xff.toByte)


    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])


    hBaseRDD.foreachPartition(
      partition => {
        if (partition != null && !partition.isEmpty) {
          val conn = Hbase.createNewConnection()
          val profile_friend = conn.getTable(TableName.valueOf("profile_friend"))
          val rd_beautiful_fans = conn.getTable(TableName.valueOf("rd_al_temp"))
          rd_beautiful_fans.setWriteBufferSize(10 * 1024 * 1024)
          partition.foreach(
            data => {
              if (data._2.containsColumn(HbaseColumn.CF, PROFILE_TARGET_ID) && data._2.containsColumn(HbaseColumn.CF, PROFILE_TARGET)) {
                val targetId = data._2.getValue(HbaseColumn.CF, PROFILE_TARGET_ID)
                val target = data._2.getValue(HbaseColumn.CF, PROFILE_TARGET)
                val gender = data._2.getValue(HbaseColumn.CF, PROFILE_GENDER)
                val lon = data._2.getValue(HbaseColumn.CF, PROFILE_LAST_LOCATION_LON)
                val lat = data._2.getValue(HbaseColumn.CF, PROFILE_LAST_LOCATION_LAT)
                if (targetId != null && target != null) {
                  val targetId_str = new String(targetId)
                  val target_str = new String(target)
                  target_str match {
                    case "weibo" => {
                      val scan = new Scan()
                      scan.setBatch(1000)
                      scan.setCacheBlocks(false)
                      scan.setStartRow(Hbase.profileRowKey(targetId_str, target_str))
                      scan.setStopRow(Hbase.profileRowKey(targetId_str, target_str) ++ HbaseColumn.STOP)

                      val scanner = profile_friend.getScanner(scan)

                      val it = scanner.iterator()

                      val list = new ArrayBuffer[String]()
                      while (it.hasNext()) {
                        val result = it.next()
                        val row = result.getRow.drop(16)

                        val follow = new String(row)

                        val oid = follow.replaceAll(":weibo", "")

                        //logger.info(broadcastVar)

                        val brc_val = broadcastVar.value

                        // logger.info(brc_val.length)
                        //logger.info(brc_val)


                        brc_val.profiles.get(oid) match {
                          case Some(name) => {
                            list.+=(name + ":" + oid)
                          }
                          case None =>
                        }
                      }
                      if (!list.isEmpty) {
                        val put = new Put(targetId)
                        val sb = new StringBuilder()
                        list.foreach(
                          x => sb.append(x + ",")
                        )
                        put.addColumn("f1".getBytes, "q1".getBytes, sb.toString().getBytes())
                        put.addColumn("f1".getBytes, "q2".getBytes, gender)
                        put.addColumn("f1".getBytes, "q3".getBytes, lon)
                        put.addColumn("f1".getBytes, "q4".getBytes, lat)
                        rd_beautiful_fans.put(put)
                      }

                      scanner.close()
                    }

                    case _ =>
                  }

                }
              }

            }
          )
          rd_beautiful_fans.close()
          profile_friend.close()
          conn.close()
        }
      }
    )
  }


  case class BrcRD2Al(profiles: mutable.Map[String, String])

}
