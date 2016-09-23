package com.remarkmedia.spark.task

import java.net.URI

import com.remarkmedia.spark.common.HbaseColumn.Profile
import com.remarkmedia.spark.common.{Hbase, HbaseColumn}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{Connection, Put, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by hz on 16/7/4.
  */
object Beautiful extends App with Profile {
  // val logger = Logger.getLogger(Beautiful.getClass)
  //  val conn = Hbase.createNewConnection()
  //  if (!conn.getAdmin.tableExists(TableName.valueOf("rd_home_fans"))) {
  //    val ht = new HTableDescriptor(TableName.valueOf("rd_home_fans"))
  //    ht.addFamily(new HColumnDescriptor("f1"))
  //    conn.getAdmin.createTable(ht)
  //
  //    conn.getAdmin
  //  }
  //  conn.close()

  val sparkConf = new SparkConf().setAppName("rd_profile_al_temp_my").setMaster("yarn-cluster")
  val sc = new SparkContext(sparkConf)

  val conn = Hbase.createNewConnection()

  val conf1: Configuration = new Configuration()

  val fs = FileSystem.get(URI.create("hdfs://phb01-vb.hz.kankanapp.com.cn:8020/user/hdfs/weibouser_ss.txt"), conf1)
  val in = fs.open(new Path("hdfs://phb01-vb.hz.kankanapp.com.cn:8020/user/hdfs/weibouser_ss.txt"))
  var profiles = mutable.Map[String, String]()
  val it = Source.fromInputStream(in).getLines()
  it.foreach(
    line => {

      val calls = line.split("  ")
      profiles += calls(1).trim -> calls(0).trim
    }
  )

  //reader.close()
  in.close()

  val brc = new Brc(conn, profiles)

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


        // val conn = Hbase.createNewConnection()
        //val conn_RD = Hbase_RD.createNewConnection()
        val profile_friend = broadcastVar.value.conn.getTable(TableName.valueOf("profile_friend"))
        val rd_beautiful_fans = broadcastVar.value.conn.getTable(TableName.valueOf("rd_ss"))
        rd_beautiful_fans.setWriteBufferSize(10 * 1024 * 1024)
        //        val conf: Configuration = new Configuration()
        //        var profiles = mutable.Map[String, String]()

        //val end=Array[Byte]

        //        val fs = FileSystem.get(URI.create("hdfs://phb01-vb.hz.kankanapp.com.cn:8020/user/hdfs/weibouser_ss.txt"), conf)
        //        val in = fs.open(new Path("hdfs://phb01-vb.hz.kankanapp.com.cn:8020/user/hdfs/weibouser_ss.txt"))
        //        val reader = new BufferedReader(new InputStreamReader(in))
        //        val it = Source.fromInputStream(in).getLines()
        //        it.foreach(
        //          line => {
        //            //logger.info(line)
        //            val calls = line.split("  ")
        //            profiles += calls(1).trim -> calls(0).trim
        //          }
        //        )


        //        reader.close()
        //        in.close()

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

                      brc.profiles.get(oid) match {
                        case Some(name) => {
                          list.+=(name + ":" + oid)
                        }
                        case None =>
                      }


                    }
                    //                    val list = new ArrayBuffer[String]()
                    //                    profiles.foreach(
                    //                      kv => {
                    //                        val get = new Get(Hbase.profileFriendRowKey(targetId_str, target_str, kv._2))
                    //                        if (profile_friend.exists(get)) {
                    //                          list.+=(kv._1 + ":" + kv._2)
                    //                        }
                    //                      }
                    //                    )
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
        /// conn.close()
        // conn_RD.close()

      }

    }

  )


}

case class Brc(conn: Connection, profiles: mutable.Map[String, String])
