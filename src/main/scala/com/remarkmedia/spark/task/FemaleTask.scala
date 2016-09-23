package com.remarkmedia.spark.task

import com.remarkmedia.spark.common.{Hbase, Profile}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hz on 16/6/6.
  */
object FemaleTask extends Profile {
  def main(args: Array[String]) {


    val sparkConf = new SparkConf().setAppName("FemaleTask").setMaster("yarn-cluster")

    val sc = new SparkContext(sparkConf)

    //    val scan = new Scan()
    //    scan.setCacheBlocks(false)
    //    scan.setBatch(10000)

    //    val proto = ProtobufUtil.toScan(scan)
    //    val ScanToString = Base64.encodeBytes(proto.toByteArray())

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "profile")
    // conf.set(TableInputFormat.SCAN, scan.toString)
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "phb01,phb02,phb03")
    conf.setLong("hbase.rpc.timeout", 24 * 60 * 60 * 1000)




    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    hBaseRDD.foreachPartition(

      partition => {
        if (partition != null && !partition.isEmpty) {
          val conn = Hbase.createNewConnection()
          val table = conn.getTable(TableName.valueOf("weibo_female"))
          partition.foreach(
            data => {
              val gender_val = Bytes.toInt(data._2.getValue(f, gender))
              val targetId_val = new String(data._2.getValue(f, targetId))
              val target_val = new String(data._2.getValue(f, target))
              target_val match {
                case "weibo" => {
                  gender_val match {
                    case 2 => {
                      table.put(new Put(targetId_val.getBytes()).addColumn("info".getBytes(), "gender".getBytes(), Bytes.toBytes(gender_val)))
                    }
                    case _ =>
                  }
                }

                case _ =>
              }

            }
          )
        }

      }

    )

  }
}
