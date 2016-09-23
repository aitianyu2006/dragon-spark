package com.remarkmedia.spark.task

import java.nio.ByteBuffer

import com.remarkmedia.spark.common.Hbase
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hz on 16/4/8.
  */
object IversonFans {

  def main(args: Array[String]) {
    /**
      * config
      */
    val sparkConf = new SparkConf().setAppName("ImageMerge").setMaster("yarn-cluster")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    /**
      * scan
      */
    val scan = new Scan()
    //scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("\\*\\-1251103863:weibo")))
    scan.setCacheBlocks(false)
    scan.setBatch(10000)

    val proto = ProtobufUtil.toScan(scan)
    val ScanToString = Base64.encodeBytes(proto.toByteArray())


    /**
      * setconf
      */
    conf.set(TableInputFormat.INPUT_TABLE, "profile_friend")
    conf.set(TableInputFormat.SCAN, ScanToString)
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "phb02,phb03,phb04")
    conf.setLong("hbase.rpc.timeout", 24 * 60 * 60 * 1000)
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val cache = hBaseRDD.map(
      data => {
        val buffer=ByteBuffer.wrap(data._1.copyBytes())

        val profileKey=new Array[Byte](16)

        buffer.get(profileKey)

        val uidBytes = new Array[Byte](buffer.limit() - buffer.position())
        buffer.get(uidBytes)

        val uid=new String(uidBytes)

        if (uid.endsWith(":weibo") && (uid.contains("5866576206") || uid.contains("1251103863"))) {
          (profileKey, uid)
        }else{
          ("other".getBytes(), "other")
        }
      }
    ).foreachPartition(
      partition => {
        val conn = Hbase.createNewConnection()
        val fans = conn.getTable(TableName.valueOf("iverson_fans"))

        partition.foreach(
          data=>{
            val id=new String(data._1)
            if(!id.equals("other")){
              val  put=new Put(data._1)

              put.addColumn("p_id".getBytes(),"p_rowkey".getBytes(),data._1)
              put.addColumn("p_id".getBytes(),"focus".getBytes(),data._2.getBytes())
              fans.put(put)

              //put.addColumn("other".getBytes)
            }
          }
        )

        fans.close()
        conn.close()
      }

    )

    //cache.saveAsTextFile("hdfs://phb01-vb.hz.kankanapp.com.cn/user/spark/Iverson/")

  }

}
