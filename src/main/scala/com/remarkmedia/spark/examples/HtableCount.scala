package com.remarkmedia.spark.examples

import com.remarkmedia.spark.util.Logs
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by hz on 15/11/23.
  *
  * ./spark-submit  --class com.remarkmedia.spark.tablecount.HtableCount  --master yarn-cluster *.jar  tableName
  */
object HtableCount extends Logs {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: <table_name>")
      System.exit(1)
    }

    info("begin count table,table name:" + args(0))

    val sparkConf = new SparkConf().setAppName("HtableCount:" + args(0)).setMaster("yarn-cluster")
    val sc = new SparkContext(sparkConf)

    val scan = new Scan()
    scan.setCacheBlocks(false)

    scan.setBatch(10000)
    val proto = ProtobufUtil.toScan(scan)
    val ScanToString = Base64.encodeBytes(proto.toByteArray())
    val conf = HBaseConfiguration.create()

    conf.set(TableInputFormat.INPUT_TABLE, args(0))
    conf.set(TableInputFormat.SCAN, ScanToString)
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "phb01,phb02,phb02")
    conf.setLong("hbase.rpc.timeout", 24 * 60 * 60 * 1000)

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val tableCount = hBaseRDD.count()
    printf(args(0) + "count:" + tableCount)
    info(args(0) + "count:" + tableCount)
    sc.stop()


  }


}
