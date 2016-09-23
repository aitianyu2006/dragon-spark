package com.remarkmedia.spark.task

import com.remarkmedia.spark.common.{Hbase, HbaseColumn, Profile}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put, Scan}
import org.apache.hadoop.hbase.filter.{CompareFilter, SingleColumnValueFilter, SubstringComparator}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hz on 16/7/28.
  */
object InputHbase extends App with Profile {

  val sparkConf = new SparkConf().setAppName("InputHbase").setMaster("yarn-cluster")
  sparkConf.registerKryoClasses(Array(classOf[InputHbaseBrc]))
  val conn = Hbase.createNewConnection()
  val sc = new SparkContext(sparkConf)




  sc.textFile("/user/hdfs/uniqtags_full.txt").foreachPartition(
    p => {
      val conn = Hbase.createNewConnection()
      val table = conn.getTable(TableName.valueOf("tag_feeds"))
      p.foreach(
        line => {
          val put = new Put(DigestUtils.md5(line))
          put.addColumn(HbaseColumn.CF, targetId, line.getBytes())
          table.put(put)


        }
      )
      table.close()
    }
  )
}

case class InputHbaseBrc(conn: Connection)
