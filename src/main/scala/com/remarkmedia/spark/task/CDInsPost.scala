package com.remarkmedia.spark.task

import com.remarkmedia.spark.common.{Hbase, HbaseColumn, Profile}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by hz on 16/8/3.
  */
object CDInsPost extends Profile {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CDInsPost").setMaster("yarn-cluster")
    val sc = new SparkContext(sparkConf)

    sc.textFile("/user/hdfs/ig_user.txt", 8).mapPartitions(
      p => {
        val buffer = mutable.Buffer[String]()
        val conn = Hbase.createNewConnection()
        val profile = conn.getTable(TableName.valueOf("profile"))
        val profile_friend = conn.getTable(TableName.valueOf("profile_friend"))
        p.foreach(
          line => {
            // println(line)
            val userInfo = line.split("\t")

            if (userInfo.length == 2) {
              val sb = new StringBuilder()
              val scan = new Scan()
              //println(userInfo(0))
              scan.setStartRow(Hbase.profileRowKey(userInfo(0).trim, "instagram"))
              scan.setStopRow(Hbase.profileRowKey(userInfo(0).trim, "instagram") ++ HbaseColumn.STOP)
              scan.setBatch(1000)
              scan.setCacheBlocks(false)
              val scanner = profile_friend.getScanner(scan)

              sb.append(userInfo(0).trim)
              sb.append(":")
              sb.append(userInfo(1).trim)
              sb.append(",")
              val it = scanner.iterator()

              while (it.hasNext) {
                val result = it.next()
                val rowKey = result.getRow
                val friendRowKey: String = rowKey.drop(16)

                val profile_result = profile.get(new Get(DigestUtils.md5(friendRowKey)))

                if (!profile_result.isEmpty) {
                  val user_name: String = profile_result.getValue(HbaseColumn.CF, name)
                  sb.append(friendRowKey.replaceAll(":instagram", ""))
                  sb.append(":")
                  sb.append(user_name)
                  sb.append(",")
                }
              }

              scanner.close()
              println(sb.toString())
              buffer.+=:(sb.toString())
            }
          }
        )
        buffer.iterator
      }
    ).saveAsTextFile("/user/hdfs/ig_user_info/")

  }

  implicit def bytes2String(bytes: Array[Byte]): String = {
    var f: String = ""
    if (bytes != null && bytes.length > 0) {
      f = Bytes.toString(bytes)
    }
    f
  }

}


