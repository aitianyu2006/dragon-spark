package com.remarkmedia.spark.task

import java.nio.ByteBuffer
import java.util

import com.remarkmedia.spark.common.{Hbase, HbaseColumn}
import com.remarkmedia.spark.common.HbaseColumn.{Post_mate, Profile}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer


/**
  * Created by hz on 16/8/18.
  */
object WeiboCount extends Post_mate with Profile {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("FemaleTask").setMaster("yarn-cluster")

    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "post_meta")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "phb01,phb02,phb03")
    conf.setLong("hbase.rpc.timeout", 24 * 60 * 60 * 1000)




    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val map = hBaseRDD.mapPartitions(
      p => {
        val keywords = "抱臂摸后背,菲鱼抱,反手摸肚脐弱爆了,反手摸后背,菲尔普斯抱臂"
        val keywordArray = keywords.split(",")
        val map = scala.collection.mutable.Map[String, util.HashSet[String]]()
        p.foreach(
          line => {
            val tag = line._2.getValue(HbaseColumn.CF, POST_META_TAG)
            val content: String = line._2.getValue(HbaseColumn.CF, POST_META_content)
            val target: String = line._2.getValue(HbaseColumn.CF, POST_META_TARGET)
            val target_ID: String = line._2.getValue(HbaseColumn.CF, POST_META_TARGET_ID)

            if (tag != null && tag.length >= 4) {
              val buffer = ByteBuffer.wrap(tag)
              val strings = ArrayBuffer[String]()
              while (buffer.remaining() > 0) {
                val size = buffer.getInt()
                val stringBytes = new Array[Byte](size)
                buffer.get(stringBytes)
                strings += Bytes.toString(stringBytes)
              }
              val tags = strings.toArray
              tags.foreach(
                tag => {
                  if (keywordArray.contains(tag)) {
                    map.get(tag) match {
                      case Some(set) => {
                        set.add(target_ID + ":" + target)
                        map.+=(tag -> set)
                      }
                      case None => {
                        val set = new util.HashSet[String]()
                        set.add(target_ID + ":" + target)
                        map.+=(tag -> set)
                      }
                    }
                  }
                }
              )
            }

            keywordArray.foreach(
              keyword => {
                if (content.contains(keyword)) {
                  map.get(keyword) match {
                    case Some(set) => {
                      set.add(target_ID + ":" + target)
                      map.+=(keyword -> set)
                    }
                    case None => {
                      val set = new util.HashSet[String]()
                      set.add(target_ID + ":" + target)
                      map.+=(keyword -> set)
                    }
                  }
                }
              }
            )


          }
        )
        map.iterator
      }
    )

    val reduceUser = map.reduceByKey((a, b) => {
      a.addAll(b)
      a
    })

    reduceUser.collect().foreach(
      data => {
        println(data._1 + ":" + data._2.size())
      }
    )

    reduceUser.mapPartitions(
      p => {
        val map = scala.collection.mutable.Map[Int, Int]()
        val conn = Hbase.createNewConnection()
        val profile_table = conn.getTable(TableName.valueOf("profile"))
        p.foreach(
          user => {
            val it = user._2.iterator()
            while (it.hasNext) {
              val userId = it.next()
              val profileKey = DigestUtils.md5(userId)
              val result = profile_table.get(new Get(profileKey))
              val gender: Int = result.getValue(HbaseColumn.CF, PROFILE_GENDER)

              map.get(gender) match {
                case Some(a) => {
                  val b = a + 1
                  map.+=(gender -> b)

                }
                case None => map.+=(gender -> 1)
              }


            }
          }
        )
        map.iterator
      }
    ).reduceByKey(_ + _).collect().foreach(
      data => {
        println(data._1 + ":" + data._2)
      }
    )


  }

  implicit def bytes2String(bytes: Array[Byte]): String = {
    var i = ""
    if (bytes != null && bytes.length > 0) {
      i = Bytes.toString(bytes)
    }
    i
  }

  implicit def bytes2Int(bytes: Array[Byte]): Int = {
    var i = -1
    if (bytes != null && bytes.length > 0) {
      i = Bytes.toInt(bytes)
    }
    i
  }
}
