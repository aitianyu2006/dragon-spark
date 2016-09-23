package com.remarkmedia.spark.task

import java.nio.ByteBuffer

import com.remarkmedia.spark.common.HbaseColumn
import com.remarkmedia.spark.common.HbaseColumn.{Post_mate, Profile}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by hz on 16/8/19.
  */
object Phelps extends Post_mate with Profile {
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
        val map = scala.collection.mutable.Map[String, Int]()
        p.foreach(
          line => {
            val tag = line._2.getValue(HbaseColumn.CF, POST_META_TAG)
            val content: String = line._2.getValue(HbaseColumn.CF, POST_META_content)

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
                    map.get(content) match {
                      case Some(set) => {
                        val v = set + 1
                        map.+=(content -> v)
                      }
                      case None => {
                        map.+=(content -> 1)
                      }
                    }
                  }
                }
              )
            }

            keywordArray.foreach(
              keyword => {
                if (content.contains(keyword)) {
                  map.get(content) match {
                    case Some(set) => {
                      val v = set + 1
                      map.+=(content -> v)
                    }
                    case None => {
                      map.+=(content -> 1)
                    }
                  }
                }
              }
            )


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
