package com.remarkmedia.spark.task

import java.nio.ByteBuffer
import java.util
import java.util.regex.Pattern
import com.google.common.collect.{ArrayListMultimap, Multimap}
import com.remarkmedia.spark.common.Hbase
import com.remarkmedia.spark.util.Logs
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Get, Put, Result}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.MD5Hash
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hz on 16/3/8.
  */
object ImageMerge extends Logs {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("ImageMerge").setMaster("yarn-cluster")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "image")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "phb02,phb03,phb04")
    conf.setLong("hbase.rpc.timeout", 24 * 60 * 60 * 1000)



    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    hBaseRDD.map(
      data => {
        val rowkey = data._1.get()
        val post_key = ByteBuffer.wrap(rowkey, 0, 31)
        val list = new util.ArrayList[Result]()
        list.add(data._2)
        (post_key, list)

      }
    ).reduceByKey((v1, v2) => {
      v1.addAll(v2)
      v1
    }).foreachPartition(
      partition => {
        val conn = Hbase.createNewConnection()
        val post = conn.getTable(TableName.valueOf("post_meta"))
        val image = conn.getTable(TableName.valueOf("image_grouped"))
        val cf = Array(0x41.toByte)
        val cqid = Array(0x00.toByte)
        val cqoriginalUrl = Array(0x01.toByte)
        val cqurl = Array(0x02.toByte)
        val cqwidth = Array(0x03.toByte)
        val cqheight = Array(0x04.toByte)
        val cqimagetype = Array(0x05.toByte)

        val post_cq_from = Array(0x02.toByte)

        val image_size_middle = "middle"
        val image_size_large = "large"
        val image_size_thumbnail = "thumbnail"
        val id_pattern = Pattern.compile("([^\\/]*)\\.")

        if (partition.nonEmpty) {
          partition.foreach(
            data => {
              val postResut = post.get(new Get(data._1.array()))
              if (!postResut.isEmpty) {
                val dataFrom = postResut.getValue(cf, post_cq_from)
                val mapList: Multimap[String, Image] = ArrayListMultimap.create()
                if (dataFrom.nonEmpty) {
                  val datafromStr = new String(dataFrom)
                  val it = data._2.iterator()
                  if (datafromStr.equals("weibo") || datafromStr.equals("twitter")) {

                    while (it.hasNext) {
                      val result = it.next()
                      val imagetype = new String(result.getValue(cf, cqimagetype))
                      val url = new String(result.getValue(cf, cqoriginalUrl))
                      val m1 = id_pattern.matcher(url.substring(url.lastIndexOf('/')))
                      if (m1.find()) {
                        val imageid = m1.group()
                        val image = new Image(imagetype, result)
                        mapList.put(imageid, image)
                      }
                    }
                  }
                  if (datafromStr.equals("pinterest")) {

                    while (it.hasNext) {
                      val result = it.next()
                      val imagetype = new String(result.getValue(cf, cqimagetype))
                      val url = new String(result.getValue(cf, cqoriginalUrl))
                      val image = new Image(imagetype, result)
                      mapList.put(MD5Hash.getMD5AsHex(url.getBytes), image)

                    }
                  }
                  if (datafromStr.equals("facebook") || datafromStr.equals("instagram")) {

                    while (it.hasNext) {
                      val result = it.next()
                      val imagetype = new String(result.getValue(cf, cqimagetype))
                      val url = new String(result.getValue(cf, cqoriginalUrl))

                      val start = url.indexOf("_") + 1
                      val end = url.lastIndexOf("_")

                      val image = new Image(imagetype, result)
                      mapList.put(url.substring(start, end), image)
                    }
                  }
                  if (!mapList.isEmpty) {
                    val map = mapList.asMap()
                    val itmap = map.entrySet().iterator()
                    var index = 1
                    val putList = new util.ArrayList[Put]()
                    while (itmap.hasNext) {
                      val entry = itmap.next()
                      val list = entry.getValue
                      val itlist = list.iterator()
                      while (itlist.hasNext) {
                        val image = itlist.next()
                        val buffer = ByteBuffer.allocate(34)
                        val index_byte = index.toByte
                        var size: Byte = 0
                        if (image_size_large.equals(image.image_type)) {
                          size = 2
                        } else if (image_size_middle.equals(image.image_type)) {
                          size = 1
                        } else if (image_size_thumbnail.equals(image.image_type)) {
                          size = 0
                        } else {
                          size = 0
                        }

                        buffer.put(data._1)
                        buffer.put(index_byte)
                        buffer.put(size)

                        val put = new Put(buffer.array())
                        put.addColumn(cf, cqid, entry.getKey.getBytes())
                        put.addColumn(cf, cqoriginalUrl, image.result.getValue(cf, cqoriginalUrl))
                        put.addColumn(cf, cqurl, image.result.getValue(cf, cqurl))
                        put.addColumn(cf, cqwidth, image.result.getValue(cf, cqwidth))
                        put.addColumn(cf, cqheight, image.result.getValue(cf, cqheight))
                        put.addColumn(cf, cqimagetype, image.result.getValue(cf, cqimagetype))

                        putList.add(put)
                      }
                      index = index + 1
                    }
                    image.put(putList)
                  }
                }
              }
            }
          )
        }
        post.close()
        image.close()
        conn.close()
      }
    )

  }

  case class Image(image_type: String, result: Result)

}


