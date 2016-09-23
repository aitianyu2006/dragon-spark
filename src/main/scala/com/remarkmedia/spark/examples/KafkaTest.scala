package com.remarkmedia.spark.examples

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.util.Date
import java.util.zip.GZIPInputStream

import com.remarkmedia.spark.util.Logs
import kafka.serializer.DefaultDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by hz on 16/1/18.
  *
  * ./spark-submit  --class com.remarkmedia.spark.examples.KafkaTest --master yarn-cluster --conf spark.yarn.am.waitTime=1000s --conf spark.driver.cores=8 --conf spark.yarn.executor.memoryOverhead=1024  /tmp/dragon-spark_2.10-0.1.jar
  */
object KafkaTest extends Logs {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("kafka_stream")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("Kakashi_Checkpoint)")
    val topicMap = "raw_weibo_high".split(",").map((_, 8)).toMap
    val infoStr = createStream(ssc, "pkf01,pkf02,pkf03", "Kakashi", topicMap).map(data => {

      val buffer = ByteBuffer.wrap(data._2)
      val trackIdLengthBytes = new Array[Byte](4)
      buffer.get(trackIdLengthBytes)
      val trackIdBytes = new Array[Byte](new String(trackIdLengthBytes).toInt)
      buffer.get(trackIdBytes)

      val zippedRaw = new Array[Byte](buffer.limit() - buffer.position())
      buffer.get(zippedRaw)

      val crawlerStr = new String(decompress(zippedRaw), "utf-8")

      info(crawlerStr)


      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      implicit val formats = DefaultFormats
      val json = parse(crawlerStr)
      val collect = json.extract[CollectResourceAndProfile]

      collect.profiles


    })

    infoStr.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def createStream(ssc: StreamingContext,
                   zkQuorum: String,
                   groupId: String,
                   topics: Map[String, Int],
                   storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
                  ): ReceiverInputDStream[(String, Array[Byte])] = {
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum, "group.id" -> groupId,
      "zookeeper.connection.timeout.ms" -> "10000")
    KafkaUtils.createStream[String, Array[Byte], DefaultDecoder, DefaultDecoder](
      ssc, kafkaParams, topics, storageLevel)
  }

  def decompress(gzipData: Array[Byte]): Array[Byte] = {
    val buffer: Int = 1024
    var gis: GZIPInputStream = null
    var bos: ByteArrayOutputStream = null
    try {
      gis = new GZIPInputStream(new ByteArrayInputStream(gzipData))
      bos = new ByteArrayOutputStream()

      var count: Int = 0
      val byteArray = new Array[Byte](buffer)

      do {
        bos.write(byteArray, 0, count)
        count = gis.read(byteArray)
      } while (count != -1)

      val postByteArray = bos.toByteArray
      postByteArray

    } finally {
      if (gis != null) gis.close()
      if (bos != null) bos.close()
    }
  }

  case class CollectResourceAndProfile(resources: List[Post] = Nil, profiles: List[Profile] = Nil)


  case class Post
  (id: String,
   location: Location = Location(0, 0),
   tags: List[String] = Nil,
   sourceTime: Long = 0,
   sourceUserId: String = "",
   commentsCount: Int = 0,
   likesCount: Int = 0,
   images: List[Image] = Nil, //一张图片的不同尺寸，可能有三张（大、中、小）
   comments: List[Comment] = Nil,
   postType: String = "image",
   //video: Video = null,
   textContent: String = "")

  case class Location(longitude: Double, latitude: Double)

  case class Image(originalUrl: String, url: String, width: Int, height: Int, imageType: String = "large")

  //case class Video(id: String = "1", originalVideoUrl: String, originalCoverUrl: String, coverUrl: String, videoUrl: String)

  case class Comment(userId: String, username: String, originalProfileUrl: String, profileUrl: String, content: String, createdTime: Long)

  case class Profile(_id: String = "",
                     url: String = "",
                     name: String = "",
                     face: String = "",
                     //status: String = "no description",
                     friends: List[String] = Nil, //direct friends (from SNS)
                     date: String = "",
                     source: String = "",
                     fansNum: Long = 0,
                     postCount: Long = 0,
                     //age: Int = 20,
                     //platformIdentity: Int = 0,
                     sex: String = "",
                     //birthday: String = "",
                     lastLocation: Location = null,
                     evaluation: String = "evaluation",
                     //userId: Option[String],
                     //keywords: List[String] = Nil,
                     createdate:String="",
                     lastUpdate: Long = 0)

  //case class CollectResourceAndProfile(resources: List[Post] = Nil, profiles: List[Profile] = Nil)

}
