package com.remarkmedia.spark.stream

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.util
import java.util.Date
import java.util.zip.GZIPInputStream

import com.google.common.hash.Hashing
import com.remarkmedia.spark.common.Hash.ReginInfo
import com.remarkmedia.spark.common.{HbaseColumn, Hbase_RD}

import com.remarkmedia.spark.util.Logs
import kafka.serializer.DefaultDecoder
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.JsonMethods

import scala.collection.mutable.ArrayBuffer
import org.json4s._


/**
  * Created by hz on 16/8/12.
  */
object RD_ReplicationEye extends Logs with ReplicationBytesSupport {
  implicit val formats = DefaultFormats

  def main(args: Array[String]) {
    var topic = ""
    var partition = 8

    if (args.length == 2) {
      topic = args(0)
      partition = args(1).toInt

    } else {
      info("please int topic and Partition")
      System.exit(0)
    }

//    val word2Vec = new Word2Vec()
//      .setInputCol("message")
//      .setOutputCol("features")
//      .setVectorSize(VECTOR_SIZE)
//      .setMinCount(1)

    val sparkConf = new SparkConf().setAppName("kafka_streamming" + topic)
    val ssc = new StreamingContext(sparkConf, Seconds(30))
    val topicMap = topic.split(",").map((_, partition)).toMap
    ssc.checkpoint("Kakashi_Checkpoint)" + "_" + topic)

    createStream(ssc, "pkf01,pkf02,pkf03", "Kakashi", topicMap).foreachRDD(data => {
      data.coalesce(2).foreachPartition(
        p => {
          val second_info = this.getHashInfo()
          val conn = Hbase_RD.createNewConnection()

          val profile_tw = conn.getTable(TableName.valueOf("profile_tw"))
          val profile_ins = conn.getTable(TableName.valueOf("profile_ins"))
          val profile_weibo = conn.getTable(TableName.valueOf("profile_weibo"))
          val profile_fb = conn.getTable(TableName.valueOf("profile_fb"))

          val profile_friends_ins = conn.getTable(TableName.valueOf("profile_friends_ins"))
          val profile_friends_tw = conn.getTable(TableName.valueOf("profile_friends_tw"))
          val profile_friends_weibo = conn.getTable(TableName.valueOf("profile_friends_weibo"))

          val profile_follow_ins = conn.getTable(TableName.valueOf("profile_follow_ins"))
          val profile_follow_tw = conn.getTable(TableName.valueOf("profile_follow_tw"))
          val profile_follow_weibo = conn.getTable(TableName.valueOf("profile_follow_weibo"))

          p.foreach(
            value => {
              val profile_tw_put_List = new util.ArrayList[Put]()
              val profile_ins_put_List = new util.ArrayList[Put]()
              val profile_weibo_put_List = new util.ArrayList[Put]()
              val profile_fb_put_List = new util.ArrayList[Put]()

              try {
                val buffer = ByteBuffer.wrap(value._2)
                val trackIdLengthBytes = new Array[Byte](4)
                buffer.get(trackIdLengthBytes)
                val trackIdBytes = new Array[Byte](new String(trackIdLengthBytes).toInt)
                buffer.get(trackIdBytes)
                val zippedRaw = new Array[Byte](buffer.limit() - buffer.position())
                buffer.get(zippedRaw)
                val crawlerStr = new String(decompress(zippedRaw), "utf-8")
                val collect = JsonMethods.parse(crawlerStr).extract[CollectResourceAndProfile]

                collect.profiles.foreach(
                  profile => {
                    if (profile._id != null && profile.source != null) {
                      val starkey = getRegionKey(second_info, profile._id, profile.source)
                      val profile_put = convertPut(starkey, profile)
                      val friends = convertFriendsPut(starkey, profile)
                      val follows = convertFollowPut(second_info, profile)
                      profile.source match {
                        case "weibo" => {
                          profile_weibo_put_List.add(profile_put)
                          profile_follow_weibo.put(follows)
                          profile_friends_weibo.put(friends)
                        }

                        case "facebook" => {
                          profile_fb_put_List.add(profile_put)
                        }

                        case "twitter" => {
                          profile_tw_put_List.add(profile_put)
                          profile_follow_tw.put(follows)
                          profile_friends_tw.put(friends)
                        }

                        case "instagram" => {
                          if (profile.friends != null && !profile.friends.isEmpty) {
                            profile_follow_ins.put(follows)
                            profile_friends_ins.put(friends)
                          }
                        }
                      }
                    }

                  }
                )
                profile_tw.put(profile_tw_put_List)
                profile_fb.put(profile_fb_put_List)
                profile_weibo.put(profile_weibo_put_List)
                profile_ins.put(profile_ins_put_List)

              } catch {
                case e: Exception => {
                  logger.error(e.toString, e)
                }
              }
            }

          )

          profile_tw.close()
          profile_fb.close()
          profile_weibo.close()
          profile_ins.close()

          profile_follow_tw.close()
          profile_follow_ins.close()
          profile_follow_weibo.close()


          profile_friends_ins.close()
          profile_friends_weibo.close()
          profile_friends_tw.close()


        }
      )
    })

    ssc.start()
    ssc.awaitTermination()

  }

  def to_GENDER(sex: String) = {
    var _GENDER = 0
    sex match {
      case "male" => _GENDER = 1
      case "female" => _GENDER = 2
      case _ =>
    }
    _GENDER

  }

  def createStream(ssc: StreamingContext,
                   zkQuorum: String,
                   groupId: String,
                   topics: Map[String, Int],
                   storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
                  ): ReceiverInputDStream[(String, Array[Byte])] = {
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum, "group.id" -> groupId,
      "zookeeper.connection.timeout.ms" -> "1000000")
    KafkaUtils.createStream[String, Array[Byte], DefaultDecoder, DefaultDecoder](
      ssc, kafkaParams, topics, storageLevel)
  }

  def profileRowKey(targetId: String, target: String, region: Long) = {
    Bytes.toBytes(region) ++ DigestUtils.md5(targetId + ":" + target)
  }

  def profileRowKey(targetIdtaget: String, region: Long) = {
    Bytes.toBytes(region) ++ DigestUtils.md5(targetIdtaget)
  }

  def profileFriends(targetId: String, target: String, region: Long, follow: Array[Byte]) = {
    profileRowKey(targetId, target, region) ++ follow
  }

  def profileFollowIndex(targetIdtaget: String, region: Long, follow: Array[Byte]) = {
    profileRowKey(targetIdtaget, region) ++ follow
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

  def convertPut(starkey: Long, profile: Profile) = {

    val profile_put = new Put(this.profileRowKey(profile._id, profile.source, starkey))
    profile_put.addColumn("f1".getBytes(), "q0".getBytes(), profile._id) //taget id
    profile_put.addColumn("f1".getBytes(), "q1".getBytes(), profile.source) //taget
    profile_put.addColumn("f1".getBytes(), "q2".getBytes(), profile.face) //头像
    profile_put.addColumn("f1".getBytes(), "q3".getBytes(), profile.name) //name
    profile_put.addColumn("f1".getBytes(), "q4".getBytes(), profile.face) //头像2
    profile_put.addColumn("f1".getBytes(), "q5".getBytes(), "") //状态
    profile_put.addColumn("f1".getBytes(), "q6".getBytes(), profile.source) //taget
    profile_put.addColumn("f1".getBytes(), "q7".getBytes(), Bytes.toBytes(profile.fansNum.getOrElse(0l))) //fansNum
    profile_put.addColumn("f1".getBytes(), "q8".getBytes(), Bytes.toBytes(profile.postCount.getOrElse(0l))) //postCount
    profile_put.addColumn("f1".getBytes(), "q9".getBytes(), System.currentTimeMillis())
    profile_put.addColumn("f1".getBytes(), "q10".getBytes(), 20) //年龄
    profile_put.addColumn("f1".getBytes(), "q11".getBytes(), to_GENDER(profile.sex.getOrElse("_"))) //性别
    profile_put.addColumn("f1".getBytes(), "q12".getBytes(), "") //生日
    profile_put.addColumn("f1".getBytes(), "q13".getBytes(), profile.lastLocation.getOrElse(new Location(Some(0.0), Some(0.0))).longitude.get) //地理位置
    profile_put.addColumn("f1".getBytes(), "q14".getBytes(), profile.lastLocation.getOrElse(new Location(Some(0.0), Some(0.0))).latitude.get)
    profile_put.addColumn("f1".getBytes(), "q15".getBytes(), Bytes.toBytes(profile.evaluation.getOrElse(Evaluation.STANDARD))) //自我评价
    profile_put.addColumn("f1".getBytes(), "q16".getBytes(), System.currentTimeMillis()) //create date
    profile_put.addColumn("f1".getBytes(), "q17".getBytes(), System.currentTimeMillis()) //lasta updata
    profile_put.addColumn("f1".getBytes(), "q18".getBytes(), profile.friends.size)

    profile_put

  }


  def convertFriendsPut(starkey: Long, profile: Profile) = {
    val putList = new util.ArrayList[Put]()
    if (profile.friends != null && !profile.friends.isEmpty) {
      profile.friends.foreach(
        friend => {
          val friendsRowkey = profileFriends(profile._id, profile.source, starkey, (friend + ":" + profile.source).getBytes())
          val put_friends = new Put(friendsRowkey)
          put_friends.addColumn("f1".getBytes(), "q1".getBytes(), HbaseColumn.STOP)
          putList.add(put_friends)
        }
      )
    }
    putList
  }


  def convertFollowPut(map: util.TreeMap[Long, ReginInfo], profile: Profile) = {
    val putList = new util.ArrayList[Put]()
    if (profile.friends != null && !profile.friends.isEmpty) {
      profile.friends.foreach(
        friend => {
          val starkey = Hashing.murmur3_128().hashBytes((friend + ":" + profile.source).getBytes()).asLong()
          val followRowKey = profileFollowIndex((friend + ":" + profile.source), starkey, (profile._id + ":" + profile.source).getBytes())
          val put_follow = new Put(followRowKey)
          put_follow.addColumn("f1".getBytes(), "q1".getBytes(), HbaseColumn.STOP)
          putList.add(put_follow)
        }
      )
    }
    putList
  }


  def getHashInfo(): util.TreeMap[Long, ReginInfo] = {
    val regions = new ArrayBuffer[ReginInfo]()
    val second_info = new util.TreeMap[Long, ReginInfo]()
    val hash = Hashing.murmur3_128()
    var i = 1
    while (i < 128) {
      regions.+=(new ReginInfo(i, "regions:" + i))
      i = i + 1
    }

    regions.foreach(
      shardInfo => {
        second_info.put(hash.hashBytes(("regions:" + shardInfo.id).getBytes()).asLong(), shardInfo)
      }
    )
    second_info
  }

  def getRegionKey(map: util.TreeMap[Long, ReginInfo], targetId: String, target: String): Long = {
    val hashKey = Hashing.murmur3_128().hashBytes((targetId + ":" + target).getBytes()).asLong()
    val tail = map.tailMap(hashKey)
    var starkey = map.firstKey()
    if (!tail.isEmpty) {
      starkey = tail.firstKey()
    }
    starkey
  }


  case class CollectResourceAndProfile(resources: List[Post] = Nil, profiles: List[Profile] = Nil)


  //  case class Post
  //  (id: String,
  //   location: Location = Location(0, 0),
  //   tags: List[String] = Nil,
  //   sourceTime: Long = 0,
  //   sourceUserId: String = "",
  //   commentsCount: Int = 0,
  //   likesCount: Int = 0,
  //   images: List[Image] = Nil,
  //   comments: List[Comment] = Nil,
  //   postType: String = "image",
  //   //video: Video = null,
  //   textContent: String = "")
  //
  //  case class Location(longitude: Double, latitude: Double)
  //
  //  case class Image(originalUrl: String, url: String, width: Int, height: Int, imageType: String = "large")
  //
  //  case class Comment(userId: String, username: String, originalProfileUrl: String, profileUrl: String, content: String, createdTime: Long)
  //
  //  case class Profile(_id: String = "",
  //                     url: String = "",
  //                     name: String = "",
  //                     face: String = "",
  //                     friends: List[String] = Nil,
  //                     date: String = "",
  //                     source: String = "",
  //                     fansNum: Long = 0,
  //                     postCount: Long = 0,
  //                     //age: Int = 20,
  //                     //platformIdentity: Int = 0,
  //                     sex: String = "",
  //                     //birthday: String = "",
  //                     lastLocation: Location = null,
  //                     evaluation: String = "evaluation",
  //                     //userId: Option[String],
  //                     //keywords: List[String] = Nil,
  //                     createdate: String = "",
  //                     lastUpdate: Long = 0)
  //
  //}


  case class Post
  (id: String,
   location: Option[Location] = None,
   tags: Option[List[String]] = None,
   sourceTime: Long = 0,
   sourceUserId: String = "",
   commentsCount: Int = 0,
   likesCount: Int = 0,
   images: Option[List[Image]] = None, //一张图片的不同尺寸，可能有三张（大、中、小）
   comments: Option[List[Comment]] = None,
   postType: String = "image",
   video: Option[Video] = None,
   textContent: String = "")

  case class Location(longitude: Option[Double], latitude: Option[Double], name: String = "", id: String = "")

  case class Image(id: String = "1", originalUrl: String, url: String, width: Int, height: Int, imageType: String = "lager")

  case class Video(id: String = "1", originalVideoUrl: String, originalCoverUrl: String, coverUrl: String, videoUrl: String)

  case class Comment(userId: String, username: String, originalProfileUrl: String, profileUrl: String, content: String, createdTime: Long)

  object Evaluation {
    val PRIORITY = "priority"
    val STANDARD = "standard"
  }

  case class Profile(_id: String = "",
                     url: String = "",
                     name: String = "",
                     face: String = "",
                     status: Option[String] = Some("no description"),
                     friends: List[String] = Nil, //direct friends (from SNS)
                     date: String = "",
                     source: String = "",
                     fansNum: Option[Long] = Some(0),
                     postCount: Option[Long] = Some(0),
                     createdate: Option[Date] = Some(new Date()),
                     age: Option[Int] = Some(20),
                     platformIdentity: Option[Int] = Some(20),
                     sex: Option[String] = Some(""),
                     birthday: Option[String] = Some(""),
                     lastLocation: Option[Location] = Some(new Location(Some(0.0), Some(0.0))),
                     evaluation: Option[String] = Some(Evaluation.STANDARD),
                     userId: Option[String] = Some(""),
                     keywords: Option[List[String]] = None,
                     lastUpdate: Option[Long] = Some(System.currentTimeMillis()))

}


trait ReplicationBytesSupport {

  implicit def Double2Bytes(value: Double): Array[Byte] = {
    var bytes = Bytes.toBytes(0.0f)
    if (value != null) {
      bytes = Bytes.toBytes(value)
    }
    bytes
  }

  implicit def String2Bytes(value: String): Array[Byte] = {
    var bytes = Bytes.toBytes("")
    if (value != null) {
      bytes = Bytes.toBytes(value)
    }
    bytes
  }

  implicit def Int2Bytes(value: Int): Array[Byte] = {
    var bytes = Bytes.toBytes(0)
    if (value != null) {
      bytes = Bytes.toBytes(value)
    }
    bytes
  }

  implicit def Long2Bytes(value: Long): Array[Byte] = {
    var bytes = Bytes.toBytes(0)
    if (value != null) {
      bytes = Bytes.toBytes(value)
    }
    bytes
  }

}
