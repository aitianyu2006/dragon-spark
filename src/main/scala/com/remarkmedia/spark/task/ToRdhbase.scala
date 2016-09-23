package com.remarkmedia.spark.task

import java.util

import com.google.common.hash.Hashing
import com.remarkmedia.spark.common.Hash.ReginInfo
import com.remarkmedia.spark.common.{HashKey, Hbase, HbaseColumn, Hbase_RD}
import com.remarkmedia.spark.common.HbaseColumn.Profile
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.BroadcastFactory
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by hz on 16/7/14.
  */
object ToRdhbase extends App with Profile {
  val sparkConf = new SparkConf().setAppName("ToRdhbase").setMaster("yarn-cluster")
  val sc = new SparkContext(sparkConf)
  val conf = HBaseConfiguration.create()
  conf.set(TableInputFormat.INPUT_TABLE, "profile")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  conf.set("hbase.zookeeper.quorum", "phb01,phb02,phb03")
  conf.setLong("hbase.rpc.timeout", 24 * 60 * 60 * 1000)

  val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    classOf[org.apache.hadoop.hbase.client.Result])


  hBaseRDD.foreachPartition(

    p => {
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
      if (p != null && !p.isEmpty) {
        val conn = Hbase_RD.createNewConnection()
        val profile_tw = conn.getTable(TableName.valueOf("profile_tw"))
        val profile_ins = conn.getTable(TableName.valueOf("profile_ins"))
        val profile_weibo = conn.getTable(TableName.valueOf("profile_weibo"))
        val profile_fb = conn.getTable(TableName.valueOf("profile_fb"))

        val profile_friends_fb = conn.getTable(TableName.valueOf("profile_friends_fb"))
        val profile_follow_ins = conn.getTable(TableName.valueOf("profile_follow_ins"))
        val profile_follow_tw = conn.getTable(TableName.valueOf("profile_follow_tw"))
        val profile_follow_weibo = conn.getTable(TableName.valueOf("profile_follow_weibo"))



        val profile_follow_fb = conn.getTable(TableName.valueOf("profile_follow_fb"))
        val profile_friends_ins = conn.getTable(TableName.valueOf("profile_friends_ins"))
        val profile_friends_tw = conn.getTable(TableName.valueOf("profile_friends_tw"))
        val profile_friends_weibo = conn.getTable(TableName.valueOf("profile_friends_weibo"))



        val conn_p = Hbase.createNewConnection()
        val profile_friend = conn_p.getTable(TableName.valueOf("profile_friend"))

        p.foreach(
          data => {
            val rowkey = data._1.get()
            val _TARGET_ID = data._2.getValue(HbaseColumn.CF, PROFILE_TARGET_ID)
            val TARGET = data._2.getValue(HbaseColumn.CF, PROFILE_TARGET)
            val _URL = data._2.getValue(HbaseColumn.CF, PROFILE_URL)
            val _NAME = data._2.getValue(HbaseColumn.CF, PROFILE_NAME)
            val _AVATAR = data._2.getValue(HbaseColumn.CF, PROFILE_AVATAR)
            val _STATUS = data._2.getValue(HbaseColumn.CF, PROFILE_STATUS)
            val _SOURCE = data._2.getValue(HbaseColumn.CF, PROFILE_SOURCE)
            val _FANS_COUN = data._2.getValue(HbaseColumn.CF, PROFILE_FANS_COUNT)
            val _POST_COUNT = data._2.getValue(HbaseColumn.CF, PROFILE_POST_COUNT)
            val _CREATE_DATA = data._2.getValue(HbaseColumn.CF, PROFILE_CREATE_DATA)
            val AVG = data._2.getValue(HbaseColumn.CF, PROFILE_AGE)
            val _GENDER = data._2.getValue(HbaseColumn.CF, PROFILE_GENDER)
            val _BIRTHDAY = data._2.getValue(HbaseColumn.CF, PROFILE_BIRTHDAY)
            val _LOCATION_LON = data._2.getValue(HbaseColumn.CF, PROFILE_LAST_LOCATION_LON)
            val _LAST_LOCATION_LAT = data._2.getValue(HbaseColumn.CF, PROFILE_LAST_LOCATION_LAT)
            val _EVALUATION = data._2.getValue(HbaseColumn.CF, PROFILE_EVALUATION)
            val _LAST_UPDATE = data._2.getValue(HbaseColumn.CF, PROFILE_LAST_UPDATE)
            val _FRIENDS_COUNT = data._2.getValue(HbaseColumn.CF, PROFILE_FRIENDS_COUNT)
            val _FLAGS = data._2.getValue(HbaseColumn.CF, PROFILE_FLAGS)
            if (TARGET != null && _TARGET_ID != null) {
              val target = new String(TARGET)

              target match {
                case "weibo" => {
                  val hashKey = hash.hashBytes((new String(_TARGET_ID) + ":" + new String(TARGET)).getBytes()).asLong()
                  val tail = second_info.tailMap(hashKey)
                  var starkey = second_info.firstKey()
                  if (!tail.isEmpty) {
                    starkey = tail.firstKey()
                  }
                  val rd_rowkey = this.profileRowKey(new String(_TARGET_ID), new String(TARGET), starkey)
                  val put = new Put(rd_rowkey)
                  put.addColumn("f1".getBytes(), "q0".getBytes(), _TARGET_ID)
                  put.addColumn("f1".getBytes(), "q1".getBytes(), TARGET)
                  put.addColumn("f1".getBytes(), "q2".getBytes(), _URL)
                  put.addColumn("f1".getBytes(), "q3".getBytes(), _NAME)
                  put.addColumn("f1".getBytes(), "q4".getBytes(), _AVATAR)
                  put.addColumn("f1".getBytes(), "q5".getBytes(), _STATUS)
                  put.addColumn("f1".getBytes(), "q6".getBytes(), _SOURCE)
                  put.addColumn("f1".getBytes(), "q7".getBytes(), _FANS_COUN)
                  put.addColumn("f1".getBytes(), "q8".getBytes(), _POST_COUNT)
                  put.addColumn("f1".getBytes(), "q9".getBytes(), _CREATE_DATA)
                  put.addColumn("f1".getBytes(), "q10".getBytes(), AVG)
                  put.addColumn("f1".getBytes(), "q11".getBytes(), _GENDER)
                  put.addColumn("f1".getBytes(), "q12".getBytes(), _BIRTHDAY)
                  put.addColumn("f1".getBytes(), "q13".getBytes(), _LOCATION_LON)
                  put.addColumn("f1".getBytes(), "q14".getBytes(), _LAST_LOCATION_LAT)
                  put.addColumn("f1".getBytes(), "q15".getBytes(), _EVALUATION)
                  put.addColumn("f1".getBytes(), "q16".getBytes(), _CREATE_DATA)
                  put.addColumn("f1".getBytes(), "q17".getBytes(), _LAST_UPDATE)
                  put.addColumn("f1".getBytes(), "q18".getBytes(), _FRIENDS_COUNT)
                  put.addColumn("f1".getBytes(), "q19".getBytes(), _FLAGS)
                  profile_weibo.put(put)

                  val scan = new Scan()
                  scan.setStartRow(rowkey)
                  scan.setStopRow(rowkey ++ HbaseColumn.STOP)
                  scan.setBatch(1000)
                  scan.setCacheBlocks(false)
                  val scanner = profile_friend.getScanner(scan)

                  val it = scanner.iterator()
                  val put_follow_list = new util.ArrayList[Put]()
                  val put_friends_list = new util.ArrayList[Put]()
                  while (it.hasNext) {

                    val result = it.next()
                    val follow_key = result.getRow.drop(16)
                    val follow_key_str = new String(follow_key)

                    val friendsRowkey = profileFriends(new String(_TARGET_ID), new String(TARGET), starkey, follow_key)
                    val put_friends = new Put(friendsRowkey)
                    put_friends.addColumn("f1".getBytes(), "q1".getBytes(), HbaseColumn.STOP)
                    put_friends_list.add(put_friends)

                    val hashKey = hash.hashBytes(follow_key).asLong()
                    val followRowKey = profileFollowIndex(follow_key_str, hashKey, (new String(_TARGET_ID) + ":" + new String(TARGET)).getBytes())

                    val put_follow = new Put(followRowKey)
                    put_follow.addColumn("f1".getBytes(), "q1".getBytes(), HbaseColumn.STOP)
                    put_follow_list.add(put_follow)


                  }

                  scanner.close()


                  profile_friends_weibo.put(put_friends_list)
                  profile_follow_weibo.put(put_follow_list)


                }

                case "facebook" => {
                  val hashKey = hash.hashBytes((new String(_TARGET_ID) + ":" + new String(TARGET)).getBytes()).asLong()
                  val tail = second_info.tailMap(hashKey)
                  var starkey = second_info.firstKey()
                  if (!tail.isEmpty) {
                    starkey = tail.firstKey()
                  }
                  val rd_rowkey = this.profileRowKey(new String(_TARGET_ID), new String(TARGET), starkey)
                  val put = new Put(rd_rowkey)
                  put.addColumn("f1".getBytes(), "q0".getBytes(), _TARGET_ID)
                  put.addColumn("f1".getBytes(), "q1".getBytes(), TARGET)
                  put.addColumn("f1".getBytes(), "q2".getBytes(), _URL)
                  put.addColumn("f1".getBytes(), "q3".getBytes(), _NAME)
                  put.addColumn("f1".getBytes(), "q4".getBytes(), _AVATAR)
                  put.addColumn("f1".getBytes(), "q5".getBytes(), _STATUS)
                  put.addColumn("f1".getBytes(), "q6".getBytes(), _SOURCE)
                  put.addColumn("f1".getBytes(), "q7".getBytes(), _FANS_COUN)
                  put.addColumn("f1".getBytes(), "q8".getBytes(), _POST_COUNT)
                  put.addColumn("f1".getBytes(), "q9".getBytes(), _TARGET_ID)
                  put.addColumn("f1".getBytes(), "q10".getBytes(), AVG)
                  put.addColumn("f1".getBytes(), "q11".getBytes(), _GENDER)
                  put.addColumn("f1".getBytes(), "q12".getBytes(), _BIRTHDAY)
                  put.addColumn("f1".getBytes(), "q13".getBytes(), _LOCATION_LON)
                  put.addColumn("f1".getBytes(), "q14".getBytes(), _LAST_LOCATION_LAT)
                  put.addColumn("f1".getBytes(), "q15".getBytes(), _EVALUATION)
                  put.addColumn("f1".getBytes(), "q16".getBytes(), _CREATE_DATA)
                  put.addColumn("f1".getBytes(), "q17".getBytes(), _LAST_UPDATE)
                  put.addColumn("f1".getBytes(), "q18".getBytes(), _FRIENDS_COUNT)
                  put.addColumn("f1".getBytes(), "q19".getBytes(), _FLAGS)
                  profile_fb.put(put)

                  val scan = new Scan()
                  scan.setStartRow(rowkey)
                  scan.setStopRow(rowkey ++ HbaseColumn.STOP)
                  scan.setBatch(1000)
                  scan.setCacheBlocks(false)
                  val scanner = profile_friend.getScanner(scan)

                  val it = scanner.iterator()
                  val put_follow_list = new util.ArrayList[Put]()
                  val put_friends_list = new util.ArrayList[Put]()
                  while (it.hasNext) {

                    val result = it.next()
                    val follow_key = result.getRow.drop(16)
                    val follow_key_str = new String(follow_key)

                    val friendsRowkey = profileFriends(new String(_TARGET_ID), new String(TARGET), starkey, follow_key)
                    val put_friends = new Put(friendsRowkey)
                    put_friends.addColumn("f1".getBytes(), "q1".getBytes(), HbaseColumn.STOP)
                    put_friends_list.add(put_friends)

                    val hashKey = hash.hashBytes(follow_key).asLong()
                    val followRowKey = profileFollowIndex(follow_key_str, hashKey, (new String(_TARGET_ID) + ":" + new String(TARGET)).getBytes())

                    val put_follow = new Put(followRowKey)
                    put_follow.addColumn("f1".getBytes(), "q1".getBytes(), HbaseColumn.STOP)
                    put_follow_list.add(put_follow)


                  }

                  scanner.close()


                  profile_friends_fb.put(put_friends_list)
                  profile_follow_fb.put(put_follow_list)

                }

                case "twitter" => {
                  val hashKey = hash.hashBytes((new String(_TARGET_ID) + ":" + new String(TARGET)).getBytes()).asLong()
                  val tail = second_info.tailMap(hashKey)
                  var starkey = second_info.firstKey()
                  if (!tail.isEmpty) {
                    starkey = tail.firstKey()
                  }
                  val rd_rowkey = this.profileRowKey(new String(_TARGET_ID), new String(TARGET), starkey)
                  val put = new Put(rd_rowkey)
                  put.addColumn("f1".getBytes(), "q0".getBytes(), _TARGET_ID)
                  put.addColumn("f1".getBytes(), "q1".getBytes(), TARGET)
                  put.addColumn("f1".getBytes(), "q2".getBytes(), _URL)
                  put.addColumn("f1".getBytes(), "q3".getBytes(), _NAME)
                  put.addColumn("f1".getBytes(), "q4".getBytes(), _AVATAR)
                  put.addColumn("f1".getBytes(), "q5".getBytes(), _STATUS)
                  put.addColumn("f1".getBytes(), "q6".getBytes(), _SOURCE)
                  put.addColumn("f1".getBytes(), "q7".getBytes(), _FANS_COUN)
                  put.addColumn("f1".getBytes(), "q8".getBytes(), _POST_COUNT)
                  put.addColumn("f1".getBytes(), "q9".getBytes(), _TARGET_ID)
                  put.addColumn("f1".getBytes(), "q10".getBytes(), AVG)
                  put.addColumn("f1".getBytes(), "q11".getBytes(), _GENDER)
                  put.addColumn("f1".getBytes(), "q12".getBytes(), _BIRTHDAY)
                  put.addColumn("f1".getBytes(), "q13".getBytes(), _LOCATION_LON)
                  put.addColumn("f1".getBytes(), "q14".getBytes(), _LAST_LOCATION_LAT)
                  put.addColumn("f1".getBytes(), "q15".getBytes(), _EVALUATION)
                  put.addColumn("f1".getBytes(), "q16".getBytes(), _CREATE_DATA)
                  put.addColumn("f1".getBytes(), "q17".getBytes(), _LAST_UPDATE)
                  put.addColumn("f1".getBytes(), "q18".getBytes(), _FRIENDS_COUNT)
                  put.addColumn("f1".getBytes(), "q19".getBytes(), _FLAGS)
                  profile_tw.put(put)

                  val scan = new Scan()
                  scan.setStartRow(rowkey)
                  scan.setStopRow(rowkey ++ HbaseColumn.STOP)
                  scan.setBatch(1000)
                  scan.setCacheBlocks(false)
                  val scanner = profile_friend.getScanner(scan)

                  val it = scanner.iterator()
                  val put_follow_list = new util.ArrayList[Put]()
                  val put_friends_list = new util.ArrayList[Put]()
                  while (it.hasNext) {

                    val result = it.next()
                    val follow_key = result.getRow.drop(16)
                    val follow_key_str = new String(follow_key)

                    val friendsRowkey = profileFriends(new String(_TARGET_ID), new String(TARGET), starkey, follow_key)
                    val put_friends = new Put(friendsRowkey)
                    put_friends.addColumn("f1".getBytes(), "q1".getBytes(), HbaseColumn.STOP)
                    put_friends_list.add(put_friends)

                    val hashKey = hash.hashBytes(follow_key).asLong()
                    val followRowKey = profileFollowIndex(follow_key_str, hashKey, (new String(_TARGET_ID) + ":" + new String(TARGET)).getBytes())

                    val put_follow = new Put(followRowKey)
                    put_follow.addColumn("f1".getBytes(), "q1".getBytes(), HbaseColumn.STOP)
                    put_follow_list.add(put_follow)


                  }

                  scanner.close()


                  profile_friends_tw.put(put_friends_list)
                  profile_follow_tw.put(put_follow_list)

                }


                case "instagram" => {
                  val hashKey = hash.hashBytes((new String(_TARGET_ID) + ":" + new String(TARGET)).getBytes()).asLong()
                  val tail = second_info.tailMap(hashKey)
                  var starkey = second_info.firstKey()
                  if (!tail.isEmpty) {
                    starkey = tail.firstKey()
                  }
                  val rd_rowkey = this.profileRowKey(new String(_TARGET_ID), new String(TARGET), starkey)
                  val put = new Put(rd_rowkey)
                  put.addColumn("f1".getBytes(), "q0".getBytes(), _TARGET_ID)
                  put.addColumn("f1".getBytes(), "q1".getBytes(), TARGET)
                  put.addColumn("f1".getBytes(), "q2".getBytes(), _URL)
                  put.addColumn("f1".getBytes(), "q3".getBytes(), _NAME)
                  put.addColumn("f1".getBytes(), "q4".getBytes(), _AVATAR)
                  put.addColumn("f1".getBytes(), "q5".getBytes(), _STATUS)
                  put.addColumn("f1".getBytes(), "q6".getBytes(), _SOURCE)
                  put.addColumn("f1".getBytes(), "q7".getBytes(), _FANS_COUN)
                  put.addColumn("f1".getBytes(), "q8".getBytes(), _POST_COUNT)
                  put.addColumn("f1".getBytes(), "q9".getBytes(), _TARGET_ID)
                  put.addColumn("f1".getBytes(), "q10".getBytes(), AVG)
                  put.addColumn("f1".getBytes(), "q11".getBytes(), _GENDER)
                  put.addColumn("f1".getBytes(), "q12".getBytes(), _BIRTHDAY)
                  put.addColumn("f1".getBytes(), "q13".getBytes(), _LOCATION_LON)
                  put.addColumn("f1".getBytes(), "q14".getBytes(), _LAST_LOCATION_LAT)
                  put.addColumn("f1".getBytes(), "q15".getBytes(), _EVALUATION)
                  put.addColumn("f1".getBytes(), "q16".getBytes(), _CREATE_DATA)
                  put.addColumn("f1".getBytes(), "q17".getBytes(), _LAST_UPDATE)
                  put.addColumn("f1".getBytes(), "q18".getBytes(), _FRIENDS_COUNT)
                  put.addColumn("f1".getBytes(), "q19".getBytes(), _FLAGS)
                  profile_ins.put(put)

                  val scan = new Scan()
                  scan.setStartRow(rowkey)
                  scan.setStopRow(rowkey ++ HbaseColumn.STOP)
                  scan.setBatch(1000)
                  scan.setCacheBlocks(false)
                  val scanner = profile_friend.getScanner(scan)

                  val it = scanner.iterator()
                  val put_follow_list = new util.ArrayList[Put]()
                  val put_friends_list = new util.ArrayList[Put]()
                  while (it.hasNext) {

                    val result = it.next()
                    val follow_key = result.getRow.drop(16)
                    val follow_key_str = new String(follow_key)

                    val friendsRowkey = profileFriends(new String(_TARGET_ID), new String(TARGET), starkey, follow_key)
                    val put_friends = new Put(friendsRowkey)
                    put_friends.addColumn("f1".getBytes(), "q1".getBytes(), HbaseColumn.STOP)
                    put_friends_list.add(put_friends)

                    val hashKey = hash.hashBytes(follow_key).asLong()
                    val followRowKey = profileFollowIndex(follow_key_str, hashKey, (new String(_TARGET_ID) + ":" + new String(TARGET)).getBytes())

                    val put_follow = new Put(followRowKey)
                    put_follow.addColumn("f1".getBytes(), "q1".getBytes(), HbaseColumn.STOP)
                    put_follow_list.add(put_follow)


                  }

                  scanner.close()


                  profile_friends_ins.put(put_friends_list)
                  profile_follow_ins.put(put_follow_list)

                }
                case _ =>
              }


            }
          }
        )
        profile_ins.close()
        profile_tw.close()
        profile_weibo.close()
        profile_fb.close()

        profile_follow_fb.close()
        profile_follow_ins.close()
        profile_follow_tw.close()
        profile_follow_weibo.close()

        profile_friend.close()
        profile_friends_fb.close()
        profile_friends_ins.close()
        profile_friends_tw.close()
        profile_friends_weibo.close()

        conn.close()
      }
    }
  )

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

}




