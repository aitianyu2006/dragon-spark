package com.remarkmedia.spark.common

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

/**
  * Created by hz on 16/6/14.
  */
object Hbase {
  var conn: Connection = null

  def createNewConnection(): Connection = {
    if (conn == null || conn.isClosed) {
      val configuration = HBaseConfiguration.create()
      configuration.set("hbase.zookeeper.property.clientPort", "2181")
      configuration.set("hbase.zookeeper.quorum", "prhb01,prhb02,prhb03")
      configuration.setLong("hbase.rpc.timeout", 24 * 60 * 60 * 1000)
      conn = ConnectionFactory.createConnection(configuration)
    }
    conn
  }


  def profileRowKey(targetId: String, target: String) = {
    DigestUtils.md5(targetId + ":" + target)
  }

  def profileFriendRowKey(targetId: String, target: String, friendId: String): Array[Byte] = {
    profileRowKey(targetId, target) ++ (friendId + ":" + target).getBytes()
  }
}

object Hbase_RD {
  var conn: Connection = null

  def createNewConnection(): Connection = {
    if (conn == null || conn.isClosed) {
      val configuration = HBaseConfiguration.create()
      configuration.set("hbase.zookeeper.property.clientPort", "2181")
      configuration.set("hbase.zookeeper.quorum", "prhb01,prhb02,prhb03")
      configuration.setLong("hbase.rpc.timeout", 24 * 60 * 60 * 1000)
      conn = ConnectionFactory.createConnection(configuration)
    }
    conn
  }

}


object Hbase_stage {
  var conn: Connection = null

  def createNewConnection(): Connection = {
    if (conn == null || conn.isClosed) {
      val configuration = HBaseConfiguration.create()
      configuration.set("hbase.zookeeper.property.clientPort", "2181")
      configuration.set("hbase.zookeeper.quorum", "shb01.hzvb.kankanapp.com.cn,shb02.hzvb.kankanapp.com.cn,shb03.hzvb.kankanapp.com.cn")
      configuration.setLong("hbase.rpc.timeout", 24 * 60 * 60 * 1000)
      conn = ConnectionFactory.createConnection(configuration)
    }
    conn
  }

}
