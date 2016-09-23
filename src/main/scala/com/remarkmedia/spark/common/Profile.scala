package com.remarkmedia.spark.common

/**
  * Created by hz on 16/6/6.
  */
trait Profile {
  val f = Array[Byte](0x41.toByte)
  val targetId = Array[Byte](0x00.toByte)
  val target = Array[Byte](0x01.toByte)
  val url = Array[Byte](0x02.toByte)
  val name = Array[Byte](0x03.toByte)
  val avatar = Array[Byte](0x04.toByte)
  val status = Array[Byte](0x05.toByte)
  val source = Array[Byte](0x06.toByte)
  val fansCount = Array[Byte](0x07.toByte)
  val postCount = Array[Byte](0x08.toByte)
  val createDate = Array[Byte](0x09.toByte)
  val age = Array[Byte](0x0A.toByte)
  val gender = Array[Byte](0x0B.toByte)
  val birthday = Array[Byte](0x0C.toByte)
  val lastLocationLon = Array[Byte](0x0D.toByte)
  val lastLocationLat = Array[Byte](0x0E.toByte)
  val evaluation = Array[Byte](0x0F.toByte)
  val lastUpdate = Array[Byte](0x10.toByte)
  val friendsCount = Array[Byte](0x11.toByte)
  val flags = Array[Byte](0x12.toByte)
}
