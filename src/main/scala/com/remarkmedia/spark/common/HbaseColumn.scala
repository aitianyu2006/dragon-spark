package com.remarkmedia.spark.common

/**
  * Created by hz on 16/6/14.
  */
object HbaseColumn {
  val CF = Array[Byte](0x41.toByte)
  val RD_FOLLOW = Array[Byte](0x80.toByte)
  val STOP = Array[Byte](0xff.toByte)


  trait Profile {
    val PROFILE_TARGET_ID = Array[Byte](0x00.toByte)
    val PROFILE_TARGET = Array[Byte](0x01.toByte)
    val PROFILE_URL = Array[Byte](0x02.toByte)
    val PROFILE_NAME = Array[Byte](0x03.toByte)
    val PROFILE_AVATAR = Array[Byte](0x04.toByte)
    val PROFILE_STATUS = Array[Byte](0x05.toByte)
    val PROFILE_SOURCE = Array[Byte](0x06.toByte)
    val PROFILE_FANS_COUNT = Array[Byte](0x07.toByte)
    val PROFILE_POST_COUNT = Array[Byte](0x08.toByte)
    val PROFILE_CREATE_DATA = Array[Byte](0x09.toByte)
    val PROFILE_AGE = Array[Byte](0x0A.toByte)
    val PROFILE_GENDER = Array[Byte](0x0B.toByte)
    val PROFILE_BIRTHDAY = Array[Byte](0x0C.toByte)
    val PROFILE_LAST_LOCATION_LON = Array[Byte](0x0D.toByte)
    val PROFILE_LAST_LOCATION_LAT = Array[Byte](0x0E.toByte)
    val PROFILE_EVALUATION = Array[Byte](0x0F.toByte)
    val PROFILE_LAST_UPDATE = Array[Byte](0x10.toByte)
    val PROFILE_FRIENDS_COUNT = Array[Byte](0x11.toByte)
    val PROFILE_FLAGS = Array[Byte](0x12.toByte)
    val RM_POST_COUNT = Array[Byte](0x16.toByte)
  }

  trait Post_mate {
    val POST_META_TABLE_NAME = "post_meta"
    val POST_META_OSS = Array[Byte](0x0F.toByte)
    val POST_META_ID = Array[Byte](0x00.toByte)
    val POST_META_TARGET_ID = Array[Byte](0x01.toByte)
    val POST_META_TARGET = Array[Byte](0x02.toByte)
    val POST_META_TIMESTAMP = Array[Byte](0x03.toByte)
    val POST_META_IDENTITY = Array[Byte](0x0E.toByte)
    val POST_META_TAG = Array[Byte](0x0A.toByte)
    val POST_META_locationLat = Array[Byte](0x0A.toByte)
    val POST_META_locationLon = Array[Byte](0x0A.toByte)
    val POST_META_content = Array[Byte](0x09.toByte)


  }

  trait image_table {
    val image_table_name = "image"
    val id = Array[Byte](0x00.toByte)
    val originalUrl = Array[Byte](0x02.toByte)
    val tanqucdn = Array[Byte](0x06.toByte)
    val tanqucdn_base_url = "http://rd-images.tqcdn-kamui.tanqu.com.cn/"
  }

}

