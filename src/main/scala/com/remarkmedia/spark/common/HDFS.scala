package com.remarkmedia.spark.common

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

/**
  * Created by hz on 16/7/5.
  */
object HDFS {
  val conf: Configuration = new Configuration()
  val fs: FileSystem = FileSystem.get(conf)



}
