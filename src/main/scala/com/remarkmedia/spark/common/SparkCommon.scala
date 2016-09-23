package com.remarkmedia.spark.common

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hz on 16/7/5.
  */
object SparkCommon {
  def getSparkContext(name: String): SparkContext = {
    val sparkConf = new SparkConf().setAppName(name).setMaster("yarn-cluster")
    val sc = new SparkContext(sparkConf)
    sc
  }
}
