package com.remarkmedia.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hz on 16/8/1.
  */
object Brctest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("test").setMaster("yarn-cluster")
    val sc = new SparkContext(sparkConf)
    val broads = sc.broadcast(3)
    val lists = List(1, 2, 3, 4, 5)
    val listRDD = sc.parallelize(lists)
    val results = listRDD.map(x => x * broads.value)
    results.foreach(x => println("增加后的结果：" + x))
    sc.stop
  }
}
