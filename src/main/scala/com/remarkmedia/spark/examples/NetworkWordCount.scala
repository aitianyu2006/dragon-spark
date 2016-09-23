//package com.remarkmedia.spark.examples
//
//import org.apache.spark.SparkConf
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
///**
//  * Created by hz on 16/4/1.
//  */
//object NetworkWordCount {
//  def main(args: Array[String]) {
////    if (args.length < 2) {
////      System.err.println("Usage: NetworkWordCount <hostname> <port>")
////      System.exit(1)
////    }
//    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
//    val ssc = new StreamingContext(sparkConf, Seconds(5))
//    val lines = ssc.socketTextStream("phb03", 9999, StorageLevel.MEMORY_AND_DISK_SER)
//    val words = lines.flatMap(_.split(" "))
//
//    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
//    wordCounts.print()
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}