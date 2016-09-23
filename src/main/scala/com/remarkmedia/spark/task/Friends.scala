package com.remarkmedia.spark.task

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by hz on 16/8/12.
  */
object Friends {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Fiends").setMaster("yarn-cluster")
    val sc = new SparkContext(sparkConf)
    val file = new File("/tmp/out.txt")
    val write = new BufferedWriter(new FileWriter(file))
    sc.textFile("/user/hdfs/part-00000.csv", 32).mapPartitions(
      p => {
        val map = scala.collection.mutable.Map[String, mutable.ArrayBuffer[String]]()
        p.foreach(
          line => {
            val fiends = line.split(",")
            if (fiends.length > 1) {
              val fiends_1 = fiends.drop(1)
              fiends_1.foreach(
                x => {
                  map.get(x) match {
                    case Some(v) => {
                      v.+=(fiends(0))
                    }
                    case None => {
                      val buffer = mutable.ArrayBuffer[String]()
                      buffer.+=(fiends(0))
                      map.+=(x -> buffer)
                    }
                  }
                }
              )
            }

          }
        )

        map.iterator
      }
    ).reduceByKey((x, y) => {
      y.foreach(
        v => {
          x.+=(v)
        }

      )
      x
    }).filter(a => (a._2.length > 1)).sortBy(x => x._2.length, false).collect().foreach(
      data => {

        val sb = new StringBuilder(data._1)
        sb.append("|")

        data._2.foreach(
          user => {
            sb.append(user)
            sb.append(",")
          }
        )
        sb.append("|")
        sb.append(data._2.length)
        write.write(sb.toString() + "\n")
        println(sb.toString() + "\n")
      }
    )

    write.close()
    sc.stop()

  }
}