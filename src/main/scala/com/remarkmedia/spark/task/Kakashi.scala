package com.remarkmedia.spark.task

import java.util

import com.remarkmedia.spark.util.Logs
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hz on 15/12/31.
  */
object Kakashi extends Logs {

  def main(args: Array[String]) {
    if (args.length != 2) {
      println("input gangma,alifile")
      System.exit(1)

    }

    val sparkConf = new SparkConf().setAppName("Kakashi")
    sparkConf.registerKryoClasses(Array(classOf[GangmaProduct], classOf[Aliqua]))

    val sc = new SparkContext(sparkConf)
    val file = sc.textFile(args(1)).map(data => {
      if (!data.trim.equals("")) {
        data
      }
    }).collect()

    val gmproduct = sc.textFile(args(0)).map(
      line => {
        import org.json4s._
        import org.json4s.jackson.JsonMethods._
        implicit val formats = DefaultFormats
        val json = parse(line)
        val aliqua = json.extract[Aliqua]
        var name = "无匹配"
        file.foreach(
          line => {
            val str = line.toString
            val strArray = str.split("[+]")
            if (strArray.length > 1) {
              if (aliqua.trip_title.contains(strArray(0)) || aliqua.trip_title.contains(strArray(1))) {
                name = str
              }
            } else {
              if (aliqua.trip_title.contains(str)) {
                name = str
              }
            }

          }
        )

        if (name != "无匹配") {
          (aliqua.trip_title, name)
        } else {
          ("1", "1")
        }


      }).collect()


    var count = 1
    gmproduct.foreach(
      data => {
        if (!(data._1 == "1")) {
          count = count + 1
        }
      }
    )

    info("----------------------------" + count + "-------------------------------")

  }

  case class GangmaProduct(product: util.ArrayList[String]) extends Serializable

  case class Aliqua(trip_title: String, price: Int) extends Serializable

}


