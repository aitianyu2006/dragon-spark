//package com.remarkmedia.spark.task
//
//import java.text.Collator
//import java.util
//import java.util.{Locale, Collections}
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.hbase.client.{Put, HConnection, HConnectionManager}
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.json4s.jackson.JsonMethods._
//import org.json4s.jackson.{JsonMethods, Serialization}
//import org.json4s._
//
//
///**
//  * Created by hz on 16/1/11.
//  */
//object TourService {
//  def main(args: Array[String]) {
//    if (args.length < 2) {
//      System.err.println("Usage: <table_name> <ctiy file>")
//      System.exit(1)
//    }
//
//
//    val sparkConf = new SparkConf().setAppName("TourService").setMaster("yarn-cluster")
//    val sc = new SparkContext(sparkConf)
//
//    val cityNum = sc.textFile(args(1)).map(data => {
//      val datas = data.split("\t")
//      (datas(0), datas(1))
//    }).collect().toMap
//
//    val conf = HBaseConfiguration.create()
//
//    conf.set(TableInputFormat.INPUT_TABLE, args(0))
//    conf.set("hbase.zookeeper.property.clientPort", "2181")
//    conf.set("hbase.zookeeper.quorum", "phb02,phb03,phb04")
//    conf.setLong("hbase.rpc.timeout", 24 * 60 * 60 * 1000)
//
//    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
//      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
//      classOf[org.apache.hadoop.hbase.client.Result])
//
//    sparkConf.registerKryoClasses(Array(classOf[TourProduct], classOf[TourProductAppraise], classOf[TourProductStats]))
//
//    hBaseRDD.map(data => {
//
//      val dest = new String(data._2.getValue("trip".getBytes(), "dest".getBytes()))
//      val destArray = dest.trim.split(" ").sortWith((e1, e2) => Collator.getInstance(Locale.CHINESE).compare(e1, e2) < 0)
//      val fromcity = new String(data._2.getValue("trip".getBytes(), "fromcity".getBytes()))
//      val price = new String(data._2.getValue("trip".getBytes(), "price".getBytes()))
//      implicit val formats = DefaultFormats
//      val negative = parse(new String(data._2.getValue("trip".getBytes(), "negative".getBytes())))
//      val positive = parse(new String(data._2.getValue("trip".getBytes(), "positive".getBytes())))
//      val negativejson = negative.extract[ProductComment]
//      val positivejson = positive.extract[ProductComment]
//
//      val list = new util.ArrayList[TourProduct]()
//
//      list.add(new TourProduct("test_title", dest, fromcity, price.toInt, negativejson.count.toInt ,positivejson.count.toInt, negativejson.comment, positivejson.comment))
//      val key: StringBuilder = new StringBuilder()
//      destArray.foreach(
//        data => key.append(data)
//      )
//
//      cityNum.get(fromcity) match {
//        case Some(num) => key.append(num)
//        case None => key.append("None")
//      }
//
//      (key.toString(), new TourProductStats(list))
//    }).reduceByKey((v1, v2) => v1.merge(v2)).foreachPartition(
//      partition => {
//        val conn = createNewConnection
//        val table = conn.getTable("trip_assessment".getBytes())
//        if (!partition.isEmpty) {
//          partition.foreach(
//            data => {
//              val list = data._2.list
//              if (!list.isEmpty) {
//                Collections.sort(list, new ComparatorTourProduct())
//                if (list.size() > 3) {
//                  val size = list.size()
//                  val section = size / 3
//                  val sectionOne = list.subList(0, section)
//                  val sectionTwo = list.subList(section, section + section)
//                  val sectionThree = list.subList(section + section, size)
//
//                  val itone = sectionOne.iterator()
//                  val negativeOneComments = new util.ArrayList[Comment]()
//                  val positiveOneComments = new util.ArrayList[Comment]()
//                  var negativeOne = 0
//                  var positiveOne = 0
//                  while (itone.hasNext()) {
//                    val tourProduct = itone.next()
//                    negativeOne = negativeOne + tourProduct.negativeCount
//                    positiveOne = positiveOne + tourProduct.positiveCount
//
//                    if (tourProduct.negativeComment != null && !"".equals(tourProduct.negativeComment.comment)) {
//                      if (negativeOneComments.size < 10) {
//                        negativeOneComments.add(tourProduct.negativeComment)
//                      }
//
//                    }
//
//                    if (tourProduct.positiveComment != null && !"".equals(tourProduct.positiveComment.comment)) {
//                      if (positiveOneComments.size < 10) {
//                        positiveOneComments.add((tourProduct.positiveComment))
//                      }
//                    }
//
//                  }
//
//                  val ittwo = sectionTwo.iterator()
//                  val negativeTwoComments = new util.ArrayList[Comment]()
//                  val positiveTwoComments = new util.ArrayList[Comment]()
//                  var negativeTwo = 0
//                  var positiveTwo = 0
//                  while (ittwo.hasNext()) {
//                    val tourProduct = ittwo.next()
//                    negativeTwo = negativeTwo + tourProduct.negativeCount
//                    positiveTwo = positiveTwo + tourProduct.positiveCount
//
//                    if (tourProduct.negativeComment != null && !"".equals(tourProduct.negativeComment.comment)) {
//                      if (negativeTwoComments.size < 10) {
//                        negativeTwoComments.add(tourProduct.negativeComment)
//                      }
//
//                    }
//
//                    if (tourProduct.positiveComment != null && !"".equals(tourProduct.positiveComment.comment)) {
//                      if (positiveTwoComments.size < 10) {
//                        positiveTwoComments.add(tourProduct.positiveComment)
//                      }
//                    }
//
//                  }
//
//                  val itThree = sectionThree.iterator()
//                  val negativeThreeComments = new util.ArrayList[Comment]()
//                  val positiveThreeComments = new util.ArrayList[Comment]()
//                  var negativeThree = 0
//                  var positiveThree = 0
//
//                  while (itThree.hasNext()) {
//                    val tourProduct = itThree.next()
//                    negativeThree = negativeThree + tourProduct.negativeCount
//                    positiveThree = positiveThree + tourProduct.positiveCount
//
//                    if (tourProduct.negativeComment != null && !"".equals(tourProduct.negativeComment.comment)) {
//                      if (negativeThreeComments.size < 10) {
//                        negativeThreeComments.add(tourProduct.negativeComment)
//                      }
//
//                    }
//
//                    if (tourProduct.positiveComment != null && !"".equals(tourProduct.positiveComment)) {
//                      if (positiveThreeComments.size < 10) {
//                        positiveThreeComments.add(tourProduct.positiveComment)
//                      }
//                    }
//
//
//                  }
//                  val tourProductAppraiseList = new util.ArrayList[TourProductResult]()
//
//                  tourProductAppraiseList.add(new TourProductResult((sectionOne.get(0).price.toString + "-" + sectionOne.get(sectionOne.size() - 1).price.toString), negativeOne, positiveOne, negativeOneComments, positiveOneComments))
//                  tourProductAppraiseList.add(new TourProductResult((sectionTwo.get(0).price.toString + "-" + sectionTwo.get(sectionTwo.size() - 1).price.toString), negativeTwo, positiveTwo, negativeTwoComments, positiveTwoComments))
//                  tourProductAppraiseList.add(new TourProductResult((sectionThree.get(0).price.toString + "-" + sectionThree.get(sectionThree.size() - 1).price.toString), negativeThree, positiveThree, negativeThreeComments, positiveThreeComments))
//
//                  val put = new Put(data._1.getBytes())
//
//                  put.add("appraise".getBytes(), "dest".getBytes(), data._1.getBytes)
//                  put.add("appraise".getBytes(), "products".getBytes(), writePrettyJson(tourProductAppraiseList).getBytes)
//                  table.put(put)
//                } else {
//                  val itone = list.iterator()
//                  val negativeComments = new util.ArrayList[Comment]()
//                  val positiveComments = new util.ArrayList[Comment]()
//
//
//                  var negative = 0
//                  var positive = 0
//                  while (itone.hasNext()) {
//                    val tourProduct = itone.next()
//
//                    negative = negative + tourProduct.negativeCount
//                    positive = positive + tourProduct.positiveCount
//
//                    if (tourProduct.negativeComment != null && !"".equals(tourProduct.negativeComment.comment)) {
//                      if (negativeComments.size < 10) {
//                        negativeComments.add(tourProduct.negativeComment)
//                      }
//
//                    }
//
//                    if (tourProduct.positiveComment != null && !"".equals(tourProduct.positiveComment.comment)) {
//                      if (positiveComments.size < 10) {
//                        positiveComments.add(tourProduct.positiveComment)
//                      }
//
//                    }
//
//                  }
//                  val tourProductAppraiseList = new util.ArrayList[TourProductResult]()
//                  tourProductAppraiseList.add(new TourProductResult(list.get(0).price.toString + "-" + list.get(list.size - 1).price.toString, negative, positive, negativeComments, positiveComments))
//
//                  val put = new Put(data._1.getBytes())
//                  put.add("appraise".getBytes(), "dest".getBytes(), data._1.getBytes)
//                  put.add("appraise".getBytes(), "products".getBytes(), writePrettyJson(tourProductAppraiseList).getBytes())
//                  table.put(put)
//                }
//              }
//
//
//            }
//          )
//        }
//
//        table.close()
//        conn.close()
//      }
//
//    )
//
//
//  }
//
//  def createNewConnection(): HConnection = {
//    val zookeepers = "phb02,phb03,phb04"
//    val configuration: Configuration = HBaseConfiguration.create
//    configuration.set("hbase.zookeeper.property.clientPort", "2181")
//    configuration.set("hbase.zookeeper.quorum", zookeepers)
//    configuration.setLong(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY, 24 * 3600 * 1000)
//    val conn = HConnectionManager.createConnection(configuration)
//    conn
//  }
//
//
//  case class TourProduct(title: String, dest: String, fromcity: String, price: Int, negativeCount: Int, positiveCount: Int, negativeComment: Comment,positiveComment: Comment) extends Serializable
//
//  case class TourProductAppraise(appraise: Double, price: String, product: util.List[TourProduct]) extends Serializable
//
//  case class TourProductStats(list: util.List[TourProduct]) extends Serializable {
//
//    def merge(other: TourProductStats): TourProductStats = {
//      list.addAll(other.list)
//      new TourProductStats(list)
//    }
//  }
//
//  case class ProductComment(count: Double, comment: Comment) extends Serializable
//
//  case class Comment(comment: String, score: Double)
//
//  case class TourProductResult(price_interval: String, negative: Int, positive: Int, negativeComments: util.ArrayList[Comment], positiveComments: util.ArrayList[Comment])
//
//  class ComparatorTourProduct extends util.Comparator[TourProduct] {
//    def compare(o1: TourProduct, o2: TourProduct): Int = {
//      (o1.price - o2.price)
//    }
//
//  }
//
//  private implicit lazy val formats = org.json4s.DefaultFormats
//
//  def writePrettyJson[A <: AnyRef](a: A): String = {
//    JsonMethods.pretty(JsonMethods.render(Extraction.decompose(a)(formats)))
//  }
//
//}
