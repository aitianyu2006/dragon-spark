package com.remarkmedia.spark.task

import java.net.URI
import java.text.SimpleDateFormat

import com.remarkmedia.spark.common.HbaseColumn
import com.remarkmedia.spark.common.HbaseColumn.Post_mate
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source

/**
  * Created by hz on 16/8/8.
  */
object FansTang extends Post_mate {
  val dfs = "hdfs://phb01-vb.hz.kankanapp.com.cn:8020/user/hdfs/"

  def main(args: Array[String]) {
    val date = args(0).toString
    //val dateEndStr = args(0).toString
    val tags = args(1).toString

    val sparkConf = new SparkConf().setAppName("FemaleTask").setMaster("yarn-cluster")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, POST_META_TABLE_NAME)
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "phb01,phb02,phb03")
    conf.setLong("hbase.rpc.timeout", 24 * 60 * 60 * 1000)


    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val profiles = mutable.Buffer[String]()
    val filmNames = mutable.Buffer[String]()
    val actorNames = mutable.Buffer[String]()
    val filmInfos = mutable.Buffer[String]()
    val filmMap = scala.collection.mutable.Map[String, Array[String]]()

    val fs = FileSystem.get(URI.create(dfs + tags), new Configuration())
    val in = fs.open(new Path(dfs + tags))
    val it = Source.fromInputStream(in).getLines()
    it.foreach(
      line => {
        filmInfos.+=(line.trim.toLowerCase)
        val names = line.split(",")
        names.foreach(
          name => profiles.+=(name.trim)
        )
        val actorName = names.drop(1)
        val filmName = names.dropRight(1)
        filmName.foreach(
          data => filmNames.+=:(data.toLowerCase)
        )
        actorName.foreach(
          data => actorNames.+=:(data.toLowerCase)
        )
        filmMap.+=(filmName(0) -> actorName)
      }
    )




    val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val timeStar = format.parse(date + " 00:00:00").getTime
    val timeEnd = format.parse(date + " 23:59:59").getTime
    val films = new Films(profiles, filmNames, actorNames, filmInfos, timeStar, timeEnd)
    val broadcastVar = sc.broadcast(films)

    val filmsFans = hBaseRDD.mapPartitions(x => {
      val map = scala.collection.mutable.Map[String, Long]()
      x.foreach(
        data => {
          val time: Long = data._2.getValue(HbaseColumn.CF, POST_META_TIMESTAMP)
          if (time >= broadcastVar.value.timeStar && time <= broadcastVar.value.timeEnd) {
            val target: String = data._2.getValue(HbaseColumn.CF, POST_META_TARGET)
            target match {
              case "weibo" => {
                val content: String = data._2.getValue(HbaseColumn.CF, POST_META_content)
                broadcastVar.value.names.foreach(
                  name => {

                    if (content.toLowerCase.contains(name)) {
                      map.get(name) match {
                        case Some(v) => {
                          map.+=(name -> (v + 1l))
                        }
                        case None => {
                          map.+=(name -> 1l)
                        }
                      }

                    }
                  }
                )
              }
              case _ =>
            }
          }
        }
      )
      map.iterator
    }
    ).reduceByKey(_ + _).collect()


    println("filmsFans:" + filmsFans.size)
    val filmActorMap = scala.collection.mutable.Map[String, Long]()
    filmsFans.foreach(
      film => {
        println(film._1 + ":" + film._2)
        filmActorMap.+=(film._1 -> film._2)
      }
    )

    val reslut = mutable.Buffer[String]()
    filmMap.foreach(
      infos => {
        val sb = new StringBuilder()
        filmActorMap.get(infos._1) match {
          case Some(v) => sb.append(infos._1 + ":" + v + "|")
          case None => sb.append(infos._1 + ":" + 0 + "|")
        }

        filmActorMap.get(infos._2(0)) match {
          case Some(v) => sb.append(infos._2(0) + ":" + v + "|")
          case None => sb.append(infos._1 + ":" + 0 + "|")
        }

        if (infos._2.length > 1) {
          infos._2.drop(1).foreach(
            actor => {
              filmActorMap.get(actor) match {
                case Some(v) => sb.append(actor + ":" + v + ",")
                case None => sb.append(infos._1 + ":" + 0 + ",")
              }
            }
          )
        }
        reslut.+=(sb.toString())
      }
    )

    val out = fs.append(new Path(dfs + date + ".csv"))
    reslut.foreach(
      x => {
        val line = x + "\n"
        out.write(line.getBytes, 0, line.getBytes().length)
      }

    )

    out.close()
    in.close()
    fs.close()
    sc.stop()


  }

  //  def FlatMapPartitions(it: Iterator[(ImmutableBytesWritable, Result)]): Iterator[(String, Long)] = {
  //    val r = "\\[em\\][\\S]+\\[\\/\\S+\\]"
  //    val r1 = "\\[[0-9a-zA-Z\u4e00-\u9fa5]+\\]"
  //    val r2 = "[@][0-9a-zA-Z\u4e00-\u9fa5_-]+"
  //
  //    val map = Map[String, Long]()
  //
  //    it.foreach(
  //      data => {
  //        val time: Long = data._2.getValue(HbaseColumn.CF, POST_META_TIMESTAMP)
  //        if (time >= broadcastVar.value.time) {
  //          val target: String = data._2.getValue(HbaseColumn.CF, POST_META_TARGET)
  //          target match {
  //            case "weibo" => {
  //              var content: String = data._2.getValue(HbaseColumn.CF, POST_META_content)
  //              content = content.replaceAll(r, "").replaceAll(r1, "").replaceAll(r2, "").replaceAll("[^\\w|^\u4E00-\u9FA5]|[\\^|\\|]", "")
  //              broadcastVar.value.names.foreach(
  //                name => {
  //                  if (content.contains(name)) {
  //                    map.get(name) match {
  //                      case Some(v) => {
  //                        map.+(name -> (v + 1l))
  //                      }
  //                      case None => map.+(name -> 1l)
  //                    }
  //
  //                  }
  //                }
  //
  //              )
  //            }
  //            case _ =>
  //          }
  //        }
  //
  //      }
  //    )
  //    map.iterator
  //  }

  implicit def bytes2int(bytes: Array[Byte]): Long = {
    var i: Long = -1
    if (bytes != null && bytes.length > 0) {
      i = Bytes.toLong(bytes)
    }
    i
  }

  implicit def bytes2String(bytes: Array[Byte]): String = {
    var f: String = ""
    if (bytes != null && bytes.length > 0) {
      f = Bytes.toString(bytes)
    }
    f
  }


}

case class Films(names: mutable.Buffer[String], filmNames: mutable.Buffer[String], actorNames: mutable.Buffer[String], filmInfos: mutable.Buffer[String], timeStar: Long, timeEnd: Long)


