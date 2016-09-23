package com.remarkmedia.spark.APP

import java.io.{BufferedWriter, FileWriter}

import com.remarkmedia.spark.examples.KafkaTest.CollectResourceAndProfile
import org.apache.hadoop.hbase.client.Put

import scala.io.Source

/**
  * Created by hz on 16/7/1.
  */
object SubmitApp {
  def main(args: Array[String]): Unit = {
    //
    //    val write = new BufferedWriter(new FileWriter(("/Users/hz/Downloads/home")))
    //    val it = Source.fromFile("/Users/hz/Downloads/家居.txt").getLines()
    //    it.foreach(
    //      line => {
    //        var cell = line.split("\t")
    //        println(cell.length)
    //        if (cell.length == 2) {
    //          write.write(cell(0).trim + ":" + cell(1).trim)
    //          write.write("\n")
    //        } else {
    //          cell = line.split(" ")
    //          if (cell.length == 2) {
    //            write.write(cell(0).trim + ":" + cell(1).trim)
    //            write.write("\n")
    //          }
    //
    //        }
    //
    //      }
    //    )
    //    write.close()
    //  }

    //    val str="{\n    \"profiles\": [\n        {\n            \"face\": \"http://tva3.sinaimg.cn/crop.0.0.180.180.180/46cc894ajw1e8qgp5bmzyj2050050aa8.jpg\",\n            \"sex\": \"male\",\n            \"url\": \"http://weibo.com/1187809610\",\n            \"friends\": [\n                2913709715\n            ],\n            \"lastLocation\": {\n                \"name\": \"\",\n                \"latitude\": 0,\n                \"longitude\": 0\n            },\n            \"evaluation\": \"priority\",\n            \"date\": \"2016-07-27\",\n            \"lastUpdate\": 1454564941000,\n            \"name\": \"乐咔咔HOT笑话\",\n            \"source\": \"weibo\",\n            \"createdate\": \"2016-07-27 19:10\",\n            \"postCount\": 689,\n            \"fansNum\": 635,\n            \"_id\": \"1187809610:weibo\"\n        }\n    ],\n    \"resources\": [\n        {\n            \"id\": \"3925685410035629:weibo\",\n            \"tags\": [],\n            \"textContent\": \"刚刚下载了豆丁文档:当代研究生英语读写教程教习题答案下网页链接\",\n            \"sourceTime\": 1451439799000,\n            \"location\": {\n                \"name\": \"\",\n                \"latitude\": 0,\n                \"longitude\": 0\n            },\n            \"postType\": \"text\",\n            \"commentsCount\": 0,\n            \"sourceUserId\": \"1187809610:weibo\",\n            \"likesCount\": 0\n        },\n        {\n            \"id\": \"3882144798074788_46cc894ajw1evmja6kvjij20k028atfk:weibo\",\n            \"tags\": [],\n            \"textContent\": \"[?5D奇酷手机三剑齐出鞘，青春版旗舰版尊享版预约火热进行中：青春版9??0:00，现货发售！岂止安全?.5英寸1080P大屏够高清，全金属机身颜值高，边框窄盛于无，指纹智键护隐私，1300万后?20800万前置摄像头，全搭载?60 OS操作系统，首发价1199元起！@360奇酷手机 预约开宝箱赢好礼：\",\n            \"postType\": \"image\",\n            \"sourceTime\": 1441058909000,\n            \"location\": {\n                \"name\": \"\",\n                \"latitude\": 0,\n                \"longitude\": 0\n            },\n            \"images\": [\n                {\n                    \"height\": 320,\n                    \"width\": 320,\n                    \"url\": \"http://ww2.sinaimg.cn/bmiddle/46cc894ajw1evmja6kvjij20k028atfk.jpg\",\n                    \"originalUrl\": \"http://ww2.sinaimg.cn/bmiddle/46cc894ajw1evmja6kvjij20k028atfk.jpg\",\n                    \"imageType\": \"middle\"\n                },\n                {\n                    \"height\": 640,\n                    \"width\": 640,\n                    \"url\": \"http://ww2.sinaimg.cn/large/46cc894ajw1evmja6kvjij20k028atfk.jpg\",\n                    \"originalUrl\": \"http://ww2.sinaimg.cn/large/46cc894ajw1evmja6kvjij20k028atfk.jpg\",\n                    \"imageType\": \"large\"\n                },\n                {\n                    \"height\": 180,\n                    \"width\": 180,\n                    \"url\": \"http://ww2.sinaimg.cn/thumbnail/46cc894ajw1evmja6kvjij20k028atfk.jpg\",\n                    \"originalUrl\": \"http://ww2.sinaimg.cn/thumbnail/46cc894ajw1evmja6kvjij20k028atfk.jpg\",\n                    \"imageType\": \"thumbnail\"\n                }\n            ],\n            \"commentsCount\": 0,\n            \"sourceUserId\": \"1187809610:weibo\",\n            \"likesCount\": 0\n        },\n        {\n            \"id\": \"3876379185968212_46cc894ajw1ev4540aa2yj2064064glo:weibo\",\n            \"tags\": [],\n            \"textContent\": \"给你推荐一个京东“砍啊砍”游戏，里面的商品能够砍价哦，优惠力度很大！我在京东客户端砍价成功砍掉99.00元，你也来试试吧！网页链接我在京东客户端成功砍掉99.00元，你也来试试吧！@京东 网页链接\",\n            \"postType\": \"image\",\n            \"sourceTime\": 1439684280000,\n            \"location\": {\n                \"name\": \"\",\n                \"latitude\": 0,\n                \"longitude\": 0\n            },\n            \"images\": [\n                {\n                    \"height\": 320,\n                    \"width\": 320,\n                    \"url\": \"http://ww2.sinaimg.cn/bmiddle/46cc894ajw1ev4540aa2yj2064064glo.jpg\",\n                    \"originalUrl\": \"http://ww2.sinaimg.cn/bmiddle/46cc894ajw1ev4540aa2yj2064064glo.jpg\",\n                    \"imageType\": \"middle\"\n                },\n                {\n                    \"height\": 640,\n                    \"width\": 640,\n                    \"url\": \"http://ww2.sinaimg.cn/large/46cc894ajw1ev4540aa2yj2064064glo.jpg\",\n                    \"originalUrl\": \"http://ww2.sinaimg.cn/large/46cc894ajw1ev4540aa2yj2064064glo.jpg\",\n                    \"imageType\": \"large\"\n                },\n                {\n                    \"height\": 180,\n                    \"width\": 180,\n                    \"url\": \"http://ww2.sinaimg.cn/thumbnail/46cc894ajw1ev4540aa2yj2064064glo.jpg\",\n                    \"originalUrl\": \"http://ww2.sinaimg.cn/thumbnail/46cc894ajw1ev4540aa2yj2064064glo.jpg\",\n                    \"imageType\": \"thumbnail\"\n                }\n            ],\n            \"commentsCount\": 0,\n            \"sourceUserId\": \"1187809610:weibo\",\n            \"likesCount\": 0\n        },\n        {\n            \"id\": \"3872837972082698:weibo\",\n            \"tags\": [\n                \"全国民间高手炒股大赛\"\n            ],\n            \"textContent\": \"我于 08-06  13:46 在新浪模拟交易中买入股票,#全国民间高手炒股大赛#火爆进行中，快来跟高手一起挣钱吧！民间高手大赛第三季_...\",\n            \"sourceTime\": 1438839987000,\n            \"location\": {\n                \"name\": \"\",\n                \"latitude\": 0,\n                \"longitude\": 0\n            },\n            \"postType\": \"text\",\n            \"commentsCount\": 0,\n            \"sourceUserId\": \"1187809610:weibo\",\n            \"likesCount\": 0\n        },\n        {\n            \"id\": \"3831168154027723:weibo\",\n            \"tags\": [],\n            \"textContent\": \"我于 04-13  14:05 在新浪模拟交易中买入股票,全国民间高手炒股大赛火爆进行中，快来跟高手一起挣钱吧！2015全国民间高手...\",\n            \"sourceTime\": 1428905130000,\n            \"location\": {\n                \"name\": \"\",\n                \"latitude\": 0,\n                \"longitude\": 0\n            },\n            \"postType\": \"text\",\n            \"commentsCount\": 0,\n            \"sourceUserId\": \"1187809610:weibo\",\n            \"likesCount\": 0\n        },\n        {\n            \"id\": \"3824960290220181:weibo\",\n            \"tags\": [],\n            \"textContent\": \"我于 03-27  10:57 在新浪模拟交易中买入股票,全国民间高手炒股大赛火爆进行中，快来跟高手一起挣钱吧！2015全国民间高手...\",\n            \"sourceTime\": 1427425060000,\n            \"location\": {\n                \"name\": \"\",\n                \"latitude\": 0,\n                \"longitude\": 0\n            },\n            \"postType\": \"text\",\n            \"commentsCount\": 0,\n            \"sourceUserId\": \"1187809610:weibo\",\n            \"likesCount\": 0\n        },\n        {\n            \"id\": \"3822977869177910_46cc894ajw1eqds1z0avfj20ku112juv:weibo\",\n            \"tags\": [\n                \"快的打车代金券\"\n            ],\n            \"textContent\": \"#快的打车代金券# 看快乐大本营，代金券奖品摇不停！3月21日20:00-22:00看快乐大本营，和吴亦凡、陈伟霆、杨洋一起摇5000万@快的打车 代金券！现在就要打车券？戳我领取网页链接\",\n            \"postType\": \"image\",\n            \"sourceTime\": 1426952414000,\n            \"location\": {\n                \"name\": \"\",\n                \"latitude\": 0,\n                \"longitude\": 0\n            },\n            \"images\": [\n                {\n                    \"height\": 320,\n                    \"width\": 320,\n                    \"url\": \"http://ww2.sinaimg.cn/bmiddle/46cc894ajw1eqds1z0avfj20ku112juv.jpg\",\n                    \"originalUrl\": \"http://ww2.sinaimg.cn/bmiddle/46cc894ajw1eqds1z0avfj20ku112juv.jpg\",\n                    \"imageType\": \"middle\"\n                },\n                {\n                    \"height\": 640,\n                    \"width\": 640,\n                    \"url\": \"http://ww2.sinaimg.cn/large/46cc894ajw1eqds1z0avfj20ku112juv.jpg\",\n                    \"originalUrl\": \"http://ww2.sinaimg.cn/large/46cc894ajw1eqds1z0avfj20ku112juv.jpg\",\n                    \"imageType\": \"large\"\n                },\n                {\n                    \"height\": 180,\n                    \"width\": 180,\n                    \"url\": \"http://ww2.sinaimg.cn/thumbnail/46cc894ajw1eqds1z0avfj20ku112juv.jpg\",\n                    \"originalUrl\": \"http://ww2.sinaimg.cn/thumbnail/46cc894ajw1eqds1z0avfj20ku112juv.jpg\",\n                    \"imageType\": \"thumbnail\"\n                }\n            ],\n            \"commentsCount\": 0,\n            \"sourceUserId\": \"1187809610:weibo\",\n            \"likesCount\": 0\n        },\n        {\n            \"id\": \"3811746907525587:weibo\",\n            \"tags\": [\n                \"春晚\"\n            ],\n            \"textContent\": \"#春晚#  come on\",\n            \"sourceTime\": 1424274744000,\n            \"location\": {\n                \"name\": \"\",\n                \"latitude\": 0,\n                \"longitude\": 0\n            },\n            \"postType\": \"text\",\n            \"commentsCount\": 0,\n            \"sourceUserId\": \"1187809610:weibo\",\n            \"likesCount\": 0\n        },\n        {\n            \"id\": \"3811742293664920:weibo\",\n            \"tags\": [\n                \"春晚\"\n            ],\n            \"textContent\": \"#春晚#今晚春晚还不错！勾引红包来\",\n            \"sourceTime\": 1424273643000,\n            \"location\": {\n                \"name\": \"\",\n                \"latitude\": 0,\n                \"longitude\": 0\n            },\n            \"postType\": \"text\",\n            \"commentsCount\": 0,\n            \"sourceUserId\": \"1187809610:weibo\",\n            \"likesCount\": 0\n        }\n    ]\n}"
    //
    //
    //    import org.json4s._
    //    import org.json4s.jackson.JsonMethods._
    //    implicit val formats = DefaultFormats
    //    val json = parse(str)
    //    val collect = json.extract[CollectResourceAndProfile]

    //    val lon_value=103.3
    //    val lat_value=30.8
    //    if ((lon_value >= 103.29f && lon_value <= 104.94f) && (lat_value >= 30.2f && lat_value <= 31.04f)) {
    ////      val put = new Put(result._2.getRow)
    ////      result._2.rawCells().foreach(
    ////        cell => put.add(cell)
    ////      )
    ////      rd_cd_post.put(put)
    //      println("ok")
    //    }

//    val myList = Array(1.9, 2.9, 3.4, 3.5)
    //
    //    val myLidst1 = myList.drop(2)
    //
    //    myLidst1.foreach(
    //      data=>println(data)
    //    )
    //
    //    val myLidst2 = myList.dropRight(2)
    //
    //    myLidst2.foreach(
    //      data=>println(data)
    //    )

    val a="12345"

    println(a.contains("345"))

  }
}
