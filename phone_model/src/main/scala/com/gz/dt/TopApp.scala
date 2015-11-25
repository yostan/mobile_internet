package com.gz.dt

import java.net.URLDecoder

import org.apache.spark.{SparkContext, SparkConf}

import scala.io.Source

/**
 * Created by naonao on 2015/6/25
 */
class TopApp {

}

object AppName extends Enumeration {

  //type AppName = Value
  protected case class Val(val name: String, val list: List[String]) extends super.Val

  implicit def value2Val(v: Value) = v.asInstanceOf[Val]

  val IOSAPP = Val("iOSAPP", List("cfnetwork/"))
  val WECHAT = Val("WeChat", List("micromessenger", "wechat"))
  val SOHUNEWS = Val("sohunews", List("sohunews", "sohu news","搜狐新闻"))
  val QZONE = Val("QZone", List("qzone", "qq空间"))
  val QQMUSIC = Val("QQMusic", List("qqmusic", "qq%e9%9f%b3%e4%b9%90", "qq音乐"))
  val QQMAIL = Val("QQ邮箱", List("qq%e9%82%ae%e7%ae%b1", "qq邮箱"))
  //放在QQ之前，先匹配
  val QQLIVE = Val("QQLive", List("qqlive"))
  val TENGXUNNEWS = Val("腾讯新闻", List("%e8%85%be%e8%ae%af%e6%96%b0%e9%97%bb", "腾讯新闻"))
  val QQ = Val("QQ", List("qq"))
  val WEISHI360 = Val("360卫士", List("%c2%a0360%e5%8d%ab%e5%a3%ab", "360卫士"))
  val WEIBO = Val("WeiBo", List("weibo", "微博"))
  val MOMO = Val("Momo", List("momochat", "陌陌"))

  //val TAOBAO = Val("手机淘宝", List("%E6%89%8B%E6%9C%BA%E6%B7%98%E5%AE%9D", "手机淘宝"))
  //val YIXIN = Val("易信", List("%E6%98%93%E4%BF%A1", "易信"))
  //val TENGXUNWEIBO = Val("腾讯微博", List("%E8%85%BE%E8%AE%AF%E5%BE%AE%E5%8D%9A", "腾讯微博"))
  //val TTDT = Val("天天动听", List("%E5%A4%A9%E5%A4%A9%E5%8A%A8%E5%90%AC", "天天动听"))
  //val KUWOMUSIC = Val("酷我音乐", List("%E9%85%B7%E6%88%91%E9%9F%B3%E4%B9%90", "酷我音乐"))
  //val WANGYIMUSIC = Val("网易云音乐", List("%E7%BD%91%E6%98%93%E4%BA%91%E9%9F%B3%E4%B9%90", "网易云音乐"))
  val TUBAMAP = Val("图吧地图", List("%e5%9b%be%e5%90%a7%e5%9c%b0%e5%9b%be", "图吧地图"))
  //val WNL = Val("万年历", List("%E4%B8%87%E5%B9%B4%E5%8E%86", "万年历"))
  //val TBT = Val("同步推", List("%E5%90%8C%E6%AD%A5%E6%8E%A8", "同步推"))
  //val BAIDUNEWS = Val("百度新闻", List("%E7%99%BE%E5%BA%A6%E6%96%B0%E9%97%BB", "百度新闻"))
  //val BAIDUBROWSER = Val("百度浏览器", List("%E7%99%BE%E5%BA%A6%E6%B5%8F%E8%A7%88%E5%99%A8", "百度浏览器"))
 // val UCBROWSER = Val("UC浏览器", List("UC%E6%B5%8F%E8%A7%88%E5%99%A8", "UC浏览器"))
 // val QQBROWSER = Val("QQ浏览器", List("QQ%E6%B5%8F%E8%A7%88%E5%99%A8", "QQ浏览器"))
  //val MEITUXX = Val("美图秀秀", List("%E7%BE%8E%E5%9B%BE%E7%A7%80%E7%A7%80", "美图秀秀"))
  //val MEIYANXJ = Val("美颜相机", List("%E7%BE%8E%E9%A2%9C%E7%9B%B8%E6%9C%BA", "美颜相机"))
  //val YIHAOZHUANCHE = Val("一号专车", List("%E4%B8%80%E5%8F%B7%E4%B8%93%E8%BD%A6", "一号专车"))
  //val JIAKAO = Val("驾考宝典", List("%E9%A9%BE%E8%80%83%E5%AE%9D%E5%85%B8", "驾考宝典"))
  //val CYZS = Val("穿衣助手", List("%E7%A9%BF%E8%A1%A3%E5%8A%A9%E6%89%8B", "穿衣助手"))
  //val SINASPORT = Val("新浪体育", List("%E6%96%B0%E6%B5%AA%E4%BD%93%E8%82%B2", "新浪体育"))
  //val GAODE = Val("高德导航", List("%E9%AB%98%E5%BE%B7%E5%AF%BC%E8%88%AA", "高德导航"))
  //val CJKCB = Val("超级课程表", List("%E9%AB%98%E5%BE%B7%E5%AF%BC%E8%88%AA", "超级课程表"))







}

object TopApp {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: TopApp <data-path> <save-path>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("top-app").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //val r1 = sc.wholeTextFiles(args(0))
   // val r2 = r1.map(_._2).map(_.split("\n")).flatMap(line => line)
    val rdd1 = sc.textFile(args(0))
    //val rdd2 = rdd1.filter(ss => ss.length != 0).map(log => log.split("\\|", -1)).filter(arr => arr(26) != "" && arr(1) != "").map(arr => (arr(1), arr(26)))

   // val rdd3 = rdd2.filter(t => !t._2.startsWith("Mozilla") && !t._2.startsWith("Dalvik"))

   // val rdd4 = rdd3.map { t => (t._1, pickupAppName(t._2)) }

    val rdd4 = rdd1.filter(t => !t.startsWith("Mozilla") && !t.startsWith("Dalvik")).map(x => pickupAppName(x))
    //rdd4.foreach(println)
    //val rdd5 = rdd4.distinct().map(x => (x._2, 1L)).reduceByKey(_ + _).filter(t => t._2 > 2L).map(t => (t._2, t._1)).sortByKey(false)



    val rdd5 = rdd4.map(x => (x, 1L)).reduceByKey(_ + _).filter(t => t._2 > 2L).map(t => (t._2, t._1)).sortByKey(false)
    rdd5.saveAsTextFile(args(1))
    //rdd5.foreach(println)

    sc.stop()
  }


  def pickupAppName(str: String): String = {
    var appName = "Unknown"
    for (a <- AppName.values) {
      for (l <- a.list) {
        if (str.toLowerCase.contains(l)) {
          appName = a.name
          if(appName.equals("iOSAPP")){
            appName = URLDecoder.decode(str.split("\\/")(0), "utf-8")
          }
          return appName
        }
      }
    }
    if (appName.equals("Unknown")) {
      val pattern_string = "([^\\/|\\s|\\;|\\(]*)".r
      val src = Source.fromString(str).mkString
      val mat = pattern_string.findFirstIn(src).getOrElse("Unknown")

      appName = mat
    }
    return appName
  }


//  def main (args: Array[String]) {
//    val str = "; NineGameClient/android ve/1.5.0 si/ba45f1d5-5974-49dd-a94c-6d9fcba32036 ch/WM_12027 ss/480x854 nt/3g"// 1.4.0 (Android;4.1.2;LENOVO;Lenovo A860e)"
//    val pattern_string = "([^\\/|\\s|\\;|\\(]*)".r
//    val src = Source.fromString(str).mkString
//    val mat = pattern_string.findFirstIn(src).getOrElse("Unknown")
//    println(mat)
//  }


}
