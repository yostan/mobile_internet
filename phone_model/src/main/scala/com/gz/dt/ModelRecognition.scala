package com.gz.dt


import eu.bitwalker.useragentutils.{Manufacturer, UserAgent}
import org.apache.spark.{SparkContext, SparkConf}
import scala.io.Source
import scala.util.control.Breaks._

/**
 * Created by naonao on 2015/6/17
 */
class ModelRecognition {

}

object PhoneBrand extends Enumeration {

  protected case class Val(val name: String, val list: List[String]) extends super.Val

  implicit def value2Val(v: Value) = v.asInstanceOf[Val]

  val IPHONE = Val("iPhone", List("ios", "iphone", "mac"))
  val HUAWEI = Val("HuaWei", List("huawei"))
  val XIAOMI = Val("XiaoMi", List("xiaomi", "miui"))
  val SAMSUNG = Val("SAMSUNG", List("samsung", "galaxy"))
  val COOLPAD = Val("Coolpad", List("coolpad"))
  val LENOVO = Val("Lenovo", List("lenovo"))

  val NUBIA = Val("Nubia", List("nubia"))
  val ZTE = Val("ZTE", List("zte"))
  val VIVO = Val("Vivo", List("vivo"))
  val HTC = Val("HTC", List("htc"))
  val OPPO = Val("OPPO", List("oppo"))
  val GOOGLE = Val("Nexus", List("nexus"))
  val LG = Val("LG", List("lg-"))
  val NOKIA = Val("NOKIA", List("nokia"))
  val GIONEE = Val("金立", List("gionee"))
  val SONY = Val("Sony", List("sony"))
  val SMARTISAN = Val("Smartisan", List("smartisan"))
  val CHANGHONG = Val("ChangHong", List("changhong"))
  val HISENSE = Val("Hisense", List("hisense"))
  val TCL = Val("TCL", List("tcl"))
  val MEIZU = Val("Meizu", List("meizu"))
  val DOPOD = Val("Dopod", List("dopod"))
  val MOTOROLA = Val("Motorola", List("motorola"))
  val PHILIPS = Val("Philips", List("philips"))
  val BLACKBERRY = Val("BlackBerry", List("blackberry"))
  val ONEPLUS = Val("OnePlus", List("oneplus"))
  val DOOV = Val("Doov", List("doov"))
  val K_TOUCH = Val("K-Touch", List("k-touch"))
  val ASUS = Val("ASUS", List("asus"))
  val koobee = Val("Koobee", List("koobee"))
  val BENWEE = Val("本为", List("benwee"))


}


object ModelRecognition {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: ModelRecognition <path-data> <save-path>")
      System.exit(1)
    }

    //val pp = Pattern.compile("Version\\/(([0-9]+)\\.?([\\w]+)?(\\.[\\w]+)?)[\\s]+UC(?!Browser|BRowse|BRows|BRow|BRo|BR|B)")
    //val mat = pp.matcher("Mozilla/5.0 (Linux; U; Android 4.4.4; zh-CN; Coolpad 8675-A Build/KTU84P) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 UCb")
    //println(mat.find())

    val conf = new SparkConf().setAppName("phone-recognition").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile(args(0), 1)
    val rdd2 = rdd1.filter(ss => ss.length != 0).map(log => log.split("\\|", -1)).filter(arr => arr(26) != "" && arr(1) != "").map(arr => (arr(1), arr(26)))

//    var i = 0L
//    val r2 = rdd1.map { line =>
//      i += 1L
//      (i, line)
//    }
    val dist = rdd2.distinct().groupByKey().map { iter =>
      val it = iter._2.iterator
      val s1 = it.next()
      var rest = ("", "")
      if (s1.contains("Mozilla") || s1.contains("Dalvik")) {
        rest = (iter._1, "br" + s1)
      } else {
        rest = (iter._1, s1)
      }

      breakable {
        while (it.hasNext) {
          if(rest._2.startsWith("br")){
            break
          }
          val elem = it.next()
          if (elem.contains("Mozilla") || elem.contains("Dalvik")) {
            rest = (iter._1, "br" + elem)
            break
          }
        }
      }
      rest
    }


    //rdd2.take(2).flatten.foreach(println)
    // rdd2.map(arr => (arr(1),arr.length)).take(20).foreach(println)
    // rdd2.take(30).foreach(println)

    val browser_os = dist.map { u =>
      //browser 入口
      if (u._2.startsWith("br")) {
        val userAgent = UserAgent.parseUserAgentString(u._2)
        val browerName = userAgent.getBrowser().getName()
        val os = userAgent.getOperatingSystem().getName()
        (u._1, (browerName, os))
      } else {
        //app 入口
        if (u._2.toLowerCase.contains("iphone") || u._2.toLowerCase.contains("ios") || u._2.toLowerCase.contains("mac os")) {
          (u._1, ("Unknown", "iOS"))
        } else if (u._2.toLowerCase.contains("windows phone") || u._2.toLowerCase.contains("windows") || u._2.toLowerCase.contains("nokia")) {
          (u._1, ("Unknown", "Windows"))
        }else if(u._2.toLowerCase.contains("xiaomi") || u._2.toLowerCase.contains("miui")){
          (u._1, ("Unknown", "MIUI"))
        } else {
          (u._1, ("Unknown", "Android"))
        }
      }
    }

    //phone type
    val phone = dist.map { u =>
      if (u._2.startsWith("br")) {
        val pattern_string = "\\(([^\\)]*)\\)".r
        val src = Source.fromString(u._2).mkString
        val mat = pattern_string.findFirstIn(src).getOrElse("Unknown")

        var spString = ""
        val sp = mat.split("\\;")
        if (sp.length >= 2) {
          if (mat.startsWith("(iP")) {
            val sss = sp(1).split(" ")
            if (sss.length >= 5) {
              spString = "iphone_" + sss(4)
            } else {
              spString = "iOS"
            }
          } else if (mat.startsWith("(iOS")) {
            spString = "iOS"
          } else if (mat.startsWith("(Linux")) {
            //android, windowphone,etc
            val lin = sp(sp.length - 1).split(" ")
            if (lin.length >= 2) {
              spString = lin(1)
            } else {
              spString = "android_phone"
            }
          } else if (mat.startsWith("(Windows")) {
            spString = "Windows"
          } else {
            spString = "Unknown"
          }
        } else {
          spString = "Unknown"
        }

        (u._1, spString)
      } else {
        var phoneBrand = "Unknown"
        breakable {
          for (a <- PhoneBrand.values) {
            for (l <- a.list) {
              if (u._2.toLowerCase.contains(l)) {
                phoneBrand = a.name
                break
              }
            }
          }
        }
        (u._1, phoneBrand)
      }
    }


    //    val phone = rdd3.map { x =>
    //      var spString = ""
    //      val sp = x._2.split("\\;")
    //      if (sp.length >= 2) {
    //        if (x._2.startsWith("(iP")) {
    //          val sss = sp(1).split(" ")
    //          if (sss.length >= 5) {
    //            spString = "iphone_" + sss(4)
    //          } else {
    //            spString = "iOS"
    //          }
    //        } else if (x._2.startsWith("(iOS")) {
    //          spString = "iOS"
    //        } else if (x._2.startsWith("(Linux")) {
    //          //android, windowphone,etc
    //          val lin = sp(sp.length - 1).split(" ")
    //          if (lin.length >= 2) {
    //            spString = lin(1)
    //          } else {
    //            spString = "android_phone"
    //          }
    //        } else if (x._2.startsWith("(Windows")) {
    //          spString = "Windows"
    //        } else {
    //          spString = "Unknown"
    //        }
    //      } else {
    //        spString = "Unknown"
    //      }
    //
    //      (x._1, spString)
    //    }

    val browser_os_phone = browser_os.leftOuterJoin(phone).map { tup =>
      (tup._1, (tup._2._1._1, tup._2._1._2, tup._2._2.getOrElse("unknownPhone")))
    }.sortByKey()

    //browser_os_phone.collect().foreach(println)
    browser_os_phone.saveAsTextFile(args(1))
    sc.stop()

  }
}
