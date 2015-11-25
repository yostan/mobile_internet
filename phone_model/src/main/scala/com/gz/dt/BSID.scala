package com.gz.dt

import java.text.SimpleDateFormat

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable._

/**
 * Created by naonao on 2015/6/24
 */
class BSID {

}

object BSID {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: BSID <path-data>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("BS").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile(args(0))
    //val rdd2 = rdd1.filter(ss => ss.length != 0).map(log => log.split("\\|", -1)).filter(arr => arr(12) != "" && arr(17) != "" && arr(1) != "").map(arr => (arr(1), (arr(17),arr(17) ,arr(12))))
    val rdd2 = rdd1.map(x => x.split(" ")).map(a => (a(0), (a(1), a(2), a(3)))).sortBy(_._2._1, numPartitions = 1) //.sortByKey(numPartitions = 4)

    //rdd2.foreach(println)


    ////reduce by key
    //    var i = 0
    //    val rdd3 = rdd2.reduceByKey { (a, b) =>
    //      val list = new ListBuffer[(String, String, String)]()
    //      i += 1
    //      println(s"i: $i")
    //      println(s"b: $b")
    //      var tup3 = ("", "", "")
    //      if (a._3.equals(b._3)) {
    //        tup3 = (a._1, b._2, a._3)
    //      } else {
    //        list.append((a._1, a._2, a._3))
    //        //list.foreach(println)
    //        //println("---------")
    //        tup3 = (b._1, b._2, b._3)
    //      }
    //      if (b == "") {
    //        list.append(tup3)
    //      (list.toList.mkString("|"), "", "")
    //    } else {
    //      tup3
    //    }
    //    }


    // rdd3.foreach(println)


    //group by key
    val rd1 = rdd2.groupByKey().mapValues { t =>
      val list = new ListBuffer[(String, String, String)]()
      val it = t.iterator
      var t1 = it.next()
      var tup3 = ("", "", "")
      while (it.hasNext) {
        val t2 = it.next()
        if (t1._3.equals(t2._3)) {
          t1 = (t1._1, t2._2, t1._3)
        } else {
          list.append(t1)
          t1 = (t1._2, t2._2, t2._3)
        }
      }
      list.append(t1)
      list.toList
    }

    val d1 = 20150515080000L
    val d2 = 20150515180000L
    val df = new SimpleDateFormat("yyyyMMddHHmmss")
    val rd2 = rd1.mapValues { t2 =>
      val li = t2.map { a =>
        val ind = if (a._1.toLong >= d1 && a._2.toLong <= d2) 1 else 2
        val dur = df.parse(a._2).getTime/1000 - df.parse(a._1).getTime/1000
        (a._1, a._2, a._3, ind, dur)
      }
      li
    }

    val rd3 = rd2.map { x =>
      val r = x._2.map(t => ((x._1, t._3, t._4), t._5))
      r
    }.flatMap(u => u)

    val rd4 = rd3.reduceByKey(_ + _)
    val rd5 = rd4.map(a => ((a._1._1, a._1._3, a._2), a._1._2)).sortByKey(false)

    val rd6 = rd5.map(a => ((a._1._1, a._1._2),a._2)).reduceByKey((a, b) => a)

    rd6.foreach(println)

    sc.stop()

  }



}
