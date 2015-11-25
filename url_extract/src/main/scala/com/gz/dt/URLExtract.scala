package com.gz.dt

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by naonao on 2015/7/15.
 */
class URLExtract {

}

object URLExtract {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: URLExtract <path-data>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("url-extract").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val r1 = sc.textFile(args(0), 4)
    val r2 = r1.filter(x => x.length != 0).map(ss => ss.split("\\|", -1)).filter(a => a(1) != "" && a(27) != "").map(arr => (arr(1), arr(27)))
//    val r3 = r2.groupByKey(20)
//    val r4 = r3.map{x =>
//      val it = x._2.iterator
//      val ab = new ArrayBuffer[String]()
//      while (it.hasNext){
//        ab.append(it.next())
//      }
//      (x._1, ab.toArray)
//    }

    val rd1 = r2.map(x => x._1 + " " + x._2)
    rd1.saveAsTextFile(args(1))


    println("done................")
    sc.stop()

  }
}
