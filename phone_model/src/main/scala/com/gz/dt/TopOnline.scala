package com.gz.dt

import java.text.SimpleDateFormat

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by naonao on 2015/6/23
 */
class TopOnline {

}


object TopOnline {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: TopOnline <path-data> <save-path>")
      System.exit(1)
    }



    val conf = new SparkConf().setAppName("TOP-ONLINE").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile(args(0), 4)
    val rdd2 = rdd1.filter(ss => ss.length != 0).map(log => log.split("\\|", -1)).filter(arr => arr(17) != "" && arr(1) != "").map(arr => (arr(1), arr(17))).filter(_._2.length == 14)

    val df = new SimpleDateFormat("yyyymmddhhmmss")
    val start_day = df.parse("20150515000000").getTime / 1000
    val end_day = df.parse("20150515235959").getTime / 1000

    val rdd3 = rdd2.map { t =>
      val start = df.parse(t._2).getTime / 1000
      if (start >= start_day && start <= end_day) {
        val ind = ((start - start_day) / 3600).toInt
        val uid = t._1 + "|" + ind
        (uid, 1L)
      } else {
        (t._1 + "|00", 0L)
      }
    }.reduceByKey(_ + _)

    //rdd3.collect().foreach(println)
    rdd3.saveAsTextFile(args(1))


    sc.stop()

  }
}
