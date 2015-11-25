package com.gz.dt

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by naonao on 2015/7/6.
 */
class ServiceOnline {

}

object ServiceType extends Enumeration with Serializable {
  protected case class Val(val name: String, val idString: String) extends super.Val

  implicit def value2Val(v: Value) = v.asInstanceOf[Val]

  val CDMA1X = Val("2G", "33")
  val EVDO = Val("3G", "59")
  val EVDO2 = Val("3G", "32876")
  val LTE = Val("4G", "6")
  val WIFI = Val("wifi", "64")

}

object ServiceOnline {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: ServiceOnline <data-path> <save-path>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("service-option").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val r1 = sc.textFile(args(0))

    val r2 = r1.filter(s => s.length != 0).map(log => log.split("\\|", -1)).filter(arr => arr(1) != "" && arr(1).startsWith("86") && arr(14) != "").map(a => (a(1), a(14)))

    //println("--------- "+r2.count())
    val r3 = r2.map(x => (x._2, 1L)).reduceByKey(_+_)

    val r4 = r3.map(t => pickupServiceName(t)).reduceByKey(_+_)


    r4.foreach(println)


    sc.stop()
  }

  def pickupServiceName(str: (String, Long)): (String, Long) = {
    for(e <- ServiceType.values){
      if(str._1.equals(e.idString)){
        return (e.name, str._2)
      }
    }
    ("Unknown", str._2)
  }

}
