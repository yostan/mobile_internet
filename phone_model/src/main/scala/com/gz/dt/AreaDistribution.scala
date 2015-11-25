package com.gz.dt

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
 * Created by naonao on 2015/7/9.
 */
class AreaDistribution {

}

object AreaDistribution {
//  def main(args: Array[String]) {
//    if (args.length < 2) {
//      System.err.println("Usage: AreaDistribution <path-data> <tableName> <zkQuorum> <colFamily>")
//      System.exit(1)
//    }
//
//    val conf = new SparkConf().setAppName("area-dist")//.setMaster("local[4]")
//    val sc = new SparkContext(conf)
//
//    //val rrr = sc.newAPIHadoopFile(args(1), classOf[LzoTextInputFormat],classOf[NullWritable] ,classOf[Text], job.getConfiguration)
//    //read lzo file,(key: NullWritable, value: Text)
//    val r1 = sc.sequenceFile[Null, String](args(0), 20).map(_._2)
//    println("part: "+r1.partitions.length)
//    val r2 = r1.map(s => s.split("\\|", -1)).filter(a => a(9) != "" && a(10) != "").map(arr => (arr(9), arr(10)))
//
//    //漫游用户
//    val r3 = r2.filter(t => !t._1.equals(t._2))
//    //用户漫游地区 统计
//    val r4 = r3.map(x => (x._2, 1L)).reduceByKey(_+_)
//    r4.take(5).foreach(println)
//
//    //以用户归属地为group，统计漫游地区
//    val r5 = r3.map(x => ((x._2, x._1), 1L)).reduceByKey(_+_)//.map(t => (t._1._1, (t._1._2, t._2)))
//    val r6 = r5.map(t => (t._2, t._1)).sortByKey(false, 1).groupBy(_._2._1).map{iter =>
//        val it = iter._2.iterator
//        var i = 0
//        val ab = new ArrayBuffer[String]()
//        breakable(
//          while(it.hasNext){
//            val v = it.next()
//            val str = v._2._2 + ":" + v._1
//            ab.append(str)
//            i += 1
//            if(i >= 5){ //top 5
//              break()
//            }
//          }
//        )
//        (iter._1, ab.toArray.mkString("|"))
//      }
//
//    r6.take(2).foreach(println)
//    r6.saveAsTextFile(args(1))
//
//    sc.stop()
//  }


  def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("area-dist").setMaster("local[2]")
        val sc = new SparkContext(conf)
    println("aa"+0.toString+"aa")
  sc.stop()
  }

}
