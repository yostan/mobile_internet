package com.gz.dt

import com.hadoop.mapreduce.LzoTextInputFormat
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.{SparkContext, SparkConf}

import scala.io.Source

/**
 * Created by naonao on 2015/7/2.
 */
class DDR {

}

object DDR {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: DDR <path-data> <tableName> <zkQuorum> <colFamily>")
      System.exit(1)
    }

   // val job = new Job()
   // job.setOutputFormatClass(classOf[TextOutputFormat[NullWritable, Text]])
    //job.setInputFormatClass(classOf[LzoTextInputFormat[NullWritable, Text]])

    val conf = new SparkConf().setAppName("url")//.setMaster("local[4]")
    val sc = new SparkContext(conf)


    val r1 = sc.textFile(args(0))


    val r2 = r1.filter(s => s.length != 0).map(log => log.split("\\|", -1)).filter(arr => arr(29) != "").map(a => a(29))

    val r3 = r2.map(x => (x, 1L)).reduceByKey(_ + _).sortBy(tup => tup._2, false)

    //r3.take(50).foreach(println)

   // val r4 = r3.take(10).foreach(println)
   // val rd1 = sc.parallelize(r4).map(x => (x._1, x._2.toString))

    //hbase ops
    //val hbaseOps = new HBaseUtils(args(1), args(2), args(3))
    //hbaseOps.createTableAndWrite(rd1)
    println("done-------------")




//    val ss = "http://alimp3.changba.com/vod1/music/afa7171326759734bed02c754fc3e625.mp3"
//    val pat = "http://([^\\/]*)".r
//    val src = Source.fromString(ss).mkString
//    val mat = pat.findFirstIn(src).getOrElse("Unknown-web")
//    println(mat)


    sc.stop()
  }
}

