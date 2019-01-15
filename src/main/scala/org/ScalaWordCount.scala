package org

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object ScalaWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Wordcount")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("data/textfile")
    val rdd2 = rdd.flatMap(x => x.split(" ")).map(x => (x, 1)).reduceByKey(_+_)

    rdd2.take(10).foreach(x=> println(x))
  }
}
