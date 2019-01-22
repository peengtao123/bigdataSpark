package org.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

public class WordCountJava {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("wordCoutn").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> text = sc.textFile("hdfs://hadoop:9000/data/dataset");
        text.foreach(x->System.out.println(x));

        sc.stop();
    }
}
