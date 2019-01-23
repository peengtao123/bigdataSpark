package org.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class WordCountJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("wordcount").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> text = sc.textFile("data");
        text.flatMap(x-> Arrays.asList(x.split(" ")).iterator()).foreach(x -> System.out.println(x));

        sc.stop();
    }
}
