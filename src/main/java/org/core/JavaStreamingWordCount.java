package org.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class JavaStreamingWordCount {
    public static void main(String[] args) throws Exception {
        // Create the context with a 1 second batch size
        SparkConf sparkConf = new SparkConf().setAppName("StreamingWordCount");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

        JavaDStream<String> lines = ssc.textFileStream("data");

        lines.print();
        lines.foreachRDD(x-> System.out.println(x));
        ssc.start();
        ssc.awaitTermination();
    }

}
