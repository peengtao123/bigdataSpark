package org;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkQuery {
    public static void main(String []args){
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .getOrCreate();

        Dataset<Row> shops = spark.read().csv("D:/data/shop_info.txt");
//        shops.foreach(x->System.out.println(x));
    }
}
