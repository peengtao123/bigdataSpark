package org;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkFive {
    public static void main(String []args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> shops = spark.read().csv("D:/data/shop_info.txt");
        Dataset<Row> pays = spark.read().csv("D:\\data\\user_pay.txt");
        Dataset<Row> views = spark.read().csv("D:\\data\\user_view.txt");

        views.cache();
        shops.createOrReplaceTempView("shops");
        views.createOrReplaceTempView("views");


        spark.sql("select b._c0,b._c1,b._c3,count(*) from views a,shops b where b._c0=a._c1 group by b._c0,b._c1,b._c3 order by count(*) desc limit(50)").show();

        spark.stop();
    }
}
