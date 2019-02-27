package org;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkFour {
    public static void main(String []args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
//                .master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> views = spark.read().csv("D:\\data\\user_view.csv");
        views.createOrReplaceTempView("views");

        ///每周浏览量
        spark.sql("select _c1,weekofyear(_c2),count(*) from views where _c1='1692' group by _c1,weekofyear(_c2) order by weekofyear(_c2)").show();
        ///每天浏览量
        spark.sql("select _c1,substring(_c2,0,10),count(*) from views where _c1='1692' group by _c1,substring(_c2,0,10) order by substring(_c2,0,10)").show();
        ///每月浏览量
        spark.sql("select _c1,month(_c2),count(*) from views where _c1='1692' group by _c1,month(_c2) order by month(_c2) asc").show();

        spark.stop();
    }
}
