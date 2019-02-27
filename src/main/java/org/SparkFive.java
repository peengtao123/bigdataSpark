package org;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.core.SparkBase;

public class SparkFive extends SparkBase {
    public static void main(String []args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> shops = spark.read().csv("D:/data/shop_info.csv");
        Dataset<Row> pays = spark.read().csv("D:\\data\\user_pay.csv");
        Dataset<Row> views = spark.read().csv("D:\\data\\user_view.csv");

        views.createOrReplaceTempView("views");


        Dataset<Row> sqlDF = spark.sql("select b._c0,b._c1,b._c3,count(*) from views a,shops b where b._c0=a._c1 group by b._c0,b._c1,b._c3 order by count(*) desc limit(50)");
        sqlDF.show(50);

        spark.stop();
    }
}
