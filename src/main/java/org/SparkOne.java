package org;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.text.DecimalFormat;

public class SparkOne {
    public static void main(String []args){
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> shops = spark.read().csv("D:/data/shop_info.txt");
        Dataset<Row> pays = spark.read().csv("D:\\data\\user_pay.txt");
        Dataset<Row> views = spark.read().csv("D:\\data\\user_view.txt");

        shops.createOrReplaceTempView("shops");
        pays.createOrReplaceTempView("pays");
        views.createOrReplaceTempView("views");

        ///平均日交易额前三商家 ,count(*)*b._c3/datediff(max(a._c2),min(a._c2))
        Dataset<Row> sqlDF = spark.sql("SELECT a._c1,count(*)*b._c3/datediff(max(a._c2),min(a._c2)) as avgPay FROM pays a,shops b where a._c1=b._c0 group by a._c1,b._c3 order by avgPay desc ");
        sqlDF.show();
    }
}
