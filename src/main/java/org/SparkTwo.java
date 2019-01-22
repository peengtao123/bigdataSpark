package org;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkTwo {
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



        //奶茶前五
        spark.sql("select * from shops a,pays b where a._c0=b._c1 and a._c1 in('北京','上海','广州','深圳') and a._c9='奶茶' order by 0.7*(a._c4/5)+0.3*a._c3").show();
        //中式快餐前五
        spark.sql("select * from shops a,pays b where a._c0=b._c1 and a._c1 in('北京','上海','广州','深圳') and a._c9='中式快餐' order by 0.7*(a._c4/5)+0.3*a._c3").show();

        spark.stop();
    }
}
