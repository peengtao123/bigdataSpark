package org;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.DecimalFormat;

public class SparkThree {
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

        views.cache();
        shops.createOrReplaceTempView("shops");
        pays.createOrReplaceTempView("pays");
        views.createOrReplaceTempView("views");

//        spark.sql("select * from shops ").show();
//        Dataset<Row> sqlDF = spark.sql("select b._c1,count(*) from shops a,pays b where a._c0=b._c1 group by b._c1");
//        System.out.println(sqlDF.count());
        ///平均日交易额前三商家 ,count(*)*b._c3/datediff(max(a._c2),min(a._c2))
//        Dataset<Row> sqlDF = spark.sql("SELECT a._c1,count(*)*b._c3/datediff(max(a._c2),min(a._c2)) as avgPay FROM pays a,shops b where a._c1=b._c0 group by a._c1,b._c3 order by avgPay desc ");
//        sqlDF.show();
        String sql = "select distinct a._c0 as user_id from views a where a._c1=1629 and substring(a._c2,0,10)='2016-10-";
        ///前三家公司1629，517,58   5556715
        for(int i=1;i<=31;i++){
            Dataset<Row> sqlDF = spark.sql(sql+new DecimalFormat("00").format(i)+"'");
            sqlDF.cache();
            for(int j=i;j<=31;j++){
                String date=(new DecimalFormat("00").format(j)+"/"+new DecimalFormat("00").format(i));
                System.out.println(spark.sql(sql+new DecimalFormat("00").format(j)+"'")
                        .intersect(sqlDF)
                        .count()+"------"+date);
            }
        }

        spark.stop();
    }
}
