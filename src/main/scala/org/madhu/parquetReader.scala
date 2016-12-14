package org.madhu

import org.apache.spark._
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * @author ${user.name}
 */
object parquetReader {
  
  def main(args : Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName("parquetReader")
    val sc = new SparkContext(conf)
    val ss = org.apache.spark.sql.SparkSession.builder.getOrCreate();
    import ss.implicits._

    val newdf = ss.read.parquet("/tpcds-100GBnvme/catalog_sales/");
    println("Total records " + newdf.collect().length);
    newdf.show();
  }

}
