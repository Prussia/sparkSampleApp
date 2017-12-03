package org.madhu

import org.apache.spark._
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * @author ${user.name}
 */
object parquetRW {
  
  case class customer(intType : Int, longType : Long, doubleType : Double, filler : String)
  
  def main(args : Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)

    sc.getConf.set("spark.hadoop.mapred.output.compress", "true")
    sc.getConf.set("spark.hadoop.mapred.output.compression.codec", "true")
    sc.getConf.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
    sc.getConf.set("spark.hadoop.mapred.output.compression.type", "BLOCK")



    println("MADHU"+ args.length)
    val ss = org.apache.spark.sql.SparkSession.builder.getOrCreate();
    import ss.implicits._

    if(args.length == 0) {
      println("app write snappy/zlib/none sizeInMB dirname-prefix (file:///home/madhu/snappy-) ")
      println("app read")
      return
    }

    val filler = "Filler".padTo(999,"#").mkString

    var df : DataFrame = null.asInstanceOf[DataFrame]

    if(args(0).equalsIgnoreCase("write")) {
      val fname = args(3)
      println("fname="+fname);
      val length = args(2).toInt * 1024;
      df = sc.parallelize(1 to length).map(x => customer(x,x*10, x*100,x + filler)).toDF()
      df.write.option("compression",args(1)).mode(SaveMode.Overwrite).parquet(fname);
      df.show()
    }
    else {
      val fname = args(1)
      println("fname="+fname);
      val newdf = ss.read.parquet(fname);
      println("Total records " + newdf.collect().length);

      newdf.coalesce(1).write
        .format("com.databricks.spark.csv")
        .mode("overwrite")
        .option("header","true")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .save("/Users/prussia/workspaces/data/export_order.csv");

      newdf.show();
    }

  }

}
