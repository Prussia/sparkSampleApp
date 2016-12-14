package org.madhu

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.network.protocol.Encoders.Strings
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * @author ${user.name}
 */
object parquetRW {
  
  case class customer(intType : Int, longType : Long, doubleType : Double, filler : String)
  
  def main(args : Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
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
      val fname = args(3)+"-customer.parquet"
      println("fname="+fname);
      val length = args(2).toInt * 1024;
      df = sc.parallelize(1 to length).map(x => customer(x,x*10, x*100,x + filler)).toDF()
      df.write.option("compression",args(1)).mode(SaveMode.Overwrite).parquet(fname);
      df.show()
    }
    else {
      val fname = args(1)+"-customer.parquet"
      println("fname="+fname);
      val newdf = ss.read.parquet(fname);
      println("Total records " + newdf.collect().length);
      newdf.show();
    }

  }

}
