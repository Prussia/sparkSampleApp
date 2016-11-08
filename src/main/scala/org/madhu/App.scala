package org.madhu

import org.apache.spark._
import org.apache.spark.SparkContext._

/**
 * @author ${user.name}
 */
object App {
  
   case class customer(id : Int, name : String, age : Int, other : String)
  
  def main(args : Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)

    val ss = org.apache.spark.sql.SparkSession.builder.getOrCreate(); 
    import ss.implicits._

    val df = sc.parallelize(1 to 10).map(x => customer(x,"name"+x, (x/10) * 10,"junk" +x)).toDF()

    df.write.parquet("customer.parquet")

    val newdf = ss.read.parquet("customer.parquet")

    newdf.show();


  }

}
