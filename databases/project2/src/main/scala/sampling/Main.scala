package sampling

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import java.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import java.io._


object Main {
  def main(args: Array[String]) {

    
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val session = SparkSession.builder().getOrCreate();
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

   // val rdd = RandomRDDs.uniformRDD(sc, 100000)
   // val rdd2 = rdd.map(f => Row.fromSeq(Seq(f * 2, (f*10).toInt)))

//    val table = session.createDataFrame(rdd2, StructType(
 //     StructField("A1", DoubleType, false) ::
  //    StructField("A2", IntegerType, false) ::
   //   Nil
   // ))
    
    
    
    val inputFile= "../lineitem_small.tbl"
    val input = new File(getClass.getResource(inputFile).getFile).getPath

    
    
        val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load(input)
      
      val customerDF = session.read.load(new File(getClass.getResource("../customer.parquet").getFile).getPath)
      val ordersDF = session.read.load(new File(getClass.getResource("../order.parquet").getFile).getPath)
      val supplierDF = session.read.load(new File(getClass.getResource("../supplier.parquet").getFile).getPath)
      val nationDF = session.read.load(new File(getClass.getResource("../nation.parquet").getFile).getPath)
      val regionDF = session.read.load(new File(getClass.getResource("../region.parquet").getFile).getPath)
      val partDF = session.read.load(new File(getClass.getResource("../part.parquet").getFile).getPath)
      val partsuppDF = session.read.load(new File(getClass.getResource("../partsupp.parquet").getFile).getPath)
      

    var desc = new Description
    desc.lineitem = df
    desc.e = 0.1
    desc.ci = 0.8
    desc.customer = customerDF
    desc.orders = ordersDF
    desc.supplier = supplierDF
    desc.nation = nationDF
    desc.region = regionDF
    desc.part = partDF
    desc.partsupp = partsuppDF

    val tmp = Sampler.sample(desc.lineitem, 1000000, desc.e, desc.ci)
    desc.samples = tmp._1
    desc.sampleDescription = tmp._2

    //desc.samples = null
    //desc.sampleDescription = List()
    
    // check storage usage for samples

    // Execute first query
      val r = Executor.execute_Q1(desc, session, List("3"))
      for (x <- r) println(x)
    
  }     
}
