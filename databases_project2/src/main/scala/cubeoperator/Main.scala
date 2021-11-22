package cubeoperator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import java.io._

object Main {
  def main(args: Array[String]) {
    val local = true
    
    
    if(local){
      
    val reducers = 5
    val inputFile= "../lineorder_small.tbl"
    val input = new File(getClass.getResource(inputFile).getFile).getPath

    val aggAttr = "lo_supplycost"
    val agg = "MAX"
    val naive = false
    
    val groupingList = List("lo_shippriority", "lo_shipmode")

    //val inputFile = args(0)
    
    val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[16]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)

    //sqlContext.setConf("spark.sql.shuffle.partitions", reducers.toString())
    
    
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load(input)

    val rdd = df.rdd
    val schema = df.schema.toList.map(x => x.name)
    val dataset = new Dataset(rdd, schema)
    val cb = new CubeOperator(reducers)

    

    if(naive){
      val t3 = System.nanoTime
      val res2 = cb.cube_naive(dataset, groupingList, aggAttr, agg)
      val r2 = res2.count()
      val t4 = System.nanoTime
      for(row <- res2) println(row)
      println("Affected Rows: " + r2  + ", Time (s): " +  ((t4-t3)/(Math.pow(10,9))) )
    }else{
      val t1 = System.nanoTime
      val res = cb.cube(dataset, groupingList, aggAttr, agg)
      val r = res.count()
      val t2 = System.nanoTime    
      for(row <- res) println(row)
     println("Affected Rows: " + r  + ", Time (s): " +  ((t2-t1)/(Math.pow(10,9))) )
    }
    
    

         val q1 = df.cube(groupingList(0), groupingList.slice(1, groupingList.length):_*)
         .agg(max(aggAttr) as "sum supplycost")
        q1.show(Int.MaxValue - 1)
   

    
      
    //val t3 = System.nanoTime
    //Perform the same query using SparkSQL

        
    //val t4 = System.nanoTime
    
      
      
    }else{
   
      
    val inputFile= args(0)
    val reducers = args(1).toInt
    val aggAttr = args(2)
    val agg = args(3)
    val naive = if(args(4).equals("true")) true else false

    val groupingList = args.slice(5, args.length).toList
    

    
    val sparkConf = new SparkConf().setAppName("CS422-Project2")//.setMaster("local[16]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)

    //sqlContext.setConf("spark.sql.shuffle.partitions", reducers.toString())
    
    
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load(inputFile)

    val rdd = df.rdd
    val schema = df.schema.toList.map(x => x.name)
    val dataset = new Dataset(rdd, schema)
    val cb = new CubeOperator(reducers)
    
    
    if(naive){
      val t3 = System.nanoTime
      val res2 = cb.cube_naive(dataset, groupingList, aggAttr, agg)
      val r2 = res2.count()
      val t4 = System.nanoTime
      println("Affected Rows: " + r2  + ", Time (s): " +  ((t4-t3)/(Math.pow(10,9))) )
    }else{
      val t1 = System.nanoTime
      val res = cb.cube(dataset, groupingList, aggAttr, agg)
      val r = res.count()
      val t2 = System.nanoTime    
     println("Affected Rows: " + r  + ", Time (s): " +  ((t2-t1)/(Math.pow(10,9))) )
    }
    



    
        /*
    //val t3 = System.nanoTime
    //Perform the same query using SparkSQL
        val q1 = df.cube(groupingList(0))
         .agg(avg(aggAttr) as "sum supplycost")
        q1.show(Int.MaxValue - 1)
    //val t4 = System.nanoTime
    */
      
      
      
    }
    
    

   


  }
}