package simjoin

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import com.typesafe.config.{ConfigFactory, Config}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

import scala.io.Source
import java.io._

object Main {
  def main(args: Array[String]) {  
    
    val local = false
    if(local){
      
      
    val inputFile="../dblp_small.csv"    
    val numAnchors = 3
    val distanceThreshold = 3
    val attrIndex = 0    
        
    val input = new File(getClass.getResource(inputFile).getFile).getPath    
    val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[*]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)   
    
    val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .load(input)      
    
    val rdd = df.rdd
    val schema = df.schema.toList.map(x => x.name)    
    val dataset = new Dataset(rdd, schema)           
    
    
    val t1 = System.nanoTime    
    val sj = new SimilarityJoin(numAnchors, distanceThreshold)
    val res = sj.similarity_join(dataset, attrIndex)           
    val c = res.count()
    val t2 = System.nanoTime
            
    
    
    // cartesian
    
    val t1Cartesian = System.nanoTime
    val cartesian = rdd.map(x => (x(attrIndex), x)).cartesian(rdd.map(x => (x(attrIndex), x)))
                                   .filter(x => (x._1._2(attrIndex).toString() != x._2._2(attrIndex).toString() && edit_distance(x._1._2(attrIndex).toString(), x._2._2(attrIndex).toString()) <= distanceThreshold))
                                   
    val carSize = cartesian.count
    val t2Cartesian = System.nanoTime
   
    println("dataset size: " + dataset.getRDD().count())
    println("Affected rows (Clusterjoin): " + c)
    println("Elapsed (Clusterjoin): " + (t2-t1)/(Math.pow(10,9)))
    
    println("Affected rows (Cartesian): " + carSize)
    println("Elapsed: (Cartesian)" + (t2Cartesian-t1Cartesian)/(Math.pow(10,9)))
    }
    else{
      
          
      
    val inputFile= args(0)    
    val numAnchors = args(1).toInt
    val distanceThreshold = args(2).toInt
    val attrIndex = args(3).toInt 

        
    val sparkConf = new SparkConf().setAppName("CS422-Project2")//.setMaster("local[*]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)   
    
    val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .load(inputFile)      
    
    val rdd = df.rdd 
    val schema = df.schema.toList.map(x => x.name)    
    val dataset = new Dataset(rdd, schema)           
    
    
    val t1 = System.nanoTime    
    val sj = new SimilarityJoin(numAnchors, distanceThreshold)
    val res = sj.similarity_join(dataset, attrIndex)  
    val simjoinSize = res.count()
    val t2 = System.nanoTime
            
    
    
    // cartesian
   
    
    val t1Cartesian = System.nanoTime
    val cartesian = rdd.map(x => (x(attrIndex), x)).cartesian(rdd.map(x => (x(attrIndex), x)))
                                   .filter(x => (x._1._2(attrIndex).toString() != x._2._2(attrIndex).toString() && edit_distance(x._1._2(attrIndex).toString(), x._2._2(attrIndex).toString()) <= distanceThreshold))
                                   
    val carSize = cartesian.count
    val t2Cartesian = System.nanoTime
    
    
    
    
    println("dataset size: " + dataset.getRDD().count())
    println("Affected rows (Clusterjoin): " + simjoinSize)
    println("Elapsed (Clusterjoin): " + (t2-t1)/(Math.pow(10,9)))
    
    println("Affected rows (Cartesian): " + carSize)
    println("Elapsed: (Cartesian)" + (t2Cartesian-t1Cartesian)/(Math.pow(10,9)))
      
      
      
      
    }
      
    
    
    
    
  } 
  
  def edit_distance(s: String, t:String): Int = {
    val m = s.length()
    val n = t.length()
    
    var d = Array.fill[Int](m + 1, n + 1)(0)
    
    for (i <- 1 until m + 1){
      d(i)(0) = i
    }
    
    for (j <- 1 until n + 1){
      d(0)(j) = j
    }
    
    for (j <- 1 until n + 1){
      for (i <- 1 until m + 1){
        val substitutionCost = if (s(i - 1) == t(j - 1)) 0 else 1  
        d(i)(j) = List(d(i-1)(j) + 1, d(i)(j - 1) + 1, d(i-1)(j-1) + substitutionCost).min
      }
    }
    

    //println(d(m-1)(n-1))
    d(m)(n)
  }
  
}
