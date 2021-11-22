package simjoin

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import scala.util.Random

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class SimilarityJoin(numAnchors: Int, distThreshold:Int) extends java.io.Serializable {
  val logger = LoggerFactory.getLogger("SimilarityJoin")
  var rdd: RDD[String] = null
  
  /*
   * this method gets as input a dataset and the index of an attribute
   * of the dataset, and returns the result of the similarity self join on
   * that attribute.
   * */  
  
  
  def similarity_join(dataset: Dataset, attrIndex: Int) : RDD[(String, String)] = {
    
    
    rdd = dataset.getRDD().map(row=> row(attrIndex).toString())
    val pa = numAnchors /  rdd.count().toDouble
    
    
    val rddWithIds = rdd.zipWithUniqueId()
    
    val anchors = rddWithIds.sample(false, pa, 42).map(anchor => List(anchor))
    
    
    //val pa = numAnchors /  rddWithIds.count().toDouble
    
    
    //val anchors = rddWithIds.filter(row => rand.nextDouble() <= pa).map(anchor => List(anchor))
    //println("anchors", anchors.count())

    def assignPoint(row: ((String, Long), (List[(String,Long)])) ) = {
      
      val closest = row._2.map( anch => (edit_distance(row._1._1, anch._1), (anch) )).min
      
      //val closest = row._2.map(anch => ( edit_distance(row._1, anch), anch )  ).min 
      val homePlusOuter = row._2.map(anch => (edit_distance(row._1._1, anch._1), anch)).filter(c => c._1 <= closest._1 + 2*distThreshold)
      
      val ret = homePlusOuter.map(e => (row._1, e._2))

        ret
        
    }
    
    
    val partitions = 
      rddWithIds.cartesian(anchors)
    //  .map(k => (k._2, k._1))
      .reduceByKey((x,y) => x ::: y, numAnchors)
      .flatMap(row => assignPoint(row))
       .map(k => (k._2, List(k._1)))
       .reduceByKey((x,y) => x ::: y, numAnchors)

      //for (row <- partitions) println(row)
       
       
        
    val combs =   partitions
    .flatMap(row => row._2.combinations(2).toList)
    .flatMap(f => List((f(0), f(1)), (f(1),   f(0))))
    
    val res = combs
    .distinct()
    .map(row => (row._1._1, row._2._1))
    .filter(f => !f._1.equals(f._2) && edit_distance(f._1, f._2) <= distThreshold)
           
           
           
  //x.collect()
       //x.collect().ma
    //for (row <- combs) println(row)
    //println(res.count())
   
    
    res
  }
  
   

  def edit_distance(sList: List[String]):Int = {
    val s = sList(0)
    val t = sList(1)
    edit_distance(s,t)
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

