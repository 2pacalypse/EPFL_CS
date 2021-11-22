package cubeoperator

import org.apache.spark.rdd.RDD

class CubeOperator(reducers: Int) {

  /*
 * This method gets as input one dataset, the grouping attributes of the cube (CUBE BY clause)
 * the attribute on which the aggregation is performed
 * and the aggregate function (it has to be one of "COUNT", "SUM", "MIN", "MAX", "AVG")
 * and returns an RDD with the result in the form of <key = string, value = double> pairs.
 * The key is used to uniquely identify a group that corresponds to a certain combination of attribute values.
 * You are free to do that following your own naming convention.
 * The value is the aggregation result.
 * You are not allowed to change the definition of this function or the names of the aggregate functions.
 * */
  def cube(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()
    

    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)
    
    val aggVal = (rowVal:Any) => agg match {
      case  "COUNT" => 1.doubleValue()
      case  "MIN"   => if(rowVal.isInstanceOf[Int]) rowVal.asInstanceOf[Int].toDouble else rowVal.asInstanceOf[Double]
      case  "MAX"   => if(rowVal.isInstanceOf[Int]) rowVal.asInstanceOf[Int].toDouble else rowVal.asInstanceOf[Double]
      case  "SUM"   => if(rowVal.isInstanceOf[Int]) rowVal.asInstanceOf[Int].toDouble else rowVal.asInstanceOf[Double]
      case  "AVG"   => (if(rowVal.isInstanceOf[Int]) rowVal.asInstanceOf[Int].toDouble else rowVal.asInstanceOf[Double], 1.doubleValue())
    }
    
   def aggFunction() = agg match{
     case "COUNT" =>(x:Any, y:Any) => x.asInstanceOf[Double]+y.asInstanceOf[Double]
     case "MIN" => (x:Any, y:Any) => if (x.asInstanceOf[Double] < y.asInstanceOf[Double]) (x.asInstanceOf[Double]) else (y.asInstanceOf[Double])
     case "MAX" => (x:Any, y:Any) => if (x.asInstanceOf[Double] > y.asInstanceOf[Double]) x.asInstanceOf[Double] else y.asInstanceOf[Double]
     case "SUM" => (x:Any, y:Any) => x.asInstanceOf[Double]+y.asInstanceOf[Double]
     case "AVG" => (x:Any, y:Any) => ((x.asInstanceOf[(Double,Double)])._1 + (y.asInstanceOf[(Double,Double)])._1, (x.asInstanceOf[(Double,Double)])._2 + (y.asInstanceOf[(Double,Double)])._2) 
   }
    
    
    //phase 1
    val p1mapped = rdd.map(
        row =>
            (
            index.map(i => (i,row(i))).toList,
            aggVal(row(indexAgg))
            )    
        )
        
    val p1reduced = p1mapped.reduceByKey(aggFunction, reducers)
    
    
    //phase 2
    val partialResults = p1reduced.flatMap(
      row => (0 to groupingAttributes.length).toList.flatMap(
          n => row._1.combinations(n).map(
              partial => (partial, row._2)
         )
       )    
     )     
    
    val reducedPartial = partialResults.reduceByKey(aggFunction, reducers)
    
    val reducedFormatted = reducedPartial.map(
        row => (
            index.map(
                i => row._1.find(p => p._1 == i)
                  match{case Some((x,y)) => y case _ => "*"}
            ).mkString(","),
            agg match {
              case "AVG"=> row._2.asInstanceOf[(Double,Double)]._1 /row._2.asInstanceOf[(Double,Double)]._2
              case _ => row._2.asInstanceOf[Double]}
          )
     ) 
        
    reducedFormatted
  }

  def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {
    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()
    
    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)
    
    //TODO naive algorithm for cube computation
    
    val aggVal = (rowVal:Any) => agg match {
      case  "COUNT" => 1.doubleValue()
      case  "MIN"   => if(rowVal.isInstanceOf[Int]) rowVal.asInstanceOf[Int].toDouble else rowVal.asInstanceOf[Double]
      case  "MAX"   => if(rowVal.isInstanceOf[Int]) rowVal.asInstanceOf[Int].toDouble else rowVal.asInstanceOf[Double]
      case  "SUM"   => if(rowVal.isInstanceOf[Int]) rowVal.asInstanceOf[Int].toDouble else rowVal.asInstanceOf[Double]
      case  "AVG"   => (if(rowVal.isInstanceOf[Int]) rowVal.asInstanceOf[Int].toDouble else rowVal.asInstanceOf[Double], 1.doubleValue())
    }
    
   def aggFunction() = agg match{
     case "COUNT" =>(x:Any, y:Any) => x.asInstanceOf[Double]+y.asInstanceOf[Double]
     case "MIN" => (x:Any, y:Any) => if (x.asInstanceOf[Double] < y.asInstanceOf[Double]) (x.asInstanceOf[Double]) else (y.asInstanceOf[Double])
     case "MAX" => (x:Any, y:Any) => if (x.asInstanceOf[Double] > y.asInstanceOf[Double]) x.asInstanceOf[Double] else y.asInstanceOf[Double]
     case "SUM" => (x:Any, y:Any) => x.asInstanceOf[Double]+y.asInstanceOf[Double]
     case "AVG" => (x:Any, y:Any) => ((x.asInstanceOf[(Double,Double)])._1 + (y.asInstanceOf[(Double,Double)])._1, (x.asInstanceOf[(Double,Double)])._2 + (y.asInstanceOf[(Double,Double)])._2) 
   }
    
    
    val mapped = rdd.flatMap(
        row => (0 to groupingAttributes.length).toList.flatMap(
            n => index.map(
                i=> (i,row(i))).toList.combinations(n).map(
                    partial=> (partial, aggVal(row(indexAgg))))  
        )
          )
          
     val reduced = mapped.reduceByKey(aggFunction, reducers)
     
     val reducedFormatted = reduced.map(
        row => (
            index.map(
                i => row._1.find(p => p._1 == i)
                  match{case Some((x,y)) => y case _ => "*"}
            ).mkString(","),
            agg match {
              case "AVG"=> row._2.asInstanceOf[(Double,Double)]._1 /row._2.asInstanceOf[(Double,Double)]._2
              case _ => row._2.asInstanceOf[Double]}
          )
     ) 


     
    reducedFormatted
  }

}
