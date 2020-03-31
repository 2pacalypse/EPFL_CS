package sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.SizeEstimator
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import scala.math.pow
import org.apache.commons.math3.distribution.NormalDistribution

object Sampler {

  //returns the size of the row in bytes
  def getRowSize(row: org.apache.spark.sql.Row): Int = {
    (5 * IntegerType.defaultSize +
      3 * DoubleType.defaultSize +
      row(8).toString().length() +
      row(9).toString().length() +
      3 * TimestampType.defaultSize +
      row(13).toString().length() +
      row(14).toString().length() +
      row(15).toString().length())
  }

  //returns a list of sampled rows
  def scasrs(
    zipped: (((List[Any], List[org.apache.spark.sql.Row]), Int), Int)): List[org.apache.spark.sql.Row] = {
    val r = zipped._1._1
    val nh = zipped._1._2
    val Nh = zipped._2

    val p = nh / Nh.toDouble
    val delta = 5 * 1e-5

    val gamma1 = -scala.math.log(delta) / Nh
    val q1 = scala.math.min(1, p + gamma1 + scala.math.sqrt(gamma1 * gamma1 + 2 * gamma1 * p))

    val gamma2 = -2 * scala.math.log(delta) / (3 * Nh)
    val q2 = scala.math.max(0, p + gamma2 - scala.math.sqrt(gamma2 * gamma2 + 3 * gamma2 * p))

    val rand = new scala.util.Random

    val rowWithKeys = r._2.map(row => (row, rand.nextDouble()))

    val accepted = rowWithKeys.filter(line => line._2 < q2).map(item => item._1)
    val waiting = rowWithKeys.filter(line => (line._2 < q1 && line._2 >= q2))
    val waitingCount = scala.math.ceil(scala.math.max(0, nh - accepted.length)).toInt
    val processed = (waiting.sortBy(_._2).take(waitingCount)).map(item => item._1)

    val res = accepted ::: processed

    return res
  }

  //returns nhs, relative error, variance
  def find_nhs(Nhs: RDD[Int], Shs: RDD[Double], t: Double, e: Double, cap: Int, zao2: Double): (RDD[Int], Double, Double) = {
    val nhs = Nhs.map(N => List(N, cap).min)
    val v = nhs.zip(Nhs).zip(Shs).map(
      z => (1 - z._1._1.toDouble / z._1._2) * pow(z._1._2, 2) * z._2 * 1 / z._1._1.toDouble).sum()
    val ae = pow(v, 0.5) * zao2
    val re = ae / t
    //println(re, e)
    if (re > e) return find_nhs(Nhs, Shs, t, e, cap + 1, zao2)
    else return (nhs, re, v)
  }

  //returns Nhs, yhu, Shs, t in the sampling book
  def find_stats(
    reduced: RDD[(List[Any], List[org.apache.spark.sql.Row])]): (RDD[Int], RDD[Double], RDD[Double], Double) = {
    val Nhs = reduced.map(t => t._2.length)
    val yhu = reduced.map(t => t._2.map(r => r.getDouble(5)).sum / t._2.length)
    val Shs = reduced.zip(yhu).zip(Nhs).map(z =>
      if (z._1._1._2.length == 1) 0
      else z._1._1._2.map(r => (pow(r.getDouble(5) - z._1._2, 2))).sum / (z._2 - 1))

    val t = reduced.map(t => t._2.map(r => r.getDouble(5)).sum).sum()

    return (Nhs, yhu, Shs, t)
  }

  //returns sample, variance, number of rows in the sample
  def compute_sample(
    reduced: RDD[(List[Any], List[org.apache.spark.sql.Row])],
    ci:      Double,
    e:       Double): (RDD[org.apache.spark.sql.Row], Double, Long) = {

    val sn = new NormalDistribution(0, 1);
    val zao2 = (sn.inverseCumulativeProbability(ci + (1 - ci) / 2))

    val (nhs_upper, yhu, shs, t) = find_stats(reduced)
    val (nhs, re, v) = find_nhs(nhs_upper, shs, t, e, 1, zao2)

    val sampled = reduced.zip(nhs).zip(nhs_upper).flatMap(z => scasrs(z))
    val n = sampled.count()

    return (sampled, v, n)

  }

  def sampleForQCS(
    qcs:         List[Int],
    lineitemRDD: RDD[org.apache.spark.sql.Row],
    ci:          Double,
    e:           Double): (RDD[org.apache.spark.sql.Row], Long, Double) = {

    val mapped = lineitemRDD.map(row => (qcs.map(i => row(i)).toList, List(row)))
    val reduced = mapped.reduceByKey((x, y) => x ::: y)

    val (sample, v, n) = compute_sample(reduced, ci, e)
    //println(n)

    return (sample, n, sample.map(row => getRowSize(row)).sum())
  }

  def considerBudget(
    sortedSamples: List[((RDD[org.apache.spark.sql.Row], Long, Double), Int)],
    budget:        Long): List[((RDD[org.apache.spark.sql.Row], Long, Double), Int)] = {
    val head = sortedSamples.head
    val tail = sortedSamples.tail
    if (head._1._3.toLong <= budget) return List(head) ::: considerBudget(tail, budget - head._1._3.toLong)
    else return List()
  }

  def sample(lineitem: DataFrame, storageBudgetBytes: Long, e: Double, ci: Double): (List[RDD[_]], _) = {
    // TODO: implement

    //L EXTENDED PRICE = 5

    val lineitemRDD = lineitem.rdd
    val lineitemschema = lineitem.columns

    //val q1QCS = List("l_shipdate", "l_returnflag", "l_linestatus")
    //val q1QCSIndex = q1QCS.map(x => lineitemschema.indexOf(x))

    val q1QCS = List(10, 8, 9)
    val q3QCS = List(0, 10) // Supserset q7
    val q5QCS = List(0, 2) //superset q7
    val q6QCS = List(10, 6, 4)
    val q7QCS = List(2, 0, 10)
    val q9QCS = List(2, 1, 0)
    val q10QCS = List(0, 8)
    val q12QCS = List(0, 14, 11, 10, 12)
    val q17QCS = List(4, 1) //supserset q19
    val q18QCS = List(0, 4)
    val q19QCS = List(1, 4, 14, 13)
    val q20QCS = List(1, 2, 10, 4)

    val QCSList = List(q1QCS, q3QCS, q5QCS, q6QCS, q7QCS, q9QCS, q10QCS, q12QCS, q17QCS, q18QCS, q19QCS, q20QCS)
    val qNumbers = List(1, 3, 5, 6, 7, 9, 10, 12, 17, 18, 19, 20)

    val allSamples = QCSList.zip(qNumbers).map(z => (sampleForQCS(z._1, lineitemRDD, ci, e), z._2))
    val samplesSorted = allSamples.sortBy(_._1._3)
    val takenSamples = considerBudget(samplesSorted, storageBudgetBytes)

    val ret = takenSamples.map(t => t._1._1)
    val ret2 = takenSamples.map(t => t._2)
    //println(ret2)

    return (ret, ret2)

    //for (a<- takenSamples ) println(a._1._1,a._1._2, a._1._3, a._2)

    //val (sample, n) = sampleForQCS(q1QCS, lineitemRDD, 0.5, 0.5)

    //val AQCS = List("l_partkey", "l_suppkey", "l_shipdate", "l_quantity")
    //val AQCSIndex = AQCS.map(x => lineitemschema.indexOf(x))
    //println(AQCSIndex)
    //trySample(reduced, 2)

    //for (a<- reduced.zip(nhs).zip(Nhs).zip(yhu).zip(Shs)) println(a)

    //lineitem.dtypes.foreach(e=> println(e))

    //println(cols)
    //lineitem.groupBy(cols)

  }
}
