import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object wilcoxon
{
  def U (X: RDD[Double], C: RDD[Int]): Double =
  {
    val XC = X.zip (C)
    U (XC)
  }

  def U (XC: RDD[(Double, Int)]): Double =
  {
    val XC_sorted = XC.sortBy (p => p._1)
    val foo = XC_sorted.zipWithIndex ()
    val bar = foo.map (pq => (pq._1._1, (pq._1._2, pq._2)))
    val baz = bar.aggregateByKey ((0L, 0L)) ((a, q) => (a._1 + 1, a._2 + q._2), (a, b) => (a._1 + b._1, a._2 + b._2))
    val quux = baz.map (pq => (pq._1, pq._2._2 / pq._2._1.toDouble))
    val mumble = XC_sorted.join (quux)
    val blurf = mumble.filter (pq => pq._2._1 == 1)
    val rank_sum = blurf.aggregate (0.0) ((a, pq) => a + pq._2._2, (a, b) => a + b)
    val n = mumble.count ()
    val n1 = blurf.count ()
    val n0 = n - n1

    ((rank_sum + n1) - n1*(n1 + 1.0)/2.0)/(n1 * n0.toDouble)
  }

  def allU (CXXX : RDD[LabeledPoint]): Seq[Double] =
  {
    val C = CXXX.map (p => p.label.toInt)
    val m = CXXX.first ().features.size
    0 to m - 1 map (i => U (CXXX.map (p => p.features(i)), C))
  }
}
