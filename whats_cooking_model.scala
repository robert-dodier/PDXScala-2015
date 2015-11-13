import org.apache.spark.mllib.util.MLUtils
val x = MLUtils.loadLibSVMFile (sc, "kaggle/whats_cooking_train.data-libsvm")

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
val lr = new LogisticRegressionWithLBFGS()
val model = lr.run (x)
model.getThreshold
val foo = x.map (p => (model.predict (p.features), p.label))
val n11 = foo.filter (ab => ab._1 == 1.0 && ab._2 == 1.0).count
val n10 = foo.filter (ab => ab._1 == 1.0 && ab._2 == 0.0).count
val n01 = foo.filter (ab => ab._1 == 0.0 && ab._2 == 0.1).count
val n00 = foo.filter (ab => ab._1 == 0.0 && ab._2 == 0.0).count
model.clearThreshold
val bar = x.map (p => (p.label, model.predict (p.features)))
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
val metrics = new BinaryClassificationMetrics (foo, 20)
metrics.areaUnderROC

val roc = metrics.roc.collect
import com.quantifind.charts.Highcharts._
line (roc)

val m = 10
val n = x.count
val ix = x.zipWithIndex
val p_hat = (0 until m).map (i => { val model1 = lr.run (ix.filter { case (x, j) => j % m != i }.map { case (x, j) => x });
                     model1.clearThreshold;
                     ix.filter { case (x, j) => j % m == i }.map { case (x, j) => (model1.predict (x.features), x.label) }})
p_hat.map (rdd => rdd.count)
val p_hat_all = p_hat.tail.foldLeft (p_hat.head) ((a, b) => a.union (b))
p_hat_all.count

val metrics = new BinaryClassificationMetrics (foo, 20)
metrics.areaUnderROC
val foo = p_hat_all.map (p => (p._1, if (p._2 == 1.0) 1; else 0))
wilcoxon.U (foo)
