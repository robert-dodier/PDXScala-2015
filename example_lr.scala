import org.apache.spark.mllib.util.MLUtils
val x = MLUtils.loadLibSVMFile (sc, "/home/robert/tmp/apache-spark-git/spark/data/mllib/sample_binary_classification_data.txt")

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

val roc = metrics.roc.collect
import com.quantifind.charts.Highcharts._
line (roc)
