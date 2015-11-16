import org.apache.spark.mllib.optimization.{SquaredL2Updater, L1Updater}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils

val x = MLUtils.loadLibSVMFile (sc, "kaggle/stackoverflow-train-sample-replace-text-by-length-numeric.libsvm")
x.cache

import org.apache.spark.mllib.classification.SVMWithSGD
val svm = new SVMWithSGD ()
svm.optimizer.setNumIterations (100)
svm.optimizer.setStepSize (1.0)
// svm.optimizer.setUpdater (new L1Updater ())
svm.optimizer.setUpdater (new SquaredL2Updater ())
svm.optimizer.setRegParam (0.01)

val model = svm.run (x)
model.getThreshold
val foo = x.map (p => (model.predict (p.features), p.label))
val true_positives = foo.filter (ab => ab._1 == 1.0 && ab._2 == 1.0).count
val false_positives = foo.filter (ab => ab._1 == 1.0 && ab._2 == 0.0).count
val false_negatives = foo.filter (ab => ab._1 == 0.0 && ab._2 == 1.0).count
val true_negatives = foo.filter (ab => ab._1 == 0.0 && ab._2 == 0.0).count
model.clearThreshold
val bar = x.map (p => (p.label, model.predict (p.features)))
import org.apache.spark.mllib.stat.Statistics
val summary = Statistics.colStats (bar.map {case (a, b) => Vectors.dense (a, b)})
summary.mean
summary.min
summary.max

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
val metrics = new BinaryClassificationMetrics (foo, 20)
metrics.areaUnderROC

val roc = metrics.roc.collect
import com.quantifind.charts.Highcharts._
line (roc)
