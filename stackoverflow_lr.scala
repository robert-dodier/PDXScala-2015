import org.apache.spark.mllib.util.MLUtils
val x = MLUtils.loadLibSVMFile (sc, "kaggle/stackoverflow-train-sample-replace-text-by-length-numeric.libsvm")
x.cache

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
val lr = new LogisticRegressionWithLBFGS()
val model = lr.run (x)
model.getThreshold
val output_class = x.map (p => (model.predict (p.features), p.label))
val true_positives = output_class.filter (ab => ab._1 == 1.0 && ab._2 == 1.0).count
val false_positives = output_class.filter (ab => ab._1 == 1.0 && ab._2 == 0.0).count
val false_negatives = output_class.filter (ab => ab._1 == 0.0 && ab._2 == 1.0).count
val true_negatives = output_class.filter (ab => ab._1 == 0.0 && ab._2 == 0.0).count
model.clearThreshold
val output_score = x.map (p => (model.predict (p.features), p.label))
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
val metrics = new BinaryClassificationMetrics (output_score, 20)
val roc = metrics.roc.collect
metrics.areaUnderROC

import com.quantifind.charts.Highcharts._
line (roc)
title ("ROC")
xAxis ("False Positive rate")
yAxis ("True Positive rate")
