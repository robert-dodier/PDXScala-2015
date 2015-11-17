import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

val x = MLUtils.loadLibSVMFile (sc, "kaggle/stackoverflow-train-sample-replace-text-by-length-numeric.libsvm")
val summary = Statistics.colStats (x.map {case LabeledPoint (l, f) => f})
val means = summary.mean.toArray
val sds = summary.variance.toArray.map {u => Math.sqrt (u)}
val y = x.map {case LabeledPoint (l, f) => LabeledPoint (l, Vectors.dense (((f.toArray zip means) zip sds).map {case ((u, v), w) => (u - v)/w}))}

val data = y.toDF
val splits = data.randomSplit(Array(0.5, 0.5), seed = 1L)
val train = splits(0)
val test = splits(1)

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier

// val layers = Array (5, 2)
val layers = Array (5, 14, 2)
val mlpc = new MultilayerPerceptronClassifier ().setLayers (layers).setMaxIter (100)
val model = mlpc.fit (train)
val test_result = model.transform (test)

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
val predictionAndLabels = test_result.select ("prediction", "label")
val evaluator = new MulticlassClassificationEvaluator ()
evaluator.evaluate (test_result)
