import org.apache.spark.mllib.util.MLUtils
val x = MLUtils.loadLibSVMFile (sc, "kaggle/stackoverflow-train-sample-replace-text-by-length-numeric.libsvm")
x.cache

import QuadraticDiscriminant._
metrics.areaUnderROC
