import org.apache.spark.mllib.util.MLUtils
val x = MLUtils.loadLibSVMFile (sc, "kaggle/whats_cooking_train.data-libsvm")

val x0=x.first()
val U = wilcoxon.allU (x)
U.max
U.min
val m = x0.features.size
val U_big = 0 to m - 1 filter (i => U(i) < 0.05 || U(i) > 0.95)

((0 until U.length) zip U) sortBy (p => -p._2)
((0 until U.length) zip U) sortBy (p => - (if (p._2 < 0.5) 1 - p._2 else p._2))

