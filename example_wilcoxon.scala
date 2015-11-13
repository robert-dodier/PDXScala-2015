import org.apache.spark.mllib.util.MLUtils
val x = MLUtils.loadLibSVMFile (sc, "/home/robert/tmp/apache-spark-git/spark/data/mllib/sample_binary_classification_data.txt")

import java.io.PrintStream
val f = new PrintStream ("tmp.data")
val y = x.map (p => (p.label + "   " + p.features.toDense.toArray.mkString (" ")))
val a = y.collect ()
for (aa <- a) f.println (aa)
f.flush
f.close

val x0=x.first()
x0.label
x0.features
val U = wilcoxon.allU (x)
U.max
U.min
val m = x0.features.size
0 to m - 1 filter (i => U(i) < 0.05 || U(i) > 0.95)
U(350)
x.map (p => p.features(350)).collect ()
val C = x.map (p => p.label.toInt)
C.collect
C.collect.sum

