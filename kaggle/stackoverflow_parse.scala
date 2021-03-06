import org.apache.spark.mllib.clustering.{LDA, LDAModel, LDAUtils, LDAOptimizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Date
import java.io.{PrintStream, FileOutputStream}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.regression.LabeledPoint

object stackoverflow_parse
{
  def replace_tags_with_scores (x: RDD [(Long, Double, Int, Int, Int, Int, String, String, String, String, String, Int)]) =
  {
    val tags_scores = tags_topics_scores (x)
    val x1 = (x.map {case (postId, d, i1, i2, i3, i4, tag1, tag2, tag3, tag4, tag5, i5) => (postId, (i5, d, i1, i2, i3, i4))}).join (tags_scores)
    x1.map {case (postId, ((i5, d, i1, i2, i3, i4), v)) => new LabeledPoint (i5, Vectors.dense (d, i1.toDouble, i2.toDouble, i3.toDouble, i4.toDouble, v(0), v(1), v(2), v(3), v(4), v(5)))}
  }

  def tags_topics_scores (x: RDD [(Long, Double, Int, Int, Int, Int, String, String, String, String, String, Int)]): RDD [(Long, Vector)] =
  {
    val foo = tags_lda (x)
    val tags_array = foo._1
    val model = foo._2
    tags_topics_scores (x, tags_array, model.topicsMatrix.transpose)
  }

  def tags_topics_scores (x: RDD [(Long, Double, Int, Int, Int, Int, String, String, String, String, String, Int)], tags_array: Array [String], topicsMatrix: Matrix): RDD [(Long, Vector)] =
  {
    x.map {case (postId, d, i1, i2, i3, i4, tag1, tag2, tag3, tag4, tag5, i5) => (postId, score_tags (tags_array, topicsMatrix, tag1, tag2, tag3, tag4, tag5))}
  }

  def score_tags (tags_array: Array [String], topicsMatrix: Matrix, tag1: String, tag2: String, tag3: String, tag4: String, tag5: String) =
  {
    val v = make_tag_vector (tags_array, tag1, tag2, tag3, tag4, tag5)
    topicsMatrix.multiply (v)
  }

  def tags_lda (x: RDD [(Long, Double, Int, Int, Int, Int, String, String, String, String, String, Int)]) =
  {
    val foo = tags_bag_of_words (x)
    val tags_array = foo._1
    val y = foo._2.cache
    val n_topics = 6
    val lda = new LDA ().setK (n_topics).setMaxIterations (20)
    val model = lda.run (y)
    (tags_array, model)
  }

  def tags_bag_of_words (x: RDD [(Long, Double, Int, Int, Int, Int, String, String, String, String, String, Int)]) =
  {
    val tags_array = find_unique_tags (x)
    val y = x.map {case (postId, d, i1, i2, i3, i4, tag1, tag2, tag3, tag4, tag5, i5) => (postId, make_tag_vector (tags_array, tag1, tag2, tag3, tag4, tag5))}
    (tags_array, y)
  }

  def find_unique_tags (x: RDD [(Long, Double, Int, Int, Int, Int, String, String, String, String, String, Int)]) =
  {
    val tags = scala.collection.mutable.Set [String] ()

    x.collect.foreach (r => {if (r._7 != "") tags += r._7
                             if (r._8 != "") tags += r._8
                             if (r._9 != "") tags += r._9
                             if (r._10 != "") tags += r._10
                             if (r._11 != "") tags += r._11})

    val tags_array = tags.toArray
    scala.util.Sorting.quickSort (tags_array)
    tags_array
  }

  def make_tag_vector (a: Array [String], s1: String, s2: String, s3: String, s4: String, s5: String) =
  {
    val indices = Array (a.indexOf (s1), a.indexOf (s2), a.indexOf (s3), a.indexOf (s4), a.indexOf (s5))
    val indices_nonempty = indices.filter (i => i != -1)
    val values = Array.fill [Double] (indices_nonempty.length) (1.0)
    Vectors.sparse (a.length, indices_nonempty, values)
  }

  def read_stackoverflow_records_transformed (sc : SparkContext, filename : String) =
  {
    val x = read_stackoverflow_records (sc, filename)
    val x1 = count_closed_posts (x)
    transform_stackoverflow_records (x1)
  }

  // returns: (postId, ownerAgeAtPostCreationInDays, reputationAtPostCreation, ownerUndeletedAnswerCountAtPostTime, ownerClosedPostCountAtPostTime, bodyMarkdownLength, statusClosedForAnyReason)
  def numeric_fields_only (x : RDD [(Long, Double, Int, Int, Int, Int, String, String, String, String, String, Int)]) : RDD [(Long, Double, Double, Double, Double, Double, Int)] =
  {
    x.map { case (l: Long, d: Double, i1: Int, i2: Int, i3: Int, i4: Int, s1: String, s2: String, s3: String, s4: String, s5: String, i5: Int) => (l, d, i1.toDouble, i2.toDouble, i3.toDouble, i4.toDouble, i5) }
  }

  def printAsLibSVM (x: RDD [(Long, Double, Double, Double, Double, Double, Int)], filename: String) =
  {
    val s = new PrintStream (new FileOutputStream (filename))
    x.collect.foreach { case (l, d1, d2, d3, d4, d5, i) => s.println (i + " 1:" + d1 + " 2:" + d2 + " 3:" + d3 + " 4:" + d4 + " 5:" + d5) }
    s.close
  }

  // returns: (postId, ownerAgeAtPostCreationInDays, reputationAtPostCreation, ownerUndeletedAnswerCountAtPostTime, ownerClosedPostCountAtPostTime, bodyMarkdownLength, tag1, tag2, tag3, tag4, tag5, statusClosedForAnyReason)
  def transform_stackoverflow_records (x : RDD [(Long, ((Long, java.util.Date, java.util.Date, Int, Int, String, Int, String, String, String, String, String, Option [java.util.Date], String), Int))]): RDD [(Long, Double, Int, Int, Int, Int, String, String, String, String, String, Int)] =
  {
    x.map { case (ownerUserId : Long, ((postId: Long, postCreationDate : java.util.Date, ownerCreationDate : java.util.Date, reputationAtPostCreation : Int, ownerUndeletedAnswerCountAtPostTime : Int, title : String, bodyMarkdownLength : Int, tag1 : String, tag2 : String, tag3 : String, tag4 : String, tag5 : String, postClosedDate : Option [Date], openStatus : String), ownerClosedPostCountAtPostTime : Int)) => 
      val ownerAgeAtPostCreationInDays = (postCreationDate.getTime - ownerCreationDate.getTime) / (86400.0 * 1000.0)
      val statusClosedForAnyReason = if (openStatus != "open") 1 else 0
      (postId, ownerAgeAtPostCreationInDays, reputationAtPostCreation, ownerUndeletedAnswerCountAtPostTime, ownerClosedPostCountAtPostTime, bodyMarkdownLength, tag1, tag2, tag3, tag4, tag5, statusClosedForAnyReason) }
  }

  // returns: (ownerUserId, ((postId, postCreationDate, ownerCreationDate, reputationAtPostCreation, ownerUndeletedAnswerCountAtPostTime, title, bodyMarkdownLength, tag1, tag2, tag3, tag4, tag5, postClosedDate, openStatus), ownerClosedPostCountAtPostTime)) 
  def read_stackoverflow_records (sc : SparkContext, filename : String) =
  {
    val parsed = sc.textFile (filename, 1)
                   .map { line =>
                     try
                     {
                       val items = line.split (',')
                       val  PostId = items(0).toLong
                       val  PostCreationDate = my_date_parse (items(1))
                       val  OwnerUserId = items(2).toLong
                       val  OwnerCreationDate = my_date_parse (items(3))
                       val  ReputationAtPostCreation = if (items(4) == "") 0 else items(4).toInt
                       val  OwnerUndeletedAnswerCountAtPostTime = if (items(5) == "") 0 else items(5).toInt
                       val  Title = items(6)
                       val  BodyMarkdownLength = items(7).toInt
                       val  Tag1 = items(8)
                       val  Tag2 = items(9)
                       val  Tag3 = items(10)
                       val  Tag4 = items(11)
                       val  Tag5 = items(12)
                       val  PostClosedDate : Option [java.util.Date] = if (items(13) == "") { None } else { Some (my_date_parse (items(13))) }
                       val  OpenStatus = items(14)
                       (OwnerUserId, (PostId, PostCreationDate, OwnerCreationDate, ReputationAtPostCreation, OwnerUndeletedAnswerCountAtPostTime, Title, BodyMarkdownLength, Tag1, Tag2, Tag3, Tag4, Tag5, PostClosedDate, OpenStatus))
                     }
                     catch
                     {
                       case e: Throwable => { println ("EXCEPTION " + e + " ON THIS LINE: " + line); throw e }
                     }}
    parsed
  }

  def my_date_parse (s: String): Date =
  {
    val date_fmt_middle_endian = new java.text.SimpleDateFormat ("MM/dd/yyyy HH:mm:ss")
    val date_fmt_big_endian_ymd = new java.text.SimpleDateFormat ("yyyy-MM-dd")

    try date_fmt_middle_endian.parse (s)
    catch 
    {
      case e: java.text.ParseException => date_fmt_big_endian_ymd.parse (s)
    }
  }

  def count_closed_posts (x : RDD [(Long, (Long, java.util.Date, java.util.Date, Int, Int, String, Int, String, String, String, String, String, Option [java.util.Date], String))]) =
  {
      val x1 = x.aggregateByKey(0)((acc, value) => acc + (if (value._14 == "open") 0 else 1), (acc1, acc2) => acc1 + acc2)
      x.join (x1)
  }

// Running this main program requires getting a lot of classpath stuff
// straightened out; as it stands I get: java.lang.NoSuchMethodException: akka.remote.RemoteActorRefProvider
// Easier at this point just to call read_stackoverflow_records from Spark shell.
// def main (args : Array [String])
// {
//   val conf = new SparkConf ().setAppName ("stackoverflow_parse").setMaster ("local")
//   val sc = new SparkContext (conf)
//   val parsed = read_stackoverflow_records (sc, args(0))
//   System.out.println ("parsed=" + parsed)
// }
}
