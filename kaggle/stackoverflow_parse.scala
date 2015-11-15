import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Date
import java.io.{PrintStream, FileOutputStream}

object stackoverflow_parse
{
  def read_stackoverflow_records_transformed (sc : SparkContext, filename : String) =
  {
    val x = read_stackoverflow_records (sc, filename)
    val x1 = count_closed_posts (x)
    transform_stackoverflow_records (x1)
  }

  def numeric_fields_only (x : RDD [(Double, Int, Int, Int, String, String, String, String, String, Int)]) : RDD [(Double, Double, Double, Double, Int)] =
  {
    x.map { case (d: Double, i1: Int, i2: Int, i3: Int, s1: String, s2: String, s3: String, s4: String, s5: String, i4: Int) => (d, i1.toDouble, i2.toDouble, i3.toDouble, i4) }
  }

  def printAsLibSVM (x: RDD [(Double, Double, Double, Double, Int)], filename: String) =
  {
    val s = new PrintStream (new FileOutputStream (filename))
    x.collect.foreach { case (d1, d2, d3, d4, i) => s.println (i + " 1:" + d1 + " 2:" + d2 + " 3:" + d3 + " 4:" + d4) }
    s.close
  }

  def transform_stackoverflow_records (x : RDD [(Long, ((java.util.Date, java.util.Date, Int, Int, String, String, String, String, String, String, String, Option [java.util.Date], String), Int))]): RDD [(Double, Int, Int, Int, String, String, String, String, String, Int)] =
  {
    x.map { case (ownerUserId : Long, ((postCreationDate : java.util.Date, ownerCreationDate : java.util.Date, reputationAtPostCreation : Int, ownerUndeletedAnswerCountAtPostTime : Int, title : String, bodyMarkdown : String, tag1 : String, tag2 : String, tag3 : String, tag4 : String, tag5 : String, postClosedDate : Option [Date], openStatus : String), ownerClosedPostCountAtPostTime : Int)) => 
      val ownerAgeAtPostCreationInDays = (postCreationDate.getTime - ownerCreationDate.getTime) / (86400.0 * 1000.0)
      val statusClosedForAnyReason = if (openStatus != "open") 1 else 0
      (ownerAgeAtPostCreationInDays, reputationAtPostCreation, ownerUndeletedAnswerCountAtPostTime, ownerClosedPostCountAtPostTime, tag1, tag2, tag3, tag4, tag5, statusClosedForAnyReason) }
  }

  def read_stackoverflow_records (sc : SparkContext, filename : String) =
  {
    val date_fmt = new java.text.SimpleDateFormat ("MM/dd/yyyy HH:mm:ss")
    val parsed = sc.textFile (filename, 1)
                   .map { line =>
                     val items = line.split (',')
                     val  PostId = items(0).toLong
                     val  PostCreationDate = date_fmt.parse (items(1))
                     val  OwnerUserId = items(2).toLong
                     val  OwnerCreationDate = date_fmt.parse (items(3))
                     val  ReputationAtPostCreation = items(4).toInt
                     val  OwnerUndeletedAnswerCountAtPostTime = items(5).toInt
                     val  Title = items(6)
                     val  BodyMarkdown = items(7)
                     val  Tag1 = items(8)
                     val  Tag2 = items(9)
                     val  Tag3 = items(10)
                     val  Tag4 = items(11)
                     val  Tag5 = items(12)
                     val  PostClosedDate : Option [java.util.Date] = if (items(13) == "") { None } else { Some (date_fmt.parse (items(13))) }
                     val  OpenStatus = items(14)
                     (OwnerUserId, (PostCreationDate, OwnerCreationDate, ReputationAtPostCreation, OwnerUndeletedAnswerCountAtPostTime, Title, BodyMarkdown, Tag1, Tag2, Tag3, Tag4, Tag5, PostClosedDate, OpenStatus)) }
    parsed
  }

  def count_closed_posts (x : RDD [(Long, (java.util.Date, java.util.Date, Int, Int, String, String, String, String, String, String, String, Option [java.util.Date], String))]) =
  {
      val x1 = x.aggregateByKey(0)((acc, value) => acc + (if (value._13 == "open") 0 else 1), (acc1, acc2) => acc1 + acc2)
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
