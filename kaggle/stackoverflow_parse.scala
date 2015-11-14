import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

case class stackoverflow_record_transformed
 (OwnerAgeAtPostCreationInDays : Double,
  OwnerUserId : Long,
  ReputationAtPostCreation : Integer,
  OwnerUndeletedAnswerCountAtPostTime : Integer,
  OwnerClosedPostCountAtPostTime : Integer,
  Tag1 : String,
  Tag2 : String,
  Tag3 : String,
  Tag4 : String,
  Tag5 : String,
  StatusClosedForAnyReason : Integer)
  
case class stackoverflow_record
 (PostId : Long,
  PostCreationDate : java.util.Date,
  OwnerUserId : Long,
  OwnerCreationDate : java.util.Date,
  ReputationAtPostCreation : Integer,
  OwnerUndeletedAnswerCountAtPostTime : Integer,
  Title : String,
  BodyMarkdown : String,
  Tag1 : String,
  Tag2 : String,
  Tag3 : String,
  Tag4 : String,
  Tag5 : String,
  PostClosedDate : Option [java.util.Date],
  OpenStatus : String)

object stackoverflow_parse
{
  // Return tuple comprising:
  // OwnerAgeAtPostCreationInDays : Double,
  // ReputationAtPostCreation : Integer,
  // OwnerUndeletedAnswerCountAtPostTime : Integer,
  // OwnerClosedPostCountAtPostTime : Integer,
  // Tag1 : String,
  // Tag2 : String,
  // Tag3 : String,
  // Tag4 : String,
  // Tag5 : String,
  // StatusClosedForAnyReason : Integer
  def transform_stackoverflow_records () =
  {
    x.map { (OwnerUserId, ((PostCreationDate, OwnerCreationDate, ReputationAtPostCreation, OwnerUndeletedAnswerCountAtPostTime, Title, BodyMarkdown, Tag1, Tag2, Tag3, Tag4, Tag5, PostClosedDate, OpenStatus), OwnerClosedPostCountAtPostTime)) => 
      val OwnerAgeAtPostCreationInDays = (PostCreationDate.getTime - OwnerCreationDate.getTime) / (86400.0 * 1000.0)
      val StatusClosedForAnyReason = OpenStatus != "open"
      (OwnerAgeAtPostCreationInDays, ReputationAtPostCreation, OwnerUndeletedAnswerCountAtPostTime, OwnerClosedPostCountAtPostTime, Tag1, Tag2, Tag3, Tag4, Tag5, StatusClosedForAnyReason) }
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

  def count_closed_posts (x : RDD [(Long, Tuple13 [java.util.Date, Long, java.util.Date, Integer, Integer, String, String, String, String, String, String, String, Option [java.util.Date], String])]) =
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
