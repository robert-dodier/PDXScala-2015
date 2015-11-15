import scala.collection.mutable.Queue
import java.io.FileInputStream
import java.io.InputStreamReader
import java.io.BufferedReader
import java.io.DataInputStream
import java.io.FileOutputStream
import java.io.PrintStream

object stackoverflow_convert
{
  def main (args : Array [String])
  {
    val so_input = new BufferedReader (new InputStreamReader (new FileInputStream (args(0)), "UTF-8"))

    val header = so_input.readLine
    var line = so_input.readLine

    while (line != null)
    {
      val lines = new Queue [String] ()
      lines.enqueue (line)

      line = so_input.readLine
      while (line != null && ! line.matches ("^\\d+,\\d\\d/\\d\\d/\\d\\d\\d\\d \\d\\d:\\d\\d:\\d\\d.*$"))
      {
        lines.enqueue (line)
        line = so_input.readLine
      }

      val record = lines.reduce (_ ++ _)

      val r = "\"[^\"]*\"".r.unanchored
      val quoted_strings = r.findAllIn (record)
      val record1 = r.replaceAllIn (record, "BAZQUUX")
      val items = record1.split (',')

      if (items.length != 15)
        println ("HEY FUNNY LINE: RECORD=" + record + "; ITEMS=" + items)
      else
      {
        val title_bazquux = items(6)
        val title_quotes_count = ("BAZQUUX".r.unanchored).findAllIn (title_bazquux).length
        val body_bazquux = items(7)
        val body_quotes_count = ("BAZQUUX".r.unanchored).findAllIn (body_bazquux).length

        val body_quotes_length = (0 /: (for (s <- quoted_strings drop title_quotes_count) yield s.length))(_ + _)
        val body_length = body_bazquux.length - body_quotes_count * "BAZQUUX".length + body_quotes_length

        val title_substitute = ""
        val body_substitute = body_length

        print (items(0))
        for (i <- Range(1, 6)) print ("," + items(i))
        print ("," + title_substitute)
        print ("," + body_substitute)
        for (i <- Range(8, 15)) print ("," + items(i))
        println
      }
    }
  }
}
