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
      val record1 = record.replaceAll ("\"[^\"]*\"", "BAZQUUX")
      val n = record.length - (record1.length - "BAZQUUX".length)
      val record2 = record1.replaceAll (",BAZQUUX(BAZQUUX)*,", "," + n + ",")
      println (record2)
    }
  }
}
