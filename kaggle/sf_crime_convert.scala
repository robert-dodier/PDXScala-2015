import scala.collection.mutable.HashMap
import java.io.FileInputStream
import java.io.DataInputStream
import java.io.FileOutputStream
import java.io.PrintStream

object sf_crime_convert
{
  def main (args : Array [String])
  {
    val dayofweek_lookup = new HashMap [String, Int] ()
    for (i <- Range (0, sf_crime.dayofweek_list.length)) dayofweek_lookup += (sf_crime.dayofweek_list (i) -> i)
    
    val category_lookup = new HashMap [String, Int] ()
    for (i <- Range (0, sf_crime.category_list.length)) category_lookup += (sf_crime.category_list (i) -> i)
    
    val pddistrict_lookup = HashMap [String, Int] ()
    for (i <- Range (0, sf_crime.pddistrict_list.length)) pddistrict_lookup += (sf_crime.pddistrict_list (i) -> i)
    
    val sf_crime_input = new DataInputStream (new FileInputStream ("sf_crime_train_simplified.csv"))
    
    val header = sf_crime_input.readLine ()
    
    val sf_crime_output = new PrintStream (new FileOutputStream ("sf_crime_simplified.libsvm"))
    
    var line = sf_crime_input.readLine ()
    while (line != null)
    {
        val Array (category_string, timestamp_string, dayofweek_string, pddistrict_string, longitude_string, latitude_string)
              = line.split (",")
    
        val category = category_lookup (category_string)
    
        val hms_strings = (timestamp_string.split (" "))(1).split (":")
        val Array (hh, mm, ss) = hms_strings.map (s => s.toInt)
        val elapsed_since_midnight = hh * 3600 + mm * 60 + ss
        val day_angle = 2 * Math.PI * (elapsed_since_midnight - 12*3600) / 86400
        val day_sin = Math.sin (day_angle)
        val day_cos = Math.cos (day_angle)
    
        val dayofweek = dayofweek_lookup (dayofweek_string)
        val week_angle = 2 * Math.PI * dayofweek / 7
        val week_sin = Math.sin (week_angle)
        val week_cos = Math.cos (week_angle)
    
        val pddistrict = pddistrict_lookup (pddistrict_string)
        
        sf_crime_output.println (category + " 1:" + day_sin + " 2:" + day_cos + " 3:" + week_sin + " 4:" +  week_cos + " 5:" + pddistrict + " 6:" + longitude_string + " 7:" + latitude_string)
    
        line = sf_crime_input.readLine ()
    }
  }
}
