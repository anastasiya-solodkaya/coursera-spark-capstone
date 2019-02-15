package observatory

import java.nio.file.Paths

import com.sksamuel.scrimage.Image

object Main extends App {
  buildForYear(2015)
//  buildForYear(2013)
//  buildForYear(2012)
//  buildForYear(2011)
//  buildForYear(2010)

  private def buildForYear(year:Int) = {
    val data = Extraction.locateTemperatures(year, "/stations.csv", s"/$year.csv")
    val stats = Extraction.locationYearlyAverageRecords(data)

    println("Read complete")

    val colors = List(
      (60.0, Color(255, 255, 255)),
      (32.0, Color(255, 0, 0)),
      (12.0, Color(255, 255, 0)),
      (0.0, Color(0, 255, 255)),
      (-15.0, Color(0, 0, 255)),
      (-27.0, Color(255, 0, 255)),
      (-50.0, Color(33, 0, 107)),
      (-60.0, Color(0, 0, 0))
    )
    val image: Image = Visualization.visualize(stats, colors)

    println("image creation complete")
    //  println(Visualization.temperatures.mkString("\n"))
    image.output(Paths.get(s"/Users/Anastasiia/projects/training/scala/ecole/capstone/coursera-spark-capstone/result_$year.png").toFile)
  }
}
