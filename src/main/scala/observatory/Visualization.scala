package observatory

import com.sksamuel.scrimage.{Image, Pixel}

import scala.collection.immutable
import scala.math.{acos, cos, sin}

/**
  * 2nd milestone: basic visualization
  */
object Visualization {
  case class TemperatureDistance(temperature: Temperature, distance: Double)

  val meridianUnit = 6378.137
  val P: Double = 6

  private def areAntipodes(l1:Location, l2:Location) = {
    l1.lat == -l2.lat && (l1.lon - 180 == l2.lon || l1.lon + 180 == l2.lon)
  }

  def circleDistance(l1: Location, l2: Location): Double = {
    if(l1 == l2) {
      0.0
    }else if(areAntipodes(l1, l2)) {
      Math.PI * meridianUnit
    }else {
      val lat1 = Math.toRadians(l1.lat)
      val lat2 = Math.toRadians(l2.lat)
      acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(Math.toRadians(l2.lon) - Math.toRadians(l1.lon))) * meridianUnit
    }
  }




  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
    def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    val closest = temperatures.minBy {
      case (loc, temperature) => circleDistance(loc, location)
    }
    if(circleDistance(closest._1, location) <= 1.0) {
      closest._2
    }else {
      val val1 = temperatures.foldLeft(0.0) {
        case (total, (loc, temperature)) => total + math.pow(circleDistance(loc, location), -P) * temperature
      }
      val val2 = temperatures.foldLeft(0.0) {
        case (total, (loc, _)) => total + math.pow(circleDistance(loc, location), -P)
      }
      val1 / val2
    }
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value  The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    val lower = points.filter(p => p._1 < value)
    val higher = points.filter(p => p._1 >= value)

    def interpolateBetween(val0: (Temperature, Color), val1: (Temperature, Color)): Color = {
      def interpolateForValue(extract: Color => Int): Int = {
        ((extract(val0._2) * (val1._1 - value) + extract(val1._2) * (value - val0._1)) / (val1._1 - val0._1)).round.toInt
      }

      Color(interpolateForValue(_.red), interpolateForValue(_.green), interpolateForValue(_.blue))
    }

    (lower, higher) match {
      case (x :: _, y :: _) => interpolateBetween(lower.maxBy(_._1), higher.minBy(_._1))
      case (x :: _, Nil) => lower.maxBy(_._1)._2
      case (Nil, y :: _) => higher.minBy(_._1)._2
    }


  }


  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    visualizeSeq(temperatures, colors)
//    visualizePar(temperatures, colors)
  }

  private def visualizePar(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]) = {
    def xToLon(x: Year) = x - 180

    def yToLat(y: Year) = 90 - y

    val pixels = (for {
      y <- (0 until 180).par
      x <- (0 until 360).par
    } yield {
      val temperature = predictTemperature(temperatures, Location(yToLat(y), xToLon(x)))
      ((y, x), interpolateColor(colors, temperature).toPixel)
    }).toArray
    Image(360, 180, pixels.sortBy(_._1).map(_._2))
  }

  private def visualizeSeq(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]) = {
    def xToLon(x: Year) = x - 180

    def yToLat(y: Year) = 90 - y

    val pixels = (for {
      y <- (0 until 180)
      x <- (0 until 360)
    } yield {
      interpolateColor(colors, predictTemperature(temperatures, Location(yToLat(y), xToLon(x)))).toPixel
    }).toArray
    Image(360, 180, pixels)
  }
}

