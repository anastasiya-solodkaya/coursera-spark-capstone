package observatory

import java.time.LocalDate

import org.scalactic._
import org.scalatest.FunSuite
import org.scalactic.Tolerance._
import org.scalactic.TypeCheckedTripleEquals._

trait ExtractionTest extends FunSuite {

  implicit val tolerance: Equality[Temperature] = TolerantNumerics.tolerantDoubleEquality(0.001)

  implicit val tempEq: Equivalence[TemperatureData] = (a: TemperatureData, b: TemperatureData) => a.stationId == b.stationId &&
    a.day == b.day && a.month == b.month && tolerance.areEqual(a.temperature, b.temperature)

  test("Reading stations data test") {
    val ds = Extraction.readStationsData("/stations_small.csv")
    val result: Set[StationData] = ds.collect().toSet
    val expected = Set(
      StationData((Some("007018"), Some("000201")), 0.0, 0.0),
      StationData((Some("008268"), None), 32.950, 65.567),
      StationData((None, Some("118277")), 31.443, 89.325)
    )
    assert(result == expected, s"Loaded data set $result contains the elements as expected $expected")
  }

  test("Reading temperature data test") {
    val ds = Extraction.readTemperatureData("/temps_small.csv")
    val result: List[TemperatureData] = ds.collect().toList.sortBy(t => (t.stationId, t.month, t.day))
    val expected = List(
      TemperatureData((Some("007005"), None), 1, 1, -6.611111),
      TemperatureData((Some("007005"), None), 1, 2, -4.8888),
      TemperatureData((Some("007018"), Some("000201")), 1, 1, -19.66666),
      TemperatureData((Some("007018"), Some("000201")), 1, 2, -19.72222),
      TemperatureData((Some("007018"), Some("000201")), 1, 5, -17.27777),
      TemperatureData((Some("008268"), None), 1, 5, -16.22222),
      TemperatureData((Some("008268"), None), 1, 6, -15.61111),
      TemperatureData((None, Some("118277")), 1, 1, -10.94444),
      TemperatureData((None, Some("118277")), 1, 2, -9.61111)
    ).sortBy(t => (t.stationId, t.month, t.day))
    result.zip(expected).zipWithIndex.foreach {
      case ((r:TemperatureData, e:TemperatureData), idx) => {
        assert(tempEq.areEquivalent(r, e), s"($idx) - $r equals $e")
      }
    }

  }

  test("locateTemperatures test") {
    val sort = (t:(LocalDate, Location, Temperature)) => (t._1.getYear, t._1.getMonthValue, t._1.getDayOfMonth, t._2.lat, t._2.lon)
    val result: List[(LocalDate, Location, Temperature)] =
      Extraction.locateTemperatures(1987, "/stations_small.csv", "/temps_small.csv")
      .toList.sortBy(sort)

    val expected = List(
      (LocalDate.of(1987, 1, 1), Location(0.0, 0.0), -19.6666),
      (LocalDate.of(1987, 1, 2), Location(0.0, 0.0), -19.72222),
      (LocalDate.of(1987, 1, 5), Location(0.0, 0.0), -17.27777),
      (LocalDate.of(1987, 1, 5), Location(32.950, 65.567), -16.22222),
      (LocalDate.of(1987, 1, 6), Location(32.950, 65.567), -15.61111),
      (LocalDate.of(1987, 1, 1), Location(31.443, 89.325), -10.944444),
      (LocalDate.of(1987, 1, 2), Location(31.443, 89.325), -9.61111)
    ).sortBy(sort)

    result.zip(expected).zipWithIndex.foreach {
      case ((r:(LocalDate, Location, Temperature), e:(LocalDate, Location, Temperature)), idx) => {
        assert(r._1 == e._1 && r._2 == e._2 && tolerance.areEqual(r._3, e._3) , s"($idx) - $r equals $e")
      }
    }
  }


  test("locationYearlyAverageRecords test") {
    val dataset = Set(
      (LocalDate.of(1987, 1, 1), Location(0.0, 0.0), 25.88),
      (LocalDate.of(1987, 1, 2), Location(0.0, 0.0), 25.7),
      (LocalDate.of(1987, 1, 5), Location(0.0, 0.0), 33.62),
      (LocalDate.of(1987, 1, 5), Location(32.950, 65.567), 37.04),
      (LocalDate.of(1987, 1, 6), Location(32.950, 65.567), 39.02),
      (LocalDate.of(1987, 1, 1), Location(31.443, 89.325), 54.14),
      (LocalDate.of(1987, 1, 2), Location(31.443, 89.325), 58.46)
    )
    val result: Map[Location, Temperature] = Extraction.locationYearlyAverageRecords(dataset).toMap
    println(result.mkString("\n"))
    val expected = Map(
      (Location(0.0, 0.0), 28.4),
      (Location(32.950, 65.567), 38.03),
      (Location(31.443, 89.325), 56.3)
    )

    assert(result.size == expected.size, s"Average temperatures size ${result.size} the same as expected ${expected.size}")
    expected.keySet.map(k => (k, result(k), expected(k)))
      .foreach {
        case (k, res: Double, exp: Double) => assert(tolerance.areEqual(res, exp),
          s"Average temperatures for location $k ($res) is the same as expected $exp")
      }

  }

  test("Celsius to Fahrenheit test") {
    assert(Extraction.fahrenheitToCelsius(50) == 10, "convert 50F to 10C")
    assert(Extraction.fahrenheitToCelsius(59) == 15, "convert 59F to 15C")
    assert(Extraction.fahrenheitToCelsius(95) == 35, "convert 95F to 35C")
    assert(Extraction.fahrenheitToCelsius(14) == -10, "convert 14F to -10C")
  }
}