package observatory

import java.nio.file.Paths
import java.time.LocalDate

import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types._
import SparkConfiguration._

/**
  * 1st milestone: data extraction
  */
object Extraction {

  import spark.implicits._

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    val stations = readStationsData(stationsFile)
    val temperatures = readTemperatureData(temperaturesFile)
    temperatures.join(stations, temperatures("stationId") === stations("id"))
      .collect()
      .map(row =>
        (LocalDate.of(year, row.getAs[Int](1), row.getAs[Int](2)), Location(row.getAs(5), row.getAs(6)), row.getAs[Double](3)))
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    spark.sparkContext.parallelize(records.toSeq)
      .map {
        case (_, location, temp) => (location, temp)
      }
      .toDS()
      .groupByKey(_._1)
      .agg(typed.avg[(Location, Temperature)](_._2))
      .collect()
  }

  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString


  def readStationsData(resource: String): Dataset[StationData] = {
    val schema = StructType(
      List(
        StructField("stn", StringType, nullable = true),
        StructField("wban", StringType, nullable = true),
        StructField("lat", DoubleType, nullable = true),
        StructField("lon", DoubleType, nullable = true)
      )
    )
    val source: DataFrame = spark.read.format("csv")
      .schema(schema)
      .option("header", "false")
      .load(fsPath(resource))


    source
      .filter((r: Row) => !r.isNullAt(2) && !r.isNullAt(3))
      .map[StationData](mapStationData(_))
  }

  def readTemperatureData(resource: String): Dataset[TemperatureData] = {
    val schema = StructType(
      List(
        StructField("stn", StringType, nullable = true),
        StructField("wban", StringType, nullable = true),
        StructField("month", IntegerType, nullable = false),
        StructField("day", IntegerType, nullable = false),
        StructField("temp", DoubleType, nullable = true)
      )
    )
    val source: DataFrame = spark.read.format("csv")
      .schema(schema)
      .option("header", "false")
      .load(fsPath(resource))


    source
      .filter((r: Row) => !r.isNullAt(4))
      .map[TemperatureData](mapTemperatureData(_))
  }

  private def getNullable[T](row: Row, idx: Int) = {
    if (row.isNullAt(idx)) None else Some(row.getAs[T](idx))
  }

  private def mapStationData(row: Row): StationData = {
    StationData(
      (getNullable(row, 0), getNullable(row, 1)),
      row.getAs[Double](2),
      row.getAs[Double](3)
    )
  }

  private def mapTemperatureData(row: Row): TemperatureData = {
    TemperatureData(
      (getNullable(row, 0), getNullable(row, 1)),
      row.getAs[Int](2),
      row.getAs[Int](3),
      fahrenheitToCelsius(row.getAs(4))
    )
  }

  def fahrenheitToCelsius(fahrenheit: Double) = (fahrenheit - 32) * 5 / 9

}
