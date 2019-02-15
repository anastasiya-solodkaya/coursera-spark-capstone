package observatory

import org.apache.spark.sql.SparkSession

object SparkConfiguration {
  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Weather Changes")
      .config("spark.master", "local")
      .getOrCreate()

}
