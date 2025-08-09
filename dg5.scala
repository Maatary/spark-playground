package dg.dg5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.StructField
import org.apache.spark.storage.StorageLevel
import scribe.*
import scribe.format.*

import scala.util.chaining.*

object dg51:

    def makeSparkSession: SparkSession =
        SparkSession
            .builder()
            .appName("Example Application")
            .master("local[*]")
            .getOrCreate()

    def main(args: Array[String]): Unit =

        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder
        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt) derives TypedEncoder

        val df = spark.read.json("data/flight-data/json/2015-summary.json")
//
//        val df1 = df
//         .as[Flight]
//         .filter(flight => flight.DEST_COUNTRY_NAME == "United States")
//
//        df1.explain(true)
//        df1.show(truncate = false)
//        df1.schema pipe println

        val df2 = df
            .filter($"DEST_COUNTRY_NAME" === "United States")
            .select($"DEST_COUNTRY_NAME", $"ORIGIN_COUNTRY_NAME", $"count")

        df2.explain(true)
        df2.show(truncate = false)
        df2.schema pipe println
        df2.columns foreach println


//        df2.selectExpr("DEST_COUNTRY_NAME AS destination").show(truncate = false)
//        df2.select($"count" + 1 as "new count" ).show(truncate = false)
//        df2.select(expr("count + 1 as newCount")).show(truncate = false)
//        df2.select("count").show(truncate = false)
        df2.select(expr("count + 1")).show(truncate = false)


//        df
//         .withColumn("Sameness", expr("DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME")) //colum expression
//         .show(truncate = false)
//
//        df
//         .withColumn("Sameness", $"DEST_COUNTRY_NAME" === $"ORIGIN_COUNTRY_NAME") // sql text into column expression
//         .show(truncate = false)

        Thread.sleep(Int.MaxValue)

        spark.stop()

