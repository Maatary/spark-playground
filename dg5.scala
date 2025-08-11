package dg.dg5

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
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


        case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: Int)

        TypedEncoder[Flight].encoder.schema pipe println

        val df = spark
            .read
            .option("mode", "FAILFAST") // does not respect nullability
            .schema(TypedEncoder[Flight].encoder.schema)
            .json("data/flight-data/json/2015-summary.json")
            .as[Flight]
//
//        val df1 = df
//         .as[Flight] // not needed here we do it on read always in ETL
//         .filter(flight => flight.DEST_COUNTRY_NAME == "United States")
//
//        df1.explain(true)
//        df1.show(truncate = false)
//        df1.schema pipe println

        val df2 = df
            .filter($"DEST_COUNTRY_NAME" === "Senegal" or $"ORIGIN_COUNTRY_NAME" === "Senegal")
            .select($"DEST_COUNTRY_NAME", $"ORIGIN_COUNTRY_NAME", $"count")
            .as[Flight]
            //.select(avg($"count") as "avg_count")

        //df2.explain(true)
        df2.show(truncate = false)
        //df2.schema pipe println
        //df2.columns foreach println


//        df2.selectExpr("DEST_COUNTRY_NAME AS destination").show(truncate = false)
//        df2.select($"count" + 1 as "new count" ).show(truncate = false)
//        df2.select(expr("count + 1 as newCount")).show(truncate = false)
//        df2.select("count").show(truncate = false)
//        df2.select(expr("count + 1")).show(truncate = false)


//        df
//         .withColumn("Sameness", expr("DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME")) //colum expression
//         .show(truncate = false)
//
//        df
//         .withColumn("Sameness", $"DEST_COUNTRY_NAME" === $"ORIGIN_COUNTRY_NAME") // sql text into column expression
//         .show(truncate = false)



        Thread.sleep(Int.MaxValue)

        spark.stop()



object dg52:

    def makeSparkSession: SparkSession =
        SparkSession
            .builder()
            .appName("Example Application")
            .master("local[*]")
            //.config("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
            .getOrCreate()

    def main(args: Array[String]): Unit =

        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder
        import io.github.pashashiz.spark_encoders.TypedEncoder.given


        case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: Int)

        TypedEncoder[Flight].encoder.schema pipe println

        val raw = spark
            .read
            .textFile("data/flight-data/json/2015-summary.json")

        val flights = spark
            .read
            .option("mode", "FAILFAST") //now will fail see https://issues.apache.org/jira/browse/SPARK-49893
            .schema(TypedEncoder[Flight].encoder.schema)
            .json(raw)
            .as[Flight]

        val filteredFlights = flights
            .filter($"DEST_COUNTRY_NAME" === "Senegal" or $"ORIGIN_COUNTRY_NAME" === "Senegal")
            .select($"DEST_COUNTRY_NAME", $"ORIGIN_COUNTRY_NAME", $"count")
            .as[Flight]

        filteredFlights
            .show(truncate = false)



        Thread.sleep(Int.MaxValue)

        spark.stop()



object dg53:

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


        case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: Int)

        TypedEncoder[Flight].encoder.schema pipe println

        val flights = spark
            .read
            .textFile("data/flight-data/json/2015-summary.json")
            .withColumn("value", from_json($"value", TypedEncoder[Flight].encoder.schema, Map("mode" -> "FAILFAST")))
            .select($"value.*").as[Flight]

        val filteredFlights = flights
            .filter($"DEST_COUNTRY_NAME" === "Senegal" or $"ORIGIN_COUNTRY_NAME" === "Senegal")
            .select($"DEST_COUNTRY_NAME", $"ORIGIN_COUNTRY_NAME", $"count")
            .as[Flight]


        filteredFlights
            .show(truncate = false)

        Thread.sleep(Int.MaxValue)

        spark.stop()