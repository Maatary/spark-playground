package dg.dg6

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.*

import cats.effect.unsafe.implicits.global
import scala.concurrent.duration.*
import cats.effect.*
import cats.syntax.all.*

import scribe.*
import scribe.format.*

import scala.jdk.CollectionConverters.*
import scala.util.chaining.*


/**
 * Explode:
 *
 * . dg61 - df explode
 *
 * . dg62 - ds explode
 */

object dg61:

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


        val csv = spark
            .read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("data/retail-data/all/online-retail-dataset.csv")
            .tap { ds => ds.schema.printTreeString() }



        val csvSplitted = csv
            .withColumn("split", split($"Description", " "))
            .tap { ds => ds.show(truncate = false) }
            .tap { ds => ds.schema.printTreeString() }
            .tap { ds => ds.explain(true) }


        val csvExploded = csvSplitted
            .select(col("*"), explode($"split") as "word")
            .tap { ds => ds.show(truncate = false) }
            .tap { ds => ds.schema.printTreeString() }
            .tap { ds => ds.explain(true) }


        spark.stop()


/**
 * Explode
 *
 * ds Explode
 */
object dg62:

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

        case class Invoice(
            InvoiceNo  : Option[String],
            StockCode  : Option[String],
            Description: Option[String],
            Quantity   : Option[Int],
            InvoiceDate: Option[String],
            UnitPrice  : Option[Double],
            CustomerID : Option[Int],
            Country    : Option[String]
        )


        val csv = spark
            .read
            .format("csv")
            .option("header", "true")
            .schema(TypedEncoder[Invoice].encoder.schema)
            .load("data/retail-data/all/online-retail-dataset.csv")
            .tap { ds => ds.schema.printTreeString() }
            .as[Invoice]


        //So explode is flatMap { object => `object.array` map { elt => (object, elt)  } }.select($"_1.*", $"_2" as "word")
        val csvSplitted = csv
            .flatMap(invoice => invoice.Description.fold(Array(""))(_.split(" ")).map(word => (invoice, word)))
            .select($"_1.*", $"_2" as "word")
            .tap { ds => ds.show(truncate = false) }
            .tap { ds => ds.schema.printTreeString() }
            .tap { ds => ds.explain(true) }



        spark.stop()