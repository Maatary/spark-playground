package dg.dg5

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.StructField
import org.apache.spark.storage.StorageLevel

import cats.effect.unsafe.implicits.global
import scala.concurrent.duration.*
import cats.effect.*
import cats.syntax.all.*
import fs2.*

import scribe.*
import scribe.format.*

import scala.jdk.CollectionConverters.*
import scala.util.chaining.*


/**
 * ==Shows where things fail when parsing - Parse On Read ==
 *
 *
 * FailFast capture Malformed Text
 *
 * Deserialization enforces nullability at runtime, but it is not easy to read where the error actually occurs.
 *
 * It is better to have another appraoch for this
 */
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


        //.as[Flight] check schema conformance and make it Dataset[Flight]
        // go as far as resolve and bind where the check actually occur.
        // in this particular case, however, the check is redundant because schema does it to

        // contains {"ORIGIN_COUNTRY_NAME":"United States","DEST_COUNTRY_NAME":"Senegal","count": null}
        // It Passes in FaiFast because null is not Malformed value and also because nullability is not enforced
        // Adding {"ORIGIN_COUNTRY_NAME":"United States","DEST_COUNTRY_NAME":"Senegal","count": "r2d2"} would fail
        // https://issues.apache.org/jira/browse/SPARK-49893

        val df = spark
            .read
            .schema(TypedEncoder[Flight].encoder.schema)         // prescribe schema
            .option("mode", "FAILFAST")                          // does not respect nullability but ensure well-formed
            .json("data/flight-data/json/2015-summary-bad-2.json")
            .as[Flight]                                          // does not blow because nullability is not enforced
            .tap {_.schema.printTreeString() }
            .tap { _.explain(true) }
            .tap { _.show(truncate = false) }


        // Filter with DS API
        val df1 = df
            .filter(flight => flight.DEST_COUNTRY_NAME == "United States") //blow because of serialization assert
            .tap { _.schema.printTreeString() }
            .tap { _.explain(true) }
            .tap { _.show(truncate = false) }

        spark.stop()

/**
 * ==Spark is not 100% Lazy==
 *
 * RDD are Lazy, but Strcuctured API is not 100% lazy
 *
 * Schema inference trigger jobs
 *
 * Analysis is done at DataFrame/Datset Construction and it is effectful.
 */
object dg512:

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


        //case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: Int)

        val df = spark
            .read
            .option("mode", "FAILFAST")
            .json("data/flight-data/json/2015-summary-bad-2.json")
            //.select($"fakeCol")
            //.tap {_.schema.printTreeString() }
            //.tap { _.explain(true) }

        Thread.sleep(Int.MaxValue)

        spark.stop()





/**
 * ==Shows where things fail when parsing - Parse During Processing After Read ==
 *
 * {{{.withColumn("value", from_json($"value", TypedEncoder[Flight].encoder.schema, Map("mode" -> "FAILFAST")))}}}
 *
 * It does not change a thing in terms of malformed content.
 *
 * Nullability is still not handled.
 *
 * Nullability only fail at runtime, when explicitly forced such as with a forced deserialization e.g.
 *
 * ```collectAsList or map{identity}```
 *
 * @see [[dg56]]
 *
 */

object dg52:

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


        case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: Int) // Added Option to remove the failure

        TypedEncoder[Flight].encoder.schema pipe println

        val flights = spark
            .read
            .textFile("data/flight-data/json/2015-summary-bad-2.json")
            .withColumn("value", from_json($"value", TypedEncoder[Flight].encoder.schema, Map("mode" -> "FAILFAST")))
            .select($"value.*")
            .as[Flight]
            .map(identity) // force deserialization to blow up if schema not respected but not easy to see what is the issue
            .show(truncate = false)

        spark.stop()


/**
 * == Playing with ```to_json(struct)```===
 */
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


        case class Flight(
            DEST_COUNTRY_NAME  : String,
            ORIGIN_COUNTRY_NAME: String,
            count              : Int
        )

        val flights: Dataset[Flight] = List(
            Flight("United States", "Canada", 1000),
            Flight("France", "United Kingdom", 500),
            Flight("Japan", "China", 750)
        ).toDS()
         .tap { _.schema.printTreeString() }

        flights
            .select(struct(col("*")) as "flights")
            .tap { _.schema.printTreeString() }
            .show(truncate = false)

        flights
            .select(struct(col("*")) as "flights")
            .select(to_json($"flights") as "jsonFlights")
            .tap { _.schema.printTreeString() }
            .show(truncate = false)

        spark.stop()


/**
 * ==Playing with Json inline parsing==
 */
object dg54:

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

        val jsonFlight =
            """
              |{
              | "ORIGIN_COUNTRY_NAME":"Russia",
              | "DEST_COUNTRY_NAME":"United States",
              | "count":161
              |}
              |""".stripMargin

        val jsonFlightDS = spark
            .range(1)
            .select(lit(jsonFlight) as "jsonFlight")
            .select(from_json($"jsonFlight", TypedEncoder[Flight].encoder.schema, Map("mode" -> "FAILFAST")) as "flight")
            .tap { _.schema.printTreeString() }
            .select($"flight.*")
            .tap { _.schema.printTreeString() }
            .as[Flight]

        spark.stop()


/**
 * == Shows where things fail when parsing - Parse During Processing After Read ==
 *
 * For check ```assert_true(col(...).isNotNull).isNull)```
 *
 * @see [[dg52]]
 */
object dg56:

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


        val jsonFlight =
            """
              |{
              | "ORIGIN_COUNTRY_NAME":"Russia",
              | "DEST_COUNTRY_NAME":"United States",
              | "count": null
              |}
              |""".stripMargin

        val jsonFlightDS = spark
            .range(1)
            .select(lit(jsonFlight) as "jsonFlight")
            .select(from_json($"jsonFlight", TypedEncoder[Flight].encoder.schema, Map("mode" -> "FAILFAST")) as "flight")
            .tap { _.schema.printTreeString() }
            .select($"flight.*")
            .filter(assert_true(col("count").isNotNull).isNull)
            .as[Flight]

        jsonFlightDS
            .show(truncate = false)

        spark.stop()




/**
 * ==Legacy - Case 1 - Silent default value==
 *
 * [[https://issues.apache.org/jira/browse/SPARK-49893 SPARK-49893]]
 *
 * ===Only works with Parse On Read not in Parse during processing i.e. from_json()===
 *
 * Not great, for things like Int, put a default value like 0 for int if the value was null.
 *
 * For string if the value was null we get a null pointer exception. Not easy to know where it is happening !!!
 *
 * In general, it tries to convert in default value or blow with null pointer exception
 * i.e., it enforces in some situations.
 *
 * Not recommended, not consistent, and the error is not easy to spot like when forcing deserialization.
 */
object dg551:

    def makeSparkSession: SparkSession =
        SparkSession
            .builder()
            .appName("Example Application")
            .master("local[*]")
            .config("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
            .getOrCreate()

    def main(args: Array[String]): Unit =

        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder
        import io.github.pashashiz.spark_encoders.TypedEncoder.given


        case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: Int)


        val raw = spark
            .read
            .textFile("data/flight-data/json/2015-summary-bad.json")

        val flights = spark
            .read
            .schema(TypedEncoder[Flight].encoder.schema)
            .option("mode", "FAILFAST") //now will fail see https://issues.apache.org/jira/browse/SPARK-49893
            .json(raw)
            //.as[Flight]

        flights
            .show(truncate = false)

        spark.stop()


/**
 * ==Legacy - Case 2 - Null pointer exception==
 *
 * [[https://issues.apache.org/jira/browse/SPARK-49893 SPARK-49893]]
 *
 * ===Only works with Parse On Read not in Parse during processing i.e. from_json()===
 *
 * Not great, for things like Int, put a default value like 0 for int if the value was null.
 *
 * For string if the value was null we get a null pointer exception. Not easy to know where it is happening !!!
 *
 * In general, it tries to convert in default value or blow with null pointer exception
 * i.e., it enforces in some situations.
 *
 * Not recommended, not consistent, and the error is not easy to spot like when forcing deserialization.
 */
object dg552:

    def makeSparkSession: SparkSession =
        SparkSession
            .builder()
            .appName("Example Application")
            .master("local[*]")
            .config("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
            .getOrCreate()

    def main(args: Array[String]): Unit =

        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder
        import io.github.pashashiz.spark_encoders.TypedEncoder.given


        case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: Int)


        val raw = spark
            .read
            .textFile("data/flight-data/json/2015-summary-bad-3.json")

        val flights = spark
            .read
            .schema(TypedEncoder[Flight].encoder.schema)
            .option("mode", "FAILFAST") //now will fail see https://issues.apache.org/jira/browse/SPARK-49893
            .json(raw)
            //.as[Flight]

        flights
            .show(truncate = false)

        spark.stop()



/**
 * ==Happy Path Exploration==
 *
 * Filter with DS vs DF
 *
 * ```SelectExpr``` vs ```Select( expr() )```
 *
 */
object dg57:

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


        val df = spark
            .read
            .schema(TypedEncoder[Flight].encoder.schema)
            .option("mode", "FAILFAST")
            .json("data/flight-data/json/2015-summary.json")
            .as[Flight]
            .tap {_.schema.printTreeString()}
            .tap {_.explain(true)}
            .tap {_.show(truncate = false)}


        // Filter with DS API
        val df1 = df
            .filter(flight => flight.DEST_COUNTRY_NAME == "United States")
            .tap {_.schema.printTreeString()}
            .tap {_.explain(true)}
            .tap {_.show(truncate = false)}

        // Filter with DF API
        val df2 = df
            .filter($"DEST_COUNTRY_NAME" === "Senegal" or $"ORIGIN_COUNTRY_NAME" === "Senegal")
            .select($"DEST_COUNTRY_NAME", $"ORIGIN_COUNTRY_NAME", $"count")
            .as[Flight]
            .select(avg($"count") as "avg_count")



        //        df2.selectExpr("DEST_COUNTRY_NAME AS destination").show(truncate = false)
        //        df2.select($"count" + 1 as "new count" ).show(truncate = false)
        //        df2.select(expr("count + 1 as newCount")).show(truncate = false)
        //        df2.select("count").show(truncate = false)
        //        df2.select(expr("count + 1")).show(truncate = false)


        df
          .withColumn("Sameness", expr("DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME")) //sql text into column expression
          .show(truncate = false)

        df
          .withColumn("Sameness", $"DEST_COUNTRY_NAME" === $"ORIGIN_COUNTRY_NAME") //colum expression
          .show(truncate = false)


        val df3 = df
            .filter($"DEST_COUNTRY_NAME1" === "Senegal" or $"ORIGIN_COUNTRY_NAME" === "Senegal")
            .select($"DEST_COUNTRY_NAME1", $"ORIGIN_COUNTRY_NAME", $"count")
            .as[Flight]
            .select(avg($"count") as "avg_count")

        Thread.sleep(Int.MaxValue)

        spark.stop()



/**
 * ==IO Experiment==
 */

object dg581:

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

        val flights = spark
            .read
            .textFile("data/flight-data/json/2015-summary-bad.json")
            .withColumn("value", from_json($"value", TypedEncoder[Flight].encoder.schema, Map("mode" -> "PERMISSIVE")))
            .select($"value.*")
            .as[Flight]
            .map(identity) // force deserialization to blow up if schema not respected.

        val filteredFlights = flights
            .filter($"DEST_COUNTRY_NAME" === "Senegal" or $"ORIGIN_COUNTRY_NAME" === "Senegal")
            .select($"DEST_COUNTRY_NAME", $"ORIGIN_COUNTRY_NAME", $"count")
            .as[Flight]


        def showFlight: Dataset[Flight] => IO[Unit] =
            ds => IO {ds.show(truncate = false)}

        def query[T](dataset: Dataset[T])(f: Dataset[T] => IO[Unit]): IO[Unit] =
            f(dataset)

        val filterFlightQuery = query(filteredFlights)(showFlight)
            .onError(e => IO(error(s"Error: ${e.getMessage}")))

        filterFlightQuery.unsafeRunSync()

        spark.stop()


/**
 * == Building Dataset is Effectfull ==
 *
 * Here we don't do any action but analysis occurs and and throws.
 *
 * Could be because of as[T].
 *
 * Could be because of using any column expression.
 *
 * Resolve and Bind check column reference.
 */
object dg582:

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

        val flights = spark
            .read
            .textFile("data/flight-data/json/2015-summary.json")
            .withColumn("value", from_json($"value", TypedEncoder[Flight].encoder.schema, Map("mode" -> "PERMISSIVE")))
            .select($"value.*")
            .as[Flight]

        val filteredFlights = flights
            .filter($"DEST_COUNTRY_NAME" === "Senegal" or $"ORIGIN_COUNTRY_NAME" === "Senegal")
            .select($"DEST_COUNTRY_NAME", $"count")
            .as[Flight] // Blow at dataset construction time i.e., Analysis time !!



        spark.stop()


/**
 * Full IO experiment
 */
object dg583:

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


        def query[T](dataset: Dataset[T])(f: Dataset[T] => IO[Unit]): IO[Unit] =
            f(dataset)

        def showFlight: Dataset[Flight] => IO[Unit] =
            ds => IO {ds.show(truncate = false)}

        def flights: IO[Dataset[Flight]] = IO {
            spark
              .read
              .textFile("data/flight-data/json/2015-summary.json")
              .withColumn("value", from_json($"value", TypedEncoder[Flight].encoder.schema, Map("mode" -> "PERMISSIVE")))
              .select($"value.*")
              .as[Flight]
        }


        def filterFlights: Dataset[Flight] => IO[Dataset[Flight]] = flights => IO {
            flights
              .filter($"DEST_COUNTRY_NAME" === "Senegal" or $"ORIGIN_COUNTRY_NAME" === "Senegal")
              .select($"DEST_COUNTRY_NAME", $"count")
              .as[Flight]
        }


        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        val ffQuery =
            for
                flightDS           <- flights
                filteredFlightsDS  <- filterFlights(flightDS)
                ffQuery            <- query(filteredFlightsDS)(showFlight)
            yield ffQuery


        ffQuery
          .onError(e => IO(error(s"Error: ${e.getMessage}")))
          .unsafeRunSync()




        spark.stop()


/**
 * == Deeper IO Experiment ==
 *
 *  Build Dataset i.e. Lazy Computation Effectfully
 *
 *  This is equivalent to IO Defer Conceptually
 *
 *  When run build the Lazy Computation and run it in an affect
 *
 */
object dg584:

    def makeSparkSession: SparkSession =
        SparkSession
          .builder()
          .appName("Example Application")
          .master("local[*]")
          .getOrCreate()

    val sparkRes = Resource.make( IO { makeSparkSession } )( spark => IO { spark.stop() } )

    def main(args: Array[String]): Unit =


       // import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder
        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: Int)


        def showFlightDS: Dataset[Flight] => IO[Unit] =
            ds => IO {ds.show(truncate = false)}




        def makeFlightDS (implicit spark: SparkSession): IO[Dataset[Flight]] = IO {

            import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
            spark
              .read
              .textFile("data/flight-data/json/2015-summary.json")
              .withColumn("value", from_json($"value", TypedEncoder[Flight].encoder.schema, Map("mode" -> "FAILFAST")))
              .select($"value.*")
              .as[Flight]
        }



        def computeFilterFlightsDS (flights: Dataset[Flight]) (implicit spark: SparkSession): IO[Dataset[Flight]] = IO {

            import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
            flights
              .filter($"DEST_COUNTRY_NAME" === "Senegal" or $"ORIGIN_COUNTRY_NAME" === "Senegal")
              .as[Flight]
        }


        Stream
          .resource( sparkRes )
          .flatMap { implicit spark =>
              Stream
                .eval(makeFlightDS)
                .evalMap(computeFilterFlightsDS(_))
                .evalMap(showFlightDS)
          }
          .compile
          .drain
          .unsafeRunSync()


