package dg.dg3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.StructField
import org.apache.spark.storage.StorageLevel

import scribe.*
import scribe.format.*
import scala.util.chaining.*


/**
 * == Just Creating a Dataset with createDataset from spark session directly ==
 *
 * Typically, we use implicit conversion to create a dataset from a collection
 *
 * This shows how to do it directly.
 * You could make an empty dataset and then use .as[T] to convert it to a dataset of type T
 *
 * Can be useful for delta table, when we try to create the table with the right schema without writing data into it.
 * But better yet you could use `spark.emptyDataset[User]`
 */
object dg31:

    def makeSparkSession: SparkSession =
        SparkSession
            .builder()
            .appName("Example Application")
            .master("local[*]")
            .getOrCreate()

    def main(args: Array[String]): Unit =

        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.*
        import io.github.pashashiz.spark_encoders.TypedEncoder.*
        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        case class User(id: String)

        val usersDS = spark
            .createDataset(List(User("John"), User("Smith")))
            .tap { _.printSchema() }
            .tap { _.explain(true) }
            .tap { _.show() }


        Thread.sleep(Int.MaxValue)

        spark.stop()


/**
 * == Dataset Reuse With Caching ==
 *
 * This demonstrates the use dataframe persistence
 *
 *  - The cached dataframe can be seen in the UI
 *
 *  - The main clue of reuse is in the DataFrame/SQL query Plan.
 *
 *  - Scanning on reuse would show a number of row 0 while inMemoryScanning would show a possible number.
 *
 *
 *  {{{Query0
 *      LocalTableScan
 *        number of output rows: 5
 *
 *      InMemoryTableScan
 *        number of output rows: 5
 *  }}}
 *
 *  as opposed to
 *
 *  {{{Query1
 *     LocalTableScan
 *        number of output rows: 0
 *
 *     InMemoryTableScan
 *        number of output rows: 5
 *  }}}
 *
 *  == Skipping vs caching ==
 *
 * Skipping is occurring in the third query. It should not, however, be confused with explicit caching.
 *
 * ===Key point: skipped is not the same thing as Dataset cache.===
 * Skipped stages reuse shuffle files that were produced earlier (often earlier in the same SQL query),
 * whereas Dataset caching/persist stores RDD partitions in memory/disk.
 *
 *
 *
 */

object dg32:

    def makeSparkSession: SparkSession =
        SparkSession
            .builder()
            .appName("Example Application")
            .master("local[*]")
            .getOrCreate()

    def main(args: Array[String]): Unit =


        Logger.root
              .clearHandlers()
              .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
              .replace()

        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.*
        import io.github.pashashiz.spark_encoders.TypedEncoder.*
        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        case class Activity(userId: String, cartId: String, itemId: String)

        val activities = Seq(
          Activity("u1", "c1", "i1"),
          Activity("u1", "c1", "i2"),
          Activity("u2", "c2", "i1"),
          Activity("u3", "c3", "i3"),
          Activity("u4", "c4", "i3")
        )

        //Query0
        val activitiesDS = activities
            .toDS()
            .persist(StorageLevel.MEMORY_ONLY)
            .tap { _.printSchema() }
            .tap { _.explain(true) }
            .tap { _.show() }
        //Query1
        activitiesDS
            .map(_.userId)  // <-- This map to DF[value: String]] - the value is the column name
            .toDF("userId") // <-- Act like .withColumnRenamed("value", "userId")
            .tap { _.printSchema() }
            .tap { _.explain(true) }
            .tap { _.show() }

        //Query3
        activitiesDS
            .select("userId")
            .distinct()
            .agg(count("userId"))
            .tap { _.printSchema() }
            .tap { _.explain(true) }
            .tap { _.show() }

        Thread.sleep(Int.MaxValue)

        spark.stop()


/**
 * == Dataset Reuse Without Caching ==
 *
 * === UserIdDS is essentially re-computed 3 times: ===
 *
 * 1. when we make userIdDS
 *
 * 2. when we make activitiesDS with Aggregation-based count (returns DataFrame)
 *
 * 3. when we make activitiesDS with Action-based count (returns Long)
 *
 *
 * == Action-based Aggregation vs Dataframe-based Aggregation==
 *
 */
object dg33:

    def makeSparkSession: SparkSession =
        SparkSession
            .builder()
            .appName("Example Application")
            .master("local[*]")
            .getOrCreate()

    def main(args: Array[String]): Unit =

        Logger.root
              .clearHandlers()
              .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
              .replace()

        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.*
        import io.github.pashashiz.spark_encoders.TypedEncoder.*
        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        case class Activity(userId: String, cartId: String, itemId: String)

        val activities = Seq(
            Activity("u1", "c1", "i1"),
            Activity("u1", "c1", "i2"),
            Activity("u2", "c2", "i1"),
            Activity("u3", "c3", "i3"),
            Activity("u4", "c4", "i3")
        )

        val activitiesDS = activities
            .toDS()
            .tap { _.printSchema() }
            .tap { _.explain(true) }
            .tap { _.show() }

        activitiesDS
            .map(_.userId)  // <-- This map to DF[value: String]] - the value is the column name
            .toDF("userId") // <-- Act like .withColumnRenamed("value", "userId")
            .tap { _.printSchema() }
            .tap { _.explain(true) }
            .tap { _.show() }



        println("=== activitiesDS with Aggregation-based count (returns DataFrame) ===")

        activitiesDS
            .select("userId")
            .distinct()
            .agg(count("userId"))
            .tap { _.printSchema() }
            .tap { _.explain(true) }
            .tap { _.show() }



        println("=== activitiesDS with Action-based count (returns Long) ===")

        activitiesDS
            .select("userId")
            .distinct()
            .tap { _.printSchema() }
            .tap { _.explain(true) }
            .count()
            .tap { count => println(s"The count = $count") }

        Thread.sleep(Int.MaxValue)

        spark.stop()



object dg34:

    def makeSparkSession: SparkSession =
        SparkSession
            .builder()
            .appName("Example Application")
            .master("local[*]")
            .getOrCreate()

    def main(args: Array[String]): Unit =

        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.*
        import io.github.pashashiz.spark_encoders.TypedEncoder.*
        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: Long)

        //        val flightData2015 = spark
        //            .read
        //            .option("inferSchema", "true")
        //            .option("header", "true")
        //            .csv("data/flight-data/csv/2015-summary.csv")
        //
        //        flightData2015
        //            .as[Flight]
        //            .map(flight => flight.DEST_COUNTRY_NAME)
        //            .toDF("DEST_COUNTRY_NAME")
        //            .show()

        Thread.sleep(Int.MaxValue)

        spark.stop()


/**
 * == RDD Debug Info & NumPartitions ==
 *
 * Range - a Relation Expression - produces a dataset of rows inline with spark default parallelism
 * i.e. partitioned by the number of cores in the cluster
 *
 * That is the rows of the Relation i.e. the Dataset are partitioned by the number of cores in the cluster
 *
 */

object dg35:

    Logger.root
          .clearHandlers()
          .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
          .replace()

    def makeSparkSession: SparkSession =
        SparkSession
          .builder()
          .appName("Example Application")
          .master("local[*]")
          .getOrCreate()

    def main(args: Array[String]): Unit =

        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.*
        import io.github.pashashiz.spark_encoders.TypedEncoder.*
        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        spark
            .range(10) // <-- Uses default parallelism
            .rdd // <-- number of partition is default parallelism
            .tap { rdd => println(s"RDD debug Info::\n ${rdd.toDebugString}") }
            .tap { rdd => println(s"RDD number of partition: ${rdd.getNumPartitions}\n") }

        Thread.sleep(Int.MaxValue)

        spark.stop()
