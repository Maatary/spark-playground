package grokk.grokk1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.streaming.*

//import org.apache.spark.sql.types.*
//import org.apache.spark.sql.functions._

import scribe.*
import scribe.format.*
import scala.util.chaining.*


/**
 * == Grokking Spark Session ==
 *
 * === Exploring the bare spark configuration ===
 *
 *  - Notice that a lot of configuration available on databricks are not set up at all
 *
 *      - s3 config such as hadoop file system sa3 or credential provider,
 *      - Rocksdb config, delta config
 *
 *
 * === Initial exploration of separation of concerns ===
 *
 *  - Without IO
 *
 *  - Without separating transformation and action
 *
 *  Pure Naive exploration.
 *

 *
 */
object ProgramLogic:

    def query(spark: SparkSession): Unit =
        import spark.implicits._
        import org.apache.spark.sql.functions.col

        Seq(4, 2, 3)
          .toDS
          .groupByKey( _ % 2)
          .count()
          .toDF("key", "count") //instead of count(1) as col name for the count
          .show()


    def makeSparkSession: SparkSession =
        SparkSession
          .builder()
          .appName("Example Application")
          .master("local[*]")
          .getOrCreate()


    def runQuery(query: SparkSession => Unit)(spark: SparkSession): Unit =
        query(spark)


@main
def main(): Unit =

    Logger.root
          .clearHandlers()
          .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
          .replace()

    import ProgramLogic.*

    val spark = makeSparkSession

    spark.conf.getAll.foreach(println)

    //println(spark.conf.get("spark.hadoop.fs.s3.impl")) //<-- This need to be set explicitly
    //println(spark.conf.get("spark.hadoop.fs.s3a.impl")) //< -- This need to be set explicitly

    runQuery(query)(spark)

    println(SparkSession.active.version)

    spark.stop() // <-- Allways close spark session - A resource that needs closing


/**
 * == Grokking MapGroups ==
 *
 *
 * == Note on Schema for Atomic Types ==
 *
 *  For atomic types, Dataset[T] displays as a single column named "value" when viewed as a DataFrame.
 *
 *  There are exceptions such as for the Dataset produced by spark.range which define its own column names (e.g., "id").
 *
 *  To be more accurate, the schema of the data is a column named "value" with the type of the atomic type.
 *
 *  In the case of a Dataset[Long] produced by Range the schema is a column named "id" with the type of Long.
 *  i.e. The Schema is hardcoded.
 */
object grokk2:

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
        import io.github.pashashiz.spark_encoders.TypedEncoder.given


        val sc      = spark.sparkContext
        val base    = sc
          .parallelize(1 to 9, 3)
          .toDS()
          .persist()
          .tap {_.printSchema()} //<-- Dataset[primitiveType] always translate to a schema with on column, named value.

        val squared = base
          .map { x => { println(s"map $x"); x * x } } //<-- just for debug to show it is happening concurrently
          .persist()
          .tap {_.printSchema()} //<-- Dataset[primitiveType] always translate to a schema with on column, named value.

        val buckets = squared
            .groupByKey(x => x % 2)  //<-- Key = 0 | 1
            .mapGroups((k, it) => (k, it.toList)) // 2 groups
            .persist()
            .tap {_.printSchema()}
            .tap {_.explain(true)}
            .tap {_.show()}

        buckets
          .collect()
          .foreach(println)

        Thread.sleep(Int.MaxValue)

        spark.stop()