package biomed3471.codex

import org.apache.spark.sql.SparkSession
import scribe.{Level, Logger}

import scala.util.chaining.scalaUtilChainingOps

object ExploreApp:

    Logger.root
        .clearHandlers()
        .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
        .replace()

    def makeSparkSession: SparkSession =
        SparkSession
            .builder()
            .appName("ExploreApp")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
            .config("spark.sql.shuffle.partitions", 12)
            .master("local[*]")
            .getOrCreate()

    def main(args: Array[String]): Unit =
        val spark = makeSparkSession

        val relationOccurrencePath = "s3a://canary-data-science/wytze/BCE_validation_September_2026_II/relationships_08-10-2026.parquet"

        spark.read.parquet(relationOccurrencePath)
            .tap { _.printSchema() }

//        val nonIdenticalTwinPairs = spark
//            .read
//            .format("delta")
//            .load("s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/BIOMED-3471/2026-10-21-phase-one-230855/nonIdenticalTwinPairs/")
//            //.tap {_.printSchema()}
//            //.tap{ _.show(10, truncate = false) }
//
//
//        nonIdenticalTwinPairs
//            .count()
//            .tap { count => println(s"Non-identical twin pairs count: $count") }
//
//
//
//        val forwardRelationsWithTwins = nonIdenticalTwinPairs
//                .select("forward.*")
//                .distinct()
//
//        forwardRelationsWithTwins
//            .count()
//            .tap { count => println(s"Forward relations with twins count: $count") }
//
//
//        val backfillRelationsWithTwins = nonIdenticalTwinPairs
//                .select("backfill.*")
//                .distinct()
//
//        backfillRelationsWithTwins
//            .count()
//            .tap { count => println(s"Backfill relations with twins count: $count") }

        spark.stop()
