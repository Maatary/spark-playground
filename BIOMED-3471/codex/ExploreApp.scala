package biomed3471.codex

import biomed3471.codex.TwinSearchApp.makeSparkSession
import io.github.pashashiz.spark_encoders.TypedEncoder.given
import scribe.{Level, Logger}
import sparkops.SchemaEnforcementOps.*

import scala.util.chaining.scalaUtilChainingOps

object ExploreApp:

    Logger.root
        .clearHandlers()
        .withHandler(minimumLevel = Some(Level.Error))
        .replace()

    def main(args: Array[String]): Unit =
        val spark = makeSparkSession

        val relationOccurrencePath = "s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/BIOMED-3471/wytze-relation-occurrences/"

        spark.read.format("delta").load(relationOccurrencePath)
            .tap { _.printSchema() }

        val nonIdenticalTwinPairs = spark
            .read
            .format("delta")
            .load("s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/BIOMED-3471/2026-11-01/nonIdenticalTwinPairs/")
            .tap {_.printSchema()}
            .tap{ _.show(10, truncate = false) }


        nonIdenticalTwinPairs
            .count()
            .tap { count => println(s"Non-identical twin pairs count: $count") }

//
//
//        val forwardRelationsWithTwins = nonIdenticalTwinPairs
//                .select("forwardTwin.*")
//                .distinct()
//
//        forwardRelationsWithTwins
//            .count()
//            .tap { count => println(s"Forward relations with twins count: $count") }
//
//
//        val backfillRelationsWithTwins = nonIdenticalTwinPairs
//                .select("backfillTwin.*")
//                .distinct()
//
//        backfillRelationsWithTwins
//            .count()
//            .tap { count => println(s"Backfill relations with twins count: $count") }

        spark.stop()
