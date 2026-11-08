package biomed3471.codex

import biomed3471.codex.TwinSearchApp.makeSparkSession
import io.delta.implicits.*
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.*

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*
import scala.util.Using

/** Exports the saved twin relations and homonymous node pairs as three CSV files. */
object ExportCsvApp:

    /** Spark writes a directory; move its single data file to the requested CSV filename. */
    def writeCsv(data: DataFrame, destination: Path): Unit =
        val temporaryDirectory = Files.createTempDirectory("biomed3471-csv-")
        val sparkOutput = temporaryDirectory.resolve("data")
        try
            data.coalesce(1).write
                .option("header", "true")
                .option("encoding", "UTF-8")
                .option("escape", "\"")
                .option("ignoreLeadingWhiteSpace", "false")
                .option("ignoreTrailingWhiteSpace", "false")
                .csv(sparkOutput.toString)

            val partFile = Using.resource(Files.list(sparkOutput)) { files =>
                files.iterator().asScala.find(_.getFileName.toString.endsWith(".csv")).get
            }
            Files.move(partFile, destination)
            println(s"Exported ${data.count()} rows to $destination")
        finally
            Using.resource(Files.walk(temporaryDirectory)) { files =>
                files.iterator().asScala.toVector.reverse.foreach(Files.delete)
            }

    def main(args: Array[String]): Unit =
        val runPath = "s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/BIOMED-3471/2026-11-01"
        val downloads = Path.of(System.getProperty("user.home"), "Downloads")
        val spark = makeSparkSession
        try
            spark.sparkContext.setLogLevel("WARN")

            val nonIdenticalTwinPairs = spark.read.delta(s"$runPath/nonIdenticalTwinPairs")
            val homonymousNodePairsPerTwinPair = spark.read.delta(s"$runPath/homonymousNodePairsPerTwinPair")
            val twinPairCountPerHomonymousNodePair = spark.read.delta(s"$runPath/twinPairCountPerHomonymousNodePair")

            val twinRelationsForCsv = nonIdenticalTwinPairs.select(
                col("forwardCanonicalId"),
                col("backfillCanonicalId"),
                col("forwardTwin.relationship_type").as("relationship_type"),
                col("forwardTwin.direction").as("direction"),
                col("forwardTwin.node1_urn").as("forward_node1_urn"),
                col("forwardTwin.node1_name").as("forward_node1_name"),
                col("forwardTwin.node2_urn").as("forward_node2_urn"),
                col("forwardTwin.node2_name").as("forward_node2_name"),
                col("backfillTwin.node1_urn").as("backfill_node1_urn"),
                col("backfillTwin.node1_name").as("backfill_node1_name"),
                col("backfillTwin.node2_urn").as("backfill_node2_urn"),
                col("backfillTwin.node2_name").as("backfill_node2_name"),
                col("nonIdenticalTwinsAlsoWithinForwardFlow")
            )

            writeCsv(
                twinRelationsForCsv.orderBy("relationship_type", "forwardCanonicalId", "backfillCanonicalId"),
                downloads.resolve("nonIdenticalTwinPairs.csv")
            )
            writeCsv(
                homonymousNodePairsPerTwinPair.orderBy(
                    "matchedNodeName", "forwardNodeUri", "backfillNodeUri", "forwardCanonicalId", "backfillCanonicalId", "role"
                ),
                downloads.resolve("homonymousNodePairsPerTwinPair.csv")
            )
            writeCsv(
                twinPairCountPerHomonymousNodePair.orderBy(
                    col("nonIdenticalTwinPairCount").desc, col("matchedNodeName"), col("forwardNodeUri"), col("backfillNodeUri")
                ),
                downloads.resolve("twinPairCountPerHomonymousNodePair.csv")
            )

            println(s"Forward relations with twins: ${nonIdenticalTwinPairs.select("forwardCanonicalId").distinct().count()}")
            println(s"Backfill relations with twins: ${nonIdenticalTwinPairs.select("backfillCanonicalId").distinct().count()}")
        finally spark.stop()
