package biomed3471.codex

import biomed3471.codex.DataOps.*
import io.delta.implicits.*
import org.apache.hadoop.fs.{Path as HadoopPath}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.nio.charset.StandardCharsets
import java.time.{Instant, LocalDate}
import scala.util.Using
import scala.util.chaining.*

/** Entry point for the BIOMED-3471 steps 1–5 pipeline. */
object TwinSearchApp:

    /** Writes a Delta table, replaces any existing table and its schema at that path, and reads it back. */
    def writeAndReadBackDeltaTable(data: DataFrame, tablePath: String): DataFrame =
        data.write
            .format("delta")
            .mode(SaveMode.Overwrite)
            .option("overwriteSchema", "true")
            .save(tablePath)

        data.sparkSession.read.delta(tablePath)

    /** Creates the local Delta/S3 session with eight shuffle partitions and all available cores. */
    def makeSparkSession: SparkSession =
        SparkSession.builder()
            .appName("BIOMED-3471 Codex")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
            .config("spark.sql.shuffle.partitions", 8)
            .master("local[*]")
            .getOrCreate()


    /** Reserves a fresh run path and records its start time and input in _STARTED. Existing run paths are refused. */
    def beginRun(spark: SparkSession, runPath: String, relationOccurrencePath: String): Unit =
        val root = new HadoopPath(runPath)
        val fs   = root.getFileSystem(spark.sparkContext.hadoopConfiguration)
        require(!fs.exists(root), s"Output already exists: $runPath. Clean this run's output before rerunning.")
        val body = s"started=${Instant.now()}\ninput=$relationOccurrencePath\nphase=steps-1-through-5\n"
        Using.resource(fs.create(new HadoopPath(root, "_STARTED"), false)) { stream =>
            stream.write(body.getBytes(StandardCharsets.UTF_8))
        }

    /**
     * Finds the non-identical twins of forward-only canonical relations in backfill,
     * then identifies the homonymous node pairs to investigate. The backfill search
     * includes canonical relations shared with the forward flow.
     *
     * Declares the input, run and five table paths directly below. See the
     * <a href="README.md#direct-domain-rules">direct domain rules</a> for matching
     * and the <a href="README.md#five-saved-delta-tables">five-table overview</a>
     * for each output's contents and purpose.
     *
     * Reserves the run path, then writes and reads back each Delta table in order.
     * Later stages use the saved tables without computing earlier stages again.
     * Spark closes after the pipeline finishes or fails; completion is reported
     * only after all writes and Spark shutdown succeed.
     *
     * @param args the run date, YYYY-MM-DD, as the only argument
     */
    def main(args: Array[String]): Unit =
        val runId = args(0)
        LocalDate.parse(runId)

        val relationOccurrencePath = "s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/BIOMED-3471/wytze-relation-occurrences/"
        val runPath                = s"s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/BIOMED-3471/$runId"

        val canonicalRelationsPath                 = s"$runPath/canonicalRelations"
        val nonIdenticalTwinPairsPath              = s"$runPath/nonIdenticalTwinPairs"
        val twinPairsWithNameMatchedNodePairsPath  = s"$runPath/twinPairsWithNameMatchedNodePairs"
        val homonymousNodePairsPerTwinPairPath     = s"$runPath/homonymousNodePairsPerTwinPair"
        val twinPairCountPerHomonymousNodePairPath = s"$runPath/twinPairCountPerHomonymousNodePair"

        val spark = makeSparkSession
        try
            spark.sparkContext.setLogLevel("WARN")
            beginRun(spark, runPath, relationOccurrencePath)

            val canonicalRelations = spark.read.delta(relationOccurrencePath)
                .pipe(aggregateCanonicalRelations)
                .pipe(data => writeAndReadBackDeltaTable(data, canonicalRelationsPath))

            val nonIdenticalTwinPairs = canonicalRelations
                .pipe(findNonIdenticalTwinPairs)
                .pipe(data => writeAndReadBackDeltaTable(data, nonIdenticalTwinPairsPath))

            val twinPairsWithNameMatchedNodePairs = nonIdenticalTwinPairs
                .withNameMatchedNodePairs
                .pipe(data => writeAndReadBackDeltaTable(data, twinPairsWithNameMatchedNodePairsPath))

            val homonymousNodePairsPerTwinPair = twinPairsWithNameMatchedNodePairs
                .selectHomonymousNodePairsPerTwinPair
                .pipe(data => writeAndReadBackDeltaTable(data, homonymousNodePairsPerTwinPairPath))

            homonymousNodePairsPerTwinPair
                .countTwinPairsPerHomonymousNodePair
                .pipe(data => writeAndReadBackDeltaTable(data, twinPairCountPerHomonymousNodePairPath))

        finally spark.stop()

        println(s"Twin search completed: $runPath")
