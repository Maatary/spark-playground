package biomed3471.codex

import biomed3471.codex.DataOps.*
import biomed3471.codex.DataTypes.*
import biomed3471.codex.IOOps.*
import io.delta.implicits.*

import java.nio.file.Paths
import java.time.LocalDate
import scala.util.chaining.*

/** Entry point for the BIOMED-3471 steps 1–5 pipeline. */
object TwinSearchApp:

    /**
     * Finds the non-identical twins of forward-only canonical relations in backfill,
     * then identifies the homonymous node pairs to investigate. The backfill search
     * includes canonical relations shared with the forward flow.
     *
     * Reads the relation-occurrence Delta source configured by DefaultInputPath
     * and uses the supplied date beneath OutputPrefix. See the
     * <a href="README.md#direct-domain-rules">direct domain rules</a> for matching
     * and the <a href="README.md#six-saved-delta-tables">six-table overview</a>
     * for each output's contents and purpose.
     *
     * Reserves a fresh personal DEV S3 run and writes the six Delta tables
     * sequentially. Later stages read the committed intermediate tables to avoid
     * computing earlier stages again. Spark closes after the pipeline finishes or
     * fails; completion is reported only after all writes and Spark shutdown succeed.
     *
     * @param args the run date, YYYY-MM-DD, as the only argument
     */
    def main(args: Array[String]): Unit =
        val runId = args(0)
        require(runId.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}"), "Run date must be YYYY-MM-DD.")
        LocalDate.parse(runId)
        val config = RunConfig(
            inputPath = DefaultInputPath,
            runId = runId,
            scratchPath = Paths.get(System.getProperty("java.io.tmpdir"), "biomed3471-codex", runId)
        )

        val spark = makeSparkSession(config)
        try
            spark.sparkContext.setLogLevel("WARN")
            beginRun(spark, config)

            val canonicalRelations = spark.read.delta(config.inputPath)
                .pipe(aggregateCanonicalRelations)
                .pipe(data => writeAndReadDataset(data, config, "canonicalRelations"))

            val nonIdenticalTwinPairs = canonicalRelations
                .pipe(findNonIdenticalTwinPairs)
                .pipe(data => writeAndReadDataset(data, config, "nonIdenticalTwinPairs"))

            val twinPairsWithNameMatchedNodePairTuples = nonIdenticalTwinPairs
                .withNameMatchedNodePairTuples
                .pipe(data => writeAndReadDataset(data, config, "twinPairsWithNameMatchedNodePairTuples"))

            val homonymousNodePairs = twinPairsWithNameMatchedNodePairTuples
                .selectHomonymousNodePairs
                .pipe(data => writeAndReadDataset(data, config, "homonymousNodePairs"))

            homonymousNodePairs
                .aggregateHomonymousNodePairsWithTwinCount
                .pipe(data => writeDataset(data, config, "homonymousNodePairsWithTwinCount"))

            twinPairsWithNameMatchedNodePairTuples
                .selectTwinPairsWithMultipleNameMatchedNodePairTuples
                .pipe(data => writeDataset(data, config, "twinPairsWithMultipleNameMatchedNodePairTuples"))

        finally spark.stop()

        println(s"Twin search completed: ${outputPath(config)}")
