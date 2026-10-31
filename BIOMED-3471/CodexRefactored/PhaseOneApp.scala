package biomed3471.codexrefactored

import biomed3471.codexrefactored.DataOps.*
import biomed3471.codexrefactored.DataTypes.*
import biomed3471.codexrefactored.IOOps.*
import io.delta.implicits.*

import java.nio.file.Paths
import scala.util.chaining.*

/** Entry point for the BIOMED-3471 steps 1–5 pipeline. */
object PhaseOneApp:

    /**
     * Finds the non-identical twins of forward-only canonical relations in backfill,
     * then identifies the homonymous node pairs to investigate. The backfill search
     * includes canonical relations shared with the forward flow.
     *
     * Reads the relation-occurrence Delta source configured by DefaultInputPath
     * and uses the supplied run folder name beneath OutputPrefix. See the
     * <a href="README.md#direct-domain-rules">direct domain rules</a> for matching
     * and the <a href="README.md#six-saved-delta-tables">six-table overview</a>
     * for each output's contents and purpose.
     *
     * Reserves a fresh personal DEV S3 run and writes the six Delta tables
     * sequentially. Later stages read the committed intermediate tables to avoid
     * computing earlier stages again. Spark closes after the pipeline finishes or
     * fails; completion is reported only after all writes and Spark shutdown succeed.
     *
     * @param args the run folder name as the only argument
     */
    def main(args: Array[String]): Unit =
        val runId = args(0)
        val config = RunConfig(
            inputPath = DefaultInputPath,
            runId = runId,
            scratchPath = Paths.get(System.getProperty("java.io.tmpdir"), "biomed3471-codexrefactored", runId)
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

        println(s"Phase one completed: ${outputPath(config)}")
