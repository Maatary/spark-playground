package biomed3471.codexdeprecated

import biomed3471.codexdeprecated.DataOps.*
import biomed3471.codexdeprecated.IOOps.*

import java.nio.file.Files
import scala.util.chaining.*

/** Entry point for the deprecated BIOMED-3471 steps 1–5 pipeline. */
object TwinSearchApp:

    /**
     * Finds the non-identical twins of forward-only canonical relations in backfill,
     * then identifies the homonymous node pairs to investigate. The backfill search
     * includes both <code>backfillOnly</code> and <code>shared</code>.
     *
     * Homonymous node pairs share the same name but have different identifiers.
     * To find them within each established twin pair, Step 5 determines which
     * forward-flow node to compare with which backfill node: the node pairing.
     * It retains both comparisons in each complete result, including identical
     * name matches, then selects homonymous entries from twins with one result.
     * See <a href="README.md#from-twins-to-node-pairs">from twins to node pairs</a>
     * for how the input fields, position choices and complete results connect.
     * The <a href="README.md#conceptual-procedure-for-undirected-relations">
     * conceptual procedure for undirected relations</a> sets out the four cases
     * used to determine their homonymous node pairs.
     *
     * Delivers six Delta tables, each answering a distinct question:
     * <ul>
     * <li><code>canonicalRelations</code>: which canonical relations occur in the
     * forward flow, the backfill, or both?</li>
     * <li><code>nonIdenticalTwinPairs</code>: which forward-only canonical relations
     * have non-identical twins in backfill?</li>
     * <li><code>twinPairsWithNameMatchedNodePairTuples</code>: what are the corresponding
     * nodes in each twin pair, including both identical and homonymous node pairs?
     * Retains one complete tuple when resolved, or two alternatives when the node
     * correspondence is unresolved. Each tuple contains two node pairs.</li>
     * <li><code>homonymousNodePairs</code>: which different nodes share the same name,
     * and in which twins and roles were they found? Retains detailed entries from
     * resolved twins, including repeated entries.</li>
     * <li><code>homonymousNodePairsWithTwinCount</code>: which node pairs need
     * investigation, and how many distinct complete resolved non-identical twin
     * pairs contain each oriented node URI pair?</li>
     * <li><code>twinPairsWithMultipleNameMatchedNodePairTuples</code>: for which
     * established twins does the node correspondence remain unresolved, and what
     * are the two complete alternatives?</li>
     * </ul>
     *
     * See the <a href="README.md#six-saved-delta-tables">README's six-table overview,
     * schemas and examples</a> for each table's row contents and purpose.
     * <code>forwardOnly</code>, <code>backfillOnly</code> and <code>shared</code> are
     * selections from <code>canonicalRelations</code>, not additional saved tables.
     *
     * Reserves a new personal DEV S3 run and writes the six agreed Delta
     * datasets sequentially. Materialized canonical relations, twins and detailed
     * node pairs are read back to avoid repeating their upstream processing.
     *
     * <code>nonIdenticalTwinsAlsoWithinForwardFlow</code> records whether the backfill
     * twin belongs to <code>shared</code>: both canonical relations then also occur
     * within forward flow.
     * Name-matched node-pair tuples preserve directed roles and repeated partners.
     * RuntimeOps and the commit marker remain disabled. No vocabulary
     * classification, investigation percentages, full-data collection or cache
     * is performed. Spark closes on failure, and success is reported only after
     * all writes and Spark shutdown complete successfully.
     *
     * @param args --run <YYYY-MM-DD> [--input <delta-location>] [--scratch <directory>]
     */
    def main(args: Array[String]): Unit =
        val config = parseArguments(args)
        Files.createDirectories(config.scratchPath)

        // RuntimeOps checks and monitoring remain disabled; heap size is a launch option.
        val spark = makeSparkSession(config)
        try
            spark.sparkContext.setLogLevel("WARN")
            beginRun(spark, config)

            val canonicalRelations = readRelationOccurrences(spark, config)
                .pipe(withCanonicalId)
                .pipe(aggregateCanonicalRelations)
                .pipe(data => writeAndReadDataset(data, config, "canonicalRelations"))

            val nonIdenticalTwinPairs = canonicalRelations
                .pipe(separateCanonicalRelations)
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

            // _DATASETS_COMMITTED remains disabled alongside RuntimeOps.
        finally spark.stop()

        println(s"Deprecated twin search completed: ${outputPath(config)}")
