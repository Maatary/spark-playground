package biomed3471.codexdeprecated

import org.apache.spark.sql.DataFrame

import java.nio.file.Path
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicLong

/** Data and configuration for the deprecated BIOMED-3471 twin search. */
object DataTypes:

    /**
     * One forward position paired with one backfill position.
     *
     * Position 1 selects node1_urn and node1_name from the corresponding
     * forward/backfill struct; position 2 selects node2_urn and node2_name.
     * These choices identify which nodes to compare. They contain no node data
     * and are never stored as fields of a distributed node-pair result.
     *
     * @param forwardPosition the participating position in the forward relation
     * @param backfillPosition the participating position in its backfill twin
     */
    final case class PositionPair(forwardPosition: Int, backfillPosition: Int)

    /**
     * Two position pairs using each of the two positions on both sides once.
     *
     * Choosing a backfill position for forward position 1 fixes the other pair.
     * Reading the node fields at those positions and establishing matching names
     * produces one complete nameMatchedNodePairTuple. Two different position
     * tuples can produce the same complete node-pair result.
     */
    type PositionPairTuple = (PositionPair, PositionPair)

    /** The three disjoint source groups, retaining all canonical-relation fields. */
    final case class CanonicalRelationGroups(
        forwardOnly: DataFrame,
        shared: DataFrame,
        backfillOnly: DataFrame
    )

    /** Delta input location, output date, and local scratch directory. */
    final case class RunConfig(inputPath: String, runId: String, scratchPath: Path)

    /** Runtime monitor state; operations remain in RuntimeOps. */
    final case class MemoryMonitor(
        scheduler: ScheduledExecutorService,
        peakResidentBytes: AtomicLong,
        reportPath: Path,
        processId: Long
    )

    val straightPositionPairTuple: PositionPairTuple =
        (PositionPair(1, 1), PositionPair(2, 2))

    val crossedPositionPairTuple: PositionPairTuple =
        (PositionPair(1, 2), PositionPair(2, 1))

    val ForwardFlowSource = "Elsevier ML NLP"
    val BackfillSource    = "Elsevier Medscan NLP"

    val DefaultInputPath =
        "s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/BIOMED-3471/wytze-relation-occurrences/"

    val OutputPrefix = "s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/BIOMED-3471/deprecated/"

    val GiB                   = 1024L * 1024L * 1024L
    val MaximumHeapBytes      = 2L * GiB
    val RuntimeCeilingBytes   = 4L * GiB
    val ResidentStopBytes     = RuntimeCeilingBytes - 512L * 1024L * 1024L
    val RequiredJvmOptions    = List(
        "-XX:MaxDirectMemorySize=256m",
        "-XX:MaxMetaspaceSize=384m",
        "-XX:ReservedCodeCacheSize=128m",
        "-Xss512k"
    )

    val DatasetNames = List(
        "canonicalRelations",
        "nonIdenticalTwinPairs",
        "twinPairsWithNameMatchedNodePairTuples",
        "homonymousNodePairs",
        "homonymousNodePairsWithTwinCount",
        "twinPairsWithMultipleNameMatchedNodePairTuples"
    )
