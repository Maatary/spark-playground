package biomed3471.codex

import java.nio.file.Path

/** Data and configuration for the BIOMED-3471 twin search. */
object DataTypes:

    /** Delta input location, output date, and local scratch directory. */
    final case class RunConfig(inputPath: String, runId: String, scratchPath: Path)

    val ForwardFlowSource = "Elsevier ML NLP"
    val BackfillSource    = "Elsevier Medscan NLP"

    val DefaultInputPath =
        "s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/BIOMED-3471/wytze-relation-occurrences/"

    val OutputPrefix = "s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/BIOMED-3471/"

    val DatasetNames = List(
        "canonicalRelations",
        "nonIdenticalTwinPairs",
        "twinPairsWithNameMatchedNodePairTuples",
        "homonymousNodePairs",
        "homonymousNodePairsWithTwinCount",
        "twinPairsWithMultipleNameMatchedNodePairTuples"
    )
