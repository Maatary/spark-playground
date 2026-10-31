package biomed3471.claude

import sparkops.SchemaEnforcementOps.*
import biomed3471.claude.DataTypes.*
import biomed3471.claude.DataOps.*
import io.delta.implicits.*
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scribe.*

import scala.util.chaining.scalaUtilChainingOps

/**
 * ==The twin search of BIOMED-3471==
 *
 * The twin search counts the forward-only canonical relations that have a non-identical twin among the canonical relations with a backfill relation
 * occurrence. A forward-only canonical relation has a non-identical twin when the backfill states the same relation type between nodes with the same
 * normalized names, but with a homonym in place of one or both of its nodes. The twin search then finds the homonymous node pairs behind these
 * non-identical twins.
 *
 * The twin search uses these terms:
 *
 *   - A relation occurrence is one occurrence of a relation in one document, produced by one pipeline: the forward flow (BCE, Biomed Content
 *     Enrichment) or the backfill (MedScan).
 *   - A canonical relation is one relation type between two nodes in their roles. Many relation occurrences belong to one canonical relation. A
 *     forward-only canonical relation has relation occurrences from the forward flow only.
 *   - The graph's domain model gives each relation type one direction, directed or undirected. Section 2.2 of BioMed Data Identifier Schemes then
 *     builds the canonical relation ID from the relation type, the roles and the two node TERMite IDs. In a directed relation, such as Regulation,
 *     node 1 is the regulator and node 2 is the target. In an undirected relation, such as Binding, both nodes are partners, and the ID lists the
 *     node TERMite IDs of the two partners in lexicographic order, which means sorted as text.
 *   - A node URN is the full identifier of a node, such as `biodata:smallmol/Concept_700527820`. Its last part, `Concept_700527820`, is the node
 *     TERMite ID.
 *   - Each node has one name, which the input gives in `node1_name` or `node2_name`. Its normalized name is that name in lower case, with the spaces
 *     at both ends removed.
 *   - A homonym is a node whose normalized name is the same as the normalized name of another node.
 *   - The name key of a canonical relation is its relation type and the normalized names of its two nodes.
 *   - Two canonical relations are non-identical twins when they have the same name key but different canonical relation IDs. A twin pair is one pair
 *     of non-identical twins that the twin search finds. It holds a forward-flow relation, which is a forward-only canonical relation, and a backfill
 *     twin, which is a canonical relation with at least one backfill relation occurrence.
 *   - In step 5, each node of the forward-flow relation goes with a node of the backfill twin that has the same normalized name, by rules A to C of
 *     [[DataOps.nameMatchedNodePairTuples]]. A homonymous node pair is a forward-flow node and the backfill node that goes with it, when the two have
 *     different node URNs. A twin pair is resolved when the rules decide which forward-flow node goes with which backfill node, and unresolved when
 *     they cannot.
 *
 * The input is a list of relation occurrences. TwinSearchApp reads them, runs the five steps of [[DataOps]], and writes six Delta tables:
 *
 * {{{
 * relation occurrences, from the input
 *   step 1   the canonical relation ID of each relation occurrence
 *   step 2   one row for each canonical relation                     canonicalRelations
 *   step 3   the normalized names, and the name key
 *   step 4   the twin pairs, matched on the name key                 nonIdenticalTwinPairs
 *   step 5   every twin pair with the result of rules A to C         twinPairsWithNameMatchedNodePairTuples
 *            the homonymous node pairs of each resolved twin pair    homonymousNodePairs
 *            the number of twin pairs of each homonymous node pair   twinPairCountPerHomonymousNodePair
 *            the unresolved twin pairs                               twinPairsWithMultipleNameMatchedNodePairTuples
 * }}}
 *
 * Each table is read back after it is written, so that the next step starts from the saved table instead of computing the earlier steps again from
 * the input. At the end, TwinSearchApp prints five numbers: the twin pairs, the forward-only canonical relations with at least one non-identical
 * twin, the unresolved twin pairs, the entries of homonymousNodePairs, and the homonymous node pairs.
 *
 * Run it from the spark-playground root with a 4 GB heap. The only argument names the run folder under
 * `s3://cmdp-biomed-lakehouse-bucket-dev/d_okouya/BIOMED-3471/`, and a table that already exists there is replaced.
 *
 * {{{
 * scala-cli run . --main-class biomed3471.claude.TwinSearchApp --java-opt -Xmx4g -- 2026-10-23
 * }}}
 */
object TwinSearchApp:

    Logger.root
        .clearHandlers()
        .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
        .replace()

    /** Writes a DataFrame as a Delta table, replaces the table that already exists at the same path, columns included, and reads the table back. */
    def writeAndReadBackDeltaTable(df: DataFrame, tablePath: String): DataFrame =
        df
            .write
            .format("delta")
            .mode(SaveMode.Overwrite)
            .option("overwriteSchema", "true") // <-- without it, Delta keeps the columns of the table it replaces
            .save(tablePath)

        df.sparkSession.read.delta(tablePath)

    def makeSparkSession: SparkSession =
        SparkSession
            .builder()
            .appName("delta Application")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
            .config("spark.sql.shuffle.partitions", 8) // <-- 8 instead of the template's 12, the sizing for the laptop
            .master("local[*]")
            .getOrCreate()

    def main(args: Array[String]): Unit =

        val spark = makeSparkSession

        import spark.implicits.StringToColumn

        val relationOccurrencePath = "s3a://cmdp-biomed-lakehouse-bucket-dev/d_okouya/BIOMED-3471/wytze-relation-occurrences-delta"
        val runPath                = s"s3a://cmdp-biomed-lakehouse-bucket-dev/d_okouya/BIOMED-3471/${args(0)}"

        val canonicalRelationsPath                             = s"$runPath/canonicalRelations"
        val nonIdenticalTwinPairsPath                          = s"$runPath/nonIdenticalTwinPairs"
        val twinPairsWithNameMatchedNodePairTuplesPath         = s"$runPath/twinPairsWithNameMatchedNodePairTuples"
        val homonymousNodePairsPath                            = s"$runPath/homonymousNodePairs"
        val twinPairCountPerHomonymousNodePairPath             = s"$runPath/twinPairCountPerHomonymousNodePair"
        val twinPairsWithMultipleNameMatchedNodePairTuplesPath = s"$runPath/twinPairsWithMultipleNameMatchedNodePairTuples"

        val relationOccurrenceDS = spark
            .read
            .delta(relationOccurrencePath)
            .enforceAs[RelationOccurrence](lit(null))

        // Steps 1 and 2
        val canonicalRelationsDS = relationOccurrenceDS
            .pipe { computeCanonicalRelations }
            .pipe { ds => writeAndReadBackDeltaTable(ds.toDF(), canonicalRelationsPath) }
            .enforceAs[CanonicalRelation]($"canonicalId")

        // Steps 3 and 4
        val nonIdenticalTwinPairsDF = canonicalRelationsDS
            .pipe { computeNonIdenticalTwinPairs }
            .pipe { df => writeAndReadBackDeltaTable(df, nonIdenticalTwinPairsPath) }

        // Step 5
        val twinPairsWithNameMatchedNodePairTuplesDF = nonIdenticalTwinPairsDF
            .pipe { computeTwinPairsWithNameMatchedNodePairTuples }
            .pipe { df => writeAndReadBackDeltaTable(df, twinPairsWithNameMatchedNodePairTuplesPath) }

        val homonymousNodePairsDF = twinPairsWithNameMatchedNodePairTuplesDF
            .pipe { computeHomonymousNodePairs }
            .pipe { df => writeAndReadBackDeltaTable(df, homonymousNodePairsPath) }

        val twinPairCountPerHomonymousNodePairDF = homonymousNodePairsDF
            .pipe { computeTwinPairCountPerHomonymousNodePair }
            .pipe { df => writeAndReadBackDeltaTable(df, twinPairCountPerHomonymousNodePairPath) }

        val twinPairsWithMultipleNameMatchedNodePairTuplesDF = twinPairsWithNameMatchedNodePairTuplesDF
            .pipe { computeTwinPairsWithMultipleNameMatchedNodePairTuples }
            .pipe { df => writeAndReadBackDeltaTable(df, twinPairsWithMultipleNameMatchedNodePairTuplesPath) }

        val forwardOnlyCanonicalRelationWithTwinCount = nonIdenticalTwinPairsDF
            .select($"forwardCanonicalId")
            .distinct()
            .count()

        println(s"twinPairCount=${nonIdenticalTwinPairsDF.count()}")
        println(s"forwardOnlyCanonicalRelationWithTwinCount=$forwardOnlyCanonicalRelationWithTwinCount")
        println(s"unresolvedTwinPairCount=${twinPairsWithMultipleNameMatchedNodePairTuplesDF.count()}")
        println(s"homonymousNodePairEntryCount=${homonymousNodePairsDF.count()}")
        println(s"homonymousNodePairCount=${twinPairCountPerHomonymousNodePairDF.count()}")

        spark.stop()
