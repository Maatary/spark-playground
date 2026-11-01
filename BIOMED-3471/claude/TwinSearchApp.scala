package biomed3471.claude

import sparkops.SchemaEnforcementOps.*
import biomed3471.claude.DataTypes.*
import biomed3471.claude.DataOps.*
import io.delta.implicits.*
import org.apache.spark.sql.functions.*
import io.github.pashashiz.spark_encoders.TypedEncoder.*
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scribe.*

import scala.util.chaining.scalaUtilChainingOps

/**
 * ==The twin search of BIOMED-3471==
 *
 * The twin search counts the forward-only canonical relations that have a non-identical twin among the canonical relations supported by at least one
 * backfill relation occurrence. A forward-only canonical relation has a non-identical twin when the backfill states the same relation type between
 * nodes with the same normalized names, but with a homonym in place of one or both of its nodes. The twin search then finds the homonymous node pairs
 * behind these non-identical twins, and lists each one with the twin pairs it appears in.
 *
 * The twin search uses these terms:
 *
 *   - A relation occurrence is one occurrence of a relation in one document, produced by one pipeline: the forward flow (BCE, Biomed Content
 *     Enrichment) or the backfill (MedScan).
 *   - A canonical relation is one relation type between two nodes in their roles. Many relation occurrences belong to one canonical relation. A
 *     forward-only canonical relation is supported only by relation occurrences from the forward flow.
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
 *     twin, which is a canonical relation supported by at least one backfill relation occurrence.
 *   - A homonymous node pair is composed of a node connected by a forward-flow relation and a node connected in its place by one of its backfill
 *     twins. Both nodes have the same normalized name but different node URNs. Step 5 finds them with rules A to C of
 *     [[DataOps.nameMatchedNodePairTuples]], which pair each forward-flow node with a backfill node of the same normalized name. When the relation
 *     type of a twin pair is undirected and its four nodes are four different nodes with the same normalized name, all four are homonyms of one
 *     another, and each backfill node can stand in place of either forward-flow node, so the twin pair connects to four homonymous node pairs, each
 *     forward-flow node with each backfill node.
 *
 * TwinSearchApp reads the relation occurrences from the input and runs the five steps of [[DataOps]] in order. Steps 1 and 2 turn the relation
 * occurrences into canonical relations, steps 3 and 4 find the twin pairs, and step 5 finds the homonymous node pairs in each twin pair. The results
 * of steps 2, 4 and 5 are saved in five Delta tables:
 *
 *   - Step 1 builds the canonical relation ID of each relation occurrence.
 *   - Step 2 combines the relation occurrences of each canonical relation into one row. These rows are saved in `canonicalRelations`.
 *   - Step 3 adds a name key to each canonical relation.
 *   - Step 4 matches each forward-only canonical relation with every canonical relation that is supported by a backfill relation occurrence and has
 *     the same name key. Each match is a twin pair, and the twin pairs are saved in `nonIdenticalTwinPairs`.
 *   - Step 5 finds the homonymous node pairs of each twin pair. Its results are saved in three tables:
 *     - `twinPairsWithNameMatchedNodePairTuples` holds every twin pair with the result of rules A to C: one way to pair its nodes, or both ways when
 *       its relation type is undirected and its four nodes are four different nodes with the same normalized name.
 *     - `homonymousNodePairsPerTwinPair` holds the homonymous node pairs of each twin pair.
 *     - `twinPairCountPerHomonymousNodePair` holds each homonymous node pair once, with the number of twin pairs that connect to it.
 *
 * Each table is read back after it is written, so that the next step starts from the saved table instead of computing the earlier steps again from
 * the input. At the end, TwinSearchApp prints four numbers: the twin pairs, the forward-only canonical relations with at least one non-identical
 * twin, the entries of homonymousNodePairsPerTwinPair, and the homonymous node pairs.
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

        val canonicalRelationsPath                     = s"$runPath/canonicalRelations"
        val nonIdenticalTwinPairsPath                  = s"$runPath/nonIdenticalTwinPairs"
        val twinPairsWithNameMatchedNodePairTuplesPath = s"$runPath/twinPairsWithNameMatchedNodePairTuples"
        val homonymousNodePairsPerTwinPairPath         = s"$runPath/homonymousNodePairsPerTwinPair"
        val twinPairCountPerHomonymousNodePairPath     = s"$runPath/twinPairCountPerHomonymousNodePair"

        val relationOccurrenceDS = spark
            .read
            .delta(relationOccurrencePath)
            .enforceAs[RelationOccurrence](lit("unavailable")) // <-- a relation occurrence has no ID in the input

        // Steps 1 and 2
        val canonicalRelationsDS = relationOccurrenceDS
            .pipe { computeCanonicalRelations }
            .pipe { ds => writeAndReadBackDeltaTable(ds.toDF(), canonicalRelationsPath) }
            .enforceAs[CanonicalRelation]($"canonicalId")

        // Steps 3 and 4
        val nonIdenticalTwinPairsDS = canonicalRelationsDS
            .pipe { computeNonIdenticalTwinPairs }
            .pipe { ds => writeAndReadBackDeltaTable(ds.toDF(), nonIdenticalTwinPairsPath) }
            .enforceAs[TwinPairRecord](to_json(struct($"forwardCanonicalId", $"backfillCanonicalId")))

        // Step 5
        val twinPairsWithNameMatchedNodePairTuplesDS = nonIdenticalTwinPairsDS
            .pipe { computeTwinPairsWithNameMatchedNodePairTuples }
            .pipe { ds => writeAndReadBackDeltaTable(ds.toDF(), twinPairsWithNameMatchedNodePairTuplesPath) }
            .enforceAs[TwinPairWithNameMatchedNodePairTuples](to_json(struct($"forwardCanonicalId", $"backfillCanonicalId")))

        val homonymousNodePairsPerTwinPairDS = twinPairsWithNameMatchedNodePairTuplesDS
            .pipe { computeHomonymousNodePairsPerTwinPair }
            .pipe { ds => writeAndReadBackDeltaTable(ds.toDF(), homonymousNodePairsPerTwinPairPath) }
            .enforceAs[HomonymousNodePairWithTwinPair](to_json(struct($"forwardCanonicalId", $"backfillCanonicalId")))

        val twinPairCountPerHomonymousNodePairDS = homonymousNodePairsPerTwinPairDS
            .pipe { computeTwinPairCountPerHomonymousNodePair }
            .pipe { ds => writeAndReadBackDeltaTable(ds.toDF(), twinPairCountPerHomonymousNodePairPath) }
            .enforceAs[HomonymousNodePairWithTwinCount](to_json(struct($"forwardNodeUri", $"backfillNodeUri")))

        val forwardOnlyCanonicalRelationTwinCount = nonIdenticalTwinPairsDS
            .select($"forwardCanonicalId")
            .distinct()
            .count()

        println(s"twinPairCount=${nonIdenticalTwinPairsDS.count()}")
        println(s"forwardOnlyCanonicalRelationTwinCount=$forwardOnlyCanonicalRelationTwinCount")
        println(s"homonymousNodePairsPerTwinPairCount=${homonymousNodePairsPerTwinPairDS.count()}")
        println(s"homonymousNodePairCount=${twinPairCountPerHomonymousNodePairDS.count()}")

        spark.stop()
