package biomed3471.claude

import biomed3471.claude.DataOps.normalizedName
import biomed3471.claude.DataTypes.*
import io.delta.implicits.*
import io.github.pashashiz.spark_encoders.TypedEncoder
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.*
import scribe.*

import scala.util.chaining.scalaUtilChainingOps

/**
 * == The duplicate node names of BIOMED-3471, against the results of the twin search ==
 *
 * The list of duplicate node names attached to BIOMED-3471 gives names that more than one node has, with the entity types
 * of those nodes. The twin search finds a duplicate name only when it creates a non-identical twin: the forward flow states
 * a relation on one node, and the backfill states the same relation type, with the same normalized names, on the other
 * node. For each name of the list, DuplicateNamesApp says whether the twin search finds it, and if not, why. Like the twin
 * search, it compares each name by its normalized name, which is the name in lower case with the spaces at both ends
 * removed. It puts each name into one of four groups:
 *
 * {{{
 * in twinPairCountPerHomonymousNodePair               the twin search finds the name in a homonymous node pair
 * no node in canonicalRelations                       no node of the input has the name
 * one node in canonicalRelations                      exactly one node of the input has the name
 * several nodes in canonicalRelations, no twin pair   the name is a duplicate in the input, but it creates no twin pair
 * }}}
 *
 * DuplicateNamesApp reads canonicalRelations and twinPairCountPerHomonymousNodePair of the twin search run 2026-10-31, and
 * writes one row for each name of the list in duplicateNamesWithGroup, in the same run folder. At the end, it prints the
 * number of names in each group and in total.
 *
 * {{{
 * scala-cli run . --main-class biomed3471.claude.DuplicateNamesApp --java-opt -Xmx4g
 * }}}
 */
object DuplicateNamesApp:

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
        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        val duplicateNodeNamesPath                 = "s3a://cmdp-biomed-lakehouse-bucket-dev/d_okouya/BIOMED-3471/inputs/duplicates.csv"
        val runPath                                = "s3a://cmdp-biomed-lakehouse-bucket-dev/d_okouya/BIOMED-3471/2026-10-31"
        val canonicalRelationsPath                 = s"$runPath/canonicalRelations"
        val twinPairCountPerHomonymousNodePairPath = s"$runPath/twinPairCountPerHomonymousNodePair"
        val duplicateNamesWithGroupPath            = s"$runPath/duplicateNamesWithGroup"

        val duplicateNodeNameDS =
            spark
                .read
                .option("header", "true")
                .schema(TypedEncoder[DuplicateNodeName].encoder.schema)
                .csv(duplicateNodeNamesPath)
                .as[DuplicateNodeName]

        val canonicalRelationsDF                 = spark.read.delta(canonicalRelationsPath)
        val twinPairCountPerHomonymousNodePairDF = spark.read.delta(twinPairCountPerHomonymousNodePairPath)

        // One row for each name of the list, in normalized form, with the entity types that the list gives its nodes
        val duplicateNamesDF =
            duplicateNodeNameDS
                .select(
                    explode(
                        array(
                            struct($"name1".as("name"), $"entitytype1".as("entityType")),
                            struct($"name2".as("name"), $"entitytype2".as("entityType"))
                        )
                    ).as("duplicateNodeName")
                )
                .groupBy(normalizedName($"duplicateNodeName.name").as("normalizedName"))
                .agg(array_sort(collect_set($"duplicateNodeName.entityType")).as("entityTypes"))

        // One row for each normalized name of the input, with the nodes that have it
        val nodesByNameDF =
            canonicalRelationsDF
                .select(
                    explode(
                        array(
                            struct($"node1_urn".as("nodeUrn"), $"node1_name".as("nodeName")),
                            struct($"node2_urn".as("nodeUrn"), $"node2_name".as("nodeName"))
                        )
                    ).as("node")
                )
                .select($"node.nodeUrn", normalizedName($"node.nodeName").as("normalizedName"))
                .distinct()
                .groupBy($"normalizedName")
                .agg(
                    count(lit(1)).as("nodeCount"),
                    array_sort(collect_list($"nodeUrn")).as("nodeUrns")
                )

        // The names of the homonymous node pairs that the twin search finds
        val homonymousNodePairNamesDF =
            twinPairCountPerHomonymousNodePairDF
                .select($"matchedNodeName")
                .distinct()

        // One row for each name of the list, with its group
        val duplicateNamesWithGroupDF =
            duplicateNamesDF
                .join(nodesByNameDF, duplicateNamesDF("normalizedName") === nodesByNameDF("normalizedName"), "left")
                .join(homonymousNodePairNamesDF, duplicateNamesDF("normalizedName") === homonymousNodePairNamesDF("matchedNodeName"), "left")
                .select(
                    duplicateNamesDF("normalizedName"),
                    when(homonymousNodePairNamesDF("matchedNodeName").isNotNull, lit("in twinPairCountPerHomonymousNodePair"))
                        .when(nodesByNameDF("nodeCount").isNull, lit("no node in canonicalRelations"))
                        .when(nodesByNameDF("nodeCount") === 1, lit("one node in canonicalRelations"))
                        .otherwise(lit("several nodes in canonicalRelations, no twin pair"))
                        .as("group"),
                    duplicateNamesDF("entityTypes"),
                    nodesByNameDF("nodeCount"),
                    nodesByNameDF("nodeUrns")
                )
                .pipe { df => writeAndReadBackDeltaTable(df, duplicateNamesWithGroupPath) }

        val duplicateNameCountByGroup =
            duplicateNamesWithGroupDF
                .groupBy($"group")
                .agg(count(lit(1)).as("duplicateNameCount"))
                .orderBy(asc("group"))

        duplicateNameCountByGroup.show(false)

        println(s"duplicateNameCount=${duplicateNamesWithGroupDF.count()}")

        spark.stop()
