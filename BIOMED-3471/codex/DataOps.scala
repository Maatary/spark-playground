package biomed3471.codex

import biomed3471.codex.DataTypes.{BackfillSource, ForwardFlowSource}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.*

import scala.util.chaining.*

object DataOps:

    /**
     * Constructs the native Spark expression that restores the canonicalId omitted from the relation-occurrence export.
     *
     * Extracts the relation type and the node TERMite IDs, the final components of the node URNs. For undirected, the input already orders partners
     * lexicographically by node TERMite ID. For node1_to_node2, node 1 is regulator and node 2 is target. Names and node types do not enter the
     * canonical relation ID. The Scala function builds Spark expressions and does not evaluate data.
     *
     * @param relationshipType
     *   the exported relationship_type column
     * @param direction
     *   the exported direction column
     * @param node1Urn
     *   the first participating node's full node URN column
     * @param node2Urn
     *   the second participating node's full node URN column
     * @return
     *   the native Spark expression producing the canonical relation ID
     */
    def reconstructCanonicalId(
        relationshipType: Column,
        direction       : Column,
        node1Urn        : Column,
        node2Urn        : Column
    ): Column =
        val relationType   = lower(substring_index(relationshipType, ":", -1))
        val node1TermiteId = substring_index(node1Urn, "/", -1)
        val node2TermiteId = substring_index(node2Urn, "/", -1)

        when(
          direction === "undirected",
          concat(
            lit("biodata:"),
            relationType,
            lit("/partner/"),
            node1TermiteId,
            lit("/partner/"),
            node2TermiteId
          )
        ).otherwise(
          concat(
            lit("biodata:"),
            relationType,
            lit("/regulator/"),
            node1TermiteId,
            lit("/target/"),
            node2TermiteId
          )
        )

    /**
     * Adds canonicalId to each relation occurrence before aggregation.
     *
     * Applies the native identifier expression to the existing relation type, direction and node URN columns. Retains all input fields unchanged.
     * This transformation starts no action.
     *
     * @param canonicalRelationFields
     *   the input relation/node fields
     * @return
     *   the same input fields with canonicalId added
     */
    def withCanonicalId(canonicalRelationFields: DataFrame): DataFrame =
        canonicalRelationFields.withColumn(
          "canonicalId",
          reconstructCanonicalId(
            col("relationship_type"),
            col("direction"),
            col("node1_urn"),
            col("node2_urn")
          )
        )

    /**
     * Groups relation occurrences into one entry per canonical relation.
     *
     * First constructs canonicalId for each relation occurrence, then groups by that ID and the six relation/node fields. The model fixes those
     * fields for each canonicalId, so including them keeps them in the result without changing the groups. Each name remains attached to its node
     * URN. Undirected partners already follow the model's lexicographic ordering by node TERMite ID. Collects the occurrences' sources into a sorted
     * array without duplicates named sources. The grouping lists canonicalId first, followed by the six relation/node fields; aggregation adds
     * sources last.
     *
     * @param relationOccurrences
     *   exported relation occurrences with node fields and source
     * @return
     *   canonicalId, the six relation/node fields, and the source set
     */
    def aggregateCanonicalRelations(relationOccurrences: DataFrame): DataFrame =
        relationOccurrences
            .pipe(withCanonicalId)
            .groupBy(
              "canonicalId",
              "relationship_type",
              "direction",
              "node1_urn",
              "node1_name",
              "node2_urn",
              "node2_name"
            )
            .agg(array_sort(collect_set(col("source"))).as("sources"))

    extension (nodeName: Column)

        /**
         * Produces the normalized node name by trimming spaces at both ends and converting it to lowercase.
         */
        private def normalizedNodeName: Column = lower(trim(nodeName))

    extension (canonicalRelations: DataFrame)

        /**
         * Selects canonical relations whose occurrences all come from the forward flow.
         */
        def forwardOnly: DataFrame =
            canonicalRelations.where(
              col("sources") === array(lit(ForwardFlowSource))
            )

        /**
         * Selects canonical relations with a backfill occurrence, including those shared with the forward flow.
         */
        def backfill: DataFrame =
            canonicalRelations.where(
              array_contains(col("sources"), BackfillSource)
            )

        /**
         * Adds the name key used to find non-identical twins, stored in comparisonKey.
         *
         * Combines relation type and normalized node names. The relation type decides whether the relation is directed or undirected. Directed names
         * retain regulator/target order. Undirected names are sorted because their positions can differ between canonical relations with different
         * node TERMite IDs. Only the name key sorts names: the original node URNs, their associated names and every other input field remain
         * unchanged.
         *
         * The receiver contains canonical relations retaining their node fields.
         * @return
         *   the canonical relations with comparisonKey added
         */
        def withComparisonKey: DataFrame =
            val nodeNames              = array(col("node1_name").normalizedNodeName, col("node2_name").normalizedNodeName)
            val comparisonKeyNodeNames = when(col("direction") === "undirected", array_sort(nodeNames)).otherwise(nodeNames)

            canonicalRelations
                .withColumn("comparisonKey", struct(col("relationship_type"), comparisonKeyNodeNames.as("nodeNames")))

    /**
     * Finds each forward-only canonical relation's non-identical twins in backfill.
     *
     * Adds comparisonKey, then selects the forward-only relations and all relations with a backfill occurrence. Joins these selections on the name
     * key, comparing relation type and corresponding normalized graph names. A shared relation is eligible because its presence in forward flow does
     * not remove it from backfill. A forward-only relation has no backfill occurrence, so the joined relations have different canonical relation IDs.
     * One entry per canonical relation on each side means each twin pair appears once, without another step to remove duplicates or look up sources.
     *
     * Sets nonIdenticalTwinsAlsoWithinForwardFlow to true when the backfill twin also has forward-flow occurrences. Both canonical relations in that
     * twin pair then occur in the forward flow.
     *
     * @param canonicalRelations
     *   one entry per canonicalId with its source set
     * @return
     *   one entry per twin pair, with both canonical relation IDs, nonIdenticalTwinsAlsoWithinForwardFlow and the forwardTwin and backfillTwin
     *   structs
     */
    def findNonIdenticalTwinPairs(canonicalRelations: DataFrame): DataFrame =
        val canonicalRelationsWithKey = canonicalRelations.withComparisonKey

        canonicalRelationsWithKey.forwardOnly
            .as("forward")
            .join(
              canonicalRelationsWithKey.backfill.as("backfill"),
              col("forward.comparisonKey") === col("backfill.comparisonKey"),
              "inner"
            )
            .select(
              col("forward.canonicalId").as("forwardCanonicalId"),
              col("backfill.canonicalId").as("backfillCanonicalId"),
              array_contains(col("backfill.sources"), ForwardFlowSource)
                  .as("nonIdenticalTwinsAlsoWithinForwardFlow"),
              struct(col("forward.*")).as("forwardTwin"),
              struct(col("backfill.*")).as("backfillTwin")
            )

    /**
     * Constructs the struct used to store a name-matched node pair.
     *
     * The matching rules establish the same normalized name for both nodes. The struct contains role, forwardNodeUri, backfillNodeUri and
     * matchedNodeName, in that order.
     *
     * @param role
     *   the column containing regulator, target or partner
     * @param forwardNodeUri
     *   the chosen forward-flow node's full node URN
     * @param backfillNodeUri
     *   the chosen backfill node's full node URN
     * @param matchedNodeName
     *   their matching normalized graph name
     * @return
     *   one name-matched node pair as a native Spark struct
     */
    private def nameMatchedNodePair(
        role           : Column,
        forwardNodeUri : Column,
        backfillNodeUri: Column,
        matchedNodeName: Column
    ): Column =
        struct(
          role.as("role"),
          forwardNodeUri.as("forwardNodeUri"),
          backfillNodeUri.as("backfillNodeUri"),
          matchedNodeName.as("matchedNodeName")
        )

    /**
     * Finds the name-matched node pairs for one established twin pair.
     *
     * Directed twins pair regulator with regulator and target with target. Undirected twins use normalized names and node identity, including
     * self-reference. When an undirected twin pair has four distinct nodes sharing one normalized name, each forward-flow node forms a homonymous
     * pair with each backfill node, so all four pairs are retained.
     *
     * Defines the straight and crossed pairs once. Straight pairs forward nodes 1 and 2 with backfill nodes 1 and 2; crossed pairs them with
     * backfill nodes 2 and 1. Returns two records in forward-node order, or all four with the straight pairs followed by the crossed pairs. Repeated
     * records remain. The early self-reference branch covers repeated nodes with or without a shared node.
     *
     * Step 4 guarantees matching normalized names for every node pair retained by these rules, so matchedNodeName comes from the forward node.
     *
     * @param forwardTwin
     *   the forward-flow canonical relation struct
     * @param backfillTwin
     *   its backfill twin's canonical relation struct
     * @return
     *   one array containing two or four name-matched node pairs
     */
    private def nameMatchedNodePairs(forwardTwin: Column, backfillTwin: Column): Column =
        val forwardNode1Urn             = forwardTwin.getField("node1_urn")
        val forwardNode2Urn             = forwardTwin.getField("node2_urn")
        val backfillNode1Urn            = backfillTwin.getField("node1_urn")
        val backfillNode2Urn            = backfillTwin.getField("node2_urn")
        val forwardNode1NormalizedName  = forwardTwin.getField("node1_name").normalizedNodeName
        val forwardNode2NormalizedName  = forwardTwin.getField("node2_name").normalizedNodeName
        val backfillNode1NormalizedName = backfillTwin.getField("node1_name").normalizedNodeName

        val undirected                      = forwardTwin.getField("direction") === "undirected"
        val hasSelfReferencingRelation      = forwardNode1Urn === forwardNode2Urn || backfillNode1Urn === backfillNode2Urn
        val forwardNodesHaveDifferentNames  = forwardNode1NormalizedName =!= forwardNode2NormalizedName
        val namesInSameOrderInBothRelations = forwardNode1NormalizedName === backfillNode1NormalizedName
        val nodeSharedAtSamePosition        = forwardNode1Urn === backfillNode1Urn || forwardNode2Urn === backfillNode2Urn
        val nodeSharedAtOtherPosition       = forwardNode1Urn === backfillNode2Urn || forwardNode2Urn === backfillNode1Urn

        val forwardNode1Role = when(undirected, lit("partner")).otherwise(lit("regulator"))
        val forwardNode2Role = when(undirected, lit("partner")).otherwise(lit("target"))

        val straightNodePairs = array(
          nameMatchedNodePair(forwardNode1Role, forwardNode1Urn, backfillNode1Urn, forwardNode1NormalizedName),
          nameMatchedNodePair(forwardNode2Role, forwardNode2Urn, backfillNode2Urn, forwardNode2NormalizedName)
        )
        val crossedNodePairs  = array(
          nameMatchedNodePair(lit("partner"), forwardNode1Urn, backfillNode2Urn, forwardNode1NormalizedName),
          nameMatchedNodePair(lit("partner"), forwardNode2Urn, backfillNode1Urn, forwardNode2NormalizedName)
        )

        when(
          undirected,
          when(hasSelfReferencingRelation, straightNodePairs)
              .when(forwardNodesHaveDifferentNames && namesInSameOrderInBothRelations, straightNodePairs)
              .when(forwardNodesHaveDifferentNames, crossedNodePairs)
              .when(nodeSharedAtSamePosition, straightNodePairs)
              .when(nodeSharedAtOtherPosition, crossedNodePairs)
              .otherwise(concat(straightNodePairs, crossedNodePairs))
        ).otherwise(straightNodePairs)

    extension (nonIdenticalTwinPairs: DataFrame)

        /**
         * Adds nameMatchedNodePairs to each non-identical twin pair.
         *
         * The column contains one array of two name-matched node pairs, or four when all four undirected nodes are distinct and share one normalized
         * name. Identical pairs and repeated records remain. Each twin pair stays one row with both canonical relation structs, the flag and all
         * other existing columns unchanged.
         *
         * Finds pairs by directed roles or undirected normalized names and node identity, with the early self-reference shortcut. See <a
         * href="README.md#direct-domain-rules">the domain rules</a> and <a href="README.md#from-domain-rules-to-implementation">their
         * implementation</a>.
         *
         * The receiver contains one row per established non-identical twin pair.
         *
         * @return
         *   each twin pair with its nameMatchedNodePairs array
         */
        def withNameMatchedNodePairs: DataFrame =
            nonIdenticalTwinPairs.withColumn(
              "nameMatchedNodePairs",
              nameMatchedNodePairs(col("forwardTwin"), col("backfillTwin"))
            )

    extension (twinPairsWithNameMatchedNodePairs: DataFrame)

        /**
         * Selects homonymous node pairs with the twin pairs in which they were found.
         *
         * Turns each name-matched node pair into a row and keeps pairs whose node URNs differ. When all four undirected nodes are distinct and share
         * one normalized name, all four pairs contribute to the detail. Selects node URN/name columns first, followed by role, both canonical
         * relation IDs and the flag.
         *
         * For two self-referencing relations, each relation uses its node twice, so both records remain in the detail; directed records retain their
         * different roles. No distinct operation is applied. Identical pairs remain available in the full twin-pair result.
         *
         * The receiver contains twin pairs with both canonical relations and a nameMatchedNodePairs array.
         *
         * @return
         *   homonymous node-pair entries with the exact seven-column detail schema
         */
        def selectHomonymousNodePairsPerTwinPair: DataFrame =
            twinPairsWithNameMatchedNodePairs
                .select(
                  col("forwardCanonicalId"),
                  col("backfillCanonicalId"),
                  col("nonIdenticalTwinsAlsoWithinForwardFlow"),
                  explode(col("nameMatchedNodePairs")).as("nameMatchedNodePair")
                )
                .where(
                  col("nameMatchedNodePair.forwardNodeUri") =!= col("nameMatchedNodePair.backfillNodeUri")
                )
                .select(
                  col("nameMatchedNodePair.forwardNodeUri").as("forwardNodeUri"),
                  col("nameMatchedNodePair.backfillNodeUri").as("backfillNodeUri"),
                  col("nameMatchedNodePair.matchedNodeName").as("matchedNodeName"),
                  col("nameMatchedNodePair.role").as("role"),
                  col("forwardCanonicalId"),
                  col("backfillCanonicalId"),
                  col("nonIdenticalTwinsAlsoWithinForwardFlow")
                )

    extension (homonymousNodePairsPerTwinPair: DataFrame)

        /**
         * Aggregates homonymous node pairs and counts the distinct twin pairs that give each pair.
         *
         * Groups detail by the forward-flow node URN, backfill node URN and their normalized name. The node identities fix that name, so grouping on
         * it keeps the name without splitting a group or choosing one value from it. Counts distinct (forwardCanonicalId, backfillCanonicalId) pairs,
         * so repeated entries from one twin pair count once and several backfill twins of one forward relation count separately. Role and the
         * forward-flow flag do not split groups.
         *
         * Selects node URNs, normalized name and the LongType twin-pair count in that order.
         *
         * The receiver homonymousNodePairsPerTwinPair contains selected detailed entries, including repeated entries.
         *
         * @return
         *   one row per forward-flow/backfill node pair, its name and nonIdenticalTwinPairCount
         */
        def countTwinPairsPerHomonymousNodePair: DataFrame =
            homonymousNodePairsPerTwinPair
                .groupBy("forwardNodeUri", "backfillNodeUri", "matchedNodeName")
                .agg(
                  count_distinct(
                    struct(col("forwardCanonicalId"), col("backfillCanonicalId"))
                  ).as("nonIdenticalTwinPairCount")
                )
                .select(
                  "forwardNodeUri",
                  "backfillNodeUri",
                  "matchedNodeName",
                  "nonIdenticalTwinPairCount"
                )
