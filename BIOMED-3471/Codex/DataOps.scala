package biomed3471.codex

import biomed3471.codex.DataTypes.{
    BackfillSource,
    CanonicalRelationGroups,
    ForwardFlowSource,
    PositionPair,
    PositionPairTuple,
    crossedPositionPairTuple,
    straightPositionPairTuple
}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.*

import scala.util.chaining.*

object DataOps:

    /**
     * Constructs the native Spark expression that restores the canonicalId
     * omitted from the relation-occurrence export.
     *
     * Extracts the relation type and raw TERMite IDs from the supplied columns.
     * Undirected relations use lexicographically ordered partner IDs;
     * node1_to_node2 uses node1 as regulator and node2 as target. These are the
     * export's two direction values. Names and node types do not enter the ID.
     * The Scala function builds Spark expressions and does not evaluate data.
     *
     * @param relationshipType the exported relationship_type column
     * @param direction the exported direction column
     * @param node1Urn the first participating node URI column
     * @param node2Urn the second participating node URI column
     * @return the native Spark expression producing the canonical relation ID
     */
    def reconstructCanonicalId(
        relationshipType: Column,
        direction: Column,
        node1Urn: Column,
        node2Urn: Column
    ): Column =
        val relationType   = lower(substring_index(relationshipType, ":", -1))
        val node1TermiteId = substring_index(node1Urn, "/", -1)
        val node2TermiteId = substring_index(node2Urn, "/", -1)

        when(
            direction === "undirected",
            concat(
                lit("biodata:"), relationType,
                lit("/partner/"), least(node1TermiteId, node2TermiteId),
                lit("/partner/"), greatest(node1TermiteId, node2TermiteId)
            )
        ).otherwise(
            concat(
                lit("biodata:"), relationType,
                lit("/regulator/"), node1TermiteId,
                lit("/target/"), node2TermiteId
            )
        )

    /**
     * Restores canonicalId on the reduced view of relation occurrences.
     *
     * Applies the native identifier expression to the existing relation type,
     * direction and node URI columns. Retains every input column unchanged,
     * including each node's association with its graph name and the occurrence
     * metadata needed to trace examples. This transformation starts no action.
     *
     * @param exportedRelationOccurrences the exported relation occurrences read from Delta
     * @return the same relation occurrences with canonicalId added
     */
    def withCanonicalId(exportedRelationOccurrences: DataFrame): DataFrame =
        exportedRelationOccurrences.withColumn(
            "canonicalId",
            reconstructCanonicalId(
                col("relationship_type"), col("direction"),
                col("node1_urn"), col("node2_urn")
            )
        )

    /**
     * Groups relation occurrences into one entry per canonical relation.
     *
     * The model fixes relation type, direction and the participating graph nodes
     * and names within each canonicalId. Bundles those six fields into a struct
     * and retains one equivalent copy with first. Each name remains attached
     * to its node URI. Undirected partners already follow the model's
     * lexicographic ordering by raw TERMite IDs within each relation.
     * Collects occurrence-specific source values separately into a distinct set,
     * represented by a sorted Spark array. Expands the retained struct into the
     * six original columns.
     * No occurrence-specific metadata is selected with first.
     *
     * @param relationOccurrences relation occurrences with restored canonicalId
     * @return canonicalId, the six relation/node fields, and the source set
     */
    def aggregateCanonicalRelations(relationOccurrences: DataFrame): DataFrame =
        relationOccurrences
            .groupBy("canonicalId")
            .agg(
                first(
                    struct(
                        col("relationship_type"), col("direction"),
                        col("node1_urn"), col("node1_name"),
                        col("node2_urn"), col("node2_name")
                    )
                ).as("relationFields"),
                array_sort(collect_set(col("source"))).as("sources")
            )
            .select(col("canonicalId"), col("relationFields.*"), col("sources"))

    /**
     * Separates canonical relations into their three source groups.
     *
     * Compares the collected source arrays with the two known sources in this
     * export. Forward-only and backfill-only relations have the corresponding
     * singleton source set; shared relations have both sources. Retains the
     * fixed relation and node fields in each disjoint group for direct matching.
     *
     * @param canonicalRelations one entry per canonicalId with its source set
     * @return the forwardOnly, shared and backfillOnly canonical relations
     */
    def separateCanonicalRelations(canonicalRelations: DataFrame): CanonicalRelationGroups =
        CanonicalRelationGroups(
            forwardOnly = canonicalRelations.where(
                col("sources") === array(lit(ForwardFlowSource))
            ),
            shared = canonicalRelations.where(
                col("sources") === array_sort(array(lit(ForwardFlowSource), lit(BackfillSource)))
            ),
            backfillOnly = canonicalRelations.where(
                col("sources") === array(lit(BackfillSource))
            )
        )

    /**
     * Defines the trimmed, lowercase graph-name form used throughout twin matching.
     *
     * Shared by the comparison key, undirected position tests and matchedNodeName.
     * Builds a native Spark expression; the original graph-name fields are retained.
     *
     * @param nodeName the graph-node name column
     * @return its lowercase value after trimming leading and trailing spaces
     */
    private def normalizedNodeName(nodeName: Column): Column = lower(trim(nodeName))

    /**
     * Adds the native Spark comparison key used to find non-identical twins.
     *
     * Combines relation type and normalized node names. Relation type determines
     * directedness. Directed names retain regulator/target order. Undirected
     * names are sorted because their positions can differ between canonical
     * relations with different node IDs. Only the comparison key sorts names:
     * the original node URIs, their associated names and every other input
     * field remain unchanged.
     *
     * @param canonicalRelations canonical relations retaining their node fields
     * @return the canonical relations with comparisonKey added
     */
    def withComparisonKey(canonicalRelations: DataFrame): DataFrame =
        val nodeNames = array(normalizedNodeName(col("node1_name")), normalizedNodeName(col("node2_name")))

        canonicalRelations.withColumn(
            "comparisonKey",
            struct(
                col("relationship_type"),
                when(col("direction") === "undirected", array_sort(nodeNames))
                    .otherwise(nodeNames).as("nodeNames")
            )
        )

    /**
     * Finds each forward-only canonical relation's non-identical twins in backfill.
     *
     * Joins forwardOnly directly to backfillOnly together with shared, comparing
     * relation type and corresponding normalized graph names. A
     * shared relation is eligible because its presence in forward flow does not
     * remove it from backfill. The disjoint source groups ensure different
     * canonical IDs. One entry per ID on each side makes each matched ID pair
     * unique without another deduplication or source lookup.
     *
     * Sets nonIdenticalTwinsAlsoWithinForwardFlow from the backfill twin's source set.
     * Label: "Non-identical twins also within forward flow".
     * The flag is true for shared, false for backfillOnly. The selected forward relation
     * is forwardOnly, so true means both canonical relations in this twin pair
     * are present in forward flow. The flag records presence; vocabulary and
     * taxonomy causes belong to the later investigation. It adds no matches
     * or extra entries for a twin pair.
     *
     * @param groups the three canonical-relation source groups
     * @return one entry per twin pair, with both IDs, nonIdenticalTwinsAlsoWithinForwardFlow and complete relations
     */
    def findNonIdenticalTwinPairs(groups: CanonicalRelationGroups): DataFrame =
        groups.forwardOnly
            .pipe(withComparisonKey)
            .as("forward")
            .join(
                groups.backfillOnly.union(groups.shared)
                    .pipe(withComparisonKey)
                    .as("backfill"),
                col("forward.comparisonKey") === col("backfill.comparisonKey")
            )
            .select(
                col("forward.canonicalId").as("forwardCanonicalId"),
                col("backfill.canonicalId").as("backfillCanonicalId"),
                array_contains(col("backfill.sources"), ForwardFlowSource)
                    .as("nonIdenticalTwinsAlsoWithinForwardFlow"),
                struct(col("forward.*")).as("forward"),
                struct(col("backfill.*")).as("backfill")
            )

    extension (positionPair: PositionPair)

        /**
         * Tests whether the nodes at one paired position have the same name.
         *
         * The position choices select node1_name or node2_name from each relation.
         * Compares their normalized values. The result is a native Boolean
         * Spark expression, not a node-pair record or a dataset operation.
         *
         * The receiver positionPair holds the forward and backfill positions
         * to compare.
         *
         * @param forwardRelation the forward canonical-relation struct expression
         * @param backfillRelation its backfill twin's struct expression
         * @return whether these two positions contain nodes with matching names
         */
        private def namesMatchAtPositions(
            forwardRelation: Column,
            backfillRelation: Column
        ): Column =
            normalizedNodeName(forwardRelation.getField(s"node${positionPair.forwardPosition}_name")) ===
                normalizedNodeName(backfillRelation.getField(s"node${positionPair.backfillPosition}_name"))

        /**
         * Constructs a name-matched node-pair record at the paired positions.
         *
         * Directed names already match by the Step 4 key; undirected names match
         * within the complete position tuple's name-compatible branch.
         * Retains full node URIs in forward/backfill orientation and their
         * matching normalized name. Directed roles come from the original
         * forward position; undirected relations use partner. The public struct
         * starts with role and stores no position indices or validation-only fields.
         * Equal full URIs establish an identical name match; different full URIs
         * establish a homonymous node pair. Both kinds belong in the complete tuple.
         *
         * The receiver positionPair holds the positions whose names have
         * been matched by Step 4 or the complete tuple's condition.
         *
         * @param forwardRelation the forward canonical-relation struct expression
         * @param backfillRelation its backfill twin's struct expression
         * @param undirected whether the canonical relation is undirected
         * @return role, forwardNodeUri, backfillNodeUri and matchedNodeName
         */
        private def nameMatchedNodePair(
            forwardRelation: Column,
            backfillRelation: Column,
            undirected: Column
        ): Column =
            val directedRole = if positionPair.forwardPosition == 1 then "regulator" else "target"
            struct(
                when(undirected, lit("partner")).otherwise(lit(directedRole)).as("role"),
                forwardRelation.getField(s"node${positionPair.forwardPosition}_urn").as("forwardNodeUri"),
                backfillRelation.getField(s"node${positionPair.backfillPosition}_urn").as("backfillNodeUri"),
                normalizedNodeName(forwardRelation.getField(s"node${positionPair.forwardPosition}_name")).as("matchedNodeName")
            )

    extension (positionPairTuple: PositionPairTuple)

        /**
         * Constructs the complete two-entry node-pair tuple at permitted positions.
         *
         * Directed twins use their Step 4 name-key guarantee. Undirected twins
         * use this value within the candidate helper's name-compatible branch.
         * Reads the URI and name fields chosen by each of the two position pairs.
         * Both positions remain represented when they contain a repeated node.
         * The two structs describe two comparisons within one twin's result.
         *
         * The receiver positionPairTuple accounts for both positions on each side.
         *
         * @param forwardRelation the forward canonical-relation struct expression
         * @param backfillRelation its backfill twin's struct expression
         * @param undirected whether roles are partner rather than regulator/target
         * @return one complete tuple containing exactly two node-pair structs
         */
        private def nameMatchedNodePairTupleAtPositions(
            forwardRelation: Column,
            backfillRelation: Column,
            undirected: Column
        ): Column =
            val (firstPositionPair, secondPositionPair) = positionPairTuple
            array(
                firstPositionPair.nameMatchedNodePair(forwardRelation, backfillRelation, undirected),
                secondPositionPair.nameMatchedNodePair(forwardRelation, backfillRelation, undirected)
            )

        /**
         * Constructs zero or one name-compatible result for an undirected twin.
         *
         * Requires matching names at both paired positions before retaining
         * the complete two-entry tuple. The true branch is an outer array with
         * that one tuple; the false branch is a typed empty outer array.
         * Native when, array and slice preserve the distinction between
         * position metadata and name-matched records without a UDF or null
         * placeholder. Repeated nodes still occupy two entries in a result.
         *
         * The receiver positionPairTuple is one complete straight or crossed
         * position pairing.
         *
         * @param forwardRelation the forward canonical-relation struct expression
         * @param backfillRelation its backfill twin's struct expression
         * @return a native collection containing zero or one complete two-entry tuple
         */
        private def candidateNameMatchedNodePairTuplesAtPositions(
            forwardRelation: Column,
            backfillRelation: Column
        ): Column =
            val (firstPositionPair, secondPositionPair) = positionPairTuple
            val namesMatch =
                firstPositionPair.namesMatchAtPositions(forwardRelation, backfillRelation) &&
                    secondPositionPair.namesMatchAtPositions(forwardRelation, backfillRelation)
            val completeResult = array(positionPairTuple.nameMatchedNodePairTupleAtPositions(
                forwardRelation, backfillRelation, lit(true)
            ))
            when(namesMatch, completeResult).otherwise(slice(completeResult, 1, 0))

    extension (nameMatchedNodePairTuple: Column)

        /**
         * Counts identical name matches within one complete result.
         *
         * For an undirected twin pair sharing a node, an identical name match
         * pairs that node with itself. This accounts for one position on each
         * side and determines which nodes at the other positions to compare.
         * The caller uses this count to retain the complete results containing
         * that comparison. The identical name match stays in the complete tuple.
         *
         * Filters the native two-entry array by full URI equality and takes its
         * size. Each entry contributes separately, including repeated entries.
         *
         * The receiver nameMatchedNodePairTuple is one complete two-entry
         * native result.
         *
         * @return its native integer count of identical name matches
         */
        private def countIdenticalNodePairs: Column =
            size(filter(
                nameMatchedNodePairTuple,
                nameMatchedNodePair =>
                    nameMatchedNodePair.getField("forwardNodeUri") ===
                        nameMatchedNodePair.getField("backfillNodeUri")
            ))

    extension (nameMatchedNodePairTuples: Column)

        /**
         * Normalizes complete results and removes equivalent whole tuples.
         *
         * Different position choices can produce the same node-pair records
         * in a different listing order. A consistent order makes those complete
         * results equal while retaining both records that account for the positions.
         *
         * Projects each tuple to temporary URI-first structs before array_sort,
         * because the public record starts with role. Orders by forward URI,
         * backfill URI and then role, and restores the public field layout.
         * Deduplicates only the outer collection of complete tuples. Repeated
         * entries inside a tuple and forward/backfill orientation survive.
         * Directed roles remain attached independently of array position.
         *
         * The receiver nameMatchedNodePairTuples contains complete results
         * after name matching and the shared-node comparison.
         *
         * @return distinct whole results, each still containing two node-pair entries
         */
        private def normalizeNameMatchedNodePairTuples: Column =
            array_distinct(transform(
                nameMatchedNodePairTuples,
                nameMatchedNodePairTuple => transform(
                    array_sort(transform(
                        nameMatchedNodePairTuple,
                        nameMatchedNodePair => struct(
                            nameMatchedNodePair.getField("forwardNodeUri").as("forwardNodeUri"),
                            nameMatchedNodePair.getField("backfillNodeUri").as("backfillNodeUri"),
                            nameMatchedNodePair.getField("role").as("role"),
                            nameMatchedNodePair.getField("matchedNodeName").as("matchedNodeName")
                        )
                    )),
                    nameMatchedNodePair => struct(
                        nameMatchedNodePair.getField("role").as("role"),
                        nameMatchedNodePair.getField("forwardNodeUri").as("forwardNodeUri"),
                        nameMatchedNodePair.getField("backfillNodeUri").as("backfillNodeUri"),
                        nameMatchedNodePair.getField("matchedNodeName").as("matchedNodeName")
                    )
                )
            ))

    extension (nonIdenticalTwinPairs: DataFrame)

        /**
         * Determines which forward-flow node to compare with which backfill node.
         *
         * This node pairing lets us identify homonymous node pairs: nodes with
         * the same name but different identifiers. Reads node1_urn, node1_name,
         * node2_urn and node2_name from each twin's forward/backfill structs.
         *
         * Directed twins use straight positions: Step 4 already guarantees the
         * names match by role, so no name test is repeated. Undirected twins
         * permit straight and crossed positions and retain only complete
         * name-compatible candidates.
         *
         * When the two partners have the same name and the twins share a node,
         * pair the shared node with itself. That identical name match
         * accounts for one position in each relation. Each has exactly one other
         * position: pair the nodes there to identify the homonymous node pair.
         * countIdenticalNodePairs identifies the complete results that make
         * the shared-node comparison; both comparisons remain in the tuple.
         *
         * Normalizes and deduplicates whole results. When both twins are
         * self-referencing relations, each uses the same node in both partner
         * positions. The complete tuple then contains two records with identical
         * contents; both remain to account for both positions. Forward/backfill
         * orientation remains unchanged within each record. The Step 4 input
         * guarantees at least one result, so the final outer collection holds
         * one resolved tuple or two unresolved alternatives, each of size two.
         * One tuple describes two node comparisons; two tuples describe
         * alternative complete results for the same twin, not two twin rows.
         *
         * Keeps both canonical relations, their IDs, source sets and the
         * independent forward-flow flag unchanged. Only the final result
         * collection is added; the candidate column is temporary. No action,
         * extra metadata join or stored resolution Boolean is introduced.
         *
         * The receiver nonIdenticalTwinPairs contains matched canonical twins
         * with complete relation structs, one row per established twin pair.
         * See <a href="README.md#from-twins-to-node-pairs">the README explanation</a>
         * for the input fields, position choices and retained result structures.
         * Its <a href="README.md#conceptual-procedure-for-undirected-relations">
         * conceptual procedure for undirected relations</a> explains the four
         * cases used to determine homonymous node pairs for any undirected
         * relation type, with Binding as an example.
         *
         * @return every twin with its nameMatchedNodePairTuples result collection
         */
        def withNameMatchedNodePairTuples: DataFrame =
            val forwardRelation  = col("forward")
            val backfillRelation = col("backfill")
            val undirected       = forwardRelation.getField("direction") === "undirected"

            val directedNameMatchedNodePairTuples = array(
                straightPositionPairTuple.nameMatchedNodePairTupleAtPositions(
                    forwardRelation, backfillRelation, lit(false)
                )
            )
            val straightCandidateNameMatchedNodePairTuples =
                straightPositionPairTuple.candidateNameMatchedNodePairTuplesAtPositions(
                    forwardRelation, backfillRelation
                )
            val crossedCandidateNameMatchedNodePairTuples =
                crossedPositionPairTuple.candidateNameMatchedNodePairTuplesAtPositions(
                    forwardRelation, backfillRelation
                )

            nonIdenticalTwinPairs
                .withColumn(
                    "candidateNameMatchedNodePairTuples",
                    when(
                        undirected,
                        concat(straightCandidateNameMatchedNodePairTuples, crossedCandidateNameMatchedNodePairTuples)
                    ).otherwise(directedNameMatchedNodePairTuples)
                )
                .withColumn(
                    "nameMatchedNodePairTuples",
                    filter(
                        col("candidateNameMatchedNodePairTuples"),
                        nameMatchedNodePairTuple =>
                            nameMatchedNodePairTuple.countIdenticalNodePairs ===
                                array_max(transform(
                                    col("candidateNameMatchedNodePairTuples"),
                                    nameMatchedNodePairTuple => nameMatchedNodePairTuple.countIdenticalNodePairs
                                ))
                    ).normalizeNameMatchedNodePairTuples
                )
                .drop("candidateNameMatchedNodePairTuples")

    extension (twinPairsWithNameMatchedNodePairTuples: DataFrame)

        /**
         * Selects detailed homonymous node-pair entries from resolved twins.
         *
         * First retains twins with one complete result, then explodes its two
         * entries and keeps different full node URIs. Selects node URI/name
         * columns first, followed by role, both canonical IDs and the flag.
         * Repeated entries remain repeated: no distinct operation is applied.
         * Unresolved alternatives contribute no selected node pairs.
         * The identical name matches remain available in the complete tuple
         * attached to each twin; this projection selects its homonymous entries.
         *
         * The receiver twinPairsWithNameMatchedNodePairTuples contains full
         * twins with one or two complete results.
         *
         * @return homonymous entries with the exact seven-column detail schema
         */
        def selectHomonymousNodePairs: DataFrame =
            twinPairsWithNameMatchedNodePairTuples
                .where(size(col("nameMatchedNodePairTuples")) === 1)
                .select(
                    col("forwardCanonicalId"),
                    col("backfillCanonicalId"),
                    col("nonIdenticalTwinsAlsoWithinForwardFlow"),
                    explode(element_at(col("nameMatchedNodePairTuples"), 1)).as("nameMatchedNodePair")
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

        /**
         * Retains unresolved twins and both complete node-pair alternatives.
         *
         * Selects result collections of size two and projects both canonical
         * IDs with the complete collection. Every tuple retains two entries.
         * Canonical relation structs, source sets and the independent flag
         * remain in the full twin table rather than this dedicated projection.
         *
         * The receiver twinPairsWithNameMatchedNodePairTuples contains full
         * twins with one or two complete results.
         *
         * @return unresolved canonical twin pairs and their two complete alternatives
         */
        def selectTwinPairsWithMultipleNameMatchedNodePairTuples: DataFrame =
            twinPairsWithNameMatchedNodePairTuples
                .where(size(col("nameMatchedNodePairTuples")) === 2)
                .select("forwardCanonicalId", "backfillCanonicalId", "nameMatchedNodePairTuples")

    extension (homonymousNodePairs: DataFrame)

        /**
         * Aggregates homonymous node pairs and counts their distinct resolved twins.
         *
         * Groups detail directly by the oriented full node URI pair and keeps
         * the graph-node name fixed by those identities. Counts distinct
         * complete forward/backfill canonical-ID pairs, so repeated entries in
         * one twin count once and several backfill twins of one forward relation
         * count separately. Role and the forward-flow flag do not split groups.
         *
         * Selects node URIs, matching name and the LongType twin-pair count in
         * that order. A shared name alone does not establish one biological
         * concept; this output identifies node pairs to investigate.
         *
         * The receiver homonymousNodePairs contains selected detailed
         * entries preserving repetitions.
         *
         * @return one row per oriented node pair, its name and nonIdenticalTwinPairCount
         */
        def aggregateHomonymousNodePairsWithTwinCount: DataFrame =
            homonymousNodePairs
                .groupBy("forwardNodeUri", "backfillNodeUri")
                .agg(
                    first(col("matchedNodeName")).as("matchedNodeName"),
                    count_distinct(
                        struct(col("forwardCanonicalId"), col("backfillCanonicalId"))
                    ).as("nonIdenticalTwinPairCount")
                )
                .select(
                    "forwardNodeUri", "backfillNodeUri", "matchedNodeName", "nonIdenticalTwinPairCount"
                )
