package biomed3471.claude

import biomed3471.claude.DataTypes.*
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.*
import sparkops.SchemaEnforcementOps.*

/**
 * ==The five steps of the twin search==
 *
 * DataOps holds the Column and DataFrame operations of steps 1 to 5. The steps rely on guarantees from two pages of the data model:
 *
 *   - Section 2.2 of BioMed Data Identifier Schemes builds the canonical relation ID:
 *     [[https://elsevier.atlassian.net/wiki/spaces/ELSSIE/pages/119601216791062/BioMed+Data+Identifier+Schemes]]
 *   - Section 4.3 of BioMed Knowledge Graph (Graph) Data Model gives each node exactly one name, stored as `skos:prefLabel`:
 *     [[https://elsevier.atlassian.net/wiki/spaces/ELSSIE/pages/119601199850466/BioMed+Knowledge+Graph+Graph+Data+Model]]
 */
object DataOps:

    /**
     * The node TERMite ID in a node URN: its last part, such as `Concept_700527820` in `biodata:smallmol/Concept_700527820`.
     */
    def termiteId(nodeUrn: Column): Column = substring_index(nodeUrn, "/", -1)

    /**
     * ==Step 1: the canonical relation ID of a relation occurrence==
     *
     * Step 1 builds the canonical relation ID of each relation occurrence, because the input holds none. Section 2.2 of BioMed Data Identifier
     * Schemes builds the ID from the relation type, the roles and the two node TERMite IDs. The graph's domain model decides whether a relation type
     * is directed or undirected. In a directed relation, node 1 is the regulator and node 2 is the target, and the ID names them in that order. In an
     * undirected relation, both nodes are partners, and the ID lists the node TERMite IDs of the two partners in lexicographic order, which means
     * sorted as text. The relation occurrences already list the partners in that order, so step 1 keeps it:
     *
     * {{{
     * directed     biodata:regulation/regulator/Concept_700527820/target/Concept_86432034
     * undirected   biodata:binding/partner/Concept_86378312/partner/HGNC24008
     * }}}
     *
     * The input marks the direction of each relation occurrence in its `direction` column. Its `relationship_type` column holds values such as
     * `biomed:Regulation`, and the ID keeps the part after the colon, in lower case, such as `regulation`. Its `node1_urn` and `node2_urn` columns
     * hold node URNs, and the ID keeps the node TERMite ID of each, which is its last part.
     */
    def canonicalId(relationshipType: Column, direction: Column, node1Urn: Column, node2Urn: Column): Column =
        val relationType   = lower(substring_index(relationshipType, ":", -1))
        val node1TermiteId = termiteId(node1Urn)
        val node2TermiteId = termiteId(node2Urn)

        val undirectedId = concat(lit("biodata:"), relationType, lit(s"/$Partner/"), node1TermiteId, lit(s"/$Partner/"), node2TermiteId)
        val directedId   = concat(lit("biodata:"), relationType, lit(s"/$Regulator/"), node1TermiteId, lit(s"/$Target/"), node2TermiteId)

        when(direction === Undirected, undirectedId)
            .otherwise(directedId)

    /**
     * ==Step 2: one row for each canonical relation==
     *
     * Step 2 folds the relation occurrences of each canonical relation into their canonical relation as one row. Its column `sources` holds the
     * sorted list of the distinct `source` values of the relation occurrences, which says which pipelines have relation occurrences of the canonical
     * relation.
     *
     * Step 2 relies on the guarantees that make the canonical relation ID fix every other column of the row except `sources`. The ID holds the
     * relation type and the two node TERMite IDs (section 2.2 of BioMed Data Identifier Schemes), and the relation type decides the direction. A node
     * TERMite ID belongs to one taxonomy, so it names one node URN, and each node has exactly one name (section 4.3 of BioMed Knowledge Graph (Graph)
     * Data Model). Step 2 therefore groups on the ID together with these columns, which keeps them in the result without changing the groups. The row
     * keeps node 1 and node 2 in the order of the ID, and their names as the input gives them.
     *
     * [[DataTypes.RelationOccurrence]] defines the input fields used by steps 1 and 2.
     *
     * Columns: canonicalId, relationship_type, direction, node1_urn, node1_name, node2_urn, node2_name, sources.
     */
    def computeCanonicalRelations(relationOccurrenceDF: Dataset[RelationOccurrence]): Dataset[CanonicalRelation] =
        relationOccurrenceDF
            .withColumn("canonicalId", canonicalId(col("relationship_type"), col("direction"), col("node1_urn"), col("node2_urn")))
            .groupBy(
              col("canonicalId"),
              col("relationship_type"),
              col("direction"),
              col("node1_urn"),
              col("node1_name"),
              col("node2_urn"),
              col("node2_name")
            )
            .agg(array_sort(collect_set(col("source"))).as("sources"))
            .enforceAs[CanonicalRelation](col("canonicalId"))

    /**
     * The normalized name of a node's name: the name in lower case, with the spaces at both ends removed.
     */
    def normalizedName(name: Column): Column = lower(trim(name))



    extension (canonicalRelationsDS: Dataset[CanonicalRelation])

        /**
         * ==Step 3: the name key of each canonical relation==
         *
         * Step 3 adds a name key to each canonical relation. The key contains the relation type and the normalized names of its two nodes. Step 4
         * uses it to find non-identical twins: two canonical relations with the same name key and different canonical relation IDs.
         *
         * A normalized name is the node's name in lower case, with the spaces at both ends removed.
         *
         * The order of the names in the key depends on the relation:
         *
         *   - In a directed relation, the regulator's normalized name comes first and the target's comes second.
         *   - In an undirected relation, the two normalized names are sorted alphabetically. The canonical relation ID orders the partners by their
         *     node TERMite IDs, which can put their names in different orders across twins. Sorting the names gives both relations the same name key.
         *
         * The relation type already determines whether the relation is directed or undirected, so the name key needs no separate direction field.
         *
         * Step 3 stores the normalized names in `node1NormalizedName` and `node2NormalizedName` for steps 4 and 5 to reuse. The original names remain
         * unchanged in `node1_name` and `node2_name`.
         *
         * The name key is a struct, which Spark prints like this:
         *
         * {{{
         * directed     {biomed:Regulation, [luteolin, inflammation]}
         * undirected   {biomed:Binding, [collagen, mia3]}
         * }}}
         */
        def withNameKey: Dataset[CanonicalRelationWithNameKey] =
            val node1NormalizedName = normalizedName(col("node1_name"))
            val node2NormalizedName = normalizedName(col("node2_name"))
            val normalizedNodeNames = array(node1NormalizedName, node2NormalizedName)
            val nameKeyNodeNames    = when(col("direction") === Undirected, array_sort(normalizedNodeNames)).otherwise(normalizedNodeNames)

            canonicalRelationsDS
                .withColumn("node1NormalizedName", node1NormalizedName)
                .withColumn("node2NormalizedName", node2NormalizedName)
                .withColumn("nameKey", struct(col("relationship_type"), nameKeyNodeNames.as("nodeNames")))
                .enforceAs[CanonicalRelationWithNameKey](col("canonicalId"))


    /**
     * One of the two canonical relations of a twin pair, as one struct taken from the columns of one side of the join of step 4: its relation type,
     * its direction, its two node URNs in the order of the canonical relation ID, their names as the input gives them, and their normalized names.
     * The struct leaves out the canonical relation ID and `sources`. The twinPair holds the two IDs in its own columns, the forward-flow relation is
     * always a forward-only canonical relation, and the flag `nonIdenticalTwinsAlsoWithinForwardFlow` says whether the backfill twin also has
     * forward-flow relation occurrences.
     */
    def twinFrom(alias: String): Column =
        struct(
          col(s"$alias.relationship_type").as("relationship_type"),
          col(s"$alias.direction").as("direction"),
          col(s"$alias.node1_urn").as("node1_urn"),
          col(s"$alias.node1_name").as("node1_name"),
          col(s"$alias.node1NormalizedName").as("node1NormalizedName"),
          col(s"$alias.node2_urn").as("node2_urn"),
          col(s"$alias.node2_name").as("node2_name"),
          col(s"$alias.node2NormalizedName").as("node2NormalizedName")
        )

    /**
     * ==Step 4: the non-identical twin pairs==
     *
     * Step 4 compares each forward-only canonical relation, whose relation occurrences all come from the forward flow, with every canonical relation
     * that has at least one backfill relation occurrence. These include the identical twins, which are the canonical relations with relation
     * occurrences from both pipelines. Each match on the name key is a twin pair: the forward-only canonical relation is the forward-flow relation,
     * and the matched canonical relation is its backfill twin. The two canonical relation IDs of a twin pair always differ, because the forward-flow
     * relation has no backfill relation occurrence and its backfill twin has at least one. So step 4 does not compare the IDs.
     *
     * Step 4 relies on two earlier results: the `sources` of step 2, which say which pipelines have relation occurrences of each canonical relation,
     * and the name key of step 3, which step 4 computes first.
     *
     * The backfill side keeps the identical twins, because the question is where the backfill put the relation. In the luteolin example, the forward
     * flow states that luteolin regulates Inflammation with the luteolin node `Concept_700527820` as the regulator. The backfill states it with the
     * other luteolin node, `Concept_86446482`, in an identical twin. Without identical twins on the backfill side, this twin pair would be lost. Step
     * 4 keeps it, and its `nonIdenticalTwinsAlsoWithinForwardFlow` is true, because its backfill twin also has forward-flow relation occurrences.
     *
     * The columns `forwardTwin` and `backfillTwin` hold the forward-flow relation and its backfill twin, each built by [[twinFrom]]. The number of
     * distinct `forwardCanonicalId` values is the number of forward-only canonical relations with at least one match among the canonical relations
     * that have a backfill relation occurrence.
     *
     * Columns: forwardCanonicalId, backfillCanonicalId, nonIdenticalTwinsAlsoWithinForwardFlow, forwardTwin, backfillTwin.
     */
    def computeNonIdenticalTwinPairs(canonicalRelationsDS: Dataset[CanonicalRelation]): DataFrame =
        val canonicalRelationsWithNameKey = canonicalRelationsDS.withNameKey

        val forwardOnlyCanonicalRelations =
            canonicalRelationsWithNameKey
                .filter(col("sources") === array(lit(ForwardFlowSource)))

        val backfillCanonicalRelations =
            canonicalRelationsWithNameKey
                .filter(array_contains(col("sources"), BackfillSource)) // <-- includes the canonical relations shared with the forward flow

        forwardOnlyCanonicalRelations
            .as("forward") // <-- both sides come from canonicalRelationsWithNameKey, so each side gets its own name
            .join(
              backfillCanonicalRelations.as("backfill"), // <-- both sides come from canonicalRelationsWithNameKey, so each side gets its own name
              col("forward.nameKey") === col("backfill.nameKey"),
              "inner"
            )
            .select(
              col("forward.canonicalId").as("forwardCanonicalId"),
              col("backfill.canonicalId").as("backfillCanonicalId"),
              array_contains(col("backfill.sources"), ForwardFlowSource).as("nonIdenticalTwinsAlsoWithinForwardFlow"),
              twinFrom("forward").as("forwardTwin"),
              twinFrom("backfill").as("backfillTwin")
            )

    /**
     * One name-matched node pair, as the struct that step 5 stores: the role of both nodes, the node URN of the forward-flow node, the node URN of
     * the backfill node, and the normalized name that both nodes share. The struct has the fields of [[DataTypes.NameMatchedNodePair]].
     */
    def nameMatchedNodePair(role: Column, forwardNodeUri: Column, backfillNodeUri: Column, matchedNodeName: Column): Column =
        struct(
          role.as("role"),
          forwardNodeUri.as("forwardNodeUri"),
          backfillNodeUri.as("backfillNodeUri"),
          matchedNodeName.as("matchedNodeName")
        )

    /**
     * ==Step 5: which forward-flow node goes with which backfill node==
     *
     * The two relations of a twin pair have the same name key. So each node of the forward-flow relation has the same normalized name as the backfill
     * node that goes with it, and at least one of them has a different node URN. Step 5 finds which forward-flow node goes with which backfill node.
     * In a directed relation the roles decide. A position is the place of a node in the canonical relation ID: node 1 is at position 1, and node 2 is
     * at position 2. In an undirected relation, such as Binding, the positions do not decide: the canonical relation ID orders the two partners by
     * node TERMite ID, so the same node can be at position 1 in one relation and at position 2 in the other.
     *
     * {{{
     * forward-flow relation   biodata:binding/partner/Concept_86378312/partner/HGNC24008   names collagen, MIA3
     * backfill twin           biodata:binding/partner/HGNC24008/partner/MSCAN_12001170     names MIA3, collagen
     * }}}
     *
     * Step 5 relies on three guarantees:
     *
     *   1. Each node has exactly one name (section 4.3 of BioMed Knowledge Graph (Graph) Data Model), so each node has exactly one normalized name,
     *      and comparing two nodes by normalized name is well defined.
     *   1. The two relations of a twin pair have the same name key, because step 4 matched them on it. They therefore have the same relation type,
     *      the same direction, and the same two normalized names, each the same number of times.
     *   1. The two relations of a twin pair have different canonical relation IDs. The ID is built from the relation type, the roles and the node
     *      TERMite IDs (section 2.2 of BioMed Data Identifier Schemes), and guarantee 2 makes the relation type and the direction the same. So one of
     *      the two relations holds a node that the other does not, or the relation type is directed and the two relations hold the same two nodes
     *      with the regulator and the target swapped. For example, one EGFR node regulates another in the forward-flow relation and is regulated by
     *      it in the backfill twin.
     *
     * Step 5 uses these names:
     *
     *   - A name-matched node pair is one forward-flow node and one backfill node whose normalized names are the same. Step 5 stores it as the struct
     *     `{role, forwardNodeUri, backfillNodeUri, matchedNodeName}`.
     *   - An identical node pair is a name-matched node pair whose forward-flow node and backfill node have the same node URN.
     *   - A homonymous node pair is a name-matched node pair whose two nodes have different node URNs.
     *   - A name-matched node pair tuple is one possible answer to which forward-flow node goes with which backfill node, as step 5 stores it. It
     *     holds two name-matched node pairs, the one of forward-flow node 1 first and the one of forward-flow node 2 second.
     *   - A twin pair is resolved when step 5 can decide which forward-flow node goes with which backfill node, and unresolved when it cannot. The
     *     result holds one tuple for a resolved twin pair, and the two possible answers for an unresolved one.
     *
     * Step 5 applies the first rule that fits. A node appears in both relations when the forward-flow relation and the backfill twin both hold its
     * node URN. A relation holds the same node twice when its two nodes have the same node URN, such as an EGFR node that binds itself, and not when
     * two different nodes have the same normalized name.
     *
     *   - Rule A. In a directed relation, the forward-flow regulator goes with the backfill regulator, and the forward-flow target goes with the
     *     backfill target. Guarantee 2 makes their normalized names the same, so step 5 tests no name. The homonymous node pairs are the node pairs
     *     whose node URNs differ, and guarantee 3 makes at least one of them differ.
     *   - Rule B. When the two nodes of an undirected relation have different normalized names, each forward-flow node goes with the backfill node
     *     that has the same normalized name. Guarantee 2 makes that backfill node exist, and the two names being different makes it the only one. The
     *     homonymous node pairs are the node pairs whose node URNs differ, and guarantee 3 makes at least one of them differ.
     *   - Rule C1. When the two nodes of an undirected relation have the same normalized name and a node appears in both relations, this shared node
     *     goes with itself, and the other forward-flow node goes with the other backfill node. This other node pair is the homonymous node pair. Its
     *     two node URNs always differ, because the node pair of the shared node is an identical node pair, and guarantee 3 makes at least one node
     *     pair differ.
     *   - Rule C2. When the two nodes of an undirected relation have the same normalized name, no node appears in both relations, and at least one
     *     relation holds the same node twice, every node of the other relation goes with that repeated node. When both relations hold a node twice,
     *     the two repeated nodes go with each other. Both node pairs are homonymous node pairs, because no node appears in both relations.
     *   - Rule C3. When the two nodes of an undirected relation have the same normalized name, no node appears in both relations, and neither
     *     relation holds the same node twice, the four nodes are different nodes with the same normalized name. Each forward-flow node goes with one
     *     of its two homonyms in the backfill twin, but step 5 cannot decide which one. The twin pair is unresolved, step 5 keeps both possible
     *     answers, and it reports no homonymous node pair.
     *
     * The code writes down the two possible answers, `straightNodePairTuple` and `crossedNodePairTuple`, and states each rule as a choice between
     * them. The straight answer puts forward-flow node 1 with backfill node 1, and forward-flow node 2 with backfill node 2. The crossed answer puts
     * forward-flow node 1 with backfill node 2, and forward-flow node 2 with backfill node 1. The rules choose like this:
     *
     *   - Rule A takes the straight answer, because node 1 is the regulator and node 2 is the target in both relations.
     *   - Rule B takes the straight answer when both relations list the two names in the same order, and the crossed answer otherwise. In the
     *     collagen example, collagen is node 1 of the forward-flow relation and node 2 of the backfill twin, so rule B takes the crossed answer: the
     *     collagen node `Concept_86378312` goes with the collagen node `MSCAN_12001170`, and the MIA3 node `HGNC24008` goes with itself.
     *   - Rule C1 takes the straight answer when the shared node is at the same position in both relations, and the crossed answer otherwise.
     *   - Rule C2 takes the straight answer, because both answers hold the same node pairs.
     *   - Rule C3 keeps both answers.
     *
     * Every answer that the rules choose puts together two nodes with the same normalized name, because step 4 matched the twin pair on its name key.
     * So each node pair takes `matchedNodeName` from its forward-flow node.
     *
     * These are results of the test cases, as Spark prints them. P2, P3 and P4 are protein nodes named insulin or INSR. E1, E2, E3 and E4 are four
     * different protein nodes named EGFR, in the order of their node TERMite IDs. N1 and N2 are two more protein nodes named EGFR, and in the rule A
     * example each relation is a Regulation from one of them to itself.
     *
     * {{{
     * rule A    forward-flow relation regulator N1, target N1    backfill twin regulator N2, target N2
     *           [[{regulator, N1, N2, egfr}, {target, N1, N2, egfr}]]
     *
     * rule B    forward-flow relation P2 insulin, P3 INSR        backfill twin P3 INSR, P4 insulin
     *           [[{partner, P2, P4, insulin}, {partner, P3, P3, insr}]]
     *
     * rule C1   forward-flow relation E1, E2                     backfill twin E1, E3
     *           [[{partner, E1, E1, egfr}, {partner, E2, E3, egfr}]]
     *
     * rule C1   forward-flow relation E1, E1                     backfill twin E1, E2
     *           [[{partner, E1, E1, egfr}, {partner, E1, E2, egfr}]]
     *
     * rule C2   forward-flow relation E1, E1                     backfill twin E2, E3
     *           [[{partner, E1, E2, egfr}, {partner, E1, E3, egfr}]]
     *
     * rule C2   forward-flow relation E1, E1                     backfill twin E2, E2
     *           [[{partner, E1, E2, egfr}, {partner, E1, E2, egfr}]]
     *
     * rule C3   forward-flow relation E1, E2                     backfill twin E3, E4
     *           [[{partner, E1, E3, egfr}, {partner, E2, E4, egfr}], [{partner, E1, E4, egfr}, {partner, E2, E3, egfr}]]
     * }}}
     */
    def nameMatchedNodePairTuples: Column =
        val forwardNode1Urn             = col("forwardTwin.node1_urn")
        val forwardNode2Urn             = col("forwardTwin.node2_urn")
        val backfillNode1Urn            = col("backfillTwin.node1_urn")
        val backfillNode2Urn            = col("backfillTwin.node2_urn")
        val forwardNode1NormalizedName  = col("forwardTwin.node1NormalizedName")
        val forwardNode2NormalizedName  = col("forwardTwin.node2NormalizedName")
        val backfillNode1NormalizedName = col("backfillTwin.node1NormalizedName")

        val directed                        = col("forwardTwin.direction") =!= Undirected
        // Both relations have the same name key (guarantee 2). So when the two forward-flow names differ, the two backfill names differ too, and
        // when the names at position 1 match, the names at position 2 match too.
        val forwardNodesHaveDifferentNames  = forwardNode1NormalizedName =!= forwardNode2NormalizedName
        val namesInSameOrderInBothRelations = forwardNode1NormalizedName === backfillNode1NormalizedName
        val nodeSharedAtSamePosition        = forwardNode1Urn === backfillNode1Urn || forwardNode2Urn === backfillNode2Urn
        val nodeSharedAtOtherPosition       = forwardNode1Urn === backfillNode2Urn || forwardNode2Urn === backfillNode1Urn
        val oneRelationHoldsSameNodeTwice   = forwardNode1Urn === forwardNode2Urn || backfillNode1Urn === backfillNode2Urn

        val forwardNode1Role = when(directed, lit(Regulator)).otherwise(lit(Partner))
        val forwardNode2Role = when(directed, lit(Target)).otherwise(lit(Partner))

        val straightNodePairTuple = array(
          nameMatchedNodePair(forwardNode1Role, forwardNode1Urn, backfillNode1Urn, forwardNode1NormalizedName),
          nameMatchedNodePair(forwardNode2Role, forwardNode2Urn, backfillNode2Urn, forwardNode2NormalizedName)
        )
        val crossedNodePairTuple  = array( // <-- only undirected twin pairs take the crossed answer, so both nodes are partners
          nameMatchedNodePair(lit(Partner), forwardNode1Urn, backfillNode2Urn, forwardNode1NormalizedName),
          nameMatchedNodePair(lit(Partner), forwardNode2Urn, backfillNode1Urn, forwardNode2NormalizedName)
        )

        when(directed, array(straightNodePairTuple))                                                               // <-- rule A
            .when(forwardNodesHaveDifferentNames && namesInSameOrderInBothRelations, array(straightNodePairTuple)) // <-- rule B
            .when(forwardNodesHaveDifferentNames, array(crossedNodePairTuple))                                     // <-- rule B
            .when(nodeSharedAtSamePosition, array(straightNodePairTuple))                                          // <-- rule C1
            .when(nodeSharedAtOtherPosition, array(crossedNodePairTuple))                                          // <-- rule C1
            .when(oneRelationHoldsSameNodeTwice, array(straightNodePairTuple))                                     // <-- rule C2
            .otherwise(array(straightNodePairTuple, crossedNodePairTuple)) // <-- rule C3

    /**
     * ==Step 5: every twin pair with its name-matched node pair tuples==
     *
     * Every twin pair of step 4, with the result of rules A to C in `nameMatchedNodePairTuples`: one tuple when the twin pair is resolved, and the
     * two possible answers when it is unresolved. Each tuple keeps both of its node pairs, including an identical node pair.
     *
     * Columns: forwardCanonicalId, backfillCanonicalId, nonIdenticalTwinsAlsoWithinForwardFlow, forwardTwin, backfillTwin, nameMatchedNodePairTuples.
     */
    def computeTwinPairsWithNameMatchedNodePairTuples(nonIdenticalTwinPairsDF: DataFrame): DataFrame =
        nonIdenticalTwinPairsDF.withColumn("nameMatchedNodePairTuples", nameMatchedNodePairTuples)

    /**
     * ==Step 5: the homonymous node pairs of each resolved twin pair, the detail table==
     *
     * From each resolved twin pair, step 5 keeps the node pairs of its one tuple whose two node URNs differ: its homonymous node pairs. An unresolved
     * twin pair gives no entry.
     *
     * One row is one entry: a homonymous node pair, the role at which the twin pair gives it, and the twin pair. Repeated entries stay. A twin pair
     * in which both relations hold a node twice gives the same homonymous node pair twice.
     *
     * Columns: forwardNodeUri, backfillNodeUri, matchedNodeName, role, forwardCanonicalId, backfillCanonicalId,
     * nonIdenticalTwinsAlsoWithinForwardFlow.
     */
    def computeHomonymousNodePairsPerTwinPair(twinPairsWithNameMatchedNodePairTuplesDF: DataFrame): DataFrame =
        val resolvedTwinPair = size(col("nameMatchedNodePairTuples")) === 1 // <-- an unresolved twin pair keeps its two possible answers
        val nodeUrnsDiffer   = col("nameMatchedNodePair.forwardNodeUri") =!= col("nameMatchedNodePair.backfillNodeUri")

        twinPairsWithNameMatchedNodePairTuplesDF
            .filter(resolvedTwinPair)
            .select(
              explode(col("nameMatchedNodePairTuples").getItem(0)).as("nameMatchedNodePair"), // <-- the one tuple of a resolved twin pair
              col("forwardCanonicalId"),
              col("backfillCanonicalId"),
              col("nonIdenticalTwinsAlsoWithinForwardFlow")
            )
            .filter(nodeUrnsDiffer) // <-- keeps the homonymous node pairs
            .select(
              col("nameMatchedNodePair.forwardNodeUri"),
              col("nameMatchedNodePair.backfillNodeUri"),
              col("nameMatchedNodePair.matchedNodeName"),
              col("nameMatchedNodePair.role"),
              col("forwardCanonicalId"),
              col("backfillCanonicalId"),
              col("nonIdenticalTwinsAlsoWithinForwardFlow")
            )

    /**
     * ==Step 5: each homonymous node pair once, with its number of twin pairs, the global table==
     *
     * One row is one homonymous node pair across all resolved twin pairs. `nonIdenticalTwinPairCount` is the number of distinct twin pairs that give
     * the node pair, each twin pair identified by forwardCanonicalId and backfillCanonicalId together. A twin pair that gives the same node pair
     * twice counts once. Each node has exactly one name (section 4.3 of BioMed Knowledge Graph (Graph) Data Model), so the node pair fixes
     * matchedNodeName, and grouping on it as well does not change the groups.
     *
     * Columns: forwardNodeUri, backfillNodeUri, matchedNodeName, nonIdenticalTwinPairCount. One row has the fields of
     * [[DataTypes.HomonymousNodePairWithTwinCount]].
     */
    def computeTwinPairCountPerHomonymousNodePair(homonymousNodePairsPerTwinPairDF: DataFrame): DataFrame =
        homonymousNodePairsPerTwinPairDF
            .groupBy(col("forwardNodeUri"), col("backfillNodeUri"), col("matchedNodeName"))
            .agg(countDistinct(col("forwardCanonicalId"), col("backfillCanonicalId")).as("nonIdenticalTwinPairCount"))

    /**
     * ==Step 5: the unresolved twin pairs==
     *
     * The twin pairs of rule C3, with their two possible answers in `nameMatchedNodePairTuples`. The first is the straight answer, in which
     * forward-flow node 1 goes with backfill node 1, and forward-flow node 2 goes with backfill node 2. The second is the crossed answer, in which
     * forward-flow node 1 goes with backfill node 2, and forward-flow node 2 goes with backfill node 1.
     *
     * Columns: forwardCanonicalId, backfillCanonicalId, nameMatchedNodePairTuples.
     */
    def computeTwinPairsWithMultipleNameMatchedNodePairTuples(twinPairsWithNameMatchedNodePairTuplesDF: DataFrame): DataFrame =
        val unresolvedTwinPair = size(col("nameMatchedNodePairTuples")) === 2 // <-- rule C3 keeps both possible answers

        twinPairsWithNameMatchedNodePairTuplesDF
            .filter(unresolvedTwinPair)
            .select(col("forwardCanonicalId"), col("backfillCanonicalId"), col("nameMatchedNodePairTuples"))
