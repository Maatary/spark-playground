package biomed3471.codexrefactored

import biomed3471.codex.DataOps as ExhaustiveDataOps
import biomed3471.codexrefactored.DataOps.*
import biomed3471.codexrefactored.DataTypes.{BackfillSource, DatasetNames, DefaultInputPath, ForwardFlowSource, OutputPrefix, RunConfig}
import biomed3471.codexrefactored.IOOps.makeSparkSession
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.*

import java.nio.file.Files
import java.util.Locale
import scala.jdk.CollectionConverters.*

/** Behavior-free fixture data and explicit expected schemas for Step 5 checks. */
private object StepFiveCheckData:

    final case class Node(uri: String, name: String)

    final case class Fixture(
        label: String,
        forward: Row,
        backfill: Row,
        shared: Boolean,
        expectedResults: Vector[Vector[Row]]
    )

    final case class CheckedOutputs(
        full: Vector[Row],
        detail: Vector[Row],
        global: Vector[Row],
        unresolved: Vector[Row]
    )

    /** A twin pair's canonical relation IDs, ordered as (forwardCanonicalId, backfillCanonicalId). */
    type TwinId = (String, String)
    /** Counts tuples by their node-pair contents, with repeated node-pair records counted inside each tuple. */
    type ResultMultiset = Map[Map[Row, Int], Int]

    val BaselineRun = "2026-10-21-phase-one-230855"
    val HistoricalLayoutRun = "2026-10-21-phase-one-215353"
    val MaximumCheckedRows = 10000

    val RelationSchema = StructType(Vector(
        StructField("canonicalId", StringType),
        StructField("relationship_type", StringType),
        StructField("direction", StringType),
        StructField("node1_urn", StringType),
        StructField("node1_name", StringType),
        StructField("node2_urn", StringType),
        StructField("node2_name", StringType),
        StructField("sources", ArrayType(StringType))
    ))

    val ComparisonKeySchema = StructType(Vector(
        StructField("relationship_type", StringType),
        StructField("nodeNames", ArrayType(StringType))
    ))

    val NestedRelationSchema: StructType = RelationSchema.add("comparisonKey", ComparisonKeySchema)

    val TwinSchema = StructType(Vector(
        StructField("forwardCanonicalId", StringType),
        StructField("backfillCanonicalId", StringType),
        StructField("nonIdenticalTwinsAlsoWithinForwardFlow", BooleanType),
        StructField("forward", NestedRelationSchema),
        StructField("backfill", NestedRelationSchema)
    ))

    val NodePairSchema = StructType(Vector(
        StructField("role", StringType),
        StructField("forwardNodeUri", StringType),
        StructField("backfillNodeUri", StringType),
        StructField("matchedNodeName", StringType)
    ))

    val ResultType = ArrayType(ArrayType(NodePairSchema))
    val FullSchema: StructType = TwinSchema.add("nameMatchedNodePairTuples", ResultType)

    val DetailSchema = StructType(Vector(
        StructField("forwardNodeUri", StringType),
        StructField("backfillNodeUri", StringType),
        StructField("matchedNodeName", StringType),
        StructField("role", StringType),
        StructField("forwardCanonicalId", StringType),
        StructField("backfillCanonicalId", StringType),
        StructField("nonIdenticalTwinsAlsoWithinForwardFlow", BooleanType)
    ))

    val GlobalSchema = StructType(Vector(
        StructField("forwardNodeUri", StringType),
        StructField("backfillNodeUri", StringType),
        StructField("matchedNodeName", StringType),
        StructField("nonIdenticalTwinPairCount", LongType)
    ))

    val UnresolvedSchema = StructType(Vector(
        StructField("forwardCanonicalId", StringType),
        StructField("backfillCanonicalId", StringType),
        StructField("nameMatchedNodePairTuples", ResultType)
    ))

    val OccurrenceSchema = StructType(RelationSchema.fields.toVector
        .filterNot(field => Set("canonicalId", "sources").contains(field.name))
        :+ StructField("source", StringType))

    val LuteolinForward = Node("biodata:smallmol/Concept_700527820", "luteolin")
    val LuteolinBackfill = Node("biodata:smallmol/Concept_86446482", "luteolin")
    val Inflammation = Node("biodata:disease/Concept_86432034", "Inflammation")
    val MultipleSclerosis = Node("biodata:disease/Concept_86458342", "Multiple Sclerosis")
    val DorzolamideForward = Node("biodata:smallmol/Concept_700526633", "dorzolamide")
    val DorzolamideBackfill = Node("biodata:smallmol/Concept_86396600", "dorzolamide")
    val CarbonicAnhydraseForward = Node("biodata:functionalclass/EC_4.2.1.1", "carbonic anhydrase")
    val CarbonicAnhydraseBackfill = Node("biodata:functionalclass/PROTYP601", "carbonic anhydrase")

/** Fixture construction, assertions and bounded historical-output comparisons. */
private object StepFiveCheckOps:
    import StepFiveCheckData.*

    /**
     * Creates an explicitly synthetic node for a named fixture.
     *
     * @param scope fixture-specific identifier namespace
     * @param suffix distinguishing suffix within that fixture
     * @param name graph name, including deliberate capitalization and surrounding spaces
     * @param nodeType node-type component of the full node URN
     * @return a node that cannot be confused with a real vocabulary finding
     */
    private def fixtureNode(scope: String, suffix: String, name: String, nodeType: String = "protein"): Node =
        Node(s"biodata:$nodeType/Fixture_${scope}_$suffix", name)

    /**
     * Supplies a valid canonical relation and its name key as fixture input.
     * The name key is stored in comparisonKey.
     *
     * The canonical relation ID is part of the fixture input; it does not supply the expected
     * result for Step 1 checks.
     * Binding partners follow lexicographic node TERMite ID order in the canonical relation ID. A node TERMite ID is the final component of its
     * node URN. Each node's name stays attached to its node URN when ordering the participants.
     *
     * @param relationType exported relation type
     * @param first first supplied participant before ordering undirected partners by node TERMite ID
     * @param second second supplied participant before ordering undirected partners by node TERMite ID
     * @param sources exact occurrence-source set
     * @return one row matching NestedRelationSchema
     */
    private def relation(relationType: String, first: Node, second: Node, sources: Vector[String]): Row =
        val typeName = relationType.split(':').last.toLowerCase(Locale.ROOT)
        val undirected = typeName == "binding"
        val suppliedParticipants = Vector(first, second)
        val participants =
            if undirected then suppliedParticipants.sortBy(_.uri.split('/').last)
            else suppliedParticipants
        val firstParticipant = participants.head
        val secondParticipant = participants.last
        val rawIds = participants.map(_.uri.split('/').last)
        val canonicalId =
            if undirected then s"biodata:$typeName/partner/${rawIds.head}/partner/${rawIds.last}"
            else s"biodata:$typeName/regulator/${rawIds.head}/target/${rawIds.last}"
        val names = participants.map(_.name.trim.toLowerCase(Locale.ROOT))
        Row(
            canonicalId, relationType, if undirected then "undirected" else "node1_to_node2",
            firstParticipant.uri, firstParticipant.name, secondParticipant.uri, secondParticipant.name, sources.sorted,
            Row(relationType, if undirected then names.sorted else names)
        )

    /**
     * Constructs an explicitly specified expected name-matched node pair.
     *
     * @param role expected regulator, target or partner role
     * @param forward expected forward node
     * @param backfill expected backfill node
     * @return the name-matched node pair as a record with role first
     */
    private def pair(role: String, forward: Node, backfill: Node): Row =
        Row(role, forward.uri, backfill.uri, forward.name.trim.toLowerCase(Locale.ROOT))

    /**
     * Bundles one twin pair's input with independently specified expected tuples.
     *
     * @param label readable case name used in failures
     * @param relationType exported relation type
     * @param forward first and second forward participants
     * @param backfill first and second backfill participants
     * @param expected name-matched node pair tuples, including repeated node pairs
     * @param shared whether the backfill relation also occurs in forward flow
     * @return one fixture; no matching or resolution algorithm runs here
     */
    private def fixture(
        label: String,
        relationType: String,
        forward: (Node, Node),
        backfill: (Node, Node),
        expected: Vector[Vector[Row]],
        shared: Boolean = false
    ): Fixture =
        Fixture(
            label,
            relation(relationType, forward._1, forward._2, Vector(ForwardFlowSource)),
            relation(relationType, backfill._1, backfill._2,
                if shared then Vector(ForwardFlowSource, BackfillSource) else Vector(BackfillSource)),
            shared, expected
        )

    /**
     * Reproduces the three real twin pairs in the <a href="README.md#real-examples">README's four detailed examples</a>.
     *
     * @return twin-pair fixtures with expected tuples, their canonical relation IDs and their original full node URNs
     */
    private def realFixtures: Vector[Fixture] = Vector(
        fixture("luteolin / inflammation", "biomed:Regulation",
            (LuteolinForward, Inflammation), (LuteolinBackfill, Inflammation),
            Vector(Vector(pair("regulator", LuteolinForward, LuteolinBackfill),
                pair("target", Inflammation, Inflammation))), shared = true),
        fixture("luteolin / multiple sclerosis", "biomed:Regulation",
            (LuteolinForward, MultipleSclerosis), (LuteolinBackfill, MultipleSclerosis),
            Vector(Vector(pair("regulator", LuteolinForward, LuteolinBackfill),
                pair("target", MultipleSclerosis, MultipleSclerosis)))),
        fixture("dorzolamide / carbonic anhydrase", "biomed:DirectRegulation",
            (DorzolamideForward, CarbonicAnhydraseForward), (DorzolamideBackfill, CarbonicAnhydraseBackfill),
            Vector(Vector(pair("regulator", DorzolamideForward, DorzolamideBackfill),
                pair("target", CarbonicAnhydraseForward, CarbonicAnhydraseBackfill))))
    )

    /**
     * Specifies directed, Binding and repetition cases absent from the baseline.
     *
     * Expected name-matched node pair tuples are written out per case. The three
     * count cases intentionally share canonical relation IDs to distinguish counting twin pairs by (forwardCanonicalId, backfillCanonicalId)
     * from counting detailed entries or either canonical relation ID alone.
     *
     * @return isolated valid twin pairs and their one or two expected tuples
     */
    private def syntheticFixtures: Vector[Fixture] =
        val directedFirst = fixtureNode("Directed", "1", " EGFR ")
        val directedShared = fixtureNode("Directed", "2", "EGFR")
        val directedLast = fixtureNode("Directed", "3", "EGFR")
        val insulinForward = fixtureNode("Straight", "1", "Insulin")
        val insulinBackfill = fixtureNode("Straight", "2", "insulin")
        val receptor = fixtureNode("Straight", "3", "INSR")
        val crossedInsulinForward = fixtureNode("Crossed", "1", " Insulin ")
        val crossedInsulinBackfill = fixtureNode("Crossed", "4", "insulin")
        val crossedReceptorForward = fixtureNode("Crossed", "3", "INSR")
        val crossedReceptorBackfill = fixtureNode("Crossed", "2", " insr ")
        val sharedFirst = fixtureNode("Shared", "1", " EGFR ")
        val sharedNode = fixtureNode("Shared", "2", "egfr")
        val sharedLast = fixtureNode("Shared", "3", "EgFr")
        val sharedCrossedBackfill = fixtureNode("SharedCrossed", "1", "EGFR")
        val sharedCrossedNode = fixtureNode("SharedCrossed", "2", "EGFR")
        val sharedCrossedForward = fixtureNode("SharedCrossed", "3", "EGFR")
        val sharedLastForward = fixtureNode("SharedLast", "1", "EGFR")
        val sharedLastBackfill = fixtureNode("SharedLast", "2", "EGFR")
        val sharedLastNode = fixtureNode("SharedLast", "3", "EGFR")
        val repeatedShared = fixtureNode("RepeatedShared", "1", "EGFR")
        val repeatedOther = fixtureNode("RepeatedShared", "2", "EGFR")
        val repeatedBackfillOther = fixtureNode("RepeatedBackfillShared", "1", "EGFR")
        val repeatedBackfillShared = fixtureNode("RepeatedBackfillShared", "2", "EGFR")
        val countFirst = fixtureNode("Count", "1", "EGFR")
        val countSecond = fixtureNode("Count", "2", "EGFR")
        val countThird = fixtureNode("Count", "3", "EGFR")
        val countFourth = fixtureNode("Count", "4", "EGFR")
        val ambiguousFirst = fixtureNode("Ambiguous", "1", " EGFR ")
        val ambiguousSecond = fixtureNode("Ambiguous", "2", "egfr  ")
        val ambiguousThird = fixtureNode("Ambiguous", "3", "EGFR")
        val ambiguousFourth = fixtureNode("Ambiguous", "4", "EGFR")
        val lowercaseFirst = fixtureNode("Lowercase", "1", "EGFR")
        val lowercaseSecond = fixtureNode("Lowercase", "2", "egfr")
        val lowercaseThird = fixtureNode("Lowercase", "3", "EgFr")
        val lowercaseFourth = fixtureNode("Lowercase", "4", "eGFR")
        val processDisease = fixtureNode("Type", "Process", "fixture process", "disease")
        val processCell = fixtureNode("Type", "Process", "fixture process", "cellprocess")
        val typeTargetForward = fixtureNode("Type", "TargetForward", "fixture target")
        val typeTargetBackfill = fixtureNode("Type", "TargetBackfill", "fixture target")
        val undirectedDisease = fixtureNode("UndirectedType", "1", "fixture process", "disease")
        val undirectedCell = fixtureNode("UndirectedType", "1", "fixture process", "cellprocess")
        val undirectedForward = fixtureNode("UndirectedType", "2", "fixture process")
        val undirectedBackfill = fixtureNode("UndirectedType", "3", "fixture process")

        Vector(
            fixture("directed shared URI in opposite roles", "biomed:Regulation",
                (directedFirst, directedShared), (directedShared, directedLast),
                Vector(Vector(pair("regulator", directedFirst, directedShared), pair("target", directedShared, directedLast)))),
            fixture("directed repetition on both sides", "biomed:Regulation",
                (directedFirst, directedFirst), (directedLast, directedLast),
                Vector(Vector(pair("regulator", directedFirst, directedLast), pair("target", directedFirst, directedLast)))),
            fixture("Binding different names, straight, one homonymous pair", "biomed:Binding",
                (insulinForward, receptor), (insulinBackfill, receptor),
                Vector(Vector(pair("partner", insulinForward, insulinBackfill), pair("partner", receptor, receptor)))),
            fixture("Binding different names, crossed, two homonymous pairs", "biomed:Binding",
                (crossedInsulinForward, crossedReceptorForward), (crossedReceptorBackfill, crossedInsulinBackfill),
                Vector(Vector(pair("partner", crossedInsulinForward, crossedInsulinBackfill),
                    pair("partner", crossedReceptorForward, crossedReceptorBackfill)))),
            fixture("Binding shared URI wins", "biomed:Binding",
                (sharedFirst, sharedNode), (sharedNode, sharedLast),
                Vector(Vector(pair("partner", sharedFirst, sharedLast), pair("partner", sharedNode, sharedNode)))),
            fixture("Binding shared URI wins at the same position", "biomed:Binding",
                (sharedFirst, sharedNode), (sharedFirst, sharedLast),
                Vector(Vector(pair("partner", sharedFirst, sharedFirst), pair("partner", sharedNode, sharedLast)))),
            fixture("Binding shared URI at forward first and backfill second", "biomed:Binding",
                (sharedCrossedNode, sharedCrossedForward), (sharedCrossedBackfill, sharedCrossedNode),
                Vector(Vector(pair("partner", sharedCrossedNode, sharedCrossedNode),
                    pair("partner", sharedCrossedForward, sharedCrossedBackfill)))),
            fixture("Binding shared URI at both second positions", "biomed:Binding",
                (sharedLastForward, sharedLastNode), (sharedLastBackfill, sharedLastNode),
                Vector(Vector(pair("partner", sharedLastForward, sharedLastBackfill),
                    pair("partner", sharedLastNode, sharedLastNode)))),
            fixture("Binding repeated shared URI", "biomed:Binding",
                (repeatedShared, repeatedShared), (repeatedShared, repeatedOther),
                Vector(Vector(pair("partner", repeatedShared, repeatedShared), pair("partner", repeatedShared, repeatedOther)))),
            fixture("Binding shared URI in a self-referencing backfill relation", "biomed:Binding",
                (repeatedBackfillOther, repeatedBackfillShared), (repeatedBackfillShared, repeatedBackfillShared),
                Vector(Vector(pair("partner", repeatedBackfillOther, repeatedBackfillShared),
                    pair("partner", repeatedBackfillShared, repeatedBackfillShared)))),
            fixture("Binding repetition on both sides", "biomed:Binding",
                (countFirst, countFirst), (countSecond, countSecond),
                Vector(Vector(pair("partner", countFirst, countSecond), pair("partner", countFirst, countSecond))), shared = true),
            fixture("Binding repetition on forward side", "biomed:Binding",
                (countFirst, countFirst), (countSecond, countThird),
                Vector(Vector(pair("partner", countFirst, countSecond), pair("partner", countFirst, countThird)))),
            fixture("Binding repetition on backfill side", "biomed:Binding",
                (countFirst, countFourth), (countSecond, countSecond),
                Vector(Vector(pair("partner", countFirst, countSecond), pair("partner", countFourth, countSecond))), shared = true),
            fixture("Binding two genuinely distinct results", "biomed:Binding",
                (ambiguousFirst, ambiguousSecond), (ambiguousThird, ambiguousFourth),
                Vector(
                    Vector(pair("partner", ambiguousFirst, ambiguousThird), pair("partner", ambiguousSecond, ambiguousFourth)),
                    Vector(pair("partner", ambiguousFirst, ambiguousFourth), pair("partner", ambiguousSecond, ambiguousThird))
                )),
            fixture("Binding names equal only after lowercase, two distinct results", "biomed:Binding",
                (lowercaseFirst, lowercaseSecond), (lowercaseThird, lowercaseFourth),
                Vector(
                    Vector(pair("partner", lowercaseFirst, lowercaseThird), pair("partner", lowercaseSecond, lowercaseFourth)),
                    Vector(pair("partner", lowercaseFirst, lowercaseFourth), pair("partner", lowercaseSecond, lowercaseThird))
                )),
            fixture("Binding same raw ID under different node types is not a shared node", "biomed:Binding",
                (undirectedDisease, undirectedForward), (undirectedCell, undirectedBackfill),
                Vector(
                    Vector(pair("partner", undirectedDisease, undirectedCell), pair("partner", undirectedForward, undirectedBackfill)),
                    Vector(pair("partner", undirectedDisease, undirectedBackfill), pair("partner", undirectedForward, undirectedCell))
                )),
            fixture("same raw ID under different node types", "biomed:Regulation",
                (processDisease, typeTargetForward), (processCell, typeTargetBackfill),
                Vector(Vector(pair("regulator", processDisease, processCell), pair("target", typeTargetForward, typeTargetBackfill))))
        )

    /**
     * Creates a local DataFrame with an explicit schema, including empty inputs.
     *
     * @param spark the single check session
     * @param rows bounded fixture or historical-result rows
     * @param schema exact expected input shape
     * @return a small DataFrame; no encoder, external read or write is used
     */
    private def frame(spark: SparkSession, rows: Seq[Row], schema: StructType): DataFrame =
        spark.createDataFrame(rows.asJava, schema)

    /**
     * Collects only a bounded check result, rejecting an unexpectedly large input.
     *
     * @param data fixture or historical Step 5 result, never the occurrence export
     * @param label name for a size-limit failure
     * @return at most MaximumCheckedRows rows, including repeated rows
     */
    private def smallRows(data: DataFrame, label: String): Vector[Row] =
        val rows = data.limit(MaximumCheckedRows + 1).collect().toVector
        require(rows.size <= MaximumCheckedRows, s"$label exceeds the bounded-check limit of $MaximumCheckedRows rows")
        rows

    /**
     * Describes schema names, order and types while ignoring inferred nullability.
     *
     * @param dataType schema component to inspect
     * @return column names, order and types, including nested fields, for exact comparison
     */
    private def shape(dataType: DataType): String = dataType match
        case structType: StructType => structType.fields.map(field => s"${field.name}:${shape(field.dataType)}").mkString("{", ",", "}")
        case ArrayType(elementType, _) => s"array<${shape(elementType)}>"
        case primitive => primitive.typeName

    /**
     * Checks output names, aliases, order and nested element types.
     *
     * @param label output under review
     * @param actual produced DataFrame
     * @param expected agreed schema
     */
    private def checkSchema(label: String, actual: DataFrame, expected: StructType): Unit =
        require(shape(actual.schema) == shape(expected),
            s"$label schema differs. Expected ${shape(expected)}; got ${shape(actual.schema)}")

    /**
     * Counts entries without collapsing repeated participants or detailed rows.
     *
     * @param entries records to compare without relying on listing order
     * @return each record and the number of times it appears
     */
    private def multiset[A](entries: Seq[A]): Map[A, Int] =
        entries.groupMapReduce(identity)(_ => 1)(_ + _)

    /**
     * Represents name-matched node pair tuples as counts, ignoring listing order while retaining repetitions.
     *
     * @param results one or two name-matched node pair tuples
     * @return each tuple's node-pair counts and the number of times that tuple occurs
     */
    private def resultMultiset(results: Seq[Seq[Row]]): ResultMultiset =
        multiset(results.map(multiset))

    /**
     * Extracts the two canonical relation IDs that identify a twin pair.
     *
     * @param row a collected row for a twin pair, including an unresolved twin pair
     * @return (forwardCanonicalId, backfillCanonicalId), with the forward-flow canonical relation ID first
     */
    private def twinId(row: Row): TwinId =
        (row.getAs[String]("forwardCanonicalId"), row.getAs[String]("backfillCanonicalId"))

    /**
     * Reads nameMatchedNodePairTuples without assuming Spark's Seq subtype.
     *
     * @param row a full or unresolved Step 5 result
     * @return one or two tuples as immutable vectors, retaining repeated node-pair records
     */
    private def results(row: Row): Vector[Vector[Row]] =
        row.getSeq[scala.collection.Seq[Row]](row.fieldIndex("nameMatchedNodePairTuples")).iterator.map(_.toVector).toVector

    /**
     * Executes the four public Step 5 operations over a bounded input.
     *
     * @param input valid non-identical twin pairs, or an empty typed subset
     * @param label check group used in schema and size failures
     * @return all four small outputs; no persistence or caching occurs
     */
    private def evaluate(input: DataFrame, label: String): CheckedOutputs =
        checkSchema(s"$label input", input, TwinSchema)
        val full = input.withNameMatchedNodePairTuples
        val detail = full.selectHomonymousNodePairs
        val global = detail.aggregateHomonymousNodePairsWithTwinCount
        val unresolved = full.selectTwinPairsWithMultipleNameMatchedNodePairTuples
        checkedOutputs(full, detail, global, unresolved, label, forwardPositionOrder = true)

    /**
     * Executes the unchanged original Spark implementation on the same input.
     *
     * Calls the actual Codex operations rather than reproducing their algorithm
     * in the checks. The old and new implementations keep separate namespaces.
     *
     * @param input the same bounded twin-pair rows used by the direct implementation
     * @param label check group label
     * @return the original implementation's four checked output collections
     */
    private def evaluateExhaustive(input: DataFrame, label: String): CheckedOutputs =
        checkSchema(s"$label exhaustive input", input, TwinSchema)
        val full = ExhaustiveDataOps.withNameMatchedNodePairTuples(input)
        val detail = ExhaustiveDataOps.selectHomonymousNodePairs(full)
        val global = ExhaustiveDataOps.aggregateHomonymousNodePairsWithTwinCount(detail)
        val unresolved = ExhaustiveDataOps.selectTwinPairsWithMultipleNameMatchedNodePairTuples(full)
        checkedOutputs(full, detail, global, unresolved, s"$label exhaustive", forwardPositionOrder = false)

    /**
     * Checks schemas, collects bounded outputs and verifies their stored ordering.
     *
     * Each name-matched node pair tuple keeps both entries. Listing order is checked separately from tuple contents: direct results list the pair
     * for forward-flow node 1 first and the pair for forward-flow node 2 second, while the original implementation sorts by forwardNodeUri,
     * backfillNodeUri and role, in that order.
     *
     * @param full all twin pairs with their name-matched node pair tuples
     * @param detail selected homonymous node-pair entries, including repeated entries
     * @param global homonymous node pairs and their distinct twin-pair counts
     * @param unresolved twin pairs retaining two alternative tuples
     * @param label implementation and check group label
     * @param forwardPositionOrder whether this is the direct implementation's ordering
     * @return the four bounded collections
     */
    private def checkedOutputs(
        full: DataFrame,
        detail: DataFrame,
        global: DataFrame,
        unresolved: DataFrame,
        label: String,
        forwardPositionOrder: Boolean
    ): CheckedOutputs =
        checkSchema(s"$label full", full, FullSchema)
        checkSchema(s"$label detail", detail, DetailSchema)
        checkSchema(s"$label global", global, GlobalSchema)
        checkSchema(s"$label unresolved", unresolved, UnresolvedSchema)
        val fullRows = smallRows(full, s"$label full")
        fullRows.foreach { row =>
            val completeResults = results(row)
            require(completeResults.size == 1 || completeResults.size == 2, s"$label: wrong complete-result count")
            completeResults.foreach { tuple =>
                require(tuple.size == 2, s"$label: a complete result lost a participant")
                if forwardPositionOrder then
                    val forward = row.getAs[Row]("forward")
                    val expectedUris = Vector(forward.getAs[String]("node1_urn"), forward.getAs[String]("node2_urn"))
                    require(tuple.map(_.getAs[String]("forwardNodeUri")) == expectedUris,
                        s"$label: complete-result ordering does not follow forward positions")
                else
                    val sorted = tuple.sortBy(value => (value.getString(1), value.getString(2), value.getString(0)))
                    require(tuple == sorted, s"$label: complete-result ordering is not URI-first")
            }
        }
        CheckedOutputs(fullRows, smallRows(detail, s"$label detail"),
            smallRows(global, s"$label global"), smallRows(unresolved, s"$label unresolved"))

    /**
     * Compares direct-rule outputs with the actual exhaustive implementation.
     *
     * Compares how many times each name-matched node pair tuple appears and how many times
     * each node pair appears inside it, regardless of listing order. Also checks
     * how many times each detailed row appears. Metadata, roles, flags and
     * summary counts must match exactly.
     *
     * @param direct outputs from the new direct rules
     * @param exhaustive outputs from the unchanged original operations
     * @param label check group label
     */
    private def checkEquivalentOutputs(direct: CheckedOutputs, exhaustive: CheckedOutputs, label: String): Unit =
        val originalResults = exhaustive.full.map(row => twinId(row) -> resultMultiset(results(row))).toMap
        require(originalResults.size == exhaustive.full.size && direct.full.size == exhaustive.full.size,
            s"$label: exhaustive comparison lost or duplicated twin identities")
        require(direct.full.map(twinId).toSet == originalResults.keySet, s"$label: exhaustive comparison changed twin identities")
        direct.full.foreach { row =>
            require(resultMultiset(results(row)) == originalResults(twinId(row)),
                s"$label: direct and exhaustive complete results differ for ${twinId(row)}")
        }
        val directMetadata = direct.full.map(row => Row.fromSeq(TwinSchema.fieldNames.map(name => row.getAs[Any](name)).toVector))
        val originalMetadata = exhaustive.full.map(row => Row.fromSeq(TwinSchema.fieldNames.map(name => row.getAs[Any](name)).toVector))
        require(multiset(directMetadata) == multiset(originalMetadata), s"$label: direct and exhaustive metadata differ")
        require(multiset(direct.detail) == multiset(exhaustive.detail), s"$label: direct and exhaustive detailed multisets differ")
        require(multiset(direct.global) == multiset(exhaustive.global), s"$label: direct and exhaustive summary counts differ")
        val originalUnresolved = exhaustive.unresolved.map(row => twinId(row) -> resultMultiset(results(row))).toMap
        require(originalUnresolved.size == exhaustive.unresolved.size && direct.unresolved.size == exhaustive.unresolved.size &&
            direct.unresolved.map(twinId).toSet == originalUnresolved.keySet, s"$label: direct and exhaustive unresolved twins differ")
        direct.unresolved.foreach { row =>
            require(resultMultiset(results(row)) == originalUnresolved(twinId(row)),
                s"$label: direct and exhaustive unresolved alternatives differ")
        }

    /**
     * Calculates the expected global counts from a limited number of expected or historical detail rows.
     *
     * Counts distinct (forwardCanonicalId, backfillCanonicalId) pairs in ordinary Scala sets, independently of the Spark aggregation. Both canonical
     * relation IDs identify the twin pair, so repeated detail entries from one twin pair contribute once. Role and flag do not split node-pair groups.
     *
     * @param detail rows in the agreed seven-column detailed order
     * @return global rows with Long counts and the forward-flow and backfill node URNs
     */
    private def expectedGlobal(detail: Seq[Row]): Vector[Row] =
        detail.groupBy(row => (row.getString(0), row.getString(1))).toVector.map { case ((forward, backfill), rows) =>
            val completeTwins = rows.map(row => (row.getString(4), row.getString(5))).toSet
            Row(forward, backfill, rows.head.getString(2), completeTwins.size.toLong)
        }

    /**
     * Checks the expected results for each case, including the detail and global outputs.
     *
     * @param spark the existing check session
     * @param fixtures isolated twin-pair fixtures with literal expected name-matched node pair tuples
     * @param label check group label
     * @return verified small outputs for additional focused assertions
     */
    private def checkFixtures(spark: SparkSession, fixtures: Vector[Fixture], label: String): CheckedOutputs =
        val inputRows = fixtures.map(value => Row(value.forward.getString(0), value.backfill.getString(0), value.shared, value.forward, value.backfill))
        val expectedById = fixtures.map(value => (value.forward.getString(0), value.backfill.getString(0)) -> value).toMap
        require(expectedById.size == fixtures.size, s"$label fixtures accidentally repeat a complete twin ID")
        val input = frame(spark, inputRows, TwinSchema)
        val output = evaluate(input, label)
        checkEquivalentOutputs(output, evaluateExhaustive(input, label), label)
        require(output.full.size == fixtures.size, s"$label lost or duplicated twins")
        require(output.full.map(twinId).toSet == expectedById.keySet, s"$label changed twin identities")
        output.full.foreach { row =>
            val expected = expectedById(twinId(row))
            val actualResults = results(row)
            require(resultMultiset(actualResults) == resultMultiset(expected.expectedResults), s"${expected.label}: wrong complete results: $actualResults")
            require(row.getAs[Row]("forward") == expected.forward && row.getAs[Row]("backfill") == expected.backfill,
                s"${expected.label}: participant fields, graph names, source sets or comparison key changed")
            require(row.getAs[Boolean]("nonIdenticalTwinsAlsoWithinForwardFlow") == expected.shared, s"${expected.label}: flag changed")
        }
        val expectedDetail = fixtures.filter(_.expectedResults.size == 1).flatMap { value =>
            value.expectedResults.head.filter(pair => pair.getString(1) != pair.getString(2)).map { pair =>
                Row(pair.getString(1), pair.getString(2), pair.getString(3), pair.getString(0),
                    value.forward.getString(0), value.backfill.getString(0), value.shared)
            }
        }
        require(multiset(output.detail) == multiset(expectedDetail), s"$label: detailed entries, roles or repetitions differ")
        require(multiset(output.global) == multiset(expectedGlobal(expectedDetail)), s"$label: global complete-twin counts differ")
        val expectedUnresolved = expectedById.filter(_._2.expectedResults.size == 2)
        require(output.unresolved.size == expectedUnresolved.size, s"$label: wrong unresolved twin count")
        require(output.unresolved.map(twinId).toSet == expectedUnresolved.keySet, s"$label: wrong unresolved twins")
        output.unresolved.foreach { row =>
            require(resultMultiset(results(row)) == resultMultiset(expectedUnresolved(twinId(row)).expectedResults),
                s"$label: unresolved alternatives were changed")
        }
        output

    /**
     * Checks source aggregation, source selection and Step 4 inclusion on eight synthetic relation occurrences.
     *
     * @param spark the existing session
     * @return no value; failures identify source, name-key, canonical relation ID or metadata regressions
     */
    private def checkSourceSelection(spark: SparkSession): Unit =
        val forward = fixtureNode("Sources", "Forward", "Luteolin", "smallmol")
        val backfill = fixtureNode("Sources", "Backfill", "luteolin", "smallmol")
        val shared = fixtureNode("Sources", "Shared", "luteolin", "smallmol")
        val otherType = fixtureNode("Sources", "OtherType", "luteolin", "smallmol")
        val reverse = fixtureNode("Sources", "Reverse", "luteolin", "smallmol")
        val whitespace = fixtureNode("Sources", "Whitespace", "  LuTeOlIn  ", "smallmol")
        val target = fixtureNode("Sources", "Target", "Inflammation", "disease")
        val relations = Vector(
            relation("biomed:Regulation", forward, target, Vector(ForwardFlowSource)),
            relation("biomed:Regulation", forward, target, Vector(ForwardFlowSource)),
            relation("biomed:Regulation", backfill, target, Vector(BackfillSource)),
            relation("biomed:Regulation", shared, target, Vector(ForwardFlowSource)),
            relation("biomed:Regulation", shared, target, Vector(BackfillSource)),
            relation("biomed:DirectRegulation", otherType, target, Vector(BackfillSource)),
            relation("biomed:Regulation", target, reverse, Vector(BackfillSource)),
            relation("biomed:Regulation", whitespace, target, Vector(BackfillSource))
        )
        val occurrences = relations.map(row => Row.fromSeq(row.toSeq.slice(1, 7) :+ row.getSeq[String](7).head))
        val canonical = aggregateCanonicalRelations(frame(spark, occurrences, OccurrenceSchema))
        checkSchema("canonical relations", canonical, RelationSchema)
        val canonicalRows = smallRows(canonical, "source grouping")
        require(canonicalRows.size == 6, "Occurrence grouping failed to retain one row per canonical ID")
        require(canonicalRows.exists(_.getAs[String]("canonicalId") ==
            "biodata:regulation/regulator/Fixture_Sources_Forward/target/Fixture_Sources_Target"), "Directed canonical ID changed")
        val keyed = canonical.withComparisonKey
        checkSchema("comparison key", keyed, NestedRelationSchema)
        val forwardOnlyRows = smallRows(canonical.forwardOnly, "forwardOnly")
        val backfillRows = smallRows(canonical.backfill, "backfill")
        val sharedCount = backfillRows.count(row => row.getSeq[String](row.fieldIndex("sources")).contains(ForwardFlowSource))
        require(forwardOnlyRows.size == 1 && backfillRows.size == 5 && sharedCount == 1,
            "Canonical source selections differ")
        val twins = smallRows(findNonIdenticalTwinPairs(canonical), "Step 4 twins")
        val expectedIds = Set(
            "biodata:regulation/regulator/Fixture_Sources_Backfill/target/Fixture_Sources_Target",
            "biodata:regulation/regulator/Fixture_Sources_Shared/target/Fixture_Sources_Target",
            "biodata:regulation/regulator/Fixture_Sources_Whitespace/target/Fixture_Sources_Target"
        )
        require(twins.size == 3 && twins.map(_.getAs[String]("backfillCanonicalId")).toSet == expectedIds,
            "Step 4 omitted shared backfill or a normalized-name match, or admitted a wrong type or role order")
        twins.foreach { row =>
            val shouldBeShared = row.getAs[String]("backfillCanonicalId").contains("Fixture_Sources_Shared")
            require(row.getAs[Boolean]("nonIdenticalTwinsAlsoWithinForwardFlow") == shouldBeShared, "Incorrect within-forward-flow flag")
            require(row.getAs[Row]("forward").getAs[String]("node1_name") == "Luteolin", "Comparison-key construction changed graph names")
        }
        val binding = syntheticFixtures.find(_.label.contains("crossed, two")).get
        val bindingRows = Vector(binding.forward, binding.backfill).map(row => Row.fromSeq(row.toSeq.take(8)))
        val keyedBinding = smallRows(frame(spark, bindingRows, RelationSchema).withComparisonKey, "Binding keys")
        require(keyedBinding.map(_.getAs[Row]("comparisonKey")).distinct.size == 1, "Binding keys did not sort names")
        require(multiset(keyedBinding.map(row => Row.fromSeq(row.toSeq.take(8)))) == multiset(bindingRows),
            "Binding key construction reordered original participants")

    /**
     * Checks aggregation when one forward-flow/backfill node pair has different roles or flags.
     *
     * These three detailed fixture entries are inputs to aggregation, not claims
     * about a new Step 5 resolution. Swapping the forward-flow and backfill node URNs creates a different node pair.
     *
     * @param spark the existing session
     */
    private def checkAggregationOrientation(spark: SparkSession): Unit =
        val forward = "biodata:protein/Fixture_Orientation_Forward"
        val backfill = "biodata:protein/Fixture_Orientation_Backfill"
        val rows = Vector(
            Row(forward, backfill, "fixture receptor", "regulator",
                "biodata:regulation/regulator/Fixture_Orientation_Forward/target/Fixture_Target_1",
                "biodata:regulation/regulator/Fixture_Orientation_Backfill/target/Fixture_Target_1", false),
            Row(forward, backfill, "fixture receptor", "target",
                "biodata:regulation/regulator/Fixture_Target_2/target/Fixture_Orientation_Forward",
                "biodata:regulation/regulator/Fixture_Target_2/target/Fixture_Orientation_Backfill", true),
            Row(backfill, forward, "fixture receptor", "regulator",
                "biodata:regulation/regulator/Fixture_Orientation_Backfill/target/Fixture_Target_3",
                "biodata:regulation/regulator/Fixture_Orientation_Forward/target/Fixture_Target_3", false)
        )
        val actual = frame(spark, rows, DetailSchema).aggregateHomonymousNodePairsWithTwinCount
        checkSchema("oriented aggregation", actual, GlobalSchema)
        require(multiset(smallRows(actual, "oriented aggregation")) == multiset(Vector(
            Row(forward, backfill, "fixture receptor", 2L), Row(backfill, forward, "fixture receptor", 1L)
        )), "Global aggregation grouped by role/flag or discarded forward/backfill orientation")

    /**
     * Runs the complete local fixture suite without external reads or writes.
     *
     * @param spark the single session shared with an optional baseline check
     */
    def runFixtures(spark: SparkSession): Unit =
        require(DatasetNames == List("canonicalRelations", "nonIdenticalTwinPairs",
            "twinPairsWithNameMatchedNodePairTuples", "homonymousNodePairs",
            "homonymousNodePairsWithTwinCount", "twinPairsWithMultipleNameMatchedNodePairTuples"), "DatasetNames does not contain the six agreed outputs in order")
        val real = checkFixtures(spark, realFixtures, "real examples")
        require(real.detail.size == 4, "The three real twins must yield four detailed entries")
        require(real.global.map(row => row.getString(2) -> row.getLong(3)).toMap ==
            Map("luteolin" -> 2L, "dorzolamide" -> 1L, "carbonic anhydrase" -> 1L), "Four-record counts confused with full saved-run counts")
        val synthetic = checkFixtures(spark, syntheticFixtures, "synthetic examples")
        val countedDetail = synthetic.detail.filter(row => row.getString(0) == "biodata:protein/Fixture_Count_1" &&
            row.getString(1) == "biodata:protein/Fixture_Count_2")
        require(countedDetail.size == 4 && countedDetail.map(_.getString(4)).distinct.size == 2 &&
            countedDetail.map(_.getString(5)).distinct.size == 2, "Complete-twin fixture lost its distinguishing repetitions")
        require(synthetic.global.exists(row => row.getString(0) == "biodata:protein/Fixture_Count_1" &&
            row.getString(1) == "biodata:protein/Fixture_Count_2" && row.getLong(3) == 3L), "Complete-twin count must be 3, not 4 or 2")
        checkFixtures(spark, syntheticFixtures.filter(_.expectedResults.size == 2), "all unresolved")
        checkFixtures(spark, Vector.empty, "empty subset")
        checkSourceSelection(spark)
        checkAggregationOrientation(spark)
        println(s"PASS fixtures: ${realFixtures.size + syntheticFixtures.size} explicit twins, direct/exhaustive equivalence, source selections, empty/unresolved subsets, schemas and complete-twin counts")

    /**
     * Reads one bounded historical result table outside the new output registry.
     *
     * @param spark existing session using the established credential provider
     * @param runId historical run selected by the caller
     * @param table historical Step 5 table name, explicitly selected in code
     * @return a local DataFrame containing at most MaximumCheckedRows saved rows
     */
    private def readBaseline(spark: SparkSession, runId: String, table: String): DataFrame =
        val data = spark.read.format("delta").load(s"$OutputPrefix$runId/$table/")
        frame(spark, smallRows(data, s"baseline $table"), data.schema)

    /**
     * Checks that Step 5 results match the saved outputs, using their historical or current table names.
     *
     * BaselineRun uses the final names. HistoricalLayoutRun predates them: its detailed
     * entries are homonymousNodePairsByTwin and its summary is homonymousNodePairs.
     * Other supplied runs use homonymousNodePairs for detail and homonymousNodePairsWithTwinCount for the summary. Reads
     * each small saved table once, compares name-matched node pair tuples and metadata,
     * checks how many times each detail row appears, and independently counts distinct
     * twin pairs from saved detail. Also compares the direct and unchanged exhaustive
     * implementations on those same saved twin pairs. The occurrence export and
     * writers are never used.
     *
     * @param spark the same session already used for fixtures
     * @param runId safe historical run component under OutputPrefix
     */
    def runBaseline(spark: SparkSession, runId: String): Unit =
        val twins = readBaseline(spark, runId, "nonIdenticalTwinPairs")
        val detailTable = if runId == HistoricalLayoutRun then "homonymousNodePairsByTwin" else "homonymousNodePairs"
        val summaryTable = if runId == HistoricalLayoutRun then "homonymousNodePairs" else "homonymousNodePairsWithTwinCount"
        val savedFull = readBaseline(spark, runId, "twinPairsWithNameMatchedNodePairTuples")
        val savedDetail = readBaseline(spark, runId, detailTable)
        val savedSummary = readBaseline(spark, runId, summaryTable)
        val savedUnresolved = readBaseline(spark, runId, "twinPairsWithMultipleNameMatchedNodePairTuples")
        checkSchema("saved full results", savedFull, FullSchema)
        checkSchema("saved detail", savedDetail, DetailSchema)
        checkSchema("saved summary", savedSummary, GlobalSchema)
        checkSchema("saved unresolved", savedUnresolved, UnresolvedSchema)
        val actual = evaluate(twins, "saved-output regression")
        checkEquivalentOutputs(actual, evaluateExhaustive(twins, "saved-output regression"), "saved-output regression")
        val twinRows = smallRows(twins, "saved twins")
        val savedFullRows = smallRows(savedFull, "saved full results")
        val detailRows = smallRows(savedDetail, "saved detail")
        val summaryRows = smallRows(savedSummary, "saved summary")
        val unresolvedRows = smallRows(savedUnresolved, "saved unresolved")
        val expectedResults = savedFullRows.map(row => twinId(row) -> resultMultiset(results(row))).toMap
        require(expectedResults.size == savedFullRows.size && actual.full.size == savedFullRows.size,
            "Baseline lost or duplicated complete twin identities")
        require(actual.full.map(twinId).toSet == expectedResults.keySet, "Baseline changed complete twin identities")
        actual.full.foreach(row => require(resultMultiset(results(row)) == expectedResults(twinId(row)), s"Baseline complete results changed for ${twinId(row)}"))
        val actualMetadata = actual.full.map(row => Row.fromSeq(TwinSchema.fieldNames.map(name => row.getAs[Any](name)).toVector))
        val savedMetadata = savedFullRows.map(row => Row.fromSeq(TwinSchema.fieldNames.map(name => row.getAs[Any](name)).toVector))
        require(multiset(actualMetadata) == multiset(twinRows) && multiset(savedMetadata) == multiset(twinRows),
            "Baseline changed canonical relation structs, sources or flag")
        require(multiset(actual.detail) == multiset(detailRows), "Baseline detailed multiset differs")
        require(multiset(actual.global) == multiset(summaryRows), "Baseline summary URI/name pairs or twin counts differ")
        require(multiset(actual.global) == multiset(expectedGlobal(detailRows)), "Baseline complete-twin counts differ from independent saved-detail counts")
        val expectedUnresolved = unresolvedRows.map(row => twinId(row) -> resultMultiset(results(row))).toMap
        require(actual.unresolved.size == unresolvedRows.size && actual.unresolved.map(twinId).toSet == expectedUnresolved.keySet,
            "Baseline unresolved subset differs")
        actual.unresolved.foreach(row => require(resultMultiset(results(row)) == expectedUnresolved(twinId(row)), "Baseline unresolved alternatives differ"))
        if Set(BaselineRun, HistoricalLayoutRun).contains(runId) then
            require((actual.full.size, actual.detail.size, actual.global.size, actual.unresolved.size) == (1911, 1942, 157, 0),
                "Pinned baseline counts differ from recorded evidence")
            val counts = actual.global.map(row => (row.getString(0), row.getString(1)) -> row.getLong(3)).toMap
            require(counts((LuteolinForward.uri, LuteolinBackfill.uri)) == 66L &&
                counts((DorzolamideForward.uri, DorzolamideBackfill.uri)) == 2L &&
                counts((CarbonicAnhydraseForward.uri, CarbonicAnhydraseBackfill.uri)) == 16L,
                "Pinned baseline example counts differ from complete saved-detail evidence")
        println(s"PASS baseline $runId: ${actual.full.size} twins, ${actual.detail.size} detailed entries, ${actual.global.size} global pairs, ${actual.unresolved.size} unresolved; no investigation datasets written")

/** Small Scala entry point for fixture checks and bounded historical regression. */
object StepFiveChecks:

    /**
     * Runs fixtures, optionally followed by saved-output regression in one session.
     *
     * Reuses the normal local Spark settings and launch-supplied heap. Reads saved outputs only when a baseline run is supplied and writes no
     * investigation tables. Spark closes after the checks.
     *
     * @param args no arguments, or --baseline-run <historical-run-name>
     */
    def main(args: Array[String]): Unit =
        val baseline = args.toList match
            case Nil => None
            case "--baseline-run" :: runId :: Nil if runId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,79}") => Some(runId)
            case _ => throw new IllegalArgumentException("Usage: StepFiveChecks [--baseline-run <historical-run-name>]")
        val scratch = Files.createTempDirectory("biomed3471-step-five-checks-")
        val config = RunConfig(DefaultInputPath, "step-five-checks", scratch)
        val spark = makeSparkSession(config)
        try
            spark.sparkContext.setLogLevel("ERROR")
            StepFiveCheckOps.runFixtures(spark)
            baseline.foreach(runId => StepFiveCheckOps.runBaseline(spark, runId))
        finally spark.stop()
        println("Step 5 checks completed successfully.")
