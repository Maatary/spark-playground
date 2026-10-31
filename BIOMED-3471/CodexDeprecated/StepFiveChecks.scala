package biomed3471.codexdeprecated

import biomed3471.codexdeprecated.DataOps.*
import biomed3471.codexdeprecated.DataTypes.{BackfillSource, DatasetNames, DefaultInputPath, ForwardFlowSource, OutputPrefix, RunConfig}
import biomed3471.codexdeprecated.IOOps.makeSparkSession
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.*

import java.nio.file.Files
import java.time.LocalDate
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

    type TwinId = (String, String)
    type ResultMultiset = Map[Map[Row, Int], Int]

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

/** Fixture construction, assertions and bounded saved-output comparisons. */
private object StepFiveCheckOps:
    import StepFiveCheckData.*

    /**
     * Creates an explicitly synthetic node for a named fixture.
     *
     * @param scope fixture-specific identifier namespace
     * @param suffix distinguishing suffix within that fixture
     * @param name graph-node label, including deliberate capitalization and surrounding spaces
     * @param nodeType node-type component of the full URI
     * @return a node that cannot be confused with a real vocabulary finding
     */
    private def fixtureNode(scope: String, suffix: String, name: String, nodeType: String = "protein"): Node =
        Node(s"biodata:$nodeType/Fixture_${scope}_$suffix", name)

    /**
     * Supplies a valid canonical relation and comparison key as fixture input.
     *
     * The identifier is construction data, not the oracle for Step 1 checks.
     * Binding IDs sort raw IDs; stored participants retain their supplied order.
     *
     * @param relationType exported relation type
     * @param first first stored participant
     * @param second second stored participant
     * @param sources exact occurrence-source set
     * @return one row matching NestedRelationSchema
     */
    private def relation(relationType: String, first: Node, second: Node, sources: Vector[String]): Row =
        val typeName = relationType.split(':').last.toLowerCase(Locale.ROOT)
        val undirected = typeName == "binding"
        val rawIds = Vector(first.uri.split('/').last, second.uri.split('/').last)
        val canonicalId =
            if undirected then s"biodata:$typeName/partner/${rawIds.min}/partner/${rawIds.max}"
            else s"biodata:$typeName/regulator/${rawIds.head}/target/${rawIds.last}"
        val names = Vector(first.name, second.name).map(_.trim.toLowerCase(Locale.ROOT))
        Row(
            canonicalId, relationType, if undirected then "undirected" else "node1_to_node2",
            first.uri, first.name, second.uri, second.name, sources.sorted,
            Row(relationType, if undirected then names.sorted else names)
        )

    /**
     * States one expected node correspondence explicitly, without choosing it.
     *
     * @param role expected regulator, target or partner role
     * @param forward expected forward node
     * @param backfill expected backfill node
     * @return the public role-first node-pair record
     */
    private def pair(role: String, forward: Node, backfill: Node): Row =
        Row(role, forward.uri, backfill.uri, forward.name.trim.toLowerCase(Locale.ROOT))

    /**
     * Bundles isolated twin inputs with independently specified complete results.
     *
     * @param label readable case name used in failures
     * @param relationType exported relation type
     * @param forward first and second forward participants
     * @param backfill first and second backfill participants
     * @param expected complete expected results, preserving repeated entries
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
     * Reproduces the three real twins behind batch 07's four detailed examples.
     *
     * @return expected node correspondences with their original full identifiers
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
     * Expected correspondences are written out per case. The three count cases
     * intentionally share complete canonical IDs to distinguish twin counting
     * from counting entries or either flow's IDs alone.
     *
     * @return isolated valid twins and their one or two complete expected results
     */
    private def syntheticFixtures: Vector[Fixture] =
        val directedFirst = fixtureNode("Directed", "1", " EGFR ")
        val directedShared = fixtureNode("Directed", "2", "EGFR")
        val directedLast = fixtureNode("Directed", "3", "EGFR")
        val insulinForward = fixtureNode("Straight", "1", "Insulin")
        val insulinBackfill = fixtureNode("Straight", "2", "insulin")
        val receptor = fixtureNode("Straight", "3", "INSR")
        val crossedInsulinForward = fixtureNode("Crossed", "1", " Insulin ")
        val crossedInsulinBackfill = fixtureNode("Crossed", "2", "insulin")
        val crossedReceptorForward = fixtureNode("Crossed", "3", "INSR")
        val crossedReceptorBackfill = fixtureNode("Crossed", "4", " insr ")
        val sharedFirst = fixtureNode("Shared", "1", " EGFR ")
        val sharedNode = fixtureNode("Shared", "2", "EGFR")
        val sharedLast = fixtureNode("Shared", "3", "EGFR")
        val repeatedShared = fixtureNode("RepeatedShared", "1", "EGFR")
        val repeatedOther = fixtureNode("RepeatedShared", "2", "EGFR")
        val countFirst = fixtureNode("Count", "1", "EGFR")
        val countSecond = fixtureNode("Count", "2", "EGFR")
        val countThird = fixtureNode("Count", "3", "EGFR")
        val countFourth = fixtureNode("Count", "4", "EGFR")
        val ambiguousFirst = fixtureNode("Ambiguous", "1", " EGFR ")
        val ambiguousSecond = fixtureNode("Ambiguous", "2", "egfr  ")
        val ambiguousThird = fixtureNode("Ambiguous", "3", "EGFR")
        val ambiguousFourth = fixtureNode("Ambiguous", "4", "EGFR")
        val processDisease = fixtureNode("Type", "Process", "fixture process", "disease")
        val processCell = fixtureNode("Type", "Process", "fixture process", "cellprocess")
        val typeTargetForward = fixtureNode("Type", "TargetForward", "fixture target")
        val typeTargetBackfill = fixtureNode("Type", "TargetBackfill", "fixture target")

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
            fixture("Binding repeated shared URI", "biomed:Binding",
                (repeatedShared, repeatedShared), (repeatedShared, repeatedOther),
                Vector(Vector(pair("partner", repeatedShared, repeatedShared), pair("partner", repeatedShared, repeatedOther)))),
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
            fixture("same raw ID under different node types", "biomed:Regulation",
                (processDisease, typeTargetForward), (processCell, typeTargetBackfill),
                Vector(Vector(pair("regulator", processDisease, processCell), pair("target", typeTargetForward, typeTargetBackfill))))
        )

    /**
     * Creates a local DataFrame with an explicit schema, including empty inputs.
     *
     * @param spark the single check session
     * @param rows bounded fixture or saved-result rows
     * @param schema exact expected input shape
     * @return a small DataFrame; no encoder, external read or write is used
     */
    private def frame(spark: SparkSession, rows: Seq[Row], schema: StructType): DataFrame =
        spark.createDataFrame(rows.asJava, schema)

    /**
     * Collects only a bounded check result, rejecting an unexpectedly large input.
     *
     * @param data fixture or saved Step 5 result, never the occurrence export
     * @param label name for a size-limit failure
     * @return at most MaximumCheckedRows rows with multiplicity preserved
     */
    private def smallRows(data: DataFrame, label: String): Vector[Row] =
        val rows = data.limit(MaximumCheckedRows + 1).collect().toVector
        require(rows.size <= MaximumCheckedRows, s"$label exceeds the bounded-check limit of $MaximumCheckedRows rows")
        rows

    /**
     * Describes schema names, order and types while ignoring inferred nullability.
     *
     * @param dataType schema component to inspect
     * @return recursive shape suitable for exact contractual comparison
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
     * @return each record and its multiplicity
     */
    private def multiset[A](entries: Seq[A]): Map[A, Int] =
        entries.groupMapReduce(identity)(_ => 1)(_ + _)

    /**
     * Compares complete results independently of listing order, retaining repeats.
     *
     * @param results one or two complete two-entry node-pair results
     * @return a multiset of inner multisets, never an inner set
     */
    private def resultMultiset(results: Seq[Seq[Row]]): ResultMultiset =
        multiset(results.map(multiset))

    /**
     * Extracts the complete ordered forward/backfill canonical-ID pair.
     *
     * @param row a collected twin or unresolved row
     * @return both IDs, preserving flow orientation
     */
    private def twinId(row: Row): TwinId =
        (row.getAs[String]("forwardCanonicalId"), row.getAs[String]("backfillCanonicalId"))

    /**
     * Reads a complete-result collection without assuming Spark's Seq subtype.
     *
     * @param row a full or unresolved Step 5 result
     * @return immutable vectors with their repeated entries intact
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
        checkSchema(s"$label full", full, FullSchema)
        checkSchema(s"$label detail", detail, DetailSchema)
        checkSchema(s"$label global", global, GlobalSchema)
        checkSchema(s"$label unresolved", unresolved, UnresolvedSchema)
        CheckedOutputs(smallRows(full, label), smallRows(detail, label), smallRows(global, label), smallRows(unresolved, label))

    /**
     * Derives the global count oracle from bounded expected or saved detail.
     *
     * Uses ordinary Scala sets of COMPLETE twin IDs, independently of the Spark
     * aggregation. Role and flag never become node-pair grouping keys.
     *
     * @param detail rows in the agreed seven-column detailed order
     * @return global rows with Long counts and oriented full node URIs
     */
    private def expectedGlobal(detail: Seq[Row]): Vector[Row] =
        detail.groupBy(row => (row.getString(0), row.getString(1))).toVector.map { case ((forward, backfill), rows) =>
            val completeTwins = rows.map(row => (row.getString(4), row.getString(5))).toSet
            Row(forward, backfill, rows.head.getString(2), completeTwins.size.toLong)
        }

    /**
     * Checks explicit per-case results and their detailed/global projections.
     *
     * @param spark the existing check session
     * @param fixtures isolated twin fixtures with literal expected correspondences
     * @param label check group label
     * @return verified small outputs for additional focused assertions
     */
    private def checkFixtures(spark: SparkSession, fixtures: Vector[Fixture], label: String): CheckedOutputs =
        val inputRows = fixtures.map(value => Row(value.forward.getString(0), value.backfill.getString(0), value.shared, value.forward, value.backfill))
        val expectedById = fixtures.map(value => (value.forward.getString(0), value.backfill.getString(0)) -> value).toMap
        require(expectedById.size == fixtures.size, s"$label fixtures accidentally repeat a complete twin ID")
        val output = evaluate(frame(spark, inputRows, TwinSchema), label)
        require(output.full.size == fixtures.size, s"$label lost or duplicated twins")
        require(output.full.map(twinId).toSet == expectedById.keySet, s"$label changed twin identities")
        output.full.foreach { row =>
            val expected = expectedById(twinId(row))
            val actualResults = results(row)
            require(actualResults.forall(_.size == 2), s"${expected.label}: a complete result lost a participant")
            require(resultMultiset(actualResults) == resultMultiset(expected.expectedResults), s"${expected.label}: wrong complete results: $actualResults")
            actualResults.foreach { tuple =>
                val sorted = tuple.sortBy(value => (value.getString(1), value.getString(2), value.getString(0)))
                require(tuple == sorted, s"${expected.label}: result normalization is not URI-first")
            }
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
     * Checks directed source grouping and Step 4 inclusion on eight tiny rows.
     *
     * @param spark the existing session
     * @return no value; failures identify source, key, ID or metadata regressions
     */
    private def checkSourceGroups(spark: SparkSession): Unit =
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
        val canonical = aggregateCanonicalRelations(withCanonicalId(frame(spark, occurrences, OccurrenceSchema)))
        checkSchema("canonical relations", canonical, RelationSchema)
        val canonicalRows = smallRows(canonical, "source grouping")
        require(canonicalRows.size == 6, "Occurrence grouping failed to retain one row per canonical ID")
        require(canonicalRows.exists(_.getAs[String]("canonicalId") ==
            "biodata:regulation/regulator/Fixture_Sources_Forward/target/Fixture_Sources_Target"), "Directed canonical ID changed")
        val keyed = withComparisonKey(canonical)
        checkSchema("comparison key", keyed, NestedRelationSchema)
        val groups = separateCanonicalRelations(canonical)
        require(smallRows(groups.forwardOnly, "forwardOnly").size == 1 &&
            smallRows(groups.shared, "shared").size == 1 && smallRows(groups.backfillOnly, "backfillOnly").size == 4,
            "Canonical source groups differ")
        val twins = smallRows(findNonIdenticalTwinPairs(groups), "Step 4 twins")
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
        val keyedBinding = smallRows(withComparisonKey(frame(spark, bindingRows, RelationSchema)), "Binding keys")
        require(keyedBinding.map(_.getAs[Row]("comparisonKey")).distinct.size == 1, "Binding keys did not sort names")
        require(multiset(keyedBinding.map(row => Row.fromSeq(row.toSeq.take(8)))) == multiset(bindingRows),
            "Binding key construction reordered original participants")

    /**
     * Checks aggregation when one oriented pair occurs under different roles/flags.
     *
     * These three detailed fixture entries are inputs to aggregation, not claims
     * about a new Step 5 resolution. Reversing URI orientation creates a new pair.
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
        checkSourceGroups(spark)
        checkAggregationOrientation(spark)
        println(s"PASS fixtures: ${realFixtures.size + syntheticFixtures.size} explicit twins, source groups, empty/unresolved subsets, schemas and complete-twin counts")

    /**
     * Reads one bounded result table from the selected dated baseline.
     *
     * @param spark existing session using the established credential provider
     * @param runId baseline date selected by the caller in YYYY-MM-DD format
     * @param table Step 5 table name, explicitly selected in code
     * @return a local DataFrame containing at most MaximumCheckedRows saved rows
     */
    private def readBaseline(spark: SparkSession, runId: String, table: String): DataFrame =
        val data = spark.read.format("delta").load(s"$OutputPrefix$runId/$table/")
        frame(spark, smallRows(data, s"baseline $table"), data.schema)

    /**
     * Verifies Step 5 parity against the selected dated baseline's saved outputs.
     *
     * Reads each small saved table once, compares complete results and metadata,
     * preserves detail multiplicity, and independently counts complete twins
     * from saved detail. The occurrence export and writers are never used.
     *
     * @param spark the same session already used for fixtures
     * @param runId baseline date in YYYY-MM-DD format under OutputPrefix
     */
    def runBaseline(spark: SparkSession, runId: String): Unit =
        val twins = readBaseline(spark, runId, "nonIdenticalTwinPairs")
        val savedFull = readBaseline(spark, runId, "twinPairsWithNameMatchedNodePairTuples")
        val savedDetail = readBaseline(spark, runId, "homonymousNodePairs")
        val savedSummary = readBaseline(spark, runId, "homonymousNodePairsWithTwinCount")
        val savedUnresolved = readBaseline(spark, runId, "twinPairsWithMultipleNameMatchedNodePairTuples")
        checkSchema("saved full results", savedFull, FullSchema)
        checkSchema("saved detail", savedDetail, DetailSchema)
        checkSchema("saved summary", savedSummary, GlobalSchema)
        checkSchema("saved unresolved", savedUnresolved, UnresolvedSchema)
        val actual = evaluate(twins, "saved-output regression")
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
        println(s"PASS baseline $runId: ${actual.full.size} twins, ${actual.detail.size} detailed entries, ${actual.global.size} global pairs, ${actual.unresolved.size} unresolved; no investigation datasets written")

/** Small Scala entry point for fixture checks and bounded dated-baseline regression. */
object StepFiveChecks:

    /**
     * Runs fixtures, optionally followed by saved-output regression in one session.
     *
     * Reuses the normal local Spark settings and launch-supplied heap. The unused
     * input path only fills RunConfig; no occurrence reader, run reservation,
     * investigation writer or RuntimeOps function is called. Spark always closes.
     *
     * @param args no arguments, or --baseline-run YYYY-MM-DD
     */
    def main(args: Array[String]): Unit =
        val baseline = args.toList match
            case Nil => None
            case "--baseline-run" :: runId :: Nil if runId.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}") =>
                Some(LocalDate.parse(runId).toString)
            case _ => throw new IllegalArgumentException("Usage: StepFiveChecks [--baseline-run YYYY-MM-DD]")
        val scratch = Files.createTempDirectory("biomed3471-codexdeprecated-step-five-checks-")
        val config = RunConfig(DefaultInputPath, "step-five-checks", scratch)
        val spark = makeSparkSession(config)
        try
            spark.sparkContext.setLogLevel("ERROR")
            StepFiveCheckOps.runFixtures(spark)
            baseline.foreach(runId => StepFiveCheckOps.runBaseline(spark, runId))
        finally spark.stop()
        println("Step 5 checks completed successfully.")
