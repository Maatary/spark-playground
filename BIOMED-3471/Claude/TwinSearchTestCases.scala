package biomed3471.claude

import biomed3471.claude.DataOps.*
import biomed3471.claude.DataTypes.*
import org.apache.spark.sql.{Encoder, SparkSession}
import org.apache.spark.sql.functions.*
import scribe.*

import scala.util.chaining.scalaUtilChainingOps

/**
 * == The test cases of the twin search ==
 *
 * Each test case is a few relation occurrences whose canonical relations form one twin pair. TwinSearchTestCases runs steps
 * 1 to 5 of [[DataOps]] on each test case on its own. It prints what the steps must find next to what they find: the twin
 * pair with its two canonical relation IDs and its flag nonIdenticalTwinsAlsoWithinForwardFlow, its
 * nameMatchedNodePairTuples as Spark prints them, the entries of homonymousNodePairs, the rows of
 * twinPairCountPerHomonymousNodePair, and the number of unresolved twin pairs.
 *
 * The first nine test cases give at least one example of each rule, A to C, and they include the seven examples in the
 * Scaladoc of [[DataOps.nameMatchedNodePairTuples]]. P1, P2, P3 and P4 are protein nodes named insulin or INSR. E1, E2, E3
 * and E4 are four different protein nodes named EGFR, in the order of their node TERMite IDs. N1 and N2 are two more protein
 * nodes named EGFR, and in test case 9 each relation is a Regulation from one of them to itself. The last five test cases
 * show what the first nine do not: rule B with the names in the same order in both relations, names that differ only in
 * letter case and in spaces at their ends, a node that appears in both relations at position 2 of the forward-flow relation,
 * and a backfill twin that also has forward-flow relation occurrences.
 *
 * {{{
 * scala-cli run . --main-class biomed3471.claude.TwinSearchTestCases --java-opt -Xmx4g
 * }}}
 */
object TwinSearchTestCases:

    /** The twin pair that a test case must give, with its name-matched node pair tuples in the order that step 5 stores them. */
    case class ExpectedTwinPair(
        forwardCanonicalId: String,
        backfillCanonicalId: String,
        nonIdenticalTwinsAlsoWithinForwardFlow: Boolean,
        nameMatchedNodePairTuples: List[List[NameMatchedNodePair]]
    )

    /**
     * One row of twinPairsWithNameMatchedNodePairTuples without the columns `forwardTwin` and `backfillTwin`, with
     * nameMatchedNodePairTuples as Spark prints it.
     */
    case class TwinPairAsPrinted(
        forwardCanonicalId: String,
        backfillCanonicalId: String,
        nonIdenticalTwinsAlsoWithinForwardFlow: Boolean,
        nameMatchedNodePairTuples: String
    )

    /** One test case: the rule of step 5 that applies, what the test case shows, its relation occurrences, and the twin pair they must give. */
    case class TestCase(rule: String, description: String, relationOccurrences: List[RelationOccurrence], expectedTwinPair: ExpectedTwinPair)

    /** What steps 1 to 5 find for one test case, or what they must find. */
    case class TestCaseResult(
        twinPairs: List[TwinPairAsPrinted],
        homonymousNodePairs: List[NameMatchedNodePair],
        twinPairCountPerHomonymousNodePair: List[HomonymousNodePairWithTwinCount],
        unresolvedTwinPairCount: Long
    )

    Logger.root
        .clearHandlers()
        .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
        .replace()

    def proteinUrn(nodeId: String): String = s"biodata:protein/$nodeId"

    def bindingOccurrence(source: String, node1Id: String, node1Name: String, node2Id: String, node2Name: String): RelationOccurrence =
        RelationOccurrence("biomed:Binding", Undirected, proteinUrn(node1Id), node1Name, proteinUrn(node2Id), node2Name, source)

    def egfrBindingOccurrence(source: String, node1Id: String, node2Id: String): RelationOccurrence =
        bindingOccurrence(source, node1Id, "EGFR", node2Id, "EGFR")

    def regulationOccurrence(source: String, regulatorUrn: String, regulatorName: String, targetUrn: String, targetName: String): RelationOccurrence =
        RelationOccurrence("biomed:Regulation", Directed, regulatorUrn, regulatorName, targetUrn, targetName, source)

    def partnerNodePair(forwardNodeId: String, backfillNodeId: String, matchedNodeName: String): NameMatchedNodePair =
        NameMatchedNodePair(Partner, proteinUrn(forwardNodeId), proteinUrn(backfillNodeId), matchedNodeName)

    def egfrPartnerNodePair(forwardNodeId: String, backfillNodeId: String): NameMatchedNodePair =
        partnerNodePair(forwardNodeId, backfillNodeId, "egfr")

    /** Name-matched node pair tuples, written the way Spark prints an array of arrays of structs. */
    def asPrintedBySpark(nameMatchedNodePairTuples: List[List[NameMatchedNodePair]]): String =
        nameMatchedNodePairTuples
            .map { nameMatchedNodePairTuple =>
                nameMatchedNodePairTuple
                    .map { nodePair => s"{${nodePair.role}, ${nodePair.forwardNodeUri}, ${nodePair.backfillNodeUri}, ${nodePair.matchedNodeName}}" }
                    .mkString("[", ", ", "]")
            }
            .mkString("[", ", ", "]")

    /** The entries that homonymousNodePairs must hold for a twin pair: the homonymous node pairs of its one tuple, or none when it is unresolved. */
    def homonymousNodePairsOf(nameMatchedNodePairTuples: List[List[NameMatchedNodePair]]): List[NameMatchedNodePair] =
        nameMatchedNodePairTuples match
            case List(nameMatchedNodePairTuple) => nameMatchedNodePairTuple.filter { nodePair => nodePair.forwardNodeUri != nodePair.backfillNodeUri }
            case _                              => Nil

    def describeRelationOccurrence(relationOccurrence: RelationOccurrence): String =
        List(
            relationOccurrence.source,
            relationOccurrence.relationship_type,
            s"${relationOccurrence.node1_urn} '${relationOccurrence.node1_name}'",
            s"${relationOccurrence.node2_urn} '${relationOccurrence.node2_name}'"
        ).mkString(", ")

    def describeTwinPair(twinPair: TwinPairAsPrinted): String =
        val flag = twinPair.nonIdenticalTwinsAlsoWithinForwardFlow
        s"${twinPair.forwardCanonicalId} with ${twinPair.backfillCanonicalId}, nonIdenticalTwinsAlsoWithinForwardFlow $flag"

    def describeNodePair(nodePair: NameMatchedNodePair): String =
        s"${nodePair.forwardNodeUri} with ${nodePair.backfillNodeUri}, ${nodePair.matchedNodeName}, ${nodePair.role}"

    def describeHomonymousNodePairWithTwinCount(homonymousNodePairWithTwinCount: HomonymousNodePairWithTwinCount): String =
        List(
            s"${homonymousNodePairWithTwinCount.forwardNodeUri} with ${homonymousNodePairWithTwinCount.backfillNodeUri}",
            homonymousNodePairWithTwinCount.matchedNodeName,
            homonymousNodePairWithTwinCount.nonIdenticalTwinPairCount.toString
        ).mkString(", ")

    /** Prints one result of a test case: what the steps must find, then what they find. */
    def printExpectedAndFound(label: String, expected: List[String], found: List[String]): Unit =
        println(s"    expected $label: ${if expected.isEmpty then "none" else expected.mkString("; ")}")
        println(s"    found    $label: ${if found.isEmpty then "none" else found.mkString("; ")}")

    /** Runs steps 1 to 5 on the relation occurrences of one test case, and collects what they find. */
    def foundResultOf(spark: SparkSession, relationOccurrences: List[RelationOccurrence])(
        using Encoder[RelationOccurrence], Encoder[TwinPairAsPrinted], Encoder[NameMatchedNodePair], Encoder[HomonymousNodePairWithTwinCount]
    ): TestCaseResult =
        val twinPairsWithNameMatchedNodePairTuplesDF =
            spark
                .createDataset(relationOccurrences)
                .pipe { relationOccurrenceDS => computeCanonicalRelations(relationOccurrenceDS) }
                .pipe { canonicalRelationsDS => computeNonIdenticalTwinPairs(canonicalRelationsDS) }
                .pipe { nonIdenticalTwinPairsDF => computeTwinPairsWithNameMatchedNodePairTuples(nonIdenticalTwinPairsDF) }

        val homonymousNodePairsDF = computeHomonymousNodePairs(twinPairsWithNameMatchedNodePairTuplesDF)

        val twinPairs =
            twinPairsWithNameMatchedNodePairTuplesDF
                .select(
                    col("forwardCanonicalId"),
                    col("backfillCanonicalId"),
                    col("nonIdenticalTwinsAlsoWithinForwardFlow"),
                    col("nameMatchedNodePairTuples").cast("string").as("nameMatchedNodePairTuples") // <-- the text that show prints
                )
                .as[TwinPairAsPrinted]
                .collect()
                .toList

        val homonymousNodePairs =
            homonymousNodePairsDF
                .select(col("role"), col("forwardNodeUri"), col("backfillNodeUri"), col("matchedNodeName"))
                .as[NameMatchedNodePair]
                .collect()
                .toList
                .sortBy { nodePair => (nodePair.forwardNodeUri, nodePair.backfillNodeUri, nodePair.role) }

        val twinPairCountPerHomonymousNodePair =
            computeTwinPairCountPerHomonymousNodePair(homonymousNodePairsDF)
                .as[HomonymousNodePairWithTwinCount]
                .collect()
                .toList
                .sortBy { nodePairWithTwinCount => (nodePairWithTwinCount.forwardNodeUri, nodePairWithTwinCount.backfillNodeUri) }

        val unresolvedTwinPairCount =
            computeTwinPairsWithMultipleNameMatchedNodePairTuples(twinPairsWithNameMatchedNodePairTuplesDF)
                .count()

        TestCaseResult(twinPairs, homonymousNodePairs, twinPairCountPerHomonymousNodePair, unresolvedTwinPairCount)

    /** What steps 1 to 5 must find for the twin pair that a test case must give. */
    def expectedResultOf(expectedTwinPair: ExpectedTwinPair): TestCaseResult =
        val twinPairs =
            List(
                TwinPairAsPrinted(
                    expectedTwinPair.forwardCanonicalId,
                    expectedTwinPair.backfillCanonicalId,
                    expectedTwinPair.nonIdenticalTwinsAlsoWithinForwardFlow,
                    asPrintedBySpark(expectedTwinPair.nameMatchedNodePairTuples)
                )
            )

        val homonymousNodePairs =
            homonymousNodePairsOf(expectedTwinPair.nameMatchedNodePairTuples)
                .sortBy { nodePair => (nodePair.forwardNodeUri, nodePair.backfillNodeUri, nodePair.role) }

        val twinPairCountPerHomonymousNodePair =
            homonymousNodePairs
                .map { nodePair => HomonymousNodePairWithTwinCount(nodePair.forwardNodeUri, nodePair.backfillNodeUri, nodePair.matchedNodeName, 1L) }
                .distinct

        val unresolvedTwinPairCount = if expectedTwinPair.nameMatchedNodePairTuples.size == 2 then 1L else 0L

        TestCaseResult(twinPairs, homonymousNodePairs, twinPairCountPerHomonymousNodePair, unresolvedTwinPairCount)

    /** Prints one test case: its relation occurrences, then each result that the steps must find next to what they find. */
    def printTestCase(testCaseNumber: Int, testCase: TestCase, expected: TestCaseResult, found: TestCaseResult, passed: Boolean): Unit =
        println(s"Test case $testCaseNumber, rule ${testCase.rule}: ${testCase.description}.")
        testCase.relationOccurrences.foreach { relationOccurrence =>
            println(s"    relation occurrence: ${describeRelationOccurrence(relationOccurrence)}")
        }
        printExpectedAndFound("twin pair", expected.twinPairs.map(describeTwinPair), found.twinPairs.map(describeTwinPair))
        printExpectedAndFound(
            "nameMatchedNodePairTuples",
            expected.twinPairs.map(_.nameMatchedNodePairTuples),
            found.twinPairs.map(_.nameMatchedNodePairTuples)
        )
        printExpectedAndFound(
            "homonymousNodePairs",
            expected.homonymousNodePairs.map(describeNodePair),
            found.homonymousNodePairs.map(describeNodePair)
        )
        printExpectedAndFound(
            "twinPairCountPerHomonymousNodePair",
            expected.twinPairCountPerHomonymousNodePair.map(describeHomonymousNodePairWithTwinCount),
            found.twinPairCountPerHomonymousNodePair.map(describeHomonymousNodePairWithTwinCount)
        )
        printExpectedAndFound("unresolved twin pairs", List(expected.unresolvedTwinPairCount.toString), List(found.unresolvedTwinPairCount.toString))
        println(if passed then "    passed" else "    FAILED")
        println()

    /**
     * Runs steps 1 to 5 on the relation occurrences of one test case, prints what the steps must find next to what they find,
     * and tells whether the two are the same.
     */
    def runTestCase(spark: SparkSession, testCaseNumber: Int, testCase: TestCase)(
        using Encoder[RelationOccurrence], Encoder[TwinPairAsPrinted], Encoder[NameMatchedNodePair], Encoder[HomonymousNodePairWithTwinCount]
    ): Boolean =
        val found    = foundResultOf(spark, testCase.relationOccurrences)
        val expected = expectedResultOf(testCase.expectedTwinPair)
        val passed   = found == expected

        printTestCase(testCaseNumber, testCase, expected, found, passed)

        passed

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

        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        val testCases = List(
            TestCase(
                rule                = "B",
                description         = "the two nodes of each relation have different names, in reverse order in the backfill twin",
                relationOccurrences = List(
                    bindingOccurrence(ForwardFlowSource, "P2", "insulin", "P3", "INSR"),
                    bindingOccurrence(BackfillSource, "P3", "INSR", "P4", "insulin")
                ),
                expectedTwinPair    = ExpectedTwinPair(
                    forwardCanonicalId                     = "biodata:binding/partner/P2/partner/P3",
                    backfillCanonicalId                    = "biodata:binding/partner/P3/partner/P4",
                    nonIdenticalTwinsAlsoWithinForwardFlow = false,
                    nameMatchedNodePairTuples              = List(
                        List(partnerNodePair("P2", "P4", "insulin"), partnerNodePair("P3", "P3", "insr"))
                    )
                )
            ),
            TestCase(
                rule                = "C1",
                description         = "E1 appears in both relations, at position 1 of each",
                relationOccurrences = List(
                    egfrBindingOccurrence(ForwardFlowSource, "E1", "E2"),
                    egfrBindingOccurrence(BackfillSource, "E1", "E3")
                ),
                expectedTwinPair    = ExpectedTwinPair(
                    forwardCanonicalId                     = "biodata:binding/partner/E1/partner/E2",
                    backfillCanonicalId                    = "biodata:binding/partner/E1/partner/E3",
                    nonIdenticalTwinsAlsoWithinForwardFlow = false,
                    nameMatchedNodePairTuples              = List(
                        List(egfrPartnerNodePair("E1", "E1"), egfrPartnerNodePair("E2", "E3"))
                    )
                )
            ),
            TestCase(
                rule                = "C1",
                description         = "E2 appears in both relations, at position 1 of the forward-flow relation and position 2 of the backfill twin",
                relationOccurrences = List(
                    egfrBindingOccurrence(ForwardFlowSource, "E2", "E3"),
                    egfrBindingOccurrence(BackfillSource, "E1", "E2")
                ),
                expectedTwinPair    = ExpectedTwinPair(
                    forwardCanonicalId                     = "biodata:binding/partner/E2/partner/E3",
                    backfillCanonicalId                    = "biodata:binding/partner/E1/partner/E2",
                    nonIdenticalTwinsAlsoWithinForwardFlow = false,
                    nameMatchedNodePairTuples              = List(
                        List(egfrPartnerNodePair("E2", "E2"), egfrPartnerNodePair("E3", "E1"))
                    )
                )
            ),
            TestCase(
                rule                = "C1",
                description         = "E1 appears in both relations, and the forward-flow relation holds it twice",
                relationOccurrences = List(
                    egfrBindingOccurrence(ForwardFlowSource, "E1", "E1"),
                    egfrBindingOccurrence(BackfillSource, "E1", "E2")
                ),
                expectedTwinPair    = ExpectedTwinPair(
                    forwardCanonicalId                     = "biodata:binding/partner/E1/partner/E1",
                    backfillCanonicalId                    = "biodata:binding/partner/E1/partner/E2",
                    nonIdenticalTwinsAlsoWithinForwardFlow = false,
                    nameMatchedNodePairTuples              = List(
                        List(egfrPartnerNodePair("E1", "E1"), egfrPartnerNodePair("E1", "E2"))
                    )
                )
            ),
            TestCase(
                rule                = "C2",
                description         = "each relation holds one node twice",
                relationOccurrences = List(
                    egfrBindingOccurrence(ForwardFlowSource, "E1", "E1"),
                    egfrBindingOccurrence(BackfillSource, "E2", "E2")
                ),
                expectedTwinPair    = ExpectedTwinPair(
                    forwardCanonicalId                     = "biodata:binding/partner/E1/partner/E1",
                    backfillCanonicalId                    = "biodata:binding/partner/E2/partner/E2",
                    nonIdenticalTwinsAlsoWithinForwardFlow = false,
                    nameMatchedNodePairTuples              = List(
                        List(egfrPartnerNodePair("E1", "E2"), egfrPartnerNodePair("E1", "E2"))
                    )
                )
            ),
            TestCase(
                rule                = "C2",
                description         = "the backfill twin holds E3 twice",
                relationOccurrences = List(
                    egfrBindingOccurrence(ForwardFlowSource, "E1", "E2"),
                    egfrBindingOccurrence(BackfillSource, "E3", "E3")
                ),
                expectedTwinPair    = ExpectedTwinPair(
                    forwardCanonicalId                     = "biodata:binding/partner/E1/partner/E2",
                    backfillCanonicalId                    = "biodata:binding/partner/E3/partner/E3",
                    nonIdenticalTwinsAlsoWithinForwardFlow = false,
                    nameMatchedNodePairTuples              = List(
                        List(egfrPartnerNodePair("E1", "E3"), egfrPartnerNodePair("E2", "E3"))
                    )
                )
            ),
            TestCase(
                rule                = "C2",
                description         = "the forward-flow relation holds E1 twice",
                relationOccurrences = List(
                    egfrBindingOccurrence(ForwardFlowSource, "E1", "E1"),
                    egfrBindingOccurrence(BackfillSource, "E2", "E3")
                ),
                expectedTwinPair    = ExpectedTwinPair(
                    forwardCanonicalId                     = "biodata:binding/partner/E1/partner/E1",
                    backfillCanonicalId                    = "biodata:binding/partner/E2/partner/E3",
                    nonIdenticalTwinsAlsoWithinForwardFlow = false,
                    nameMatchedNodePairTuples              = List(
                        List(egfrPartnerNodePair("E1", "E2"), egfrPartnerNodePair("E1", "E3"))
                    )
                )
            ),
            TestCase(
                rule                = "C3",
                description         = "each relation holds two different nodes, and no node appears in both relations",
                relationOccurrences = List(
                    egfrBindingOccurrence(ForwardFlowSource, "E1", "E2"),
                    egfrBindingOccurrence(BackfillSource, "E3", "E4")
                ),
                expectedTwinPair    = ExpectedTwinPair(
                    forwardCanonicalId                     = "biodata:binding/partner/E1/partner/E2",
                    backfillCanonicalId                    = "biodata:binding/partner/E3/partner/E4",
                    nonIdenticalTwinsAlsoWithinForwardFlow = false,
                    nameMatchedNodePairTuples              = List(
                        List(egfrPartnerNodePair("E1", "E3"), egfrPartnerNodePair("E2", "E4")),
                        List(egfrPartnerNodePair("E1", "E4"), egfrPartnerNodePair("E2", "E3"))
                    )
                )
            ),
            TestCase(
                rule                = "A",
                description         = "each Regulation goes from a node to the same node",
                relationOccurrences = List(
                    regulationOccurrence(ForwardFlowSource, proteinUrn("N1"), "EGFR", proteinUrn("N1"), "EGFR"),
                    regulationOccurrence(BackfillSource, proteinUrn("N2"), "EGFR", proteinUrn("N2"), "EGFR")
                ),
                expectedTwinPair    = ExpectedTwinPair(
                    forwardCanonicalId                     = "biodata:regulation/regulator/N1/target/N1",
                    backfillCanonicalId                    = "biodata:regulation/regulator/N2/target/N2",
                    nonIdenticalTwinsAlsoWithinForwardFlow = false,
                    nameMatchedNodePairTuples              = List(
                        List(
                            NameMatchedNodePair(Regulator, proteinUrn("N1"), proteinUrn("N2"), "egfr"),
                            NameMatchedNodePair(Target, proteinUrn("N1"), proteinUrn("N2"), "egfr")
                        )
                    )
                )
            ),
            TestCase(
                rule                = "B",
                description         = "the names are in the same order in both relations, and the insulin names differ in case and end spaces",
                relationOccurrences = List(
                    bindingOccurrence(ForwardFlowSource, "P2", "insulin", "P3", "INSR"),
                    bindingOccurrence(BackfillSource, "P1", "Insulin ", "P3", "INSR")
                ),
                expectedTwinPair    = ExpectedTwinPair(
                    forwardCanonicalId                     = "biodata:binding/partner/P2/partner/P3",
                    backfillCanonicalId                    = "biodata:binding/partner/P1/partner/P3",
                    nonIdenticalTwinsAlsoWithinForwardFlow = false,
                    nameMatchedNodePairTuples              = List(
                        List(partnerNodePair("P2", "P1", "insulin"), partnerNodePair("P3", "P3", "insr"))
                    )
                )
            ),
            TestCase(
                rule                = "B",
                description         = "the names are in reverse order in the backfill twin, and the insulin names differ in case and end spaces",
                relationOccurrences = List(
                    bindingOccurrence(ForwardFlowSource, "P2", "insulin", "P3", "INSR"),
                    bindingOccurrence(BackfillSource, "P3", "INSR", "P4", "Insulin ")
                ),
                expectedTwinPair    = ExpectedTwinPair(
                    forwardCanonicalId                     = "biodata:binding/partner/P2/partner/P3",
                    backfillCanonicalId                    = "biodata:binding/partner/P3/partner/P4",
                    nonIdenticalTwinsAlsoWithinForwardFlow = false,
                    nameMatchedNodePairTuples              = List(
                        List(partnerNodePair("P2", "P4", "insulin"), partnerNodePair("P3", "P3", "insr"))
                    )
                )
            ),
            TestCase(
                rule                = "C1",
                description         = "E3 appears in both relations, at position 2 of each",
                relationOccurrences = List(
                    egfrBindingOccurrence(ForwardFlowSource, "E1", "E3"),
                    egfrBindingOccurrence(BackfillSource, "E2", "E3")
                ),
                expectedTwinPair    = ExpectedTwinPair(
                    forwardCanonicalId                     = "biodata:binding/partner/E1/partner/E3",
                    backfillCanonicalId                    = "biodata:binding/partner/E2/partner/E3",
                    nonIdenticalTwinsAlsoWithinForwardFlow = false,
                    nameMatchedNodePairTuples              = List(
                        List(egfrPartnerNodePair("E1", "E2"), egfrPartnerNodePair("E3", "E3"))
                    )
                )
            ),
            TestCase(
                rule                = "C1",
                description         = "E2 appears in both relations, at position 2 of the forward-flow relation and position 1 of the backfill twin",
                relationOccurrences = List(
                    egfrBindingOccurrence(ForwardFlowSource, "E1", "E2"),
                    egfrBindingOccurrence(BackfillSource, "E2", "E3")
                ),
                expectedTwinPair    = ExpectedTwinPair(
                    forwardCanonicalId                     = "biodata:binding/partner/E1/partner/E2",
                    backfillCanonicalId                    = "biodata:binding/partner/E2/partner/E3",
                    nonIdenticalTwinsAlsoWithinForwardFlow = false,
                    nameMatchedNodePairTuples              = List(
                        List(egfrPartnerNodePair("E1", "E3"), egfrPartnerNodePair("E2", "E2"))
                    )
                )
            ),
            TestCase(
                rule                = "A",
                description         = "the backfill twin also has a forward-flow relation occurrence, as in the luteolin example",
                relationOccurrences = List(
                    regulationOccurrence(ForwardFlowSource, "biodata:smallmol/L1", "luteolin", "biodata:disease/D1", "Inflammation"),
                    regulationOccurrence(BackfillSource, "biodata:smallmol/L2", "luteolin", "biodata:disease/D1", "Inflammation"),
                    regulationOccurrence(ForwardFlowSource, "biodata:smallmol/L2", "luteolin", "biodata:disease/D1", "Inflammation")
                ),
                expectedTwinPair    = ExpectedTwinPair(
                    forwardCanonicalId                     = "biodata:regulation/regulator/L1/target/D1",
                    backfillCanonicalId                    = "biodata:regulation/regulator/L2/target/D1",
                    nonIdenticalTwinsAlsoWithinForwardFlow = true,
                    nameMatchedNodePairTuples              = List(
                        List(
                            NameMatchedNodePair(Regulator, "biodata:smallmol/L1", "biodata:smallmol/L2", "luteolin"),
                            NameMatchedNodePair(Target, "biodata:disease/D1", "biodata:disease/D1", "inflammation")
                        )
                    )
                )
            )
        )

        val passedTestCaseCount =
            testCases
                .zipWithIndex
                .count { (testCase, index) => runTestCase(spark, index + 1, testCase) }

        println(s"$passedTestCaseCount of ${testCases.size} test cases passed")

        spark.stop()
