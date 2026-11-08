package biomed3471.codex.functionalclass

import biomed3471.codex.FunctionalClassDedupApp2.{computeRankedConceptPairs, orderConceptPairs}
import biomed3471.codex.TwinSearchApp.makeSparkSession
import biomed3471.codex.functionalclass.VocabularyLabelOps.*
import io.github.pashashiz.spark_encoders.TypedEncoder
import io.github.pashashiz.spark_encoders.TypedEncoder.given
import org.apache.spark.sql.{Encoder, Row, SparkSession}
import org.apache.spark.sql.functions.col

/** Small fixtures for label preparation, complete concept-pair identity and review ordering. */
object FunctionalClassDedupApp2Spec:

    final case class VocabularyConcept(
        vocabulary: String,
        conceptId : String,
        prefLabel : Option[String],
        altLabels : Option[Seq[Option[String]]]
    )

    given vocabularyConceptEncoder: Encoder[VocabularyConcept] = TypedEncoder[VocabularyConcept].encoder

    def checkLabelsAndPairs(using spark: SparkSession): Unit =
        val concepts = spark.createDataset(Seq(
          VocabularyConcept(
            "A",
            "same",
            Some("  Alpha\t beta  "),
            Some(Seq(
              Some("ALPHA   BETA"),
              Some("x"),
              Some(" A-B "),
              Some("cross / pref"),
              Some("alias pair"),
              None,
              Some("  ")
            ))
          ),
          VocabularyConcept("A", "A2", Some("alpha beta"), None),
          VocabularyConcept("A", "long", Some("long shared label"), None),
          VocabularyConcept(
            "B",
            "same",
            Some(" Cross / Pref "),
            Some(Seq(
              Some(" alpha beta "),
              Some("x"),
              Some("a-b"),
              Some("alias pair"),
              Some("x")
            ))
          ),
          VocabularyConcept("B", "B2", Some("ALPHA BETA"), None),
          VocabularyConcept("B", "long", Some("LONG SHARED LABEL"), None),
          VocabularyConcept("C", "same", Some("x"), None),
          VocabularyConcept("C", "unmatched", None, Some(Seq(None, Some("  "), Some("unmatched"), Some("a b"))))
        )).toDF()

        val labels         = concepts.normalizedLabels
        val expectedLabels = Set(
          Row("A", "same", "alpha beta"),
          Row("A", "same", "x"),
          Row("A", "same", "a-b"),
          Row("A", "same", "cross / pref"),
          Row("A", "same", "alias pair"),
          Row("A", "A2", "alpha beta"),
          Row("A", "long", "long shared label"),
          Row("B", "same", "alpha beta"),
          Row("B", "same", "x"),
          Row("B", "same", "a-b"),
          Row("B", "same", "cross / pref"),
          Row("B", "same", "alias pair"),
          Row("B", "B2", "alpha beta"),
          Row("B", "long", "long shared label"),
          Row("C", "same", "x"),
          Row("C", "unmatched", "unmatched"),
          Row("C", "unmatched", "a b")
        )
        val actualLabels   = labels.select("vocabulary", "conceptId", "label").collect().toSeq
        assert(actualLabels.size == expectedLabels.size && actualLabels.toSet == expectedLabels, s"Unexpected label rows: $actualLabels")

        val matches         = labels.crossVocabularyLabelMatches
        val expectedMatches = Set(
          Row("A", "same", "B", "same", "alpha beta"),
          Row("A", "same", "B", "same", "x"),
          Row("A", "same", "B", "same", "a-b"),
          Row("A", "same", "B", "same", "cross / pref"),
          Row("A", "same", "B", "same", "alias pair"),
          Row("A", "A2", "B", "same", "alpha beta"),
          Row("A", "same", "B", "B2", "alpha beta"),
          Row("A", "A2", "B", "B2", "alpha beta"),
          Row("A", "long", "B", "long", "long shared label"),
          Row("A", "same", "C", "same", "x"),
          Row("B", "same", "C", "same", "x")
        )
        val actualMatches   = matches.select("leftVocabulary", "leftConceptId", "rightVocabulary", "rightConceptId", "sharedLabel").collect().toSeq
        assert(actualMatches.size == expectedMatches.size && actualMatches.toSet == expectedMatches, s"Unexpected label matches: $actualMatches")

        val ranked        = computeRankedConceptPairs(matches)
        val expectedPairs = Seq(
          Row("A", "same", "B", "same", Seq("a-b", "alias pair", "alpha beta", "cross / pref", "x"), 5, 0),
          Row("A", "long", "B", "long", Seq("long shared label"), 1, 17),
          Row("A", "A2", "B", "B2", Seq("alpha beta"), 1, 10),
          Row("A", "A2", "B", "same", Seq("alpha beta"), 1, 10),
          Row("A", "same", "B", "B2", Seq("alpha beta"), 1, 10),
          Row("A", "same", "C", "same", Seq("x"), 1, 1),
          Row("B", "same", "C", "same", Seq("x"), 1, 1)
        )
        val actualPairs   = ranked.select(
          "leftVocabulary",
          "leftConceptId",
          "rightVocabulary",
          "rightConceptId",
          "sharedLabels",
          "sharedLabelCount",
          "singleMatchLabelLength"
        ).collect().toSeq
        assert(actualPairs == expectedPairs, s"Unexpected ranked pairs: $actualPairs")

        val reordered = orderConceptPairs(ranked.orderBy(col("leftConceptId").desc)).select(
          "leftVocabulary",
          "leftConceptId",
          "rightVocabulary",
          "rightConceptId",
          "sharedLabels",
          "sharedLabelCount",
          "singleMatchLabelLength"
        ).collect().toSeq
        assert(reordered == expectedPairs, s"Unexpected restored review order: $reordered")

    def main(args: Array[String]): Unit =
        given spark: SparkSession = makeSparkSession
        try
            spark.sparkContext.setLogLevel("ERROR")
            checkLabelsAndPairs
            println("PASS: normalized labels, cross-vocabulary concept pairs and deterministic review ordering")
        finally spark.stop()
