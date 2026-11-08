package biomed3471.codex

import biomed3471.codex.TwinSearchApp.{makeSparkSession, writeAndReadBackDeltaTable}
import biomed3471.codex.functionalclass.VocabularyLabelOps.*
import io.delta.implicits.*
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.*

import scala.util.chaining.*

/** Find ENZYMES–PROTYP concept pairs through preferred and alternative labels, then rank their lexical evidence. */
object FunctionalClassDedupApp2:

    /** Apply the review order, including identity tie-breakers; also used after reading the saved Delta table. */
    def orderConceptPairs(pairs: DataFrame): DataFrame =
        pairs.orderBy(
          col("sharedLabelCount").desc,
          col("singleMatchLabelLength").desc,
          col("leftVocabulary"),
          col("leftConceptId"),
          col("rightVocabulary"),
          col("rightConceptId")
        )

    /** Multiple shared labels rank first; single-label matches are then ordered by label length. */
    def computeRankedConceptPairs(labelMatches: DataFrame): DataFrame =
        labelMatches.sharedLabelsByConceptPair
            .withColumn("sharedLabelCount", size(col("sharedLabels")))
            .withColumn(
              "singleMatchLabelLength",
              when(col("sharedLabelCount") === 1, length(col("sharedLabels").getItem(0))).otherwise(0)
            )
            .pipe(orderConceptPairs)

    def main(args: Array[String]): Unit =
        val vocabularyPath = "s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/BIOMED-3471/functional-class-dedup"
        val outputPath     = s"$vocabularyPath/app2"

        val spark = makeSparkSession
        try
            spark.sparkContext.setLogLevel("WARN")

            // 1. Use App1's effective vocabularies, keeping vocabulary identity on every concept.
            val enzymes  = spark.read.delta(s"$vocabularyPath/enzymes")
                .select(
                  lit("ENZYMES").as("vocabulary"),
                  col("termiteId").as("conceptId"),
                  col("name").as("prefLabel"),
                  col("alias").as("altLabels")
                )
            val protyp   = spark.read.delta(s"$vocabularyPath/protyp")
                .select(
                  lit("PROTYP").as("vocabulary"),
                  col("termiteId").as("conceptId"),
                  col("name").as("prefLabel"),
                  col("alias").as("altLabels")
                )
            val concepts = enzymes.unionByName(protyp)

            // 2. Expand and normalize all nonempty labels, including short abbreviations.
            val savedLabels = concepts.normalizedLabels
                // Uncomment to exclude labels shorter than four characters.
                // .filter(length(col("label")) >= 4)
                .pipe(labels => writeAndReadBackDeltaTable(labels, s"$outputPath/labels"))

            // 3. Match labels across vocabularies, then collect and rank the evidence for each pair.
            val labelMatches      = savedLabels.crossVocabularyLabelMatches
            val savedLabelMatches = writeAndReadBackDeltaTable(labelMatches, s"$outputPath/labelMatches")
            val conceptPairs      = computeRankedConceptPairs(savedLabelMatches)
            val savedConceptPairs = writeAndReadBackDeltaTable(conceptPairs, s"$outputPath/conceptPairs")

            println(s"Unique vocabulary/concept/label rows: ${savedLabels.count()}")
            println(s"Concept-pair/shared-label matches: ${savedLabelMatches.count()}")
            println(s"Candidate concept pairs: ${savedConceptPairs.count()}")
            savedConceptPairs.pipe(orderConceptPairs).show(20, truncate = false)
            println(s"Saved Delta tables: $outputPath")
        finally spark.stop()
