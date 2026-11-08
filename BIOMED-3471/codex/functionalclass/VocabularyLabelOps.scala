package biomed3471.codex.functionalclass

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.*

/** Label preparation, indexing and cross-vocabulary matching. */
object VocabularyLabelOps:

    extension (label: Column)

        /** Collapse whitespace and ignore case, preserving punctuation. */
        def normalizedLabel: Column = lower(trim(regexp_replace(label, "\\s+", " ")))

    extension (concepts: DataFrame)

        /** Several IDs can share a preferred label within one vocabulary; retain them all. */
        def conceptsByPreferredLabel: DataFrame =
            val namedConcepts = concepts.withColumn("normalizedName", lower(trim(col("name"))))

            namedConcepts
                .groupBy("normalizedName")
                .agg(
                  sort_array(collect_list(struct(col("termiteId"), col("name"), col("alias")))).as("concepts")
                )

        /** Expand preferred and alternative labels to unique, nonempty (vocabulary, conceptId, label) rows. */
        def normalizedLabels: DataFrame =
            concepts
                .select(
                  col("vocabulary"),
                  col("conceptId"),
                  explode(concat(array(col("prefLabel")), coalesce(col("altLabels"), array()))).as("label")
                )
                .withColumn("label", col("label").normalizedLabel)
                .filter(length(col("label")) > 0)
                .distinct()

    extension (labels: DataFrame)

        /** One row per concept pair and shared label; vocabulary ordering excludes same-vocabulary and mirrored pairs. */
        def crossVocabularyLabelMatches: DataFrame =
            labels.as("left")
                .join(
                  labels.as("right"),
                  col("left.label") === col("right.label") && col("left.vocabulary") < col("right.vocabulary"),
                  "inner"
                )
                .select(
                  col("left.vocabulary").as("leftVocabulary"),
                  col("left.conceptId").as("leftConceptId"),
                  col("right.vocabulary").as("rightVocabulary"),
                  col("right.conceptId").as("rightConceptId"),
                  col("left.label").as("sharedLabel")
                )

    extension (matches: DataFrame)

        /** Keep each concept pair distinct and retain all of its shared labels as lexical evidence. */
        def sharedLabelsByConceptPair: DataFrame =
            matches
                .groupBy("leftVocabulary", "leftConceptId", "rightVocabulary", "rightConceptId")
                .agg(sort_array(collect_set(col("sharedLabel"))).as("sharedLabels"))
