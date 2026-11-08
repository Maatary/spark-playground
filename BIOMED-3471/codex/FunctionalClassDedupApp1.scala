package biomed3471.codex

import biomed3471.codex.TwinSearchApp.{makeSparkSession, writeAndReadBackDeltaTable}
import biomed3471.codex.functionalclass.VocabularyLabelOps.*
import biomed3471.codex.functionalclass.DataTypes.*
import biomed3471.codex.functionalclass.DataTypes.given
import biomed3471.codex.functionalclass.TermiteVocabularyOps.*
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.functions.col

/** Load ENZYMES and PROTYP, then collect their concepts under shared normalized preferred labels. */
object FunctionalClassDedupApp1:

    /** Match preferred labels only, retaining each vocabulary's concepts under their shared label. */
    def computePreferredLabelOverlap(enzymes: DataFrame, protyp: DataFrame): DataFrame =
        val enzymesByName = enzymes.conceptsByPreferredLabel.withColumnRenamed("concepts", "enzymes")
        val protypByName  = protyp.conceptsByPreferredLabel.withColumnRenamed("concepts", "protyp")

        enzymesByName.join(protypByName, Seq("normalizedName"), "inner")

    /** Read the same three-column TSV format and comment convention as the Dataset Importer. */
    def createDictionaryDataset(path: String)(using spark: SparkSession): Dataset[DictionaryEntry] =
        spark.read
            .schema(summon[Encoder[DictionaryEntry]].schema)
            .option("sep", "\t")
            .option("header", "false")
            .option("comment", "#")
            .csv(path)
            .as[DictionaryEntry]

    /** Ignore files can have explanatory columns; only the first column is the ID. */
    def createIgnoredIdsDataset(path: String)(using spark: SparkSession): Dataset[IgnoredTermiteId] =
        spark.read
            .schema(summon[Encoder[IgnoredTermiteId]].schema)
            .option("sep", "\t")
            .option("header", "false")
            .option("comment", "#")
            .csv(path)
            .as[IgnoredTermiteId]

    def main(args: Array[String]): Unit =
        val vocabularyPath = "/Users/maatari/Dev/IdeaProjects/cmdp-biomed-termite-vocabs"
        val outputPath     = "s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/BIOMED-3471/functional-class-dedup"

        given spark: SparkSession = makeSparkSession
        try
            spark.sparkContext.setLogLevel("WARN")
            val noIgnoredIds = spark.emptyDataset[IgnoredTermiteId]

            val enzymes      = computeVocabDataset(
              main      = createDictionaryDataset(s"$vocabularyPath/ENZYMES/curation files/enzymes.dictionary.txt"),
              extra     = createDictionaryDataset(s"$vocabularyPath/ENZYMES/curation files/enzymes.dictionary.txt.extra"),
              aug       = createDictionaryDataset(s"$vocabularyPath/ENZYMES/vocabulary files/enzymes.dictionary.aug"),
              ignore    = noIgnoredIds,
              augIgnore = noIgnoredIds
            )
            val savedEnzymes = writeAndReadBackDeltaTable(enzymes.toDF(), s"$outputPath/enzymes")

            val protyp      = computeVocabDataset(
              main      = createDictionaryDataset(s"$vocabularyPath/PROTYP/curation files/protyp.dictionary.txt"),
              extra     = createDictionaryDataset(s"$vocabularyPath/PROTYP/curation files/protyp.dictionary.txt.extra"),
              aug       = createDictionaryDataset(s"$vocabularyPath/PROTYP/vocabulary files/protyp.dictionary.aug"),
              ignore    = createIgnoredIdsDataset(s"$vocabularyPath/PROTYP/curation files/protyp.dictionary.txt.ignore"),
              augIgnore = createIgnoredIdsDataset(s"$vocabularyPath/PROTYP/vocabulary files/protyp.dictionary.aug.ignore")
            )
            val savedProtyp = writeAndReadBackDeltaTable(protyp.toDF(), s"$outputPath/protyp")

            val overlap      = computePreferredLabelOverlap(savedEnzymes, savedProtyp)
            val savedOverlap = writeAndReadBackDeltaTable(overlap, s"$outputPath/preferredLabelOverlap")

            println(s"ENZYMES concepts: ${savedEnzymes.count()}")
            println(s"PROTYP concepts: ${savedProtyp.count()}")
            println(s"Shared normalized preferred labels: ${savedOverlap.count()}")
            savedOverlap.orderBy("normalizedName")
                .select(col("normalizedName"), col("enzymes.termiteId").as("enzymesIds"), col("protyp.termiteId").as("protypIds"))
                .show(20, truncate = false)
            println(s"Saved Delta tables: $outputPath")
        finally spark.stop()
