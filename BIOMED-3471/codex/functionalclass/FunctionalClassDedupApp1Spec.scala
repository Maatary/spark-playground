package biomed3471.codex.functionalclass

import biomed3471.codex.FunctionalClassDedupApp1.{computePreferredLabelOverlap, createDictionaryDataset, createIgnoredIdsDataset}
import biomed3471.codex.TwinSearchApp.{makeSparkSession, writeAndReadBackDeltaTable}
import biomed3471.codex.functionalclass.DataTypes.*
import biomed3471.codex.functionalclass.DataTypes.given
import biomed3471.codex.functionalclass.TermiteVocabularyOps.*
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}

import java.nio.file.Files

/** Small fixtures for the loading contract and preferred-label matching; no S3 reads or writes. */
object FunctionalClassDedupApp1Spec:

    def checkLoading(using spark: SparkSession): Unit =
        val main      = spark.createDataset(Seq(
          DictionaryEntry("A", Some("Main name"), Some("shared|remove/*|tagged/@^|unknown/abc|#comment|force/<|boost/+|negative/-")),
          DictionaryEntry("B", Some("Discarded base"), Some("base-only|resurrect/*")),
          DictionaryEntry("C", Some("Final exclusion without aug"), None),
          DictionaryEntry("D", Some("Ordinary exclusion"), None),
          DictionaryEntry("F", Some("%%Literal main name"), None),
          DictionaryEntry("G", Some("Final exclusion with aug"), None),
          DictionaryEntry("H", Some("Main overridden without %%"), None)
        ))
        val extra     = spark.createDataset(Seq(
          DictionaryEntry("A", Some("Extra name"), Some("shared|extra|remove")),
          DictionaryEntry("E", Some("Extra only"), None),
          DictionaryEntry("H", Some("  Plain extra name  "), None)
        ))
        val aug       = spark.createDataset(Seq(
          DictionaryEntry("A", Some("%%%%Aug name"), Some("aug|remove")),
          DictionaryEntry("B", Some("Reintroduced"), Some("aug-only|resurrect")),
          DictionaryEntry("F", Some("   "), Some("kept/^|kept")),
          DictionaryEntry("G", Some("Also excluded"), Some("also-excluded"))
        ))
        val ignore    = spark.createDataset(Seq(IgnoredTermiteId("B"), IgnoredTermiteId("D")))
        val augIgnore = spark.createDataset(Seq(IgnoredTermiteId("C"), IgnoredTermiteId("G")))

        val actual   = computeVocabDataset(main, extra, aug, ignore, augIgnore).collect().toSet
        val expected = Set(
          TermiteConcept("A", "Aug name", Some(Seq("aug", "extra", "shared", "tagged", "unknown/abc"))),
          TermiteConcept("B", "Reintroduced", Some(Seq("aug-only", "resurrect"))),
          TermiteConcept("E", "Extra only", None),
          TermiteConcept("F", "%%Literal main name", Some(Seq("kept"))),
          TermiteConcept("H", "Plain extra name", None)
        )
        assert(actual == expected, s"Unexpected effective vocabulary: $actual")

        val emptyDictionary = spark.emptyDataset[DictionaryEntry]
        val emptyIgnore     = spark.emptyDataset[IgnoredTermiteId]
        val withoutAug      = computeVocabDataset(main, emptyDictionary, emptyDictionary, emptyIgnore, augIgnore)
        assert(withoutAug.collect().map(_.termiteId).toSet == Set("A", "B", "D", "F", "H"))

    def checkPreferredLabels(using spark: SparkSession): Unit =
        val enzymes        = spark.createDataset(Seq(
          TermiteConcept("E1", " Arginase ", Some(Seq("Alias only"))),
          TermiteConcept("E2", "ARGINASE", None),
          TermiteConcept("E3", "Enzyme only", None)
        ))
        val protyp         = spark.createDataset(Seq(
          TermiteConcept("P1", "arginase", None),
          TermiteConcept("P2", "Arginase", None),
          TermiteConcept("P3", "Alias only", Some(Seq("Enzyme only")))
        ))
        val overlaps       = computePreferredLabelOverlap(enzymes.toDF(), protyp.toDF()).collect().toSeq
        assert(overlaps.size == 1, s"Expected only the shared preferred label: $overlaps")
        val arginase       = overlaps.head
        val enzymeConcepts = arginase.getSeq[Row](arginase.fieldIndex("enzymes"))
        val protypConcepts = arginase.getSeq[Row](arginase.fieldIndex("protyp"))
        assert(arginase.getAs[String]("normalizedName") == "arginase")
        assert(enzymeConcepts.map(_.getAs[String]("termiteId")) == Seq("E1", "E2"))
        assert(protypConcepts.map(_.getAs[String]("termiteId")) == Seq("P1", "P2"))
        assert(enzymeConcepts.head.getAs[String]("name") == " Arginase ")
        assert(enzymeConcepts.head.getSeq[String](2) == Seq("Alias only"))
        assert(computePreferredLabelOverlap(enzymes.toDF(), spark.emptyDataset[TermiteConcept].toDF()).count() == 0)

    def checkDeltaRoundTrip(using spark: SparkSession): Unit =
        val directory = new Path(Files.createTempDirectory("functional-class-dedup-delta-").toUri)
        try
            val concepts      = spark.createDataset(Seq(
              TermiteConcept("A", "Alpha", Some(Seq("a", "b"))),
              TermiteConcept("B", "Beta", None)
            )).toDF()
            val savedConcepts = writeAndReadBackDeltaTable(concepts, new Path(directory, "concepts").toString)
            assert(savedConcepts.collect().toSet == concepts.collect().toSet)
            val overlap       = computePreferredLabelOverlap(savedConcepts, savedConcepts)
            val savedOverlap  = writeAndReadBackDeltaTable(overlap, new Path(directory, "overlap").toString)
            assert(savedOverlap.collect().toSet == overlap.collect().toSet)
            assert(savedOverlap.count() == 2)
        finally directory.getFileSystem(spark.sparkContext.hadoopConfiguration).delete(directory, true)

    def checkTsvReading(using spark: SparkSession): Unit =
        val directory  = Files.createTempDirectory("functional-class-dedup-checks-")
        val dictionary = directory.resolve("dictionary.txt")
        val ignore     = directory.resolve("dictionary.ignore")
        try
            Files.writeString(dictionary, "# comment\nA\tAlpha\ta|b\nA\t\tc\n")
            Files.writeString(ignore, "# comment\nA\tReplacementId\tExplanation\n")
            assert(createDictionaryDataset(dictionary.toString).collect().toSet == Set(
              DictionaryEntry("A", Some("Alpha"), Some("a|b")),
              DictionaryEntry("A", None, Some("c"))
            ))
            assert(createIgnoredIdsDataset(ignore.toString).collect().toSeq == Seq(IgnoredTermiteId("A")))
        finally
            Files.deleteIfExists(dictionary)
            Files.deleteIfExists(ignore)
            Files.delete(directory)

    def main(args: Array[String]): Unit =
        given spark: SparkSession = makeSparkSession
        try
            spark.sparkContext.setLogLevel("ERROR")
            checkLoading
            checkPreferredLabels
            checkDeltaRoundTrip
            checkTsvReading
            println("PASS: dictionary loading, preferred-label overlap, Delta round trips and TSV input fixtures")
        finally spark.stop()
