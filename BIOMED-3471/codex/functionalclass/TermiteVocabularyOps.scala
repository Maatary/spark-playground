package biomed3471.codex.functionalclass

import biomed3471.codex.functionalclass.DataTypes.*
import biomed3471.codex.functionalclass.DataTypes.given
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions.*

/** The Node Attribute Table Builder's dictionary-loading rules, expressed for this Scala 3 demonstration. */
object TermiteVocabularyOps:

    private val aliasTags = Seq("@^", "_Q", "_M", "_X", "_C", "<", "$", "`", "%", ">", "^", "@", "?", "&", "!", "*", "+", "-")

    extension (token: String)

        /** Interpret a single alias token; unknown suffixes remain part of the lexical form. */
        private def aliasChanges: AliasChanges =
            val trimmed = token.trim
            val tag     = aliasTags.find(suffix => trimmed.endsWith(s"/$suffix"))
            val name    = tag.fold(trimmed)(suffix => trimmed.dropRight(suffix.length + 1)).trim

            if name.isEmpty || name.startsWith("#") then AliasChanges(Set.empty, Set.empty)
            else
                tag match
                    case Some("*")             => AliasChanges(Set.empty, Set(name))
                    case Some("+" | "-" | "<") => AliasChanges(Set.empty, Set.empty)
                    case _                     => AliasChanges(Set(name), Set.empty)

    extension (entry: MergedDictionaryEntry)

        /** A removal applies across all surviving rows for this ID, regardless of which layer supplied it. */
        def toTermiteConcept: TermiteConcept =
            val changes = entry.aliasGroups.flatMap(_.split("\\|")).map(_.aliasChanges)
            val kept    = changes.flatMap(_.kept).toSet
            val removed = changes.flatMap(_.removed).toSet
            val aliases = (kept -- removed).toSeq.sorted
            TermiteConcept(entry.termiteId, entry.name, Option.when(aliases.nonEmpty)(aliases))

    /** Blank names cannot override a lower layer's preferred label. */
    private def nonblankName(name: Column): Column =
        when(length(trim(name)) > 0, trim(name))

    /**
     * Load order: main + extra -> ignore -> aug -> augIgnore -> one concept per ID. Preferred labels use aug, then extra, then main. Only extra/aug
     * strip leading %%.
     */
    def computeVocabDataset(
        main     : Dataset[DictionaryEntry],
        extra    : Dataset[DictionaryEntry],
        aug      : Dataset[DictionaryEntry],
        ignore   : Dataset[IgnoredTermiteId],
        augIgnore: Dataset[IgnoredTermiteId]
    ): Dataset[TermiteConcept] =
        val mainRows = main
            .withColumn("source", lit("main"))
            .withColumn("name", nonblankName(col("name")))

        val extraRows = extra
            .withColumn("source", lit("extra"))
            .withColumn("name", nonblankName(regexp_replace(col("name"), "^%%+", "")))

        val augRows = aug
            .withColumn("source", lit("aug"))
            .withColumn("name", nonblankName(regexp_replace(col("name"), "^%%+", "")))

        val base = mainRows.unionByName(extraRows)
            .join(ignore, Seq("termiteId"), "left_anti")

        val effectiveRows = base.unionByName(augRows)
            .join(augIgnore, Seq("termiteId"), "left_anti")

        val mergedEntries = effectiveRows
            .groupBy("termiteId")
            .agg(
              first(when(col("source") === "main", col("name")), ignoreNulls = true).as("mainName"),
              first(when(col("source") === "extra", col("name")), ignoreNulls = true).as("extraName"),
              first(when(col("source") === "aug", col("name")), ignoreNulls = true).as("augName"),
              collect_list(col("alias")).as("aliasGroups")
            )
            .select(
              col("termiteId"),
              coalesce(col("augName"), col("extraName"), col("mainName"), lit("")).as("name"),
              col("aliasGroups")
            )
            .as[MergedDictionaryEntry]

        mergedEntries.map(_.toTermiteConcept)
