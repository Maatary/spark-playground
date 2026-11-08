package biomed3471.codex.functionalclass

import io.github.pashashiz.spark_encoders.TypedEncoder
import io.github.pashashiz.spark_encoders.TypedEncoder.given
import org.apache.spark.sql.Encoder

object DataTypes:

    /** The three columns in a TERMite dictionary, extra or augmentation file. */
    final case class DictionaryEntry(termiteId: String, name: Option[String], alias: Option[String])

    final case class IgnoredTermiteId(termiteId: String)

    /** One effective concept, following the Node Attribute Table Builder's loading rules. */
    final case class TermiteConcept(termiteId: String, name: String, alias: Option[Seq[String]])

    /** Surviving dictionary rows have been combined; their alias strings still contain TERMite controls. */
    final case class MergedDictionaryEntry(termiteId: String, name: String, aliasGroups: Seq[String])

    final case class AliasChanges(kept: Set[String], removed: Set[String])

    given dictionaryEntryEncoder: Encoder[DictionaryEntry]             = TypedEncoder[DictionaryEntry].encoder
    given ignoredTermiteIdEncoder: Encoder[IgnoredTermiteId]           = TypedEncoder[IgnoredTermiteId].encoder
    given termiteConceptEncoder: Encoder[TermiteConcept]               = TypedEncoder[TermiteConcept].encoder
    given mergedDictionaryEntryEncoder: Encoder[MergedDictionaryEntry] = TypedEncoder[MergedDictionaryEntry].encoder
