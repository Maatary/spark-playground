package biomed3471.claude

/**
 * ===The data model of the twin search===
 *
 * DataTypes holds the shapes and the fixed values of the twin search. RelationOccurrence holds the input columns that the steps read.
 * CanonicalRelation is one row of canonicalRelations, which step 2 writes. CanonicalRelationWithNameKey is the same row with the normalized names and
 * the name key that step 3 adds, and CanonicalRelationNameKey is that name key. The first four constants are the `source` values of the two pipelines
 * and the `direction` values of the two kinds of relation. The next three are the roles of the canonical relation ID. NameMatchedNodePair is the
 * name-matched node pair that step 5 stores, and HomonymousNodePairWithTwinCount is one row of the global table, twinPairCountPerHomonymousNodePair.
 * DuplicateNodeName is one row of the list of duplicate node names attached to BIOMED-3471. DuplicateNamesApp checks that list against the results of
 * the twin search.
 */
object DataTypes:

    case class CanonicalRelationNameKey(
        relationship_type: String,
        nodeNames        : Seq[String]
    )

    case class RelationOccurrence(
        relationship_type: String,
        direction        : String,
        node1_urn        : String,
        node1_name       : String,
        node2_urn        : String,
        node2_name       : String,
        source           : String
    )

    case class CanonicalRelation(
        canonicalId      : String,
        relationship_type: String,
        direction        : String,
        node1_urn        : String,
        node1_name       : String,
        node2_urn        : String,
        node2_name       : String,
        sources          : Seq[String])

    case class CanonicalRelationWithNameKey(
        canonicalId        : String,
        relationship_type  : String,
        direction          : String,
        node1_urn          : String,
        node1_name         : String,
        node2_urn          : String,
        node2_name         : String,
        sources            : Seq[String],
        node1NormalizedName: String,
        node2NormalizedName: String,
        nameKey            : CanonicalRelationNameKey
    )

    /**
     * The two values of the `source` column of a relation occurrence. Each value names the pipeline that produced it.
     */
    val ForwardFlowSource = "Elsevier ML NLP"
    val BackfillSource    = "Elsevier Medscan NLP"

    /**
     * The two values of the `direction` column. `node1_to_node2` marks a directed relation, and `undirected` marks an undirected one.
     */
    val Directed   = "node1_to_node2"
    val Undirected = "undirected"

    /**
     * The three roles that the canonical relation ID names. A directed relation has a regulator and a target, and an undirected relation has two
     * partners.
     */
    val Regulator = "regulator"
    val Target    = "target"
    val Partner   = "partner"

    /**
     * One name-matched node pair, with the fields of the struct that step 5 stores. A name-matched node pair is a forward-flow node and a backfill
     * node whose normalized names are the same.
     */
    case class NameMatchedNodePair(
        role           : String,
        forwardNodeUri : String,
        backfillNodeUri: String,
        matchedNodeName: String
    )

    /**
     * One row of twinPairCountPerHomonymousNodePair.
     */
    case class HomonymousNodePairWithTwinCount(
        forwardNodeUri           : String,
        backfillNodeUri          : String,
        matchedNodeName          : String,
        nonIdenticalTwinPairCount: Long
    )

    /**
     * One row of the list of duplicate node names attached to BIOMED-3471: one name given twice, each time with the entity type of one of the nodes
     * that have it. The fields are optional because a column read from a file can be empty.
     */
    case class DuplicateNodeName(name1: Option[String], entitytype1: Option[String], name2: Option[String], entitytype2: Option[String])
