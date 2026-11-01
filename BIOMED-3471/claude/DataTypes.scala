package biomed3471.claude

object DataTypes:

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
        canonicalId      : String,
        relationship_type: String,
        direction        : String,
        node1_urn        : String,
        node1_name       : String,
        node2_urn        : String,
        node2_name       : String,
        sources          : Seq[String],
        nameKey          : CanonicalRelationNameKey
    )

    case class TwinPairRecord(
        forwardCanonicalId                    : String,
        backfillCanonicalId                   : String,
        nonIdenticalTwinsAlsoWithinForwardFlow: Boolean,
        forwardTwin                           : CanonicalRelation,
        backfillTwin                          : CanonicalRelation
    )

    /**
     * One node of a forward-flow canonical relation paired with one node of its backfill twin, with the same normalized name and the same role. The
     * role is regulator or target for directed relations, and partner for undirected relations. A normalized name is the node's name in lower case,
     * with leading and trailing spaces removed.
     *
     * `forwardNodeUri` and `backfillNodeUri` hold the full node URNs. Equal URNs make this an identical node pair; different URNs make it a
     * homonymous node pair. `role` holds the role shared by the two nodes, and `matchedNodeName` holds their shared normalized name.
     */
    case class NameMatchedNodePair(
        role           : String,
        forwardNodeUri : String,
        backfillNodeUri: String,
        matchedNodeName: String
    )

    /**
     * One way to pair the two nodes of a forward-flow canonical relation with the two nodes of its backfill twin. It contains exactly two
     * name-matched node pairs, each pairing a forward-flow node with a backfill node of the same normalized name and role.
     *
     * Each of the two positions of each relation, node 1 and node 2, appears in exactly one of the two pairs. So when a relation holds the same node
     * at both positions, that node appears in both pairs, and when both relations do so, the two pairs are equal. A pair can be an identical node
     * pair.
     *
     * The pair for forward-flow node 1 comes first, followed by the pair for forward-flow node 2, in the order of the canonical relation ID. Backfill
     * nodes may appear in the opposite order when matching undirected partners, as in this Binding example. F1, F2, B1 and B2 stand for node URNs,
     * and each pair below lists `{role, forwardNodeUri, backfillNodeUri, matchedNodeName}`:
     *
     * {{{
     *           forward-flow relation          backfill twin
     * node 1    F1  Collagen  ------.   .----  B1  MIA3
     *                                \ /
     *                                 X
     *                                / \
     * node 2    F2  MIA3      ------'   '----  B2  collagen
     *
     * tuple     [{partner, F1, B2, collagen}, {partner, F2, B1, mia3}]
     * }}}
     */
    type NameMatchedNodePairTuple = Seq[NameMatchedNodePair]

    /**
     * One twin pair together with its name-matched node pair tuples. The two canonical relations have different canonical relation IDs, the same
     * relation type, and matching normalized node names: by regulator/target role when directed, or as the same two partner names when undirected.
     *
     * The record holds both canonical relations and their IDs. `forwardTwin` is supported by relation occurrences from the forward flow only.
     * `backfillTwin` is supported by relation occurrences from the backfill and may also be supported by forward-flow relation occurrences.
     * `nonIdenticalTwinsAlsoWithinForwardFlow` is true when the backfill twin is also supported by forward-flow relation occurrences, so both
     * canonical relations are supported by the forward flow.
     *
     * `nameMatchedNodePairTuples` contains one tuple when the matching rules determine which forward-flow node goes with which backfill node
     * (resolved), or two alternative tuples when the rules cannot choose between them (unresolved).
     *
     * For example, E1, E2, E3 and E4 below abbreviate the URNs of four distinct nodes whose normalized name is `egfr`. These Binding twins are
     * unresolved. Each `(forward, backfill)` abbreviates one name-matched node pair:
     *
     * {{{
     * forward-flow relation: E1 binds E2       backfill twin: E3 binds E4
     *
     * nameMatchedNodePairTuples = [
     *   [(E1, E3), (E2, E4)],
     *   [(E1, E4), (E2, E3)]
     * ]
     * }}}
     *
     * All four node pairs are homonymous. What remains unresolved is which of the two tuples to select; both alternatives stay in this record.
     */
    case class TwinPairWithNameMatchedNodePairTuples(
        forwardCanonicalId                    : String,
        backfillCanonicalId                   : String,
        nonIdenticalTwinsAlsoWithinForwardFlow: Boolean,
        forwardTwin                           : CanonicalRelation,
        backfillTwin                          : CanonicalRelation,
        nameMatchedNodePairTuples             : Seq[NameMatchedNodePairTuple]
    )

    /**
     * One homonymous node pair together with one resolved twin pair that gives it. A homonymous node pair is a node of the forward-flow relation and
     * the node of the backfill twin that goes with it, when the two nodes have the same normalized name but different node URNs. The record holds the
     * two node URNs, their shared normalized name, the role at which the twin pair gives them, and the twin pair: the canonical relation IDs of its
     * forward-flow relation and of its backfill twin, and whether the backfill twin is also supported by forward-flow relation occurrences.
     */
    case class HomonymousNodePairWithTwinPair(
        forwardNodeUri                        : String,
        backfillNodeUri                       : String,
        matchedNodeName                       : String,
        role                                  : String,
        forwardCanonicalId                    : String,
        backfillCanonicalId                   : String,
        nonIdenticalTwinsAlsoWithinForwardFlow: Boolean
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
    case class DuplicateNodeName(
        name1      : Option[String],
        entitytype1: Option[String],
        name2      : Option[String],
        entitytype2: Option[String]
    )
