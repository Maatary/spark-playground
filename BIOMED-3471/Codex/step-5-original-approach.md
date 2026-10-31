# Step 5 — original approach

Preserved on 23 October 2026 for comparison with the new approach. The writeup below is retained exactly as supplied.

---

To identify homonymous node pairs—nodes that share the same name but have different identifiers—we need to determine which node in the forward-flow relation should be compared with which node in the backfill relation. We call the result of this determination a node pairing.

Step 4 has already established the non-identical twin pairs. Its output, nonIdenticalTwinPairs, contains one row per twin pair. Each row has two struct columns:

- forward holds the forward-flow relation.
- backfill holds its backfill twin.

Both retain their node1_urn, node1_name, node2_urn and node2_name fields. Step 5 uses these fields to determine the node pairing within each twin pair.

For Binding, each relation has exactly two partner positions. A complete node pairing contains two node pairs, using each position in each relation exactly once.

The stored positions do not by themselves tell us which nodes to compare. Partners are ordered lexicographically by their raw TERMite IDs, independently in each relation. Matching names can therefore appear in opposite positions. Step 4 sorted names in its comparison key; it did not rearrange the original node fields.

The four cases we will examine explain how we determine the node pairing from those names and identifiers. They cover different partner names, a shared node, a self-referencing relation with the same node in both partner positions, and the situation where two alternative complete results remain. We will connect each case to the structures used to construct and retain its result.

Both Binding positions have the role partner. Before checking the names, forward position 1 can be paired with either backfill position. Once that choice is made, forward position 2 must pair with the other backfill position, because each position is used once.

A positionPair records one forward-flow position paired with one backfill position. A positionPairTuple contains two such position pairs, covering both positions of both relations.

There are exactly two ways to pair those positions:

| positionPairTuple | First position pair | Second position pair |
|---|---|---|
| straightPositionPairTuple | Forward position 1 with backfill position 1 | Forward position 2 with backfill position 2 |
| crossedPositionPairTuple | Forward position 1 with backfill position 2 | Forward position 2 with backfill position 1 |

These position pairs tell us which node fields to read from forward and backfill. Reading the URIs and names at those positions gives the actual node pairs to compare.

When the forward-flow and backfill names match in each of the two node pairs, ignoring case, those node pairs form a possible complete nameMatchedNodePairTuple.

Each nameMatchedNodePair in that tuple contains:

role
forwardNodeUri
backfillNodeUri
matchedNodeName

For Binding, role is partner. matchedNodeName holds the matching lowercased name. Equal full URIs establish an identical name match; different full URIs establish a homonymous node pair. Both kinds remain in the complete tuple.

One nameMatchedNodePairTuple contains exactly two records, accounting for both partner positions of both relations. In Spark, it is an array of two structs.

The four cases determine which complete results we retain. Step 5 stores them in the nameMatchedNodePairTuples column of twinPairsWithNameMatchedNodePairTuples. That table still contains one row per twin pair, with both relations attached.

| Contents of nameMatchedNodePairTuples in one twin row | Meaning |
|---|---|
| One complete tuple containing two node-pair records | One complete node pairing has been determined. |
| Two alternative tuples, each containing two node-pair records | Two different complete node pairings remain; neither has been selected. |

Two records inside one tuple describe the two partner comparisons. Two tuples describe alternative complete results for the same twin.

For twins with one complete result, the later homonymousNodePairs selection keeps the records whose full URIs differ. Identical name matches remain available in the complete tuple.

———

Case 1: the two partners have different names. Each name tells us which node to compare with which.

This collagen–MIA3 twin comes from the saved 20 October investigation. The table is a reading view of one row of nonIdenticalTwinPairs. Each cell displays the name and full URI from the indicated position inside its forward or backfill struct.

| Forward position 1 | Forward position 2 | Backfill position 1 | Backfill position 2 |
|---|---|---|---|
| collagen — biodata:smallmol/Concept_86378312 | MIA3 — biodata:protein/HGNC24008 | MIA3 — biodata:protein/HGNC24008 | collagen — biodata:functionalclass/MSCAN_12001170 |

Both relations connect collagen and MIA3, but the names occupy opposite positions.

The straight position pairing compares collagen with MIA3 and MIA3 with collagen. The names do not match, so it produces no complete nameMatchedNodePairTuple.

The crossed position pairing compares collagen with collagen and MIA3 with MIA3. Both names match, giving one complete tuple.

The table below displays the two records inside that one tuple:

| role | forwardNodeUri | backfillNodeUri | matchedNodeName |
|---|---|---|---|
| partner | biodata:protein/HGNC24008 | biodata:protein/HGNC24008 | mia3 |
| partner | biodata:smallmol/Concept_86378312 | biodata:functionalclass/MSCAN_12001170 | collagen |

Now compare the full URIs in each record:

- For MIA3, the URIs are equal: this is an identical name match.
- For collagen, the URIs differ: this is the homonymous node pair.

Both records remain in the complete tuple. The later selection for homonymousNodePairs keeps only the collagen record, together with its role and twin details.

The different names determine the pairing in this case. The same procedure also works when the corresponding nodes differ for both names; then both records are homonymous node pairs.

———

Case 2: the two partners have the same name, and the relations share a node. Pairing the shared node with itself determines which other two nodes to compare.

For cases 2–4, every position has the name EGFR. The example: URIs are invented illustrations. Each input table shows the four node-URI fields from one twin pair.

| forward.node1_urn | forward.node2_urn | backfill.node1_urn | backfill.node2_urn |
|---|---|---|---|
| example:protein/EGFR_202 | example:protein/EGFR_303 | example:protein/EGFR_101 | example:protein/EGFR_202 |

Both partners have the same name, so names alone allow both straight and crossed position pairing. They do not tell us which forward-flow node should be compared with which backfill node.

However, the two relations share example:protein/EGFR_202.

First, pair forward example:protein/EGFR_202 with backfill example:protein/EGFR_202.

This establishes an identical name match: the names match, and the full URIs identify the same node. This match cannot be the homonym we are looking for, because a homonymous node pair must contain different node identities.

Pairing the shared node with itself accounts for one position in each relation. Each position participates exactly once in a complete pairing. Since each relation has exactly two positions, there is exactly one other position on each side to compare.

In this example, those other positions contain:

- Forward-flow node: example:protein/EGFR_303.
- Backfill node: example:protein/EGFR_101.

Pair those two nodes with each other. Their names match, but their full URIs differ. They form the homonymous node pair we are looking for.

This is how the shared node helps us find the homonym: pairing it with itself establishes the identical name match and determines which other two nodes must be compared.

The complete nameMatchedNodePairTuple contains both records:

| role | forwardNodeUri | backfillNodeUri | matchedNodeName |
|---|---|---|---|
| partner | example:protein/EGFR_202 | example:protein/EGFR_202 | egfr |
| partner | example:protein/EGFR_303 | example:protein/EGFR_101 | egfr |

The other pair must have different URIs: if it were also an identical name match, both relations would contain the same two nodes and would be the same Binding relation. Step 4 has already established that they are non-identical twins.

The complete tuple remains attached to the twin in twinPairsWithNameMatchedNodePairTuples. The later homonymousNodePairs selection keeps the record pairing forward example:protein/EGFR_303 with backfill example:protein/EGFR_101. The identical name match remains available in the complete tuple.

If one relation is self-referencing, the shared node occupies both of its partner positions. The rule still uses only one of its positions for the identical name match. The node at its other position is then paired with the node at the other position in its twin.

———

Case 3: the two partners have the same name, the relations share no node, and at least one relation is self-referencing. Straight and crossed position pairing give the same node pairs.

A self-referencing relation uses the same node in both partner positions.

Consider this twin pair, with EGFR at every position:

| forward.node1_urn | forward.node2_urn | backfill.node1_urn | backfill.node2_urn |
|---|---|---|---|
| example:protein/EGFR_101 | example:protein/EGFR_101 | example:protein/EGFR_202 | example:protein/EGFR_303 |

Both forward-flow positions contain example:protein/EGFR_101.

With straight pairing, backfill example:protein/EGFR_202 is paired with the forward node at position 1, and backfill example:protein/EGFR_303 with the forward node at position 2.

With crossed pairing, those forward positions are exchanged. But both contain the same node. The actual node pairs are therefore unchanged; only their listing order changes.

After putting the records in a consistent order, both complete tuples contain:

| role | forwardNodeUri | backfillNodeUri | matchedNodeName |
|---|---|---|---|
| partner | example:protein/EGFR_101 | example:protein/EGFR_202 | egfr |
| partner | example:protein/EGFR_101 | example:protein/EGFR_303 | egfr |

We retain one distinct complete tuple. We have determined which nodes to compare, even though two position pairings produced that result.

Both records have matching names and different full URIs, so both enter homonymousNodePairs.

When only the backfill relation is self-referencing, its two partner positions contain the same node. Exchanging those positions does not change the node pairs. Each record still keeps the forward-flow URI in forwardNodeUri and the backfill URI in backfillNodeUri.

When both twins are self-referencing relations, each relation uses the same node in both partner positions. The complete node pairing therefore contains two records with identical contents. Both remain because the tuple accounts for both partner positions.

For example:

| forward.node1_urn | forward.node2_urn | backfill.node1_urn | backfill.node2_urn |
|---|---|---|---|
| example:protein/EGFR_101 | example:protein/EGFR_101 | example:protein/EGFR_202 | example:protein/EGFR_202 |

Its one complete tuple contains:

| role | forwardNodeUri | backfillNodeUri | matchedNodeName |
|---|---|---|---|
| partner | example:protein/EGFR_101 | example:protein/EGFR_202 | egfr |
| partner | example:protein/EGFR_101 | example:protein/EGFR_202 | egfr |

The records do not store position numbers.

homonymousNodePairs retains both detailed entries. homonymousNodePairsWithTwinCount lists this node pair once; this twin contributes one to nonIdenticalTwinPairCount.

We remove equivalent complete tuples. When both twins are self-referencing relations, the two identical records inside the retained tuple still account for the two partner positions, so both remain.

———

Case 4: the two partners have the same name, the relations share no node, and each relation contains two distinct nodes. Two different complete node pairings remain.

Consider:

| forward.node1_urn | forward.node2_urn | backfill.node1_urn | backfill.node2_urn |
|---|---|---|---|
| example:protein/EGFR_101 | example:protein/EGFR_202 | example:protein/EGFR_303 | example:protein/EGFR_404 |

All names are EGFR. All four node URIs differ.

Straight position pairing produces this complete tuple:

| role | forwardNodeUri | backfillNodeUri | matchedNodeName |
|---|---|---|---|
| partner | example:protein/EGFR_101 | example:protein/EGFR_303 | egfr |
| partner | example:protein/EGFR_202 | example:protein/EGFR_404 | egfr |

Crossed position pairing produces this complete tuple:

| role | forwardNodeUri | backfillNodeUri | matchedNodeName |
|---|---|---|---|
| partner | example:protein/EGFR_101 | example:protein/EGFR_404 | egfr |
| partner | example:protein/EGFR_202 | example:protein/EGFR_303 | egfr |

Here, changing the position pairing changes the actual nodes paired together. The tuples remain different after their records are put in a consistent order.

The names cannot choose between them, and there is no shared node to pair with itself. Neither relation is self-referencing: each contains two distinct nodes. We cannot determine which complete node pairing to select.

We retain both complete alternatives in the nameMatchedNodePairTuples column of the same twin row. The twin also appears in twinPairsWithMultipleNameMatchedNodePairTuples with those alternatives.

Neither alternative contributes selected entries to homonymousNodePairs or its count summary. All the proposed node pairs are homonymous, but we have not established which complete pairing to use. Combining both alternatives into four selected pairs would misrepresent alternatives as established results.

———

The four cases fit into this conceptual procedure:

Start with one established Binding non-identical twin pair.

Work out which forward-flow node should be compared with which
backfill node, then identify the homonymous node pairs.

If the two partners have different names:

    Pair each forward-flow node with the backfill node whose name matches.
    The names determine which nodes to compare.

Otherwise, the two partners have the same name:

    If the two relations share a node:

        Pair the shared node with itself.
        This establishes an identical name match.

        That pairing accounts for one position in each relation.
        Each relation has exactly one other position.

        Pair the node at the other forward-flow position with
        the node at the other backfill position.

        Their names match and their identities differ.
        They form the homonymous node pair.

    Otherwise, if either relation is self-referencing:

        That relation uses the same node in both partner positions.

        Consider straight and crossed position pairing.
        They produce the same node pairs, possibly listed differently.

        Put the records in a consistent order.
        Retain one distinct complete nameMatchedNodePairTuple.
        When both twins are self-referencing relations, each uses the
        same node in both partner positions. The complete tuple then
        contains two records with identical contents. Retain both
        records because they account for both partner positions.

    Otherwise:

        Each relation contains two distinct nodes,
        and the relations share no node.

        Straight and crossed position pairing produce two different
        complete nameMatchedNodePairTuple alternatives.

        Retain both alternatives without selecting either one.

For a twin with one complete tuple, we compare the full URIs in each of its two records. An identical name match remains in the complete tuple. A record with matching names and different full URIs is selected into homonymousNodePairs, together with its role and the twin’s details. homonymousNodePairsWithTwinCount then lists each selected node pair once and counts the distinct complete twin pairs in which it was found.
