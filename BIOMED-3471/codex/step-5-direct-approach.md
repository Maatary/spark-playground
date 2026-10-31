# Step 5: determine which nodes go together within each twin pair

Step 4 supplies a non-identical twin pair: a forward-only canonical relation and its backfill twin. Step 5 determines which forward-flow node goes with which backfill node.

The model and the preceding steps already establish that:

- Each relation is between two nodes, and each node has one name.
- The twins have different canonical relation IDs and the same relation type, so they are both directed or both undirected.
- Directed twins have matching normalized names by role: regulator with regulator and target with target.
- Undirected twins have the same normalized partner names, with each name appearing the same number of times in both relations. Their name key, stored as `comparisonKey`, sorts the names without changing which name belongs to which node.

## Names and node-pair results

A **normalized name** is `lower(trim(name))`: comparison ignores case and leading/trailing spaces. The original node name remains attached to its node.

Node **identity** is determined by the full node URN, such as `biodata:protein/HGNC24008`. Its final component, `HGNC24008`, is the **node TERMite ID** used in the canonical relation ID.

A **name-matched node pair** contains one forward-flow node and one backfill node with the same normalized name. It is a **homonymous node pair** when their full node URNs differ, and an **identical node pair** when the full node URN is the same on both sides.

A **name-matched node pair tuple** contains the two name-matched node pairs of one twin pair and accounts for each use of a node once in each relation. If a relation uses one node twice, both uses must be retained. One tuple can therefore contain two node pairs with identical contents. A twin pair is **resolved** when the rules determine one such tuple, and **unresolved** when two alternatives remain.

## Directed twins: pair nodes by role

Pair the forward-flow regulator with the backfill regulator, and the forward-flow target with the backfill target. Step 4 guarantees that the two regulators have the same normalized name, and that the two targets have the same normalized name.

## Undirected twins: roles and positions do not determine matches

Both nodes of an undirected relation have the role `partner`, so their roles cannot distinguish them. The canonical relation ID orders the two partners lexicographically by node TERMite ID. Their positions follow that order.

In the collagen–MIA3 binding twin pair, `Concept_86378312` sorts before `HGNC24008`, while `MSCAN_12001170` sorts after it:

| Relation | Position 1 | Position 2 |
|---|---|---|
| Forward-flow relation | collagen: `biodata:smallmol/Concept_86378312` | MIA3: `biodata:protein/HGNC24008` |
| Backfill twin | MIA3: `biodata:protein/HGNC24008` | collagen: `biodata:functionalclass/MSCAN_12001170` |

Collagen is at forward-flow position 1 and backfill position 2; MIA3 is at forward-flow position 2 and backfill position 1. Comparing position 1 with position 1 would compare collagen with MIA3; comparing position 2 with position 2 would compare MIA3 with collagen.

## Undirected twins: matching starts with normalized names

Step 4 guarantees the same normalized partner names in both relations, with each name appearing the same number of times. When each relation has two different normalized names, names determine the matches. When each relation repeats one normalized name, node identity is needed to determine the matches.

### Two different normalized names in each relation

Each normalized name occurs once in each relation. Pair each forward-flow node with the backfill node that has the same normalized name.

In the twin pair above, pair collagen with collagen and MIA3 with MIA3. The collagen nodes have different full node URNs, forming a homonymous node pair. MIA3 is the same node in both relations, forming an identical node pair.

### One normalized name repeated twice in each relation

Every node in either relation has the same normalized name, so names alone cannot determine which nodes go together. Use node identity to distinguish the following cases.

In the examples below, `E1`, `E2`, `E3` and `E4` abbreviate distinct full node URNs. All four nodes are named EGFR. The notation `E1/E2` means forward-flow `E1` paired with backfill `E2`.

A **shared node** occurs in both relations of the twin pair. For example, `E1 binds E2` and `E2 binds E3` share `E2`.

A **self-referencing relation** uses the same node twice within that one relation. For example, `E1 binds E1` is self-referencing. `E1 binds E2` uses two distinct nodes, even though both are named EGFR.

#### A node is shared across the twins

Pair the shared node with itself. That accounts for one use of that node in each relation; pair the remaining forward-flow node with the remaining backfill node.

For forward `E1 binds E2` and backfill `E2 binds E3`, pair `E2/E2`, then account for the remaining nodes with `E1/E3`. The name-matched node pair tuple contains the identical node pair `E2/E2` and the homonymous node pair `E1/E3`.

This rule also covers self-reference with sharing. For forward `E2 binds E2` and backfill `E1 binds E2`, the identical node pair `E2/E2` accounts for one of the forward relation's two uses of `E2`. Its other use pairs with backfill `E1`, giving `E2/E1`. The tuple contains one identical node pair and one homonymous node pair.

#### No node is shared, and at least one relation repeats a node

Each node in the other relation must pair with that repeated node. This determines one name-matched node pair tuple, whether the repetition is in forward flow, backfill, or both:

| Forward-flow relation | Backfill relation | Name-matched node pair tuple |
|---|---|---|
| `E1 binds E1` | `E2 binds E3` | `E1/E2` and `E1/E3` |
| `E1 binds E2` | `E3 binds E3` | `E1/E3` and `E2/E3` |
| `E1 binds E1` | `E2 binds E2` | `E1/E2` and `E1/E2` |

Every node pair here is homonymous because no node is shared. When both relations are self-referencing, retain both node pairs with identical contents: each relation uses its node twice.

#### No node is shared, and neither relation repeats a node

For forward `E1 binds E2` and backfill `E3 binds E4`, all four nodes are distinct and have the same normalized name. All four cross-flow pairs are homonymous: `E1/E3`, `E1/E4`, `E2/E3` and `E2/E4`.

What remains unresolved is **which name-matched node pair tuple to select**. Two alternatives account for both nodes of each relation:

| Complete alternative | One node pair | Other node pair |
|---|---|---|
| One possibility | `E1/E3` | `E2/E4` |
| Other possibility | `E1/E4` | `E2/E3` |

There is no shared identity or repeated node to distinguish those alternatives. The twin pair remains unresolved, with both name-matched node pair tuples retained separately.

## Execution shortcut: handle self-reference first

The division by partner names above explains the domain cases. The implementation can handle an undirected twin pair with a self-referencing relation before testing names or searching for a shared node.

If either relation repeats a node, that node's single name makes the relation's two normalized names equal. The matching name key then makes the other relation's two normalized names equal to that name as well. Each node in the other relation must pair with the repeated node, so one name-matched node pair tuple is already determined.

This shortcut covers both repeated-name cases involving self-reference:

- **With sharing:** it produces the shared node's identical node pair and the remaining homonymous node pair, as in `E2 binds E2` with `E1 binds E2`.
- **Without sharing:** it pairs each node with the repeated node, including two records with identical contents when both relations are self-referencing, as in `E1 binds E1` with `E2 binds E2`.

The branch order is therefore:

```text
For each established non-identical twin pair:
    If directed:
        Pair regulator with regulator and target with target.
    Otherwise, if either relation is self-referencing:
        Select the name-matched node pair tuple forced by the repeated node.
    Otherwise, if each relation has two different normalized names:
        Pair nodes by matching name.
    Otherwise, if a node is shared:
        Pair the shared node with itself, then pair the remaining nodes.
    Otherwise:
        Retain both name-matched node pair tuples.
```

## Stored node pairs and tuple order

The `forwardTwin` and `backfillTwin` structs store each relation's nodes in `node1_urn`, `node1_name`, `node2_urn` and `node2_name`. These fields preserve each node's name and canonical position. For directed relations, position 1 is regulator and position 2 is target.

Two name-matched node pair tuples are equal when they contain the same node pairs with the same repetitions, regardless of listing order.

A name-matched node pair is stored as a record with fields `role`, `forwardNodeUri`, `backfillNodeUri` and `matchedNodeName`. The fields `forwardNodeUri` and `backfillNodeUri` hold full node URNs. Each tuple contains exactly two records, using each position in each relation once. The record for forward position 1 is stored first, followed by the record for forward position 2. Directed tuples therefore list regulator before target.

The implementation defines two tuple expressions once: straight pairs forward positions 1 and 2 with backfill positions 1 and 2; crossed pairs them with backfill positions 2 and 1. The branches select whole tuples according to the domain rules.

The early self-reference branch selects the straight tuple. Because one side repeats the same node, this also includes the identical node pair whenever a node is shared. For forward `E2 binds E2` and backfill `E1 binds E2`, the stored tuple is `[E2/E1, E2/E2]`.

The shared-node branch also follows forward-position order. For forward `E1 binds E2` and backfill `E2 binds E3`, it stores `[E1/E3, E2/E2]`, even though the domain rule determines the identical node pair first.

For an unresolved twin pair, the outer array stores the straight alternative first and the crossed alternative second. Both inner tuples follow forward-position order:

```text
nameMatchedNodePairTuples:
    [ E1/E3, E2/E4 ]
    [ E1/E4, E2/E3 ]
```

Both tuple expressions produce this stored order directly.

## Retain complete results, then select detail

Each name-matched node pair tuple retains both node pairs, including any identical node pair. `nameMatchedNodePairTuples` retains one two-record tuple when the twin pair is resolved, or two two-record alternatives when it remains unresolved. The twin pair remains one row in `twinPairsWithNameMatchedNodePairTuples` in either case.

For a twin pair with one tuple, `homonymousNodePairsPerTwinPair` selects its records whose full node URNs differ. Identical node pairs remain in the tuple. When both relations are self-referencing, both records remain in the detailed output: directed records retain their different roles, while undirected records have identical contents.

For an unresolved twin pair, the output policy selects no records from either alternative into `homonymousNodePairsPerTwinPair`. Both alternatives remain available in the full twin-pair table and in `twinPairsWithMultipleNameMatchedNodePairTuples`.

`twinPairCountPerHomonymousNodePair` groups the selected detail by `forwardNodeUri`, `backfillNodeUri` and `matchedNodeName`, and counts distinct twin pairs using both `forwardCanonicalId` and `backfillCanonicalId`. Repeated detail records from one twin pair contribute once to that node pair's count. Unresolved twin pairs have no selected detail, so they do not contribute to this count.
