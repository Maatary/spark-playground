# BIOMED-3471: twin search with direct domain rules

A **canonical relation** is one relation type between two specific nodes in their roles. A **relation occurrence** is one occurrence of that canonical relation in a document, produced by one pipeline. A node's **normalized name** is its name in lower case with spaces at both ends removed: `lower(trim(name))`.

**Non-identical twins** are canonical relations with different canonical relation IDs, the same relation type and matching normalized node names: by regulator/target role for directed relations, or the same two partner names for undirected relations. The twin search compares each **forward-only canonical relation**, whose occurrences all come from the forward flow, with the backfill's canonical relations. Each match is a **twin pair**. The backfill twin may also have forward-flow relation occurrences.

Step 5 determines which nodes go together within each twin pair. From resolved twin pairs, it selects pairs of different nodes with the same normalized name—the **homonymous node pairs**—to investigate.

This implementation is in package `biomed3471.codex`.

## Input and twin matching

Steps 1–5 start from the full relation-occurrence export, stored as a shared Delta source in S3. A canonical relation fixes its type, direction and participating nodes; each node has one name. Its relation occurrences therefore share six fields: `relationship_type`, `direction`, `node1_urn`, `node1_name`, `node2_urn` and `node2_name`. Construct the omitted `canonicalId` for each relation occurrence, group on it together with those six fields, and collect the distinct `source` values into the sorted `sources` array.

A **node URN** is the full node identifier, such as `biodata:smallmol/Concept_86378312`. Its final component, `Concept_86378312`, is the **node TERMite ID** used in the canonical relation ID. Step 5 uses the full node URN to compare node identity. Fields such as `node1_urn`, `forwardNodeUri` and `backfillNodeUri` contain node URNs.

The **name key**, stored as `comparisonKey`, contains relation type and normalized node names. Relation type determines whether a relation is directed or undirected: directed names retain regulator/target order; undirected names are sorted in the key.

Match each forward-only canonical relation with every backfill canonical relation that has the same name key. Backfill canonical relations include those shared with the forward flow. `findNonIdenticalTwinPairs` builds the key with `withComparisonKey`, selects the two sets with `forwardOnly` and `backfill`, and joins them on that key.

`normalizedNodeName` applies the same normalization for the name key, Step 5 name matching and `matchedNodeName`. The canonical relation structs retain each node's original name with its node URN.

`nonIdenticalTwinsAlsoWithinForwardFlow` is true when the backfill twin also has forward-flow relation occurrences: both canonical relations in that twin pair occur within forward flow. It is false when the backfill twin has only backfill relation occurrences.

Model references: [Identifier Schemes](https://elsevier.atlassian.net/wiki/spaces/ELSSIE/pages/119601216791062), [Graph Data Model](https://elsevier.atlassian.net/wiki/spaces/ELSSIE/pages/119601199850466), [CPLD to Graph Events](https://elsevier.atlassian.net/wiki/spaces/ELSSIE/pages/119601220912178).

## Direct domain rules

Before identifying which nodes differ, Step 5 determines which node in the forward-flow relation should be compared with which node in its backfill twin. Its input is one established non-identical twin pair. The relations have the same type and direction, and the same normalized names, each appearing the same number of times in both relations. Their canonical IDs differ.

A **name-matched node pair** contains one forward-flow node and one backfill node with the same normalized name. It is a **homonymous node pair** when their node URNs differ, and an **identical node pair** when the node URN is the same on both sides.

A **name-matched node pair tuple** contains the two name-matched node pairs of one twin pair and accounts for both nodes in each relation. If a relation uses the same node twice, both uses must be accounted for.

### Directed twins: pair nodes by role

Pair the forward-flow regulator with the backfill regulator, and the forward-flow target with the backfill target. Step 4 guarantees that the two regulators have the same normalized name, and that the two targets have the same normalized name.

### Undirected twins: why roles and positions cannot decide

For an undirected relation such as Binding, both nodes have the role `partner`, so roles cannot determine which forward-flow node goes with which backfill node. Comparing nodes at the same position in their canonical relation IDs cannot determine this either.

A position is the place of a node in the canonical relation ID. An undirected canonical relation ID lists the two partners in lexicographic order of their node TERMite IDs. The same normalized name can therefore occur at different positions across twins when its nodes have different node TERMite IDs.

The collagen–MIA3 twin pair shows this. `Concept_86378312` sorts before `HGNC24008`, while `MSCAN_12001170` sorts after it:

| Relation | Position 1: name and node TERMite ID | Position 2: name and node TERMite ID |
|---|---|---|
| Forward-flow relation | collagen, `Concept_86378312` | MIA3, `HGNC24008` |
| Backfill twin | MIA3, `HGNC24008` | collagen, `MSCAN_12001170` |

Comparing position 1 with position 1 would compare collagen with MIA3. The nodes with matching normalized names instead go together across positions:

- Forward-flow collagen at position 1 goes with backfill collagen at position 2. These are different nodes, forming a homonymous node pair.
- Forward-flow MIA3 at position 2 goes with backfill MIA3 at position 1. It is the same node in both relations, forming an identical node pair.

### Undirected twins: start with the normalized names

The two normalized names **within each relation** determine how to proceed. When they differ, each name identifies exactly one node in each twin. When they are the same, names alone cannot distinguish which nodes go together, so we examine node identity.

The twins have matching names, each appearing the same number of times, so both relations fall into the same name category:

| Names within each relation | Forward-flow names | Backfill names | How to proceed |
|---|---|---|---|
| Two different normalized names | collagen, mia3 | collagen, mia3 | Match by name |
| One normalized name repeated twice | egfr, egfr | egfr, egfr | Examine node identity |

#### Two different normalized names

Each normalized name occurs once in each relation. Pair each forward-flow node with the backfill node that has the same normalized name. In the collagen–MIA3 example above, each collagen node has only one possible collagen match, and each MIA3 node has only one possible MIA3 match.

#### One normalized name repeated twice

Both partners in both relations have the same normalized name. Names alone cannot distinguish which nodes go together, so the rules use node identity.

Here, we need to distinguish a node appearing in both relations from a node appearing twice in one relation:

- A **shared node** appears in both the forward-flow relation and its backfill twin, with the same node URN.
- A **self-referencing relation** contains the same node twice, identified by its node URN.

In the examples below, `E1`, `E2`, `E3` and `E4` stand for distinct node URNs, all named EGFR. `E1 binds E2` uses two distinct nodes with the same name; `E1 binds E1` uses the same node twice and is self-referencing. Equal names alone establish neither self-reference nor a shared node.

**A node is shared between the relations.** Pair that node with itself. Once this identical node pair is fixed, pair the other forward-flow partner with the other backfill partner. For `E1 binds E2` versus `E2 binds E3`, pair `E2` with `E2`, and `E1` with `E3`.

This case includes a self-referencing relation that shares its repeated node with its twin. For `E1 binds E1` versus `E1 binds E2`, pair `E1` with `E1`, and the second use of forward-flow `E1` with backfill `E2`.

**No node is shared, and at least one relation is self-referencing.** Pair each partner of the other relation with the repeated node. There is one complete result:

| Forward-flow relation | Backfill relation | Name-matched node pair tuple |
|---|---|---|
| E1 binds E1 | E2 binds E3 | E1 with E2; E1 with E3 |
| E1 binds E2 | E3 binds E3 | E1 with E3; E2 with E3 |
| E1 binds E1 | E2 binds E2 | E1 with E2, twice |

In the last example, keep both pairs with identical contents: each relation uses its node twice. With no shared node, every node pair in these results is homonymous.

**No node is shared, and each relation contains two distinct nodes.** For `E1 binds E2` versus `E3 binds E4`, two name-matched node pair tuples remain:

| Complete alternative | One node pair | Other node pair |
|---|---|---|
| First | E1 with E3 | E2 with E4 |
| Second | E1 with E4 | E2 with E3 |

Both alternatives contain homonymous node pairs: their normalized names agree and their node identities differ. No shared node or repeated node determines which name-matched node pair tuple to select, so the twin pair remains unresolved.

The [standalone Step 5 explanation](step-5-direct-approach.md) develops these same domain cases and their implementation.

## From domain rules to implementation

### Why the code checks self-reference early

The undirected implementation handles a self-referencing relation before testing names or shared identity. A repeated node has one normalized name; the twins' matching name keys therefore imply that both partners in both relations have that same name. Each partner of the other relation goes with the repeated node, which determines the name-matched node pair tuple immediately.

This shortcut covers the no-shared-node self-reference case above **and** the shared-node case when one relation is self-referencing. For `E1 binds E1` versus `E1 binds E2`, it gives the same identical and homonymous pairs as the shared-node rule.

The following pseudocode follows the actual execution order:

```text
For each established non-identical twin pair:

    Directed:
        Pair regulator with regulator and target with target.

    Undirected:
        If either relation is self-referencing:
            Pair each partner of the other relation with the repeated node.
            If both relations self-reference, retain the repeated pair twice.
        Otherwise, if the two partners have different normalized names:
            Pair each forward-flow node with the backfill node bearing its name.
        Otherwise, if a node is shared:
            Pair the shared node with itself.
            Pair the other forward-flow node with the other backfill node.
        Otherwise:
            Retain both different name-matched node pair tuples.
```

The implementation defines the straight and crossed tuple expressions once, then uses `when` to select one tuple when resolved or both alternatives when unresolved. The straight tuple pairs forward nodes 1 and 2 with backfill nodes 1 and 2; the crossed tuple pairs them with backfill nodes 2 and 1.

### Column mapping and stored order

The `forwardTwin` and `backfillTwin` structs retain `node1_urn`, `node1_name`, `node2_urn` and `node2_name`. Their columns follow canonical relation order: regulator then target for directed relations, and lexicographic node TERMite ID order for undirected partners. Canonical-ID reconstruction uses that existing order directly.

Each tuple lists the pair for forward-flow node 1 first and the pair for forward-flow node 2 second. Directed tuples therefore list the regulator before the target. Both alternatives of an unresolved twin pair use the same convention. When forward flow repeats one node, corresponding backfill columns provide the listing order for its two pairs.

Construction supplies this deterministic order without a separate tuple sort. Two tuples have the same contents when they contain the same two node pairs, with repetitions counted, regardless of listing order. Each record keeps the forward-flow and backfill nodes in their respective fields.

### Retain complete results, then select detail

Once the two node pairs are determined, keep both in the name-matched node pair tuple, including any identical node pair. If a relation uses the same node twice, retain both uses.

A `nameMatchedNodePairTuple` stores the two node pairs as records with fields `role`, `forwardNodeUri`, `backfillNodeUri` and `matchedNodeName`. `nameMatchedNodePairTuples` holds one such tuple for a resolved twin pair, or two separate tuples for an unresolved twin pair.

`twinPairsWithNameMatchedNodePairTuples` retains each twin pair, including both canonical relations, with its result. For twin pairs with one tuple, `homonymousNodePairsPerTwinPair` selects records with different node URNs. Identical node pairs remain in the tuple. The detailed output retains both entries when two self-referencing relations produce the same node pair twice.

Unresolved twin pairs retain both alternatives in the full twin-pair table and in `twinPairsWithMultipleNameMatchedNodePairTuples`. They contribute no entries to `homonymousNodePairsPerTwinPair` or `twinPairCountPerHomonymousNodePair`.

### Count distinct twin pairs for each homonymous node pair

`twinPairCountPerHomonymousNodePair` groups by `forwardNodeUri`, `backfillNodeUri` and `matchedNodeName`, then counts distinct twin pairs using both `forwardCanonicalId` and `backfillCanonicalId`. Each node has one name, so the forward-flow/backfill node URN pair already fixes the normalized name. Including that name in the grouping retains it directly without splitting a node-pair group or picking a name from one row. A twin pair contributes once to a node pair's count when both positions produce that pair. Different twin pairs sharing one canonical relation ID still count separately. Role and the forward-flow flag remain in the detail and do not split these groups.

## Saved-table flow

```text
canonicalRelations [write/read]
  -> nonIdenticalTwinPairs [write/read]
      -> twinPairsWithNameMatchedNodePairTuples [write/read]
          -> homonymousNodePairsPerTwinPair [write/read]
              -> twinPairCountPerHomonymousNodePair [write]
          -> twinPairsWithMultipleNameMatchedNodePairTuples [write]
```

## Six saved Delta tables

Each run uses a date in `YYYY-MM-DD` format. Each table has its own directory:

```text
s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/BIOMED-3471/YYYY-MM-DD/<table>/
```

| Table | What one row contains | What the table tells us |
|---|---|---|
| `canonicalRelations` | One canonical relation, with its node fields and the set of sources of its relation occurrences. | Which canonical relations occur in the forward flow, the backfill, or both? |
| `nonIdenticalTwinPairs` | One non-identical twin pair, retaining both canonical relations and the flag `nonIdenticalTwinsAlsoWithinForwardFlow`. | Which forward-only canonical relations have non-identical twins in backfill? |
| `twinPairsWithNameMatchedNodePairTuples` | One twin pair with its complete name-matched node-pair results: one tuple when resolved, or two alternative tuples when unresolved. Each tuple contains two node pairs. | What are the corresponding nodes in each twin pair, including both identical and homonymous node pairs? |
| `homonymousNodePairsPerTwinPair` | One homonymous node-pair entry from a resolved twin pair, with its normalized name, role, both canonical IDs and the forward-flow flag. Repeated entries remain. | Which different nodes share the same normalized name, and in which twin pairs and roles were they found? |
| `twinPairCountPerHomonymousNodePair` | One distinct homonymous node pair, its normalized name and `nonIdenticalTwinPairCount`. | Which node pairs need investigation, and how many distinct resolved non-identical twin pairs contain each? |
| `twinPairsWithMultipleNameMatchedNodePairTuples` | One non-identical twin pair's canonical IDs and its two alternative name-matched node pair tuples. | Which established twin pairs remain unresolved, and what are the alternatives? |

The column order is part of the output contract:

```text
canonicalRelations
  canonicalId, relationship_type, direction,
  node1_urn, node1_name, node2_urn, node2_name, sources

nonIdenticalTwinPairs
  forwardCanonicalId, backfillCanonicalId,
  nonIdenticalTwinsAlsoWithinForwardFlow, forwardTwin, backfillTwin

forwardTwin and backfillTwin structs
  canonicalId, relationship_type, direction,
  node1_urn, node1_name, node2_urn, node2_name, sources, comparisonKey
  comparisonKey: relationship_type, nodeNames

twinPairsWithNameMatchedNodePairTuples
  forwardCanonicalId, backfillCanonicalId,
  nonIdenticalTwinsAlsoWithinForwardFlow, forwardTwin, backfillTwin,
  nameMatchedNodePairTuples

nameMatchedNodePairTuples
  array of one or two complete results
    each result: array of exactly two structs
      role, forwardNodeUri, backfillNodeUri, matchedNodeName

homonymousNodePairsPerTwinPair
  forwardNodeUri, backfillNodeUri, matchedNodeName, role,
  forwardCanonicalId, backfillCanonicalId,
  nonIdenticalTwinsAlsoWithinForwardFlow

twinPairCountPerHomonymousNodePair
  forwardNodeUri, backfillNodeUri, matchedNodeName,
  nonIdenticalTwinPairCount

twinPairsWithMultipleNameMatchedNodePairTuples
  forwardCanonicalId, backfillCanonicalId, nameMatchedNodePairTuples
```

Identifiers, names and roles are strings; `sources` and `comparisonKey.nodeNames` are arrays of strings. The flag is Boolean and `nonIdenticalTwinPairCount` is Long. The table for unresolved twin pairs retains the alternatives; the full twin-pair table also retains its flag and both canonical relations.

## Real examples

These four historical detail entries illustrate the relationship between homonymous node pairs and twin pairs:

| forwardNodeUri | backfillNodeUri | matchedNodeName | role | forwardCanonicalId | backfillCanonicalId | nonIdenticalTwinsAlsoWithinForwardFlow |
|---|---|---|---|---|---|---|
| biodata:smallmol/Concept_700527820 | biodata:smallmol/Concept_86446482 | luteolin | regulator | biodata:regulation/regulator/Concept_700527820/target/Concept_86432034 | biodata:regulation/regulator/Concept_86446482/target/Concept_86432034 | true |
| biodata:smallmol/Concept_700527820 | biodata:smallmol/Concept_86446482 | luteolin | regulator | biodata:regulation/regulator/Concept_700527820/target/Concept_86458342 | biodata:regulation/regulator/Concept_86446482/target/Concept_86458342 | false |
| biodata:smallmol/Concept_700526633 | biodata:smallmol/Concept_86396600 | dorzolamide | regulator | biodata:directregulation/regulator/Concept_700526633/target/EC_4.2.1.1 | biodata:directregulation/regulator/Concept_86396600/target/PROTYP601 | false |
| biodata:functionalclass/EC_4.2.1.1 | biodata:functionalclass/PROTYP601 | carbonic anhydrase | target | biodata:directregulation/regulator/Concept_700526633/target/EC_4.2.1.1 | biodata:directregulation/regulator/Concept_86396600/target/PROTYP601 | false |

The two luteolin entries link one homonymous node pair to two twin pairs. The dorzolamide and carbonic-anhydrase entries link two homonymous node pairs to one twin pair. Aggregating these four entries produces:

| forwardNodeUri | backfillNodeUri | matchedNodeName | nonIdenticalTwinPairCount |
|---|---|---|---|
| biodata:smallmol/Concept_700527820 | biodata:smallmol/Concept_86446482 | luteolin | 2 |
| biodata:smallmol/Concept_700526633 | biodata:smallmol/Concept_86396600 | dorzolamide | 1 |
| biodata:functionalclass/EC_4.2.1.1 | biodata:functionalclass/PROTYP601 | carbonic anhydrase | 1 |

## Code organization

| File | Responsibility |
|---|---|
| `DataTypes.scala` | Behavior-free run configuration, constants and dataset names |
| `DataOps.scala` | Native Spark expressions for canonical identity, twin matching and domain node-pair correspondence |
| `IOOps.scala` | Local Spark/S3 configuration and sequential Delta persistence |
| `TwinSearchApp.scala` | Pipeline orchestration and Spark shutdown |
| `StepFiveChecks.scala` | Explicit fixtures and read-only regression against saved outputs |
| `ExploreApp.scala` | Ad hoc inspection of occurrence schemas and current twin counts |

Pure operations live in the operations modules; data declarations remain behavior-free. Transformations use built-in Spark expressions.

## Validation and check commands

Historical validation on 30 October 2026 passed 20 explicit twin-pair fixtures, equivalence with the then-present deprecated implementation, source selections, empty/unresolved subsets, schemas and distinct twin-pair counts. That suite checked this implementation's forward-position order and the deprecated implementation's order by node URN separately.

The `2026-10-31` rerun completed with a 4 GiB heap and exit status 0 at 18:13 UTC. All six Delta tables committed beneath `s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/BIOMED-3471/2026-10-31/` with these verified row counts:

| Stored table | Committed row count |
|---|---:|
| `canonicalRelations` | 15,060,586 |
| `nonIdenticalTwinPairs` | 1,911 |
| `twinPairsWithNameMatchedNodePairTuples` | 1,911 |
| `homonymousNodePairsPerTwinPair` | 1,942 |
| `twinPairCountPerHomonymousNodePair` | 157 |
| `twinPairsWithMultipleNameMatchedNodePairTuples` | 0 |

Verification of the new Delta commits confirmed these row counts and schemas matching the prior run. Every referenced data file was present with its recorded size. The old detail and summary output prefixes were absent, and the input Delta log was unchanged. No fixture suite or saved-output regression was run for this rerun.

```zsh
cd /Users/maatari/Dev/IdeaProjects/spark-playground
scala compile .
scala run . -M biomed3471.codex.StepFiveChecks --java-opt=-Xmx4g

# Optional read-only regression against the completed run:
scala run . -M biomed3471.codex.StepFiveChecks --java-opt=-Xmx4g -- \
  --baseline-run 2026-10-31
```

With no arguments, `StepFiveChecks` runs the local fixtures, comparing the implementation's results with explicit expectations for each twin pair. `--baseline-run YYYY-MM-DD` also runs read-only regression against the saved output for that date in the same Spark session.

The suite covers complete tuple contents, including repeated records, retained canonical relations and flags, roles, exact field/type/column order, selected detailed entries, unresolved alternatives and distinct twin-pair counts. Fixtures cover all four undirected cases, directed roles, self-reference on either or both sides, every shared-node position, name matching across capitalization and leading/trailing spaces and node-URN identity across node types.

Saved-output regression reads the twin-pair and Step 5 tables and compares tuple contents, metadata, detail multiplicities and counts. The fixtures cover unresolved twin pairs, including their two alternatives and exclusion from selected detail.

The repository's existing `project.scala` supplies Scala, Spark, Delta, Hadoop/AWS and JVM module options.

## Full run

From the same project root, pass the run date as the single positional application argument. The completed `2026-10-31` run used:

```zsh
AWS_PROFILE=aws-h-entellect-nonprod \
  scala run . -M biomed3471.codex.TwinSearchApp --java-opt=-Xmx4g -- \
  2026-10-31
```

`TwinSearchApp` requires a date in `YYYY-MM-DD` format. `outputPath` appends it to the current output prefix. Existing date prefixes, including partial runs, are refused; they are not overwritten and no timestamp suffix is added.

The fixed input is:

```text
s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/BIOMED-3471/wytze-relation-occurrences/
```

This Delta source contains Wytze’s complete original export with its 16 columns unchanged and is separate from the six analysis outputs.

Scratch is created automatically at `<java.io.tmpdir>/biomed3471-codex/YYYY-MM-DD/`. It contains Spark spill and disk-backed S3 upload buffers. The program does not delete previous scratch data.

The launch option sets a 4 GiB Java heap. Spark uses `local[*]`, a 32 MiB file-scan partition limit, 256 initial shuffle partitions, 32 MiB AQE advisory partitions, disabled broadcast joins and disk spill. The file-scan limit was added to reduce per-task aggregation memory after the previous 4 GiB attempt ran out of heap during canonical-relation aggregation. S3 uploads use disk buffers with one active block per stream.

S3 access reuses the existing S3A configuration and `software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider`. Refresh an expired non-PROD session using `~/aws-login-nonprod`.

Writes run sequentially with `errorifexists`. Reading the saved intermediate tables lets later steps use those results without repeating the earlier processing. `_STARTED` records the input and start time. Verify completion from exit status and all six Delta commits. The success message is printed only after the writes and Spark shutdown succeed.

