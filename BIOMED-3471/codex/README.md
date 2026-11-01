# BIOMED-3471: twin search with direct domain rules

A **canonical relation** is one relation type between two specific nodes in their roles. A **relation occurrence** is one occurrence of that canonical relation in a document, produced by one pipeline. A node's **normalized name** is its name in lower case with spaces at both ends removed: `lower(trim(name))`.

**Non-identical twins** are canonical relations with different canonical relation IDs, the same relation type and matching normalized node names: by regulator/target role for directed relations, or the same two partner names for undirected relations. The twin search compares each **forward-only canonical relation**, whose occurrences all come from the forward flow, with the backfill's canonical relations. Each match is a **twin pair**. The backfill twin may also have forward-flow relation occurrences.

Step 5 finds **homonymous node pairs**—different nodes with the same normalized name—within the twin pairs found by Step 4. It uses roles, normalized names and node identity to determine the name-matched node pairs, then selects the homonymous pairs while retaining the twin pair they came from.

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

Step 5 identifies name-matched node pairs within each established non-identical twin pair. The relations have the same type and direction, and the same normalized names, each appearing the same number of times in both relations. Their canonical IDs differ.

A **name-matched node pair** contains one forward-flow node and one backfill node with the same normalized name. It is a **homonymous node pair** when their node URNs differ, and an **identical node pair** when the node URN is the same on both sides.

### Directed twins: pair nodes by role

Pair the forward-flow regulator with the backfill regulator, and the forward-flow target with the backfill target. Step 4 guarantees that the two regulators have the same normalized name, and that the two targets have the same normalized name.

### Undirected twins: why roles and positions cannot decide

For an undirected relation such as Binding, both nodes have the role `partner`, so matching by role alone does not determine the node pairs. Comparing nodes at the same position in their canonical relation IDs cannot determine them either.

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

The two normalized names **within each relation** determine how to proceed. When they differ, each name identifies exactly one node in each twin. When they are the same, examine whether a node is shared or either relation is self-referencing.

The twins have matching names, each appearing the same number of times, so both relations fall into the same name category:

| Names within each relation | Forward-flow names | Backfill names | How to proceed |
|---|---|---|---|
| Two different normalized names | collagen, mia3 | collagen, mia3 | Match by name |
| One normalized name repeated twice | egfr, egfr | egfr, egfr | Examine node identity |

#### Two different normalized names

Each normalized name occurs once in each relation. Pair each forward-flow node with the backfill node that has the same normalized name. In the collagen–MIA3 example above, each collagen node has only one possible collagen match, and each MIA3 node has only one possible MIA3 match.

#### One normalized name repeated twice

Both partners in both relations have the same normalized name. The following cases use node identity to determine which pairs to record.

Here, we need to distinguish a node appearing in both relations from a node appearing twice in one relation:

- A **shared node** appears in both the forward-flow relation and its backfill twin, with the same node URN.
- A **self-referencing relation** contains the same node twice, identified by its node URN.

In the examples below, `E1`, `E2`, `E3` and `E4` stand for distinct node URNs, all named EGFR. `E1 binds E2` uses two distinct nodes with the same name; `E1 binds E1` uses the same node twice and is self-referencing. Equal names alone establish neither self-reference nor a shared node.

**A node is shared between the relations.** Pair that node with itself. That accounts for one use of the node in each relation; pair the remaining forward-flow node with the remaining backfill node. For `E1 binds E2` versus `E2 binds E3`, pair `E2` with `E2`, and `E1` with `E3`.

This case includes a self-referencing relation that shares its repeated node with its twin. For `E1 binds E1` versus `E1 binds E2`, pair `E1` with `E1`, and the second use of forward-flow `E1` with backfill `E2`.

**No node is shared, and at least one relation is self-referencing.** Pair each partner of the other relation with the repeated node:

| Forward-flow relation | Backfill relation | Name-matched node pairs |
|---|---|---|
| E1 binds E1 | E2 binds E3 | E1 with E2; E1 with E3 |
| E1 binds E2 | E3 binds E3 | E1 with E3; E2 with E3 |
| E1 binds E1 | E2 binds E2 | E1 with E2, twice |

In the last example, keep both pairs with identical contents: each relation uses its node twice. With no shared node, every node pair in these results is homonymous.

**No node is shared, and each relation contains two distinct nodes.** Pair each forward-flow node with both backfill nodes. For `E1 binds E2` versus `E3 binds E4`, this gives all four pairs:

| Forward-flow node | Backfill node |
|---|---|
| E1 | E3 |
| E2 | E4 |
| E1 | E4 |
| E2 | E3 |

All four pairs are homonymous: their normalized names agree and their node URNs differ.

## From domain rules to implementation

### Why the code checks self-reference early

The undirected implementation handles a self-referencing relation before testing names or shared identity. A repeated node has one normalized name; the twins' matching name keys therefore imply that both partners in both relations have that same name. Each partner of the other relation goes with the repeated node, which determines the two name-matched node pairs immediately.

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
            Pair each forward-flow node with both backfill nodes.
```

The implementation defines straight and crossed arrays of node pairs once. The straight array pairs forward nodes 1 and 2 with backfill nodes 1 and 2; the crossed array pairs them with backfill nodes 2 and 1. The `when` branches select the required array, or concatenate both arrays for four distinct nodes with the same normalized name.

### Column mapping and stored order

The `forwardTwin` and `backfillTwin` structs retain `node1_urn`, `node1_name`, `node2_urn` and `node2_name`. Their columns follow canonical relation order: regulator then target for directed relations, and lexicographic node TERMite ID order for undirected partners. Canonical-ID reconstruction uses that existing order directly.

A two-record result lists the pair for forward-flow node 1 first and the pair for forward-flow node 2 second. Directed results therefore list the regulator before the target.

The self-reference branch selects the straight array, which includes the identical node pair whenever a node is shared. These examples list the relation nodes in canonical position order:

| Branch | Forward-flow relation | Backfill twin | Stored node pairs, in order |
|---|---|---|---|
| Self-reference | E2 binds E2 | E1 binds E2 | E2 with E1; E2 with E2 |
| Shared node | E1 binds E2 | E2 binds E3 | E1 with E3; E2 with E2 |

The shared-node rule determines the identical node pair first, but storage follows forward-flow position order. A four-record result stores the straight pairs followed by the crossed pairs: forward 1 with backfill 1, forward 2 with backfill 2, forward 1 with backfill 2, and forward 2 with backfill 1.

Construction supplies this deterministic order. Each record keeps the forward-flow and backfill nodes in their respective fields.

### Retain name-matched node pairs, then select detail

Keep every node pair produced by the rules, including identical node pairs and repeated records. If a relation uses the same node twice, retain both uses.

A twin pair's `nameMatchedNodePairs` is one flat array of records with fields `role`, `forwardNodeUri`, `backfillNodeUri` and `matchedNodeName`. It contains four records when an undirected twin pair has four distinct nodes with the same normalized name, and two records in every other case.

`twinPairsWithNameMatchedNodePairs` retains each twin pair, including both canonical relations, with its result. `homonymousNodePairsPerTwinPair` selects every record whose node URNs differ. Identical node pairs remain in the full result. The four-distinct-node case contributes all four pairs to the detail. When both relations are self-referencing, both records remain in the detail: directed records retain their different roles, while undirected records have identical contents.

### Count distinct twin pairs for each homonymous node pair

`twinPairCountPerHomonymousNodePair` groups by `forwardNodeUri`, `backfillNodeUri` and `matchedNodeName`, then counts distinct twin pairs using both `forwardCanonicalId` and `backfillCanonicalId`. Each node has one name, so the forward-flow/backfill node URN pair already fixes the normalized name. Including that name in the grouping retains it directly without splitting a node-pair group or picking a name from one row. Repeated entries from one twin pair contribute once to that node pair's count. Different twin pairs sharing one canonical relation ID still count separately. Role and the forward-flow flag remain in the detail and do not split these groups.

## Saved-table flow

```text
canonicalRelations [write/read]
  -> nonIdenticalTwinPairs [write/read]
      -> twinPairsWithNameMatchedNodePairs [write/read]
          -> homonymousNodePairsPerTwinPair [write/read]
              -> twinPairCountPerHomonymousNodePair [write/read]
```

## Five saved Delta tables

Each run uses a date in `YYYY-MM-DD` format. Each table has its own directory:

```text
s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/BIOMED-3471/YYYY-MM-DD/<table>/
```

| Table | What one row contains | What the table tells us |
|---|---|---|
| `canonicalRelations` | One canonical relation, with its node fields and the set of sources of its relation occurrences. | Which canonical relations occur in the forward flow, the backfill, or both? |
| `nonIdenticalTwinPairs` | One non-identical twin pair, retaining both canonical relations and the flag `nonIdenticalTwinsAlsoWithinForwardFlow`. | Which forward-only canonical relations have non-identical twins in backfill? |
| `twinPairsWithNameMatchedNodePairs` | One twin pair with a flat array of two or four name-matched node-pair records, including identical pairs and repeated records. | Which name-matched node pairs do the rules identify in each twin pair? |
| `homonymousNodePairsPerTwinPair` | One homonymous node-pair entry, with its normalized name, role, both canonical IDs and the forward-flow flag. Repeated entries remain. | Which different nodes share the same normalized name, and in which twin pairs and roles were they found? |
| `twinPairCountPerHomonymousNodePair` | One distinct homonymous node pair, its normalized name and `nonIdenticalTwinPairCount`. | Which node pairs need investigation, and how many distinct non-identical twin pairs contain each? |

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

twinPairsWithNameMatchedNodePairs
  forwardCanonicalId, backfillCanonicalId,
  nonIdenticalTwinsAlsoWithinForwardFlow, forwardTwin, backfillTwin,
  nameMatchedNodePairs

nameMatchedNodePairs
  array of two or four structs
    role, forwardNodeUri, backfillNodeUri, matchedNodeName

homonymousNodePairsPerTwinPair
  forwardNodeUri, backfillNodeUri, matchedNodeName, role,
  forwardCanonicalId, backfillCanonicalId,
  nonIdenticalTwinsAlsoWithinForwardFlow

twinPairCountPerHomonymousNodePair
  forwardNodeUri, backfillNodeUri, matchedNodeName,
  nonIdenticalTwinPairCount
```

Identifiers, names and roles are strings; `sources` and `comparisonKey.nodeNames` are arrays of strings. The flag is Boolean and `nonIdenticalTwinPairCount` is Long.

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
| `DataTypes.scala` | Pipeline source names |
| `DataOps.scala` | Native Spark expressions for canonical identity, twin matching and name-matched node pairs |
| `TwinSearchApp.scala` | Input and output paths, Spark session, run reservation, Delta write/read helper, pipeline orchestration and Spark shutdown |
| `StepFiveChecks.scala` | Explicit fixtures and read-only regression against saved outputs |
| `ExploreApp.scala` | Ad hoc inspection of occurrence schemas and current twin counts |

Pure operations live in the operations modules; data declarations remain behavior-free. Transformations use built-in Spark expressions.

## Historical validation

Historical validation on 30 October 2026 passed 20 explicit twin-pair fixtures, equivalence with the then-present deprecated implementation, source selections, empty/unresolved subsets, schemas and distinct twin-pair counts. That suite checked this implementation's forward-position order and the deprecated implementation's order by node URN separately.

The `2026-10-31` rerun predates the flat `nameMatchedNodePairs` result and five-table layout. It completed with a 4 GiB heap and exit status 0 at 18:34 UTC, taking about 6 minutes 22 seconds. It used the simplified session settings described below: Delta/S3 integration, `local[*]`, eight shuffle partitions and defaults for everything else. Its six Delta tables committed beneath `s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/BIOMED-3471/2026-10-31/` with these verified row counts:

| Stored table | Committed row count |
|---|---:|
| `canonicalRelations` | 15,060,586 |
| `nonIdenticalTwinPairs` | 1,911 |
| `twinPairsWithNameMatchedNodePairTuples` | 1,911 |
| `homonymousNodePairsPerTwinPair` | 1,942 |
| `twinPairCountPerHomonymousNodePair` | 157 |
| `twinPairsWithMultipleNameMatchedNodePairTuples` | 0 |

Verification of that run's Delta commits confirmed these row counts and schemas matching the prior run. Every referenced data file was present with its recorded size. The old detail and summary output prefixes were absent, and the input Delta log was unchanged. No fixture suite or saved-output regression was run for this rerun. This evidence predates the persistence refactor below and the flat node-pair result.

## Check commands

```zsh
cd /Users/maatari/Dev/IdeaProjects/spark-playground
scala compile .
scala run . -M biomed3471.codex.StepFiveChecks --java-opt=-Xmx4g
```

With no arguments, `StepFiveChecks` runs the local fixtures, comparing the implementation's results with explicit expectations for each twin pair.

The suite covers flat arrays of two or four node-pair records, repeated records, retained canonical relations and flags, roles, stored order, exact field/type/column order, detailed entries and distinct twin-pair counts. Fixtures cover all four undirected cases, directed roles, self-reference on either or both sides, every shared-node position, name matching across capitalization and leading/trailing spaces and node-URN identity across node types. The four-distinct-node cases check all four pairs and their inclusion in the detail and counts.

For saved-output regression, replace `YYYY-MM-DD` below with the date of a completed run using the flat `nameMatchedNodePairs` result and five-table layout. The historical `2026-10-31` run uses the earlier format.

```zsh
scala run . -M biomed3471.codex.StepFiveChecks --java-opt=-Xmx4g -- \
  --baseline-run YYYY-MM-DD
```

This runs the fixtures and read-only regression in the same Spark session. Regression reads the twin-pair and Step 5 tables and compares node-pair contents, metadata, detail multiplicities and counts.

The repository's existing `project.scala` supplies Scala, Spark, Delta, Hadoop/AWS and JVM module options.

## Full run

From the same project root, pass an unused run date as the single positional application argument. Replace `YYYY-MM-DD` below with that date:

```zsh
AWS_PROFILE=aws-h-entellect-nonprod \
  scala run . -M biomed3471.codex.TwinSearchApp --java-opt=-Xmx4g -- \
  YYYY-MM-DD
```

`TwinSearchApp` reads the run date from `args(0)` and validates it with `LocalDate.parse`. It declares `relationOccurrencePath`, `runPath` and the five output table paths directly before creating Spark. Each output table path is beneath `s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/BIOMED-3471/YYYY-MM-DD/`.

`TwinSearchApp` also contains `makeSparkSession`, `writeAndReadBackDeltaTable` and `beginRun`. Before the pipeline starts, `beginRun(spark, runPath, relationOccurrencePath)` rejects an existing run prefix and writes an `_STARTED` marker with the input path and start time. Rerunning an existing date requires explicit cleanup of that dated output prefix first.

The fixed input is:

```text
s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/BIOMED-3471/wytze-relation-occurrences/
```

This Delta source contains Wytze’s complete original export with its 16 columns unchanged and is separate from the five analysis outputs.

The launch option sets a 4 GiB Java heap. The parameterless `makeSparkSession` uses a fixed Codex application name, Delta integration, `local[*]` and eight shuffle partitions. Apart from the Delta and S3 integration settings, all other Spark and Hadoop settings use their defaults.

S3 access reuses the existing S3A configuration and `software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider`. Refresh an expired non-PROD session using `~/aws-login-nonprod`.

All five outputs use `writeAndReadBackDeltaTable(data, tablePath)`. It writes Delta with `SaveMode.Overwrite` and `overwriteSchema=true`, then returns `spark.read.delta(tablePath)`. The initial `beginRun` guard requires a fresh dated prefix before these writes begin.

Writes run sequentially. Reading the saved intermediate tables lets later steps use those results without repeating the earlier processing. Verify completion from exit status and all five Delta commits. The success message is printed only after the writes and Spark shutdown succeed.

