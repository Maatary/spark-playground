# BIOMED-3471: phase one

Phase One finds the non-identical twins of forward-only canonical relations in backfill, then identifies the homonymous node pairs to investigate. The backfill search includes both `backfillOnly` and `shared`.

Steps 1–5 start from the full relation-occurrence export, stored as a shared Delta source in S3, reconstruct the omitted `canonicalId`, aggregate canonical relations and their source sets, find the non-identical twins, then identify their corresponding nodes.

The starting selection is `forwardOnly`; the backfill search is `backfillOnly.union(shared)`. `forwardOnly`, `backfillOnly` and `shared` are selections of canonical relations, not saved tables. The comparison key contains relation type and normalized node names. Relation type determines directedness: directed names retain regulator/target order; undirected names are sorted in the key. The stored node fields retain the model's ordering, with each name attached to its node URI. Undirected partners are ordered lexicographically by their raw TERMite IDs within each relation.

`normalizedNodeName` defines the shared `lower(trim(name))` transformation for the comparison key, undirected position tests and `matchedNodeName`. Original graph names remain available in the retained canonical relations.

`nonIdenticalTwinsAlsoWithinForwardFlow` is true when the backfill twin belongs to `shared`: both canonical relations in this twin pair also occur within forward flow. It is false when the backfill twin belongs to `backfillOnly`. The flag belongs to a twin pair; it neither changes the node correspondence nor forms part of global node-pair identity.

Model references: [Identifier Schemes](https://elsevier.atlassian.net/wiki/spaces/ELSSIE/pages/119601216791062), [Graph Data Model](https://elsevier.atlassian.net/wiki/spaces/ELSSIE/pages/119601199850466), [CPLD to Graph Events](https://elsevier.atlassian.net/wiki/spaces/ELSSIE/pages/119601220912178).

## From twins to node pairs

The [full four-case explanation of the original approach](step-5-original-approach.md) is preserved for comparison with the new approach.

To identify **homonymous node pairs**—nodes that share the same name but have different identifiers—we need to determine which node in the forward-flow relation should be compared with which node in the backfill relation. We call the result of this determination a **node pairing**.

Step 4 has already established the non-identical twin pairs. Step 5 reads `nonIdenticalTwinPairs`, with one row per twin pair. Its `forward` and `backfill` structs each retain `node1_urn`, `node1_name`, `node2_urn` and `node2_name`. These are the fields used to determine the node pairing within that twin.

Each relation has exactly two positions. A complete node pairing contains two node pairs, using each position in each relation exactly once.

For directed twins, compare regulator with regulator and target with target. Step 4 already guarantees that the names match in those roles, so Step 5 constructs the complete tuple directly.

For an undirected relation such as Binding, both nodes have the role `partner`. The first and second positions do not indicate different roles in the relation. They follow the BioMed graph’s convention of ordering partners lexicographically by their raw TERMite IDs.

**When the two twins use different identifiers for a matching name, that name can move from the first position to the second.**

The collagen–MIA3 twin pair shows this. This table illustrates the partner positions in each relation:

| Flow | Position 1 | Position 2 |
|---|---|---|
| Forward flow | collagen — `Concept_86378312` | MIA3 — `HGNC24008` |
| Backfill | MIA3 — `HGNC24008` | collagen — `MSCAN_12001170` |

The MIA3 identifier is the same in both relations. The collagen identifiers differ:

- `Concept_86378312` sorts **before** `HGNC24008`, placing collagen first in the forward-flow relation’s canonical URI.
- `MSCAN_12001170` sorts **after** `HGNC24008`, placing collagen second in the backfill relation’s canonical URI.

The two relations have the same partner names, but those names occupy opposite positions. Comparing position 1 with position 1 would compare collagen with MIA3. The node pairing therefore compares:

- **Collagen:** forward position 1 with backfill position 2.
- **MIA3:** forward position 2 with backfill position 1.

The two collagen nodes form the homonymous node pair; the MIA3 node pairs with itself.

Step 4 sorted names in its comparison key; it did not rearrange the original node fields. Forward position 1 can be paired with either backfill position. Once that choice is made, forward position 2 must pair with the other backfill position, because each position is used once.

A `positionPair` records one forward position paired with one backfill position. A `positionPairTuple` contains two such pairs, covering both positions on both sides. The Scala types `PositionPair` and `PositionPairTuple` hold these position choices locally while constructing Spark expressions; positions are not saved as node-pair fields.

**There are exactly two ways to pair those positions:**

| `positionPairTuple` | First position pair | Second position pair |
|---|---|---|
| `straightPositionPairTuple` | Forward position 1 with backfill position 1 | Forward position 2 with backfill position 2 |
| `crossedPositionPairTuple` | Forward position 1 with backfill position 2 | Forward position 2 with backfill position 1 |

These position choices identify the node fields to read from `forward` and `backfill`. When the names match in both node pairs, ignoring case and leading/trailing spaces, they form a possible complete `nameMatchedNodePairTuple`. A `nameMatchedNodePair` contains `role`, `forwardNodeUri`, `backfillNodeUri` and `matchedNodeName`. The matching normalized name is kept in `matchedNodeName`.

Equal full URIs establish an **identical name match**; different full URIs establish a **homonymous node pair**. Both kinds remain in the complete tuple. The tuple is a Spark array of exactly two structs.

When the two partners have the same name and the twins share a node, names alone allow both position pairings. **Pair the shared node with itself. Then pair the nodes at the other position in each relation.** The first comparison establishes an identical name match. It accounts for exactly one position in each relation, leaving exactly one other position on each side. The nodes at those other positions have matching names and different identifiers: they form the homonymous node pair being sought. The identical name match remains in the complete tuple.

The implementation uses `countIdenticalNodePairs` to identify the complete tuples that pair a shared node with itself. In a non-identical undirected twin pair, a complete tuple cannot contain two identical name matches: the relations already have the same relation type, so the same two nodes would make them the same canonical relation.

A **self-referencing relation** uses the same node in both partner positions. When either twin is self-referencing, straight and crossed position pairing can produce the same node-pair records in a different listing order. Put each complete tuple into a consistent order, then remove equivalent **whole tuples**. When both twins are self-referencing, each uses the same node in both partner positions, so the complete tuple contains two records with identical contents. Retain both because they account for both partner positions. Preserve forward/backfill orientation inside every record. A directed record's `role` remains attached; its array index does not define its role.

Step 5 stores the remaining complete results in `nameMatchedNodePairTuples`, in `twinPairsWithNameMatchedNodePairTuples`. That table still has one row per twin pair, with both relations attached:

| Contents of `nameMatchedNodePairTuples` in one twin row | Meaning |
|---|---|
| One complete tuple containing two node-pair records | One complete node pairing has been determined. |
| Two alternative tuples, each containing two node-pair records | Two different complete node pairings remain; neither has been selected. |

Two records inside one tuple describe the two comparisons. Two tuples describe alternative complete results for the same twin.

For twins with one complete result, `homonymousNodePairs` selects the records whose full URIs differ, retaining their roles, both canonical IDs and the flag. When both twins are self-referencing relations, both resulting records remain: each accounts for a position in each relation. Identical name matches remain available in the complete tuple. Twins with two alternatives remain in the full result table and `twinPairsWithMultipleNameMatchedNodePairTuples`; they contribute no selected records to `homonymousNodePairs`.

The global `homonymousNodePairsWithTwinCount` table groups the selected entries by the oriented URI pair, retains its fixed matching name, and counts distinct **complete** `(forwardCanonicalId, backfillCanonicalId)` pairs. A twin contributes once to a node pair's count, including when both positions produce the same node pair. Different twins sharing one canonical ID still count separately.

### Conceptual procedure for undirected relations

This procedure determines the homonymous node pairs within an established non-identical twin pair of any undirected relation type. Binding is one example. Names are compared ignoring case and leading/trailing spaces; node identity is compared using full URIs.

```text
Start with one established undirected non-identical twin pair.

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
```

For a twin with one complete tuple, select its records with different full URIs into `homonymousNodePairs`. Identical name matches remain in the complete tuple. For a twin with two alternatives, retain both in `nameMatchedNodePairTuples` and `twinPairsWithMultipleNameMatchedNodePairTuples`, without selecting entries into `homonymousNodePairs` or its count summary.

### Saved-table flow

The saved tables follow this flow:

```text
canonicalRelations [write/read]
  -> nonIdenticalTwinPairs [write/read]
      -> twinPairsWithNameMatchedNodePairTuples [write/read]
          -> homonymousNodePairs [write/read]
              -> homonymousNodePairsWithTwinCount [write]
          -> twinPairsWithMultipleNameMatchedNodePairTuples [write]
```

## Six saved Delta tables

All tables are beneath:

```text
s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/BIOMED-3471/<run>/
```

| Table | What one row contains | What the table tells us |
|---|---|---|
| `canonicalRelations` | One canonical relation, with its node fields and the set of sources of its relation occurrences. | Which canonical relations occur in the forward flow, the backfill, or both? |
| `nonIdenticalTwinPairs` | One non-identical twin pair, retaining both canonical relations and the flag `nonIdenticalTwinsAlsoWithinForwardFlow`. | Which forward-only canonical relations have non-identical twins in backfill? |
| `twinPairsWithNameMatchedNodePairTuples` | One twin pair with its complete name-matched node-pair results: one tuple when resolved, or two alternative tuples when unresolved. Each tuple contains two node pairs. | What are the corresponding nodes in each twin pair, including both identical and homonymous node pairs? |
| `homonymousNodePairs` | One homonymous node-pair entry from a resolved twin, with its matching name, role, both canonical IDs and the forward-flow flag. Repeated entries remain. | Which different nodes share the same name, and in which twins and roles were they found? |
| `homonymousNodePairsWithTwinCount` | One distinct homonymous node pair, its matching name and `nonIdenticalTwinPairCount`. | Which node pairs need investigation, and how many distinct resolved non-identical twin pairs contain each? |
| `twinPairsWithMultipleNameMatchedNodePairTuples` | One non-identical twin pair's canonical IDs and its two alternative complete node-pair tuples. | For which established twins does the node correspondence remain unresolved, and what are the alternatives? |

The column order is part of the output contract:

```text
canonicalRelations
  canonicalId, relationship_type, direction,
  node1_urn, node1_name, node2_urn, node2_name, sources

nonIdenticalTwinPairs
  forwardCanonicalId, backfillCanonicalId,
  nonIdenticalTwinsAlsoWithinForwardFlow, forward, backfill

forward and backfill structs
  canonicalId, relationship_type, direction,
  node1_urn, node1_name, node2_urn, node2_name, sources, comparisonKey
  comparisonKey: relationship_type, nodeNames

twinPairsWithNameMatchedNodePairTuples
  forwardCanonicalId, backfillCanonicalId,
  nonIdenticalTwinsAlsoWithinForwardFlow, forward, backfill,
  nameMatchedNodePairTuples

nameMatchedNodePairTuples
  array of one or two complete results
    each result: array of exactly two structs
      role, forwardNodeUri, backfillNodeUri, matchedNodeName

homonymousNodePairs
  forwardNodeUri, backfillNodeUri, matchedNodeName, role,
  forwardCanonicalId, backfillCanonicalId,
  nonIdenticalTwinsAlsoWithinForwardFlow

homonymousNodePairsWithTwinCount
  forwardNodeUri, backfillNodeUri, matchedNodeName,
  nonIdenticalTwinPairCount

twinPairsWithMultipleNameMatchedNodePairTuples
  forwardCanonicalId, backfillCanonicalId, nameMatchedNodePairTuples
```

Identifiers, names and roles are strings; `sources` and `comparisonKey.nodeNames` are arrays of strings. The flag is Boolean and `nonIdenticalTwinPairCount` is Long. The unresolved projection retains the alternatives; the full twin table also retains its flag and both canonical relations.

## Real examples

These four entries are the previously verified historical examples, shown in the final detail column order:

| forwardNodeUri | backfillNodeUri | matchedNodeName | role | forwardCanonicalId | backfillCanonicalId | nonIdenticalTwinsAlsoWithinForwardFlow |
|---|---|---|---|---|---|---|
| biodata:smallmol/Concept_700527820 | biodata:smallmol/Concept_86446482 | luteolin | regulator | biodata:regulation/regulator/Concept_700527820/target/Concept_86432034 | biodata:regulation/regulator/Concept_86446482/target/Concept_86432034 | true |
| biodata:smallmol/Concept_700527820 | biodata:smallmol/Concept_86446482 | luteolin | regulator | biodata:regulation/regulator/Concept_700527820/target/Concept_86458342 | biodata:regulation/regulator/Concept_86446482/target/Concept_86458342 | false |
| biodata:smallmol/Concept_700526633 | biodata:smallmol/Concept_86396600 | dorzolamide | regulator | biodata:directregulation/regulator/Concept_700526633/target/EC_4.2.1.1 | biodata:directregulation/regulator/Concept_86396600/target/PROTYP601 | false |
| biodata:functionalclass/EC_4.2.1.1 | biodata:functionalclass/PROTYP601 | carbonic anhydrase | target | biodata:directregulation/regulator/Concept_700526633/target/EC_4.2.1.1 | biodata:directregulation/regulator/Concept_86396600/target/PROTYP601 | false |

The two luteolin entries link one node pair to two twins. The dorzolamide and carbonic-anhydrase entries link two different node pairs to one twin. Aggregating just these four fixture entries produces counts 2, 1 and 1. Aggregating all saved detail gives:

| forwardNodeUri | backfillNodeUri | matchedNodeName | nonIdenticalTwinPairCount |
|---|---|---|---|
| biodata:smallmol/Concept_700527820 | biodata:smallmol/Concept_86446482 | luteolin | 66 |
| biodata:smallmol/Concept_700526633 | biodata:smallmol/Concept_86396600 | dorzolamide | 2 |
| biodata:functionalclass/EC_4.2.1.1 | biodata:functionalclass/PROTYP601 | carbonic anhydrase | 16 |

## Code organization

| File | Responsibility |
|---|---|
| `DataTypes.scala` | Behavior-free configuration/source groups, position metadata, inactive monitor state and constants |
| `DataOps.scala` | Native Spark expressions for canonical identity, grouping, twin matching and complete node-pair results |
| `IOOps.scala` | Argument parsing, local Spark/S3 configuration and sequential Delta persistence |
| `PhaseOneApp.scala` | Pipeline orchestration and Spark shutdown |
| `StepFiveChecks.scala` | Small fixtures and read-only regression against saved historical Step 5 outputs |
| `RuntimeOps.scala` | Existing optional JVM checks/monitoring; not invoked by the application |

Pure operations live in the operations modules; data declarations remain behavior-free. Transformations use built-in Spark expressions, without UDFs or distributed `Dataset.map`. No full input dataset is collected to the driver or cached in memory.

## Compile and check from the project root

```zsh
cd /Users/maatari/Dev/IdeaProjects/spark-playground
scala compile .
scala run . -M biomed3471.codex.StepFiveChecks --java-opt=-Xmx4g -- \
  --baseline-run 2026-10-21-phase-one-230855
```

With no arguments, `StepFiveChecks` runs fixtures only. The command above runs 15 explicit twin fixtures followed by regression against the latest completed run in one Spark session. It reads the run’s small twin/detail/summary tables, compares complete results and detail multiplicity, and independently checks complete-twin counts. For `2026-10-21-phase-one-215353`, detail is read from its historical `homonymousNodePairsByTwin` and summary from its historical `homonymousNodePairs`. Other supplied runs use the final `homonymousNodePairs` and `homonymousNodePairsWithTwinCount` names. It does not read the original occurrence export or write investigation tables. Both recorded baselines contain 1,911 twins, 1,942 detailed entries, 157 global node pairs and zero unresolved twins. Fixtures check name matching across capitalization and leading/trailing spaces. Synthetic fixtures exercise unresolved and repeated-node cases, including a shared Binding node at the same position and a directed self-relation on both sides. The latter retains regulator and target detail entries while counting their complete twin once.

The repository's existing `project.scala` supplies Scala, Spark, Delta, Hadoop/AWS and JVM module options.

## Full run

From the same project root, generate a fresh run name and pass it as an application argument:

```zsh
task_run_id=$(date -u +%Y-%m-%d-phase-one-%H%M%S)
scala run . -M biomed3471.codex.PhaseOneApp --java-opt=-Xmx4g -- \
  --run "$task_run_id" \
  --scratch "/private/tmp/biomed3471-codex/$task_run_id"
```

`IOOps.parseArguments` reads `--run` from `main`'s argument array. The shell generates the timestamp; Scala does not read it from an environment variable or JVM system property. `outputPath` appends the supplied run name to the existing S3 prefix. Existing run prefixes, including partial runs, are refused; historical outputs are not overwritten or migrated.

Optional application arguments are `--input <delta-location>` and `--scratch <local-directory>`. The default input is:

```text
s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/BIOMED-3471/wytze-relation-occurrences/
```

Both implementations read this same Delta source. It contains Wytze’s complete original export with its 16 columns unchanged; it is separate from the six analysis outputs. The original Parquet was copied within S3 and converted in place by adding the Delta transaction log. `--input` overrides the Delta location and is passed verbatim to Spark. No local source copy is required.

Scratch defaults to `<java.io.tmpdir>/biomed3471-codex/<run>/`. It contains Spark spill and disk-backed S3 upload buffers. The program does not delete previous scratch data.

The launch option sets a 4 GiB Java heap. Spark retains `local[*]`, 256 initial shuffle partitions, 32 MiB AQE advisory partitions, disabled broadcast joins and disk spill. S3 uploads use disk buffers with one active block per stream. No direct-memory cap or CPU cap is added. RuntimeOps remains inactive: no memory guard, RSS sampler or memory report is enabled.

S3 access reuses the existing S3A configuration and `software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider`. Refresh an expired existing non-PROD session using `~/aws-login-nonprod`; no additional role or authentication route is introduced.

Writes run sequentially with `errorifexists`. Committed intermediate readbacks bound lineage. `_STARTED` records the input and start time. The `_DATASETS_COMMITTED` call remains disabled; verify completion from exit status and all six Delta commits. The success message is printed only after the writes and Spark shutdown succeed.

The final naming decision of 21 October 2026 gives detailed entries the name `homonymousNodePairs` and their derived summary the name `homonymousNodePairsWithTwinCount`. The retained run `2026-10-21-phase-one-215353` predates this decision: its `homonymousNodePairsByTwin` contains detail and its `homonymousNodePairs` contains the summary. Those saved directories retain their original meanings. Earlier seven-table outputs used `changedNodeComparisons` for detail; the separate distinct-per-twin intermediate is no longer part of the pipeline.

This phase calculates the requested per-node-pair distinct twin count. Vocabulary classification, semantic review and category percentages remain separate investigation work.
