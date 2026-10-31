# The twin search of BIOMED-3471

The twin search answers this question: how many non-identical twins are there between the forward flow and the backfill because one or both of their nodes are homonyms? It also finds the homonymous node pairs behind them.

## The problem, on one example

Two different nodes of the graph are named luteolin: `biodata:smallmol/Concept_700527820` and `biodata:smallmol/Concept_86446482`. The forward flow states that luteolin regulates Inflammation, with the first luteolin node as the regulator. The backfill states the same relation, with the second luteolin node as the regulator. The graph therefore holds two canonical relations with the same relation type and the same node names, but different canonical relation IDs:

| Canonical relation ID | Regulator | Target | Pipelines of its relation occurrences |
|---|---|---|---|
| `biodata:regulation/regulator/Concept_700527820/target/Concept_86432034` | `biodata:smallmol/Concept_700527820`, luteolin | `biodata:disease/Concept_86432034`, Inflammation | forward flow |
| `biodata:regulation/regulator/Concept_86446482/target/Concept_86432034` | `biodata:smallmol/Concept_86446482`, luteolin | `biodata:disease/Concept_86432034`, Inflammation | forward flow and backfill |

The first canonical relation looks like a relation that only the forward flow found. In fact, the backfill found the same relation, on the other luteolin node. The two canonical relations are non-identical twins, and the two luteolin nodes are the homonymous node pair that makes them non-identical.

The twin search finds every twin pair of this kind, and every homonymous node pair behind them. A non-identical twin is a candidate, not yet an error, because some homonyms exist by design. The cause analysis, which comes after the twin search, decides which homonyms are errors.

## The domain

### Relation occurrences and the two pipelines

A relation occurrence is one occurrence of a relation in one document, produced by one pipeline, as [Op - BioMed CPLD To BioMed Graph Events](https://elsevier.atlassian.net/wiki/spaces/ELSSIE/pages/119601220912178/Op+-+BioMed+CPLD+To+BioMed+Graph+Events) defines it. Two pipelines produce relation occurrences:

| Pipeline | What it is | The `source` value of its relation occurrences |
|---|---|---|
| Forward flow | It is the BCE pipeline, Biomed Content Enrichment. | `Elsevier ML NLP` |
| Backfill | It is the MedScan pipeline. | `Elsevier Medscan NLP` |

### Canonical relations

A canonical relation is a relation as the graph holds it: one relation type between two nodes, each node in its role. All the relation occurrences that state the same relation type between the same two nodes, in the same roles, belong to one canonical relation.

The graph's domain model decides whether each relation type is directed or undirected. Section 2.2 of [BioMed Data Identifier Schemes](https://elsevier.atlassian.net/wiki/spaces/ELSSIE/pages/119601216791062/BioMed+Data+Identifier+Schemes) then builds the canonical relation ID from the relation type, the roles and the two node TERMite IDs:

- In a directed relation, such as Regulation, node 1 is the regulator and node 2 is the target. The ID names the regulator first and the target second.
- In an undirected relation, such as Binding, both nodes are partners. The ID lists the node TERMite IDs of the two partners in lexicographic order, which means sorted as text.

```text
directed     biodata:regulation/regulator/Concept_700527820/target/Concept_86432034
undirected   biodata:binding/partner/Concept_86378312/partner/HGNC24008
```

A canonical relation is an identical twin when it has relation occurrences from both pipelines. It is a forward-only canonical relation when all of its relation occurrences come from the forward flow.

### Nodes, names and homonyms

A node URN is the full identifier of a node, such as `biodata:smallmol/Concept_700527820`. Its middle part, `smallmol`, is the node type. Its last part, `Concept_700527820`, is the node TERMite ID, which the canonical relation ID uses.

Each node has exactly one name, such as luteolin, as section 4.3 of [BioMed Knowledge Graph (Graph) Data Model](https://elsevier.atlassian.net/wiki/spaces/ELSSIE/pages/119601199850466/BioMed+Knowledge+Graph+Graph+Data+Model) defines it with `skos:prefLabel`. The input gives it in `node1_name` and `node2_name`. The twin search compares names by their normalized name, which is the name in lower case, with the spaces at both ends removed. For example, `Luteolin` and ` luteolin ` have the same normalized name, `luteolin`.

A homonym is a node whose normalized name is the same as the normalized name of another node. The two luteolin nodes of the example are homonyms of each other.

### Non-identical twins, twin pairs and homonymous node pairs

The name key of a canonical relation is its relation type and the normalized names of its two nodes. In a directed relation, the regulator's normalized name comes first and the target's normalized name second. In an undirected relation, the two normalized names are sorted alphabetically. The canonical relation ID orders the partners by their node TERMite IDs, which can put their names in different orders across twins, and sorting the names gives both relations the same name key.

```text
{biomed:Regulation, [luteolin, inflammation]}
{biomed:Binding, [collagen, mia3]}
```

Two canonical relations are non-identical twins when they have the same name key and different canonical relation IDs. Because their name keys are the same, each node of one relation goes with a node of the other that has the same normalized name. Because their canonical relation IDs differ, at least one node goes with a different node, and those two nodes are homonyms.

A twin pair is one pair of non-identical twins that the twin search finds. It holds two canonical relations:

- The forward-flow relation is a forward-only canonical relation.
- The backfill twin is a canonical relation with at least one backfill relation occurrence. It can be an identical twin.

A twin pair is flagged when its backfill twin also has forward-flow relation occurrences. The forward flow then states the relation on both homonyms, so the two canonical relations are non-identical twins within the forward flow as well. The column `nonIdenticalTwinsAlsoWithinForwardFlow` holds this flag. The luteolin twin pair is flagged.

A homonymous node pair is a node of the forward-flow relation and the node of the backfill twin that goes with it, when the two are different nodes with the same normalized name. Each twin pair has one or two homonymous node pairs, because the forward-flow relation has two nodes, and at least one of them goes with a backfill node other than itself. Step 5 reports them only for the twin pairs where it can decide which forward-flow node goes with which backfill node. One homonymous node pair can be behind many twin pairs. In the run of 23 October 2026, the two luteolin nodes are behind 66 twin pairs.

## The input: a list of relation occurrences

The twin search starts from a list of relation occurrences. It reads seven of their columns and ignores every other column:

| Column | What it holds | Example |
|---|---|---|
| `source` | It names the pipeline that produced the relation occurrence. | `Elsevier ML NLP` |
| `relationship_type` | It names the relation type. | `biomed:Regulation` |
| `direction` | It holds `node1_to_node2` for a directed relation and `undirected` for an undirected one. | `node1_to_node2` |
| `node1_urn` | It holds the node URN of node 1. | `biodata:smallmol/Concept_700527820` |
| `node1_name` | It holds the name of node 1. | `luteolin` |
| `node2_urn` | It holds the node URN of node 2. | `biodata:disease/Concept_86432034` |
| `node2_name` | It holds the name of node 2. | `Inflammation` |

The twin search does not need a canonical relation ID in the list, because step 1 builds it from these columns.

The luteolin example starts from relation occurrences like the three below. The list holds many relation occurrences of each kind, and one of each is enough to follow the example.

| source | relationship_type | direction | node1_urn | node1_name | node2_urn | node2_name |
|---|---|---|---|---|---|---|
| Elsevier ML NLP | biomed:Regulation | node1_to_node2 | biodata:smallmol/Concept_700527820 | luteolin | biodata:disease/Concept_86432034 | Inflammation |
| Elsevier ML NLP | biomed:Regulation | node1_to_node2 | biodata:smallmol/Concept_86446482 | luteolin | biodata:disease/Concept_86432034 | Inflammation |
| Elsevier Medscan NLP | biomed:Regulation | node1_to_node2 | biodata:smallmol/Concept_86446482 | luteolin | biodata:disease/Concept_86432034 | Inflammation |

## The algorithm at a glance

The five steps go from the relation occurrences to the homonymous node pairs. The table name on the right is the table that the step writes.

```text
relation occurrences
  step 1   builds the canonical relation ID of each relation occurrence
  step 2   folds the relation occurrences into canonical relations         canonicalRelations
  step 3   builds the name key of each canonical relation
  step 4   matches each forward-only canonical relation on its name key    nonIdenticalTwinPairs
  step 5   decides which forward-flow node goes with which backfill node   twinPairsWithNameMatchedNodePairTuples
           keeps the homonymous node pairs of each resolved twin pair      homonymousNodePairs
           counts the twin pairs that give each homonymous node pair       twinPairCountPerHomonymousNodePair
           keeps the twin pairs for which it cannot decide                 twinPairsWithMultipleNameMatchedNodePairTuples
```

Steps 1 and 2 turn the relation occurrences into canonical relations. Steps 3 and 4 find the twin pairs. Step 5 finds the homonymous node pairs inside each twin pair.

## The algorithm step by step

Each step below says what it does, what it relies on, and what it gives for two examples from the run of 23 October 2026. The luteolin example is a directed relation, and the collagen example is an undirected one.

### Step 1: the canonical relation ID of each relation occurrence

Step 1 builds, for each relation occurrence, the ID of the canonical relation that it belongs to. It relies on section 2.2 of BioMed Data Identifier Schemes. The ID is `biodata:`, then the relation type in lower case without its `biomed:` prefix, then the roles and the node TERMite IDs. A node TERMite ID is the last part of a node URN. In an undirected relation, the ID lists the node TERMite IDs of the two partners in lexicographic order. The relation occurrences already list the partners in that order, so step 1 keeps it.

| Relation occurrence | Canonical relation ID |
|---|---|
| A Regulation from the luteolin node `Concept_700527820` to the Inflammation node `Concept_86432034` | `biodata:regulation/regulator/Concept_700527820/target/Concept_86432034` |
| A Regulation from the luteolin node `Concept_86446482` to the Inflammation node `Concept_86432034` | `biodata:regulation/regulator/Concept_86446482/target/Concept_86432034` |
| A Binding of the collagen node `Concept_86378312` and the MIA3 node `HGNC24008` | `biodata:binding/partner/Concept_86378312/partner/HGNC24008` |
| A Binding of the MIA3 node `HGNC24008` and the collagen node `MSCAN_12001170` | `biodata:binding/partner/HGNC24008/partner/MSCAN_12001170` |

`Concept_86378312` sorts before `HGNC24008`, and `HGNC24008` sorts before `MSCAN_12001170`. So collagen is the first partner in the first Binding ID and the second partner in the second one. Step 5 comes back to this.

### Step 2: one row for each canonical relation, in `canonicalRelations`

Step 2 folds the relation occurrences of each canonical relation into one row. The row keeps the relation type, the direction, the two node URNs in the order of the ID, and their names as the input gives them. Its column `sources` lists the pipelines of the canonical relation's relation occurrences.

The canonical relation ID fixes every other column of the row except `sources`. The ID of section 2.2 is built from the relation type, the roles and the node TERMite IDs. A node TERMite ID belongs to one taxonomy, so it names one node URN. Each node has exactly one name (section 4.3). Step 2 therefore groups on the ID together with these columns.

| canonicalId | Node 1 | Node 2 | sources |
|---|---|---|---|
| `biodata:regulation/regulator/Concept_700527820/target/Concept_86432034` | `biodata:smallmol/Concept_700527820`, luteolin | `biodata:disease/Concept_86432034`, Inflammation | `[Elsevier ML NLP]` |
| `biodata:regulation/regulator/Concept_86446482/target/Concept_86432034` | `biodata:smallmol/Concept_86446482`, luteolin | `biodata:disease/Concept_86432034`, Inflammation | `[Elsevier ML NLP, Elsevier Medscan NLP]` |
| `biodata:binding/partner/Concept_86378312/partner/HGNC24008` | `biodata:smallmol/Concept_86378312`, collagen | `biodata:protein/HGNC24008`, MIA3 | `[Elsevier ML NLP]` |
| `biodata:binding/partner/HGNC24008/partner/MSCAN_12001170` | `biodata:protein/HGNC24008`, MIA3 | `biodata:functionalclass/MSCAN_12001170`, collagen | `[Elsevier Medscan NLP]` |

The first and the third canonical relations are forward-only. The second is an identical twin.

### Step 3: the name key of each canonical relation

Step 3 adds a name key to each canonical relation. The key contains the relation type and the normalized names of its two nodes. Step 3 keeps the original names as the input gives them. The relation type already determines whether the relation is directed or undirected, so the name key needs no separate direction field.

| canonicalId | Name key |
|---|---|
| `biodata:regulation/regulator/Concept_700527820/target/Concept_86432034` | `{biomed:Regulation, [luteolin, inflammation]}` |
| `biodata:regulation/regulator/Concept_86446482/target/Concept_86432034` | `{biomed:Regulation, [luteolin, inflammation]}` |
| `biodata:binding/partner/Concept_86378312/partner/HGNC24008` | `{biomed:Binding, [collagen, mia3]}` |
| `biodata:binding/partner/HGNC24008/partner/MSCAN_12001170` | `{biomed:Binding, [collagen, mia3]}` |

The names of the last Binding are MIA3 then collagen in the order of its ID. Its name key sorts their normalized names, so both Bindings have the same name key.

### Step 4: the twin pairs, in `nonIdenticalTwinPairs`

Step 4 compares each forward-only canonical relation with every canonical relation that has at least one backfill relation occurrence, identical twins included. Each match on the name key is a twin pair. The two canonical relation IDs of a twin pair always differ, because the forward-flow relation has no backfill relation occurrence and its backfill twin has at least one.

The backfill side includes the identical twins because the question is where the backfill put the relation. In the luteolin example, the backfill twin is an identical twin. Without identical twins on the backfill side, this twin pair would be lost.

| forwardCanonicalId | backfillCanonicalId | nonIdenticalTwinsAlsoWithinForwardFlow |
|---|---|---|
| `biodata:regulation/regulator/Concept_700527820/target/Concept_86432034` | `biodata:regulation/regulator/Concept_86446482/target/Concept_86432034` | true |
| `biodata:binding/partner/Concept_86378312/partner/HGNC24008` | `biodata:binding/partner/HGNC24008/partner/MSCAN_12001170` | false |

One forward-only canonical relation can have several backfill twins. The number of distinct `forwardCanonicalId` values is therefore the number of forward-only canonical relations with at least one non-identical twin.

Each row also holds the two canonical relations of the twin pair, in the columns `forwardTwin` and `backfillTwin`. Each of them holds the relation type, the direction, the two node URNs in the order of the ID, their names as the input gives them, and their normalized names.

### Step 5: which forward-flow node goes with which backfill node

In a twin pair, each node of the forward-flow relation goes with a node of the backfill twin that has the same normalized name. In a directed relation, the roles decide: the regulator goes with the regulator, and the target goes with the target.

For an undirected relation such as Binding, both nodes have the role partner, so the roles cannot decide. The positions cannot decide either. A position is the place of a node in the canonical relation ID. The ID lists the node TERMite IDs of the two partners in lexicographic order (section 2.2), so a position only says whether a node TERMite ID sorts first. When the two relations of a twin pair have different node TERMite IDs for the same normalized name, that name can be at position 1 in one relation and at position 2 in the other.

The collagen twin pair shows this. As step 1 showed, collagen is the first partner of the forward-flow relation and the second partner of the backfill twin, because its two node TERMite IDs sort differently:

| Relation | Position 1 | Position 2 |
|---|---|---|
| Forward-flow relation | collagen, `Concept_86378312` | MIA3, `HGNC24008` |
| Backfill twin | MIA3, `HGNC24008` | collagen, `MSCAN_12001170` |

The MIA3 node has the same node TERMite ID in both relations, and the two collagen nodes have different node TERMite IDs. Comparing position 1 with position 1 would compare collagen with MIA3. So in the collagen twin pair, these nodes go together:

- The collagen node at position 1 of the forward-flow relation goes with the collagen node at position 2 of the backfill twin. The two collagen nodes form the homonymous node pair.
- The MIA3 node at position 2 of the forward-flow relation goes with the MIA3 node at position 1 of the backfill twin. It is the same node, so the MIA3 node goes with itself.

Step 5 uses these terms:

- A name-matched node pair is a forward-flow node and a backfill node whose normalized names are the same. Step 5 stores it as `{role, forwardNodeUri, backfillNodeUri, matchedNodeName}`. The four fields hold, in order, the role of both nodes, the node URN of the forward-flow node, the node URN of the backfill node, and the normalized name that both nodes share.
- An identical node pair is a name-matched node pair whose forward-flow node and backfill node have the same node URN. A homonymous node pair is a name-matched node pair whose forward-flow node and backfill node have different node URNs.
- A name-matched node pair tuple is one possible answer to which forward-flow node goes with which backfill node, as step 5 stores it. It holds two name-matched node pairs, the one of forward-flow node 1 first and the one of forward-flow node 2 second.
- A twin pair is resolved when step 5 can decide which forward-flow node goes with which backfill node, and unresolved when it cannot.

Step 5 relies on three guarantees:

1. Each node has exactly one name (section 4.3), so each node has exactly one normalized name, and comparing two nodes by normalized name is well defined.
2. The two relations of a twin pair have the same name key, because step 4 matched them on it. They therefore have the same relation type, the same direction, and the same two normalized names, each the same number of times.
3. The two relations of a twin pair have different canonical relation IDs. The ID is built from the relation type, the roles and the node TERMite IDs (section 2.2), and guarantee 2 makes the relation type and the direction the same. So one of the two relations holds a node that the other does not, or the relation type is directed and the two relations hold the same two nodes with the regulator and the target swapped. For example, one EGFR node regulates another in the forward-flow relation and is regulated by it in the backfill twin.

A node appears in both relations when the forward-flow relation and the backfill twin both hold its node URN. A relation holds the same node twice when its two nodes have the same node URN, such as an EGFR node that binds itself, and not when two different nodes have the same normalized name.

Step 5 applies the first of these rules that fits:

| Rule | When the rule applies | Which forward-flow node goes with which backfill node |
|---|---|---|
| A | The relation is directed. | The forward-flow regulator goes with the backfill regulator, and the forward-flow target goes with the backfill target. The name key already holds their normalized names in role order, so step 5 tests no name. |
| B | The relation is undirected, and its two nodes have different normalized names. | Each forward-flow node goes with the backfill node that has the same normalized name. |
| C1 | The relation is undirected, its two nodes have the same normalized name, and a node appears in both relations. | That node goes with itself, and the other forward-flow node goes with the other backfill node. |
| C2 | The relation is undirected, its two nodes have the same normalized name, no node appears in both relations, and at least one relation holds the same node twice. | Every node of the other relation goes with that node. When both relations hold a node twice, the two repeated nodes go with each other. |
| C3 | The relation is undirected, its two nodes have the same normalized name, no node appears in both relations, and neither relation holds the same node twice. | The four nodes are different nodes with the same normalized name. Each forward-flow node goes with one of its two homonyms in the backfill twin, but step 5 cannot decide which one. The twin pair is unresolved, step 5 keeps both possible answers, and it reports no homonymous node pair. |

These are the results for the two examples, as Spark prints them. The luteolin twin pair falls under rule A, and the collagen twin pair falls under rule B, with its names in reverse order in the backfill twin.

```text
luteolin, rule A
  [[{regulator, biodata:smallmol/Concept_700527820, biodata:smallmol/Concept_86446482, luteolin},
    {target, biodata:disease/Concept_86432034, biodata:disease/Concept_86432034, inflammation}]]

collagen, rule B
  [[{partner, biodata:smallmol/Concept_86378312, biodata:functionalclass/MSCAN_12001170, collagen},
    {partner, biodata:protein/HGNC24008, biodata:protein/HGNC24008, mia3}]]
```

In each twin pair, one name-matched node pair is an identical node pair: the Inflammation node, and the MIA3 node. The other is a homonymous node pair: the two luteolin nodes, and the two collagen nodes.

Inside each stored tuple, the node pair of forward-flow node 1 comes first, and the node pair of forward-flow node 2 comes second, as in the canonical relation ID. This is why the regulator comes first in the luteolin result, and the collagen node comes first in the collagen result.

Rules C1, C2 and C3 apply when the relation is undirected and its two nodes have the same normalized name. The test cases show them with E1, E2, E3 and E4, four different protein nodes named EGFR, in the order of their node TERMite IDs:

```text
rule C1   forward-flow relation E1, E2   backfill twin E1, E3   [[{partner, E1, E1, egfr}, {partner, E2, E3, egfr}]]
rule C1   forward-flow relation E1, E1   backfill twin E1, E2   [[{partner, E1, E1, egfr}, {partner, E1, E2, egfr}]]
rule C2   forward-flow relation E1, E1   backfill twin E2, E3   [[{partner, E1, E2, egfr}, {partner, E1, E3, egfr}]]
rule C2   forward-flow relation E1, E1   backfill twin E2, E2   [[{partner, E1, E2, egfr}, {partner, E1, E2, egfr}]]
rule C3   forward-flow relation E1, E2   backfill twin E3, E4   [[{partner, E1, E3, egfr}, {partner, E2, E4, egfr}],
                                                                 [{partner, E1, E4, egfr}, {partner, E2, E3, egfr}]]
```

Step 5 writes four tables from these results:

- `twinPairsWithNameMatchedNodePairTuples` holds every twin pair with its result in `nameMatchedNodePairTuples`: one tuple when the twin pair is resolved, and the two possible answers when it is unresolved.
- `homonymousNodePairs`, the detail table, holds the homonymous node pairs of each resolved twin pair, one entry each, with the role at which the twin pair gives them. Identical node pairs are left out. Repeated entries stay: a twin pair in which both relations hold a node twice gives the same homonymous node pair twice.
- `twinPairCountPerHomonymousNodePair`, the global table, holds each homonymous node pair once, with `nonIdenticalTwinPairCount`, the number of distinct twin pairs that give it.
- `twinPairsWithMultipleNameMatchedNodePairTuples` holds the unresolved twin pairs with their two possible answers. In the first answer, node 1 of the forward-flow relation goes with node 1 of the backfill twin, and node 2 goes with node 2. In the second, node 1 goes with node 2, and node 2 goes with node 1.

For the two examples, the detail table holds one entry each, and the global table counts the twin pairs that give each homonymous node pair:

| forwardNodeUri | backfillNodeUri | matchedNodeName | role | nonIdenticalTwinPairCount in the global table |
|---|---|---|---|---|
| `biodata:smallmol/Concept_700527820` | `biodata:smallmol/Concept_86446482` | luteolin | regulator | 66 |
| `biodata:smallmol/Concept_86378312` | `biodata:functionalclass/MSCAN_12001170` | collagen | partner | 128 |

## The results

### The six tables

| Table | What one row holds | Columns |
|---|---|---|
| `canonicalRelations` | Each row holds one canonical relation. | canonicalId, relationship_type, direction, node1_urn, node1_name, node2_urn, node2_name, sources |
| `nonIdenticalTwinPairs` | Each row holds one twin pair. | forwardCanonicalId, backfillCanonicalId, nonIdenticalTwinsAlsoWithinForwardFlow, forwardTwin, backfillTwin |
| `twinPairsWithNameMatchedNodePairTuples` | Each row holds one twin pair with the result of step 5. | the five columns of `nonIdenticalTwinPairs`, then nameMatchedNodePairTuples |
| `homonymousNodePairs`, the detail table | Each row holds one homonymous node pair of one resolved twin pair. | forwardNodeUri, backfillNodeUri, matchedNodeName, role, forwardCanonicalId, backfillCanonicalId, nonIdenticalTwinsAlsoWithinForwardFlow |
| `twinPairCountPerHomonymousNodePair`, the global table | Each row holds one homonymous node pair, with the number of twin pairs that give it. | forwardNodeUri, backfillNodeUri, matchedNodeName, nonIdenticalTwinPairCount |
| `twinPairsWithMultipleNameMatchedNodePairTuples` | Each row holds one unresolved twin pair. | forwardCanonicalId, backfillCanonicalId, nameMatchedNodePairTuples |

### The five numbers printed at the end

| Printed name | What it counts |
|---|---|
| `twinPairCount` | It counts the twin pairs. |
| `forwardOnlyCanonicalRelationWithTwinCount` | It counts the forward-only canonical relations with at least one non-identical twin. |
| `unresolvedTwinPairCount` | It counts the unresolved twin pairs. |
| `homonymousNodePairEntryCount` | It counts the entries of the detail table. |
| `homonymousNodePairCount` | It counts the homonymous node pairs, each one once. |

## The code

| File | What it holds |
|---|---|
| `DataTypes.scala` | It holds the data model of the twin search: the input row `RelationOccurrence`, the canonical relation rows `CanonicalRelation` and `CanonicalRelationWithNameKey`, the name key `CanonicalRelationNameKey`, the `source` and `direction` values, the three roles, the name-matched node pair `NameMatchedNodePair` that step 5 stores, and `HomonymousNodePairWithTwinCount`, one row of the global table. |
| `DataOps.scala` | It holds the five steps, each documented next to its code. |
| `TwinSearchApp.scala` | It reads the relation occurrences, runs the five steps, writes the six tables and prints the five numbers. |
| `TwinSearchTestCases.scala` | It runs the five steps on 14 small lists of relation occurrences, with at least one for each rule of step 5, and compares each result with the expected one. |

## How to run

Run both programs from the root of spark-playground, with a 4 GB heap. The test cases come first:

```bash
scala-cli run . --main-class biomed3471.claude.TwinSearchTestCases --java-opt -Xmx4g
scala-cli run . --main-class biomed3471.claude.TwinSearchApp --java-opt -Xmx4g -- 2026-10-23
```

The only argument of TwinSearchApp is the run name. It names the folder under `s3://cmdp-biomed-lakehouse-bucket-dev/d_okouya/BIOMED-3471/` where the six tables are written, and a table that already exists there is replaced. TwinSearchApp reads the relation occurrences from the value `relationOccurrencePath` at the top of `main`. To run the twin search on another list of relation occurrences, change that value.
