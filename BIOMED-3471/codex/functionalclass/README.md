# FunctionalClass vocabulary comparisons

## App1: preferred-label overlap

`biomed3471.codex.FunctionalClassDedupApp1` preserves the original Spark / Scala 3 demonstration:

1. Load the effective ENZYMES vocabulary and save/read it as Delta.
2. Load the effective PROTYP vocabulary and save/read it as Delta.
3. Group each vocabulary's concepts by `lower(trim(name))`, join the groups, and save/read the overlap.

Each overlap row contains `normalizedName`, `enzymes` and `protyp`. The last two fields are arrays of complete concepts: `termiteId`, original `name` and optional `alias`. Several IDs can share a preferred label within one vocabulary; they all remain in its array. The arrays are sorted by ID for inspection. Matching uses preferred labels only. A shared label is an observation, not a decision that the concepts are equivalent.

The inputs are explicit paths in the application, from the local `cmdp-biomed-termite-vocabs` checkout. The loading rules come from the Node Attribute Table Builder's `termite/TermiteOperations.md` and `TermiteVocabularyOps.scala`: **main + extra → ignore → aug → augIgnore**, then group by TERMite ID. The highest nonblank preferred label wins in the order aug, extra, main. Extra/aug strip leading `%%`. Alias normalization preserves lexical terms, applies exact `/*` removals across surviving rows, excludes control/commented terms, and preserves unknown suffixes. Aliases are retained for inspection but are not exploded or matched.

ENZYMES has no ignore layers in the current checkout; the application supplies empty ID datasets. PROTYP supplies both. Ignore files are read by their first TSV column even when later columns contain explanations.

`DataTypes` contains the records and named encoders. `TermiteVocabularyOps` implements loading rules. `VocabularyLabelOps` groups reusable label preparation, indexing and matching operations; each application composes its own analysis. The applications contain file reads, Delta writes, Spark lifecycle and display. The loader uses a typed `map` for alias normalization; the saved stages and comparisons use DataFrames and native Spark expressions, following `TwinSearchApp`.

## App2: ranked concept pairs

`biomed3471.codex.FunctionalClassDedupApp2` reads App1's saved effective vocabularies:

1. Tag each concept with its vocabulary, then union the two vocabularies.
2. Combine preferred and alternative labels, explode, lowercase, trim and collapse repeated whitespace. Preserve punctuation. Drop null/empty labels and deduplicate `(vocabulary, conceptId, label)`.
3. Self-join on label with `leftVocabulary < rightVocabulary`, producing one row per concept pair and shared label.
4. Group by both vocabulary names and concept IDs; collect distinct shared labels in alphabetical order.
5. Order by shared-label count descending, then label length descending for single-label matches, followed by vocabulary names and concept IDs as tie-breakers.

**Short labels participate.** The optional `.filter(length(col("label")) >= 4)` line is commented out in App2, immediately after label preparation. Uncomment it to enable that cutoff.

The final table contains `leftVocabulary`, `leftConceptId`, `rightVocabulary`, `rightConceptId`, `sharedLabels`, `sharedLabelCount` and `singleMatchLabelLength`. These are candidate pairs for review; shared labels do not establish semantic equivalence. `orderConceptPairs` applies the review ordering again when reading the saved table, since Delta does not guarantee read order.

## Run

From the `spark-playground` root:

```zsh
scala compile .
scala run . -M biomed3471.codex.functionalclass.FunctionalClassDedupApp1Spec --java-opt=-Xmx4g
scala run . -M biomed3471.codex.functionalclass.FunctionalClassDedupApp2Spec --java-opt=-Xmx4g
AWS_PROFILE=aws-h-entellect-nonprod scala run . -M biomed3471.codex.FunctionalClassDedupApp1 --java-opt=-Xmx4g
AWS_PROFILE=aws-h-entellect-nonprod scala run . -M biomed3471.codex.FunctionalClassDedupApp2 --java-opt=-Xmx4g
```

Both applications reuse `TwinSearchApp.makeSparkSession`: local Spark, eight shuffle partitions, Delta and the existing S3 credentials provider. App2 can run directly against the existing App1 outputs; App1 only needs rerunning when refreshing the inputs. Each application overwrites only its own demonstration tables:

```text
s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/BIOMED-3471/functional-class-dedup/
  enzymes/
  protyp/
  preferredLabelOverlap/
  app2/
    labels/
    labelMatches/
    conceptPairs/
```

The fixture checks cover source precedence, augmentation reintroduction, final exclusion with and without augmentation, alias controls and removals, normalization, multiple concepts per label, preferred-label-only matching, TSV parsing and Delta round trips of concepts and nested overlap rows. They use local synthetic data and clean up their temporary files.

The App2 fixture covers preferred/alternative label combinations, repeated whitespace, duplicate labels, retained short labels, punctuation, multiple concepts sharing a label, three vocabularies, repeated IDs across vocabularies, and the ranking and tie-breaker rules.

## App1 verified result — 5 November 2026

The root compilation, fixture checks and full S3 run passed. The full run used `-Xmx4g` and the clean vocabulary checkout at `9b8528fb6f7ca982badbdfa74d0d38fc1ec5f649`. Counts below were read from the saved Delta tables and also matched an independent calculation from the dictionary files.

| Saved table | Counted unit | Rows |
|---|---|---:|
| `enzymes` | Effective ENZYMES concept IDs | 6,635 |
| `protyp` | Effective PROTYP concept IDs | 8,246 |
| `preferredLabelOverlap` | Shared normalized preferred labels | 197 |

For example, `4-hydroxy-2-oxoglutarate aldolase` is one overlap row containing ENZYMES `EC_4.1.3.16` and both PROTYP IDs `PROTYP12331` and `PROTYP8009`.

## App2 verified result — 6 November 2026

Root compilation, both applications' fixture checks and the full App2 S3 run passed with `-Xmx4g`. App2 read the existing effective vocabulary snapshots (ENZYMES Delta version 1 and PROTYP version 0). Each output was saved and read back; its row count matched an independent calculation from those same inputs.

| Saved table under `app2/` | Counted unit | Rows |
|---|---|---:|
| `labels` | Distinct `(vocabulary, conceptId, normalized label)` | 153,238 |
| `labelMatches` | Distinct `(concept pair, shared label)` | 5,561 |
| `conceptPairs` | Cross-vocabulary concept pairs | 2,171 |

The pairs involve 1,280 ENZYMES concepts and 1,082 PROTYP concepts. **390 pairs are supported entirely by labels shorter than four characters** and are included in this run. The optional four-character cutoff would leave 1,781 pairs.

The first ranked pair is ENZYMES `EC_2.4.1.17` / PROTYP `PROTYP441`, with 33 shared labels. The next is `EC_3.4.21.62` / `PROTYP993`, with 29.

## Loading reference

The authoritative implementation and its explanation are in the sibling `cmdp-biomed-node-attribute-table-builder` checkout, under:

```text
modules/biomed-node-attribute-table-builder/src/main/scala/
  com/elsevier/biomed/node/attribute/table/builder/termite/
    TermiteVocabularyOps.scala
    TermiteOperations.md
```
