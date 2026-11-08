# Scala Ops modules: cohesion and naming

Clarified with the user on 6 November 2026. Applies to Scala work in this repository and informs the user's cross-project code standard.

## The correction

An `Ops` module groups operations that belong together. Its name should identify the abstraction, representation or capability that makes them a coherent group. Putting functions in a Scala `object` establishes a module's shape; it does not establish its responsibility or justify its name.

The user rejected `TwinOps$` as a default name in the current exploration: practically everything here is a data operation, so that name communicates little. Earlier guidance mentioning `DataTypes`, `TwinOps$` and `*Ops` as Haskell-style modules describes organization, not a requirement to create a `TwinOps$` object.

The user's usual distinction is that Ops provide generic operations, while services carry the particular business use. This exploratory repository does not need services simply to reproduce a production architecture. The application can compose the operations directly. The full criteria for the Ops/service boundary remain to be refined with the user; the examples below inform that discussion rather than establish an absolute taxonomy.

## Apply the distinction

- Identify the common subject and responsibility before choosing a module name. The name should explain why these operations belong together and make its scope understandable to callers.
- Group reusable operations around the abstraction, format or capability they serve. Keep the particular analysis and its composition visible in the application or existing service.
- Consider the whole API: parameters, receivers, results, package context and use sites. Renaming a miscellaneous collection to a more specific `SomethingOps` does not by itself establish cohesion.
- Prefer the simplest structure needed by the exploration. Do not introduce services, helper layers or general-purpose frameworks just to fit a naming scheme.
- Preserve the existing module style: behavior-free data, related operations in explicitly imported object modules, value-level extensions where appropriate, and named givens in their established sections. This clarification concerns grouping and naming, not a move to behavior-heavy data classes or conventional companion organization.

For the current FunctionalClass work, distinguish vocabulary-label grouping/comparison operations from the specific ENZYMES–PROTYP analysis before settling module boundaries and names. No replacement name or refactor was agreed by this clarification.

## Examples from the projects

The following examples were inspected across the local `cmdp-biomed-*` projects, including their sbt `project/` directories, and `spark-playground`. Links to other repositories assume the existing sibling checkouts under `IdeaProjects`.

| Module | Common subject and relationship to its caller |
|---|---|
| [TermiteVocabularyOps](../../../cmdp-biomed-node-attribute-table-builder/modules/biomed-node-attribute-table-builder/src/main/scala/com/elsevier/biomed/node/attribute/table/builder/termite/TermiteVocabularyOps.scala) | Constructs effective vocabularies: layer merging, exclusions, preferred names and aliases. [TermiteVocabularyService](../../../cmdp-biomed-node-attribute-table-builder/modules/biomed-node-attribute-table-builder/src/main/scala/com/elsevier/biomed/node/attribute/table/builder/termite/TermiteVocabularyService.scala) resolves configured bundles, loads layers and applies run scope. |
| [SparkDatasetOps](../../../cmdp-biomed-backfill-remapper/modules/biomed-backfill-remapper/src/main/scala/com/elsevier/cmdp/biomed/backfill/remapper/service/spark/SparkDatasetOps.scala) | Generic encoder-checked field and column operations for Spark datasets. |
| [ChangelogDatasetOps](../../../cmdp-biomed-graph-incremental-rnef-exporter/modules/biomed-graph-incremental-rnef-exporter/src/main/scala/com/elsevier/cmdp/rnef/exporter/changelog/ChangelogDatasetOps.scala) | Changelog aggregation, compaction and joins, parameterized by keys and combining functions. Its callers supply the particular entity relationships and rules. |
| [TableOps](../../../cmdp-biomed-node-builder/modules/biomed-node-builder/src/main/scala/com/elsevier/cmdp/biomed/node/builder/topology/TableOps.scala) | Generic Kafka table construction, joins, grouping and aggregation. `TopologyService` supplies concrete BioMed types and workflow composition. |
| [DockerOps](../../../cmdp-biomed-node-builder/project/DockerOps.scala) | Docker command construction in the sbt build. `ReleaseImagePublisher` composes the build operations into a release workflow. |
| [ProxFs2Ops](../../../cmdp-biomed-node-builder/project/ProxFs2Ops.scala) | Process configuration, success checks and command/failure rendering. Free functions and extensions share the same subject. |
| [SchemaEnforcementOps](../../sparkops/SchemaEnforcementOps.scala) | Spark schema/nullability enforcement and the `enforceAs` extension; the name states the contract. |

## What the examples do not imply

Ops are not necessarily pure: build modules such as [EcrOps](../../../cmdp-biomed-node-builder/project/EcrOps.scala) and [ArtifactoryOps](../../../cmdp-biomed-node-builder/project/ArtifactoryOps.scala) contain `IO`/`Stream` operations. Purity alone does not determine the module boundary.

Existing code also contains technical services and domain-specific Ops. The table builder's topology-scoped `DatasetOps` modules derive meaning from their package; imports sometimes expose that meaning explicitly, such as `shared.protyp.DatasetOps` imported as `ProtypOps`. Relation Builder's older `TwinOps$` contains the more specific `JoinOps`, `AggregationOps` and `VoteOps` groups. These examples do not make `TwinOps$` a useful default name in this repository.

Use this note as the current naming clarification alongside the user's existing Scala module conventions. Refine the remaining criteria with the user rather than treating the current interpretation as a complete rulebook.
