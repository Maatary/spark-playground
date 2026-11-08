# Repository code standards

Before creating or reorganizing Scala operation modules, read [Scala Ops modules: cohesion and naming](docs/code-standards/ops-modules.md).

Name and group operations by their cohesive subject, abstraction or capability. Do not use `TwinOps$` as a default container merely because functions operate on data. Distinguish reusable operations from the particular analysis that composes them; this exploratory repository does not require a service layer for that purpose.

Keep the user's established Haskell-style module organization and behavior-free data. The linked note refines grouping and naming and records concrete examples; it does not prescribe a repository-wide rename or a complete Ops/service taxonomy.
