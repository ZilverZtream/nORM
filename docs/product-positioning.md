# Product Positioning

nORM is an evidence-gated, tenant-safe, provider-mobile .NET ORM with broad
SQL-backed LINQ support, provider-neutral temporal history, raw SQL safety
boundaries, release-gated performance budgets, and a strict provider mobility
contract.

## What nORM Is

- An EF-style ORM for .NET applications that want familiar `DbContext`, LINQ,
  change tracking, migrations, bulk operations, and provider-specific SQL.
- A provider-mobile data layer for supported v1 shapes across SQLite, SQL
  Server, PostgreSQL, and MySQL. Supported portable shapes translate, emulate,
  or fail deterministically instead of silently drifting by provider. Strict
  provider mobility mode turns that rule into runtime behavior for generated
  nORM paths and blocks provider-bound escape hatches, including direct
  connection access, from certification.
- A generated-path tenant boundary for mapped entities with a configured tenant
  provider, including fail-closed behavior, redacted diagnostics, and
  tenant-aware temporal operations. SQL Server/PostgreSQL can add native session
  context plus reviewable and explicitly applied RLS policy DDL as defense in
  depth.
- A nORM-managed temporal history system that works through provider-specific
  history tables, triggers, tag lookup, history reads, changed-property diffs,
  restore, and pruning. SQL Server can explicitly opt into provider-native
  system-versioned storage for native `AsOf` reads and reviewed bootstrap DDL
  execution.
- A benchmark-gated runtime where public performance claims must point to
  current BenchmarkDotNet artifacts and threshold checks.

## What nORM Is Not

- It is not a complete LINQ implementation.
- It is not a full EF Core replacement.
- It is not a drop-in replacement unless a specific application workflow is
  tested and proven.
- It is not always faster than raw ADO.NET.
- It is not production-ready for every enterprise workload.
- It does not rewrite raw SQL or stored procedures to add tenant predicates.
- It does not automatically translate arbitrary caller-authored SQL Server
  stored procedure bodies into PostgreSQL/MySQL/SQLite semantics.
- It does not silently install database-native RLS policies or make SQL Server
  system-versioned temporal tables the provider-neutral default.

## Why Not Just EF Core?

EF Core remains the broadest .NET ORM choice. nORM emphasizes a narrower,
evidence-gated surface: provider-mobile generated SQL, lower-overhead hot paths,
native/fallback bulk operations, explicit tenant boundaries, and release-gated
performance budgets. Use nORM when those are the product constraints you need
and the supported v1 query surface covers your workload.

## Why Not Just NHibernate?

NHibernate is mature and feature-rich. nORM focuses on a smaller EF-style API,
provider-swappable operational scenarios, explicit release evidence, and a
modern LINQ-plus-bulk path rather than a maximal mapping framework.

## Why Not Just Dapper?

Dapper is excellent for caller-authored SQL and low overhead. nORM is for teams
that want more generated data-layer behavior: LINQ translation, change tracking,
migrations, tenant predicate generation, bulk APIs, and provider-neutral
temporal history while preserving performance evidence for hot paths.

## Provider-Bound Migration Findings

The provider-swap story is strongest for applications that keep business data
access on generated nORM APIs. Existing EF Core/Dapper/ADO.NET applications may
also contain provider-bound assets: stored procedures, raw SQL, native temporal
tables, RLS policies, collations, hand-authored DDL, and provider-specific type
assumptions. Those should be inventoried by certification tooling, classified
as migration findings, and remediated by replacing them with generated nORM
queries/writes/temporal APIs where safe. If an automatic rewrite cannot prove
equivalent semantics, nORM should flag the item for human design review.

## Unique Emphasis

nORM's niche is not one isolated feature. The RC3 sample and tests demonstrate
the combined position: a real provider-swappable application, generated-path
tenant boundary evidence, provider-neutral temporal/versioning behavior, and a
strict provider-swap certification artifact across the supported provider
matrix.

nORM can make a bounded best-in-class claim for the niche it actually proves:
provider mobility for supported generated paths, generated-path tenant
boundaries, and provider-neutral nORM-managed temporal history across SQLite,
SQL Server, PostgreSQL, and MySQL, with optional native RLS/session-context
support for SQL Server/PostgreSQL and explicit SQL Server provider-native
temporal mode. It should not claim to replace every database security policy or
make provider-native temporal storage portable across all providers.
