# Tenant And Temporal Hardening Loop

This file is the durable loop memory for the tenant and temporal hardening work.
The goal is not to add random feature surface. The goal is to move nORM from
evidence-backed RC3 capability toward a defensible market-leading tenant and
temporal story.

## Loop Order

1. Tenant fail-closed audit and fixes.
2. Tenant generated-write matrix.
3. Temporal lifecycle hardening.
4. Tenant plus temporal adversarial tests.
5. Temporal product capabilities where the API is stable.
6. Operational docs and sample expansion.
7. Tenant/temporal performance gates.

## Non-Negotiables

- No silent tenant bypass on generated query or write paths.
- Tenant misconfiguration fails with nORM-specific deterministic exceptions.
- Raw SQL, stored procedures, migrations, scaffolding, and direct connection use
  remain privileged caller-controlled paths.
- Temporal `AsOf` reads remain tenant-scoped and no-tracking.
- Public claims require tests, live-provider evidence, docs, or benchmark
  artifacts.
- Do not expand LINQ surface as part of this loop.
- Do not weaken existing benchmark governance or release gates.

## Acceptance Bar

The loop is complete only when the repository contains:

- fail-closed tenant tests for missing provider, null tenant, wrong tenant type,
  missing tenant column, and tenant mismatch;
- generated-path tenant write tests for `SaveChanges`, `ExecuteUpdate`,
  `ExecuteDelete`, bulk paths, includes/split query, compiled query, cache, and
  temporal reads;
- temporal lifecycle tests for bootstrap idempotency, cancellation, disabled
  path, and provider trigger/function behavior;
- tenant plus temporal adversarial tests across SQLite, SQL Server, PostgreSQL,
  and MySQL where applicable;
- bounded docs that state guarantees and non-guarantees;
- sample app workflows that demonstrate tenant proof, temporal history, and
  provider mobility without marketing overclaim;
- performance evidence for tenant enabled/disabled and temporal enabled/disabled
  overhead.
