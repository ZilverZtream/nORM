# nORM Security Threat Model & Injection-Resistance Audit

This is the single artifact a security reviewer can read end-to-end to judge nORM's
security posture. It states the trust boundaries, enumerates the threats to each,
names the mitigation, and — for every claim — points at the **test that proves it and
the command that reproduces it**. Nothing here is "trust us."

It is the umbrella over the four topic docs, which hold the detailed contracts:
[`raw-sql-security.md`](raw-sql-security.md),
[`stored-procedure-security.md`](stored-procedure-security.md),
[`multi-tenancy-security.md`](multi-tenancy-security.md),
[`logging-redaction.md`](logging-redaction.md).

Scope: nORM is a library that turns LINQ/CLR expressions and an explicit raw-SQL
surface into parameterized SQL against SQLite / SQL Server / PostgreSQL / MySQL. The
threat model covers what nORM *emits and logs*. It does **not** cover the security of
the database server, the network, the host, or application code above nORM.

## Assets & trust boundaries

| Asset | Boundary nORM enforces |
| --- | --- |
| Row data of other tenants | The tenant predicate is injected into **every** read and write path. |
| Table/schema/database integrity | Values become bound **parameters**; identifiers are **validated then escaped**. |
| Credentials / connection secrets | Never logged; connection strings are **redacted** by policy. |

Untrusted input enters nORM at exactly these points: (1) constants/closures inside LINQ
expressions; (2) the explicit raw-SQL / stored-procedure surface; (3) identifiers passed
to identifier-taking APIs; (4) the ambient tenant id. Each is addressed below.

## T1 — SQL injection via query values

- **Mitigation (by construction).** Value operands of a translated expression are
  emitted as provider parameters (`@p0`, `@p1`, …), never concatenated into SQL. Closure
  captures bind the same way. A whole-source scan finds **only two** interpolated
  `CommandText` sites in `src/nORM/`, and both interpolate the migration
  `PostgresAdvisoryLockKey` — a **config-supplied integer lock key**, not untrusted
  input (`PostgresMigrationRunner.cs`). There is no code path that concatenates a query
  value into SQL text.
- **Evidence:** the LINQ-parity differential fuzzer executes generated shapes against a
  live database and a LINQ-to-Objects oracle — an injected value would change results or
  error, and the sweep is DRY on the release commit (see
  [`v1-sharpening/fuzzer-dry-log.md`](v1-sharpening/fuzzer-dry-log.md)). Parameter binding
  is exercised by the full `Category=Fast` suite.

## T2 — SQL injection via identifiers

- **Mitigation.** Identifiers that reach an identifier-taking API are validated by
  `DbContext.IsSafeIdentifier` (rejecting statement breaks, quote/backtick escapes, and
  comment sequences) and then provider-escaped; they are never trusted raw.
- **Evidence:** `IdentifierInjectionTests` — `IsSafeIdentifier` returns **false** for
  `"[foo]; DROP TABLE Users--"`, `"[foo]]; DROP TABLE Users--"`, `"\"col\"\"injection\""`,
  `` "`col`injection`" ``, `"valid]; DROP TABLE x--"`, `"a--b"`, and **true** only for
  well-formed identifiers.
  `dotnet test tests/ -c Release --filter FullyQualifiedName~IdentifierInjectionTests`

## T3 — The explicit raw-SQL / stored-procedure surface

- **Mitigation.** Raw SQL is an **explicit, documented boundary**: the caller owns the
  SQL text and passes values as parameters; nORM does not silently interpolate. The
  contract (what is and isn't parameterized, how composition works, the fail-loud rules)
  is pinned in [`raw-sql-security.md`](raw-sql-security.md) and
  [`stored-procedure-security.md`](stored-procedure-security.md).
- **Evidence:** `RawSqlSecurityDocContractTests` pins the raw-SQL security doc's
  load-bearing claims against behavior; the composable-`FromSqlRaw` hardening threads the
  raw derived-table FROM through the fast paths and fails loud where it can't
  (see the changelog / git history).

## T4 — Cross-tenant data exposure (authorization boundary)

- **Mitigation.** The tenant filter is applied at **every** mapped-table emission site —
  root FROM, joins, GroupJoin/left-join flattens, SelectMany inners, navigation
  subqueries (count/scalar aggregates, both hops), and every write path — and a forged
  cross-tenant foreign key reads as *missing*, not as another tenant's value. Context
  pooling resets the tenant session key so a pooled context can't leak the prior tenant.
- **Evidence (this is nORM's most heavily-fuzzed boundary):** 39 tenant-isolation test
  files including `AdversarialTenant*`, `IncludeTenantIsolationTests`,
  `CompiledQueryTenantIsolationTests`, `M2MTenantIsolationTests`,
  `BulkTenantIsolationTests`, and the `NORM_TENANT_FUZZ_SWEEP` relational fuzzer
  (`TenantIsolationRelationalFuzzTests` — two tenant contexts over a shared graph with
  forged cross-tenant FKs, DRY on the release commit). The
  [`fuzzer-dry-log.md`](v1-sharpening/fuzzer-dry-log.md) records the specific cross-tenant
  leak kills that were found *and fixed* by this method (nav subqueries, explicit
  joins/GroupJoin, SelectMany inners). Contract: [`multi-tenancy-security.md`](multi-tenancy-security.md).
  `dotnet test tests/ -c Release --filter FullyQualifiedName~TenantIsolation`

## T5 — Credential / secret exposure in logs & errors

- **Mitigation.** Connection strings are redacted before logging by policy; SQL logging
  does not emit raw credentials. Contract: [`logging-redaction.md`](logging-redaction.md).
- **Evidence:** `ConnectionStringRedactionTests`, `LoggingRedactionTests` (and
  `InterceptorAndLoggingDocContractTests` pins the doc).
  `dotnet test tests/ -c Release --filter FullyQualifiedName~Redaction`

## T6 — Resource exhaustion (availability)

- **Mitigation.** Bounded by construction: provider `MaxSqlLength` / `MaxParameters`
  limits, `MaxGroupJoinSize`, bounded caches with memory-bound gates, adaptive timeouts,
  and cancellation-honoring async. These are guardrails, not a substitute for
  database-side resource governance.
- **Evidence:** `AdversarialQueryComplexityTests` (limit boundaries), the cache
  memory-bounds gate, `Category=…Stress` suites.

## Residual risk / explicitly out of scope

- nORM does not defend the **database server, network, or host**; deploy those per your
  own controls.
- Raw SQL is caller-owned: a caller that concatenates untrusted input into a raw-SQL
  string defeats T1/T2 — the boundary is explicit *for that reason* (see T3).
- Application-level authorization above the tenant boundary (roles, row policies beyond
  tenancy) is the application's responsibility; nORM enforces the tenant predicate, not a
  general RLS engine (though it interoperates with provider RLS — see
  [`multi-tenancy-security.md`](multi-tenancy-security.md)).

## Reviewer's one-command check

The release gate builds the artifact, runs the offline suite (including the injection,
tenant-isolation, and redaction tests above), scans for source-encoding corruption, and
proves AOT/trim honesty:

```
pwsh eng/v1-release-gate.ps1 -Mode quick -SkipBenchmark
```

Live-provider tenant isolation (all four RDBMSs) is covered by
`LiveProviderMultiTenancySecurityTests` under `Category=LiveProvider`.
