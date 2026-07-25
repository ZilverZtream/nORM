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

- **Mitigation (by construction).** Value operands of a translated expression — on the
  read path, the write path, and set-based `ExecuteUpdate`/`ExecuteDelete` — are emitted
  as provider parameters (`@p0`, `@p1`, …), never concatenated into SQL. Closure captures
  bind the same way (`@cpN`). The handful of sites that *must* inline a string (a
  compile-time separator, an enum name in a `CASE`, type metadata) go through the
  provider-aware **`DatabaseProvider.EscapeStringLiteral`**, whose MySQL override doubles
  the backslash as well as the quote — MySQL treats backslash as a string-literal escape
  under its default `sql_mode`, so bare `'`-doubling is not a safe escape there. Numeric /
  boolean / enum literals inline as culture-invariant tokens that cannot carry an escape.
  The many interpolated `CommandText` sites across the generator interpolate **provider-escaped
  identifiers** (`EscTable`/`EscCol`, `Escape(...)`) or **bound parameter names** (`@pN`/`@cpN`) —
  not query values. The only interpolated *config* value is the migration `PostgresAdvisoryLockKey`
  — a config-supplied integer lock key, not untrusted input (`PostgresMigrationRunner.cs`).
- **Hardening history (this audit).** Two query-value inlining defects were closed: (1) a computed
  `ExecuteUpdate` `SetProperty` string value was inlined with `'`-doubling only — exploitable on
  MySQL via `s.SetProperty(x => x.Col, x => x.Col + userInput)` — now **parameterized** like every
  other typed value (`BulkCudBuilder.RenderLiteral`); (2) a runtime (closure) string in a
  **projection** string operation (`a.Name.Contains/StartsWith/EndsWith(term)`,
  `string.Join(sep, g.Select(..))`, `TrimEnd(chars)`) was folded and inlined — both a MySQL
  injection surface AND a **plan-cache poisoning** bug (the baked value was replayed to later callers
  with different values). Inlined strings now route through `EscapeStringLiteral`, and a runtime value
  in a literal-only position marks the plan fold-no-cache so it is never reused across values.
- **Evidence:** `ExecuteUpdateStringLiteralParameterizationTests` (the string is bound, not
  inlined — verified against the SET-clause generator, provider-agnostically),
  `ProviderStringLiteralEscapeTests` (MySQL doubles backslash+quote; other providers double
  the quote), and the LINQ-parity differential fuzzer executes generated shapes against a
  live database and a LINQ-to-Objects oracle — an injected value would change results or
  error, and the sweep is DRY on the release commit (see
  [`v1-sharpening/fuzzer-dry-log.md`](v1-sharpening/fuzzer-dry-log.md)).

## T2 — SQL injection via identifiers

- **Mitigation.** Identifiers that reach an identifier-taking API are validated by
  `DbContext.IsSafeIdentifier` (rejecting statement breaks, quote/backtick escapes, and
  comment sequences) and then provider-escaped; they are never trusted raw.
- **Evidence:** `IdentifierInjectionTests` — `IsSafeIdentifier` returns **false** for
  `"[foo]; DROP TABLE Users--"`, `"[foo]]; DROP TABLE Users--"`, `"\"col\"\"injection\""`,
  `` "`col`injection`" ``, `"valid]; DROP TABLE x--"`, `"a--b"`, and **true** only for
  well-formed identifiers.
  `dotnet test tests/ -c Release --filter FullyQualifiedName~IdentifierInjectionTests`
- **Hardening history (this audit).** The SQL Server migration generator dropped a column's
  auto-named DEFAULT constraint through a dynamic `EXEC` that built the identifier as
  `'[' + @name + ']'`; a catalog constraint name containing `]` (a DBA can create one)
  could break out. All four sites now use `QUOTENAME(@name)` — the T-SQL-native delimited
  identifier builder (`SqlServerMigrationConstraintQuoteNameTests`).

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
- **Hardening history (this audit).** The structural validator behind the raw-query gate is now
  **provider-aware for comment semantics**, closing two MySQL bypasses: `/*! … */` conditional
  comments (whose body MySQL executes) are analyzed rather than stripped as inert, and block-comment
  nesting is disabled on MySQL/SQLite (which close at the first `*/`) so text after it is no longer
  swallowed. The SQLite dangerous-function family (`load_extension`/`readfile`/`writefile`/`pragma_*`)
  is denied explicitly, and the dangerous-pattern denylist matches at word boundaries (no more
  `script_id`/`transcript` false positives). Tests: `RawSqlCommentBypassTests`,
  `RawSqlDenylistAndIdentifierTests`.

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
- **Hardening history (this audit).** The greatest-N-per-group correlated re-scan
  (`g.OrderByDescending(x => x.Date).First()`) was tenant-isolated only *incidentally* (the
  tenant predicate rode in the captured source `WHERE`s; the re-scan's own global-filter
  fragment omitted it). Verified **no live leak**, but brittle — the re-scan now applies the
  tenant predicate **explicitly** (`CombineWithTenant`), and a run-and-diff regression
  (`GreatestNPerGroupTenantIsolationTests`, two tenants sharing a group key) proves both the
  explicit path and its independence from the source-`WHERE` capture.

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
- **Hardening history (this audit).** Two availability gaps were closed: (1) the SQLite
  `regexp` / `regexp_replace` UDFs ran a .NET `Regex` per row over an application-supplied
  pattern with **no `MatchTimeout`**, so a catastrophic-backtracking pattern (e.g. `(a+)+$`)
  could hang the worker thread (ReDoS) — both UDFs now compile with a 1s match-timeout and
  fail loud instead of hanging (`SqliteRegexpUdfTimeoutTests`); (2) the per-cache-key
  semaphore map grew without bound under adversarial `.Cacheable()` key churn (cleanup
  removed only a fixed batch per tick) — cleanup now drains unused locks to the threshold
  in one pass, keeping the map bounded (`CacheLockDrainBoundTests`).

## T7 — Mass-assignment / over-posting (write authorization)

- **Mitigation.** Writes are least-privilege by construction: `SaveChanges` writes only the
  intersection of *changed* and *mutable* columns; keys, DB-generated columns, and the
  rowversion are excluded from `UpdateColumns`; a primary-key or tenant-column mutation on a
  tracked entity is rejected loudly; and `[ReadOnlyEntity]` (view / query-only) mappings
  reject every write. Identity-defining columns cannot be re-stamped by an update.
- **Hardening history (this audit).** Two write-authorization gaps were closed: (1) the TPH
  **discriminator** — which identifies a row's concrete subtype and is stamped only on
  insert — sat in `UpdateColumns` with no guard, so an update could relabel a row as a
  sibling subtype; it is now excluded from `UpdateColumns` and rejected on both the tracked
  and `ExecuteUpdate.SetProperty` paths (`TphDiscriminatorImmutableOnUpdateTests`); (2)
  `EnsureWritableMapping` was enforced on the single-entity and `SaveChanges` paths but not
  on `BulkInsert`/`BulkUpdate`/`BulkDeleteAsync` — a read-only entity could be written
  through the bulk APIs; all three now fail loud (`BulkWriteReadOnlyEntityGuardTests`).
- **Evidence:** `TphDiscriminatorImmutableOnUpdateTests`, `BulkWriteReadOnlyEntityGuardTests`,
  `TenantColumnMutationTests`, plus the partial-column-UPDATE and change-tracker suites.

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
