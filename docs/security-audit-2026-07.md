# nORM Security Audit — July 2026

A focused, adversarial security audit of nORM, conducted against (a) the classes of
vulnerability disclosed in comparable data-access libraries over the last ~2 years and
(b) the newer bug classes that automated/AI analysis tends to surface (ReDoS, fail-open
guards, over-posting, second-order/identifier injection, resource-exhaustion under churn).

It is a companion to the [threat model](security-threat-model.md), which holds the
standing mitigations and their proving tests; this document records **this audit's
methodology, findings, and fixes**.

## Methodology

- **Two independent source-level sweeps** across nine vulnerability classes: query-value
  injection, identifier injection, second-order injection, dynamic-sort/ORDER BY,
  LIKE/wildcard, raw-SQL surface, interpolated SQL, provider-escaping correctness, and the
  cross-cutting classes (mass-assignment, ReDoS/DoS, deserialization, cache poisoning,
  info disclosure, OCC/retry integrity, AOT/type-confusion, supply chain, tenant bypass).
- **Verify every finding at the source** before acting — no fix on an agent's word alone.
  One reported "cross-tenant leak" (greatest-N-per-group) was **downgraded to
  defense-in-depth** after a run-and-diff proved no live leak (F-below).
- **Fail-first / run-and-diff for every fix**: each fix ships with a regression test that
  was confirmed to **fail on the pre-fix code** (or, for isolation, to leak under a proof
  state that neuters the mitigation), so the test genuinely guards the behavior.

## Findings & disposition

| # | Sev | Class | Finding | Status | Commit | Test |
|---|-----|-------|---------|--------|--------|------|
| 1 | **High** | Query-value injection | `ExecuteUpdate` computed `SetProperty` inlined a string with `'`-doubling only → MySQL backslash breakout | **Fixed** | `6393f594` | `ExecuteUpdateStringLiteralParameterizationTests` |
| 2 | **High** (cond.) | ReDoS / DoS | SQLite `regexp`/`regexp_replace` UDFs ran per-row `Regex` with no `MatchTimeout` | **Fixed** | `9e6c3693` | `SqliteRegexpUdfTimeoutTests` |
| 3 | Medium | Mass-assignment | TPH discriminator writable on UPDATE → subtype masquerade | **Fixed** | `8dbaf24b` | `TphDiscriminatorImmutableOnUpdateTests` |
| 4 | Medium | Mass-assignment | `[ReadOnlyEntity]` not enforced on the three bulk write paths (fail-open) | **Fixed** | `c186afd8` | `BulkWriteReadOnlyEntityGuardTests` |
| 5 | Medium | DoS | Cache-lock semaphore map grew unbounded under `.Cacheable()` key churn | **Fixed** | `3bc40e2d` | `CacheLockDrainBoundTests` |
| 6 | Low→D-i-D | Tenant isolation | greatest-N-per-group re-scan tenant-isolated only *incidentally* (no **live** leak) | **Hardened** | `b8614783` | `GreatestNPerGroupTenantIsolationTests` |
| 7 | Low | Identifier injection | SqlServer migration dropped a default constraint via `'['+@name+']'` (no `QUOTENAME`) | **Fixed** | `7012e7b4` | `SqlServerMigrationConstraintQuoteNameTests` |
| 8 | — (root cause) | Provider escaping | No provider-aware string-literal escaper (MySQL backslash) behind #1 | **Fixed** | `7d0af95a` | `ProviderStringLiteralEscapeTests` |

### 1 — ExecuteUpdate string injection (High)

`BulkCudBuilder.RenderLiteral` inlined a `string` value as `'…'` escaped with single-quote
doubling only. That is safe on SQLite/SQL Server/PostgreSQL but **not on MySQL**, which
treats `\` as a string-literal escape under its default `sql_mode` — so `\'` survives the
doubling and breaks out. Reachable end-to-end via
`ExecuteUpdate(s => s.SetProperty(x => x.Bio, x => x.Bio + userInput))` (closure capture,
ternary, and `COALESCE` branches; the multi-tenant scope id used the same path). **Fix:**
parameterize the string case exactly as the `default:` branch already does for
DateTime/Guid/etc. Verified provider-agnostically against the SET-clause generator, and
fail-first (the concat and ternary paths fail on the pre-fix inline form).

### 2 — SQLite regexp ReDoS (High, conditional)

The `regexp`/`regexp_replace` UDFs (reachable from LINQ `Regex.IsMatch(col, pattern)` on
SQLite) evaluated a .NET `Regex` per row over an application-supplied pattern with no
`MatchTimeout`. A catastrophic-backtracking pattern hangs the worker thread. **Fix:** a 1s
`MatchTimeout` on both UDFs — pathological patterns fail loud within ~1s; legitimate
per-row matches (microseconds) are unaffected.

### 3 — TPH discriminator masquerade (Medium)

The discriminator identifies a row's subtype and is stamped only on insert, yet sat in
`UpdateColumns` with no guard, so an update could relabel a row as a sibling subtype (EF
Core rejects this). **Fix:** excluded from `UpdateColumns`; fail-loud on the tracked and
`ExecuteUpdate.SetProperty` paths. Ordinary subtype-property updates still work.

### 4 — Bulk write read-only fail-open (Medium)

`EnsureWritableMapping` guarded the single-entity and `SaveChanges` paths but not
`BulkInsert`/`BulkUpdate`/`BulkDeleteAsync` — a `[ReadOnlyEntity]` mapping could be written
via the bulk APIs. **Fix:** guard added to all three; they fail loud before any statement
executes.

### 5 — Cache-lock unbounded growth (Medium)

The per-cache-key semaphore map's cleanup removed only a fixed batch per tick, so
adversarial `.Cacheable()` key churn added entries faster than they drained — a slow
memory-exhaustion DoS. **Fix:** cleanup drains unused locks to the threshold in one pass;
in-use locks are never removed.

### 6 — greatest-N-per-group tenant isolation (verified no live leak → hardened)

Reported by one sweep as a possible cross-tenant leak. **Verified with a run-and-diff**
(two tenants sharing a non-tenant-unique group key, the other tenant's row newer): the
shipped code isolates correctly — the tenant predicate rides in the captured source
`WHERE`s and is re-applied inside the re-scan. **No live leak.** But the safety was
*incidental* (the re-scan's own global-filter fragment omitted tenant, and a comment
falsely claimed otherwise), so the re-scan now applies the tenant predicate **explicitly**
(`CombineWithTenant`). Proof states in the test confirm it has teeth (neutering both
mechanisms leaks the other tenant's value) and that the explicit predicate protects
independently of the source-`WHERE` capture.

### 7 — Migration constraint-drop identifier (Low)

The SqlServer migration generator built a dynamic `EXEC` constraint identifier as
`'['+@name+']'`; a catalog default-constraint name containing `]` could break out. **Fix:**
`QUOTENAME(@name)` at all four sites.

### 8 — Provider-aware string-literal escaper (root cause / defense-in-depth)

Behind #1: nORM had no provider-aware SQL string-literal escaper — every inline-literal
site doubled only the quote. **Fix:** `DatabaseProvider.EscapeStringLiteral` (MySQL
override doubles the backslash too), with the value-literal fallback paths routed through
it so a future refactor cannot reintroduce the MySQL hazard.

## Verified safe (not exhaustive)

Confirmed at the source and not changed: no insecure deserialization (no
`BinaryFormatter`/`Newtonsoft TypeNameHandling`; the result cache stores live object
references, no serialization boundary); OCC/retry integrity (client-managed collision-
resistant tokens, fail-loud on rows-affected mismatch, `RetryingExecutionStrategy` never
replays a possibly-committed write); ORDER BY / dynamic-sort (every key is a mapped column
or parameterized visitor output — no string-based ordering API); LIKE (patterns escaped
then bound; MySQL uses a `!` escape char to dodge its backslash rule); raw-SQL /
stored-proc (caller-owned boundary with a SELECT-only/keyword-denylist/ScriptDom gate);
cache poisoning (keys incorporate tenant hash + secret-stripped connection string);
AOT/type-confusion (`Type.GetType`/`Activator` operate only on fixed literals or trusted
model metadata, never query-result strings); and the full tenant-isolation surface (fast
path hard-bails under a tenant provider; joins/GroupJoin/nav-subqueries each carry the
predicate; tenant value always parameterized; pooling resets tenant state).

## Residual / deferred (non-exploitable, tracked)

- **Supply chain (Medium, process) — mostly closed.** Added reproducible-restore lockfiles
  (`RestorePackagesWithLockFile`, `packages.lock.json` committed per project) and a CI
  `supply-chain` job that runs `dotnet list package --vulnerable --include-transitive` and
  **fails the build on any advisory not in `eng/vulnerability-allowlist.txt`**. That gate
  immediately surfaced **CVE-2025-6965 / GHSA-2m69-gcr7-jv3q** (High): the SQLite bundled by
  `SQLitePCLRaw.lib.e_sqlite3` (transitive via `Microsoft.Data.Sqlite 8.0.x`) is < 3.50.2 and
  can corrupt memory on a crafted aggregate query. **No fixed SQLitePCLRaw exists** (the whole
  2.1.x line is affected); nORM now pins the newest available native bundle (2.1.11) as
  best-effort and allowlists the advisory with justification (LOW nORM exposure — aggregate SQL
  is generated from typed LINQ with parameterized values, not attacker-driven aggregate-term
  counts). Re-check and bump when upstream ships SQLite 3.50.2+. Central Package Management (CPM)
  remains a further optional step.
- **Redaction/validation regexes (Low):** run without a `MatchTimeout`, but only over
  `MaxSqlLength`-bounded, mostly-generated text — no practical ReDoS. Hardening only.
- **Temporal history-column type string (Low):** history-table DDL concatenates a
  DB-read column *type* string raw (the name is escaped) — migration/DDL-time, DBA-scoped.

## Independent user audit (F1–F8) — reproduced and fixed

A separate whole-codebase cybersecurity audit (empirical, reproduced against SQLite and a live MySQL
8.0 instance) surfaced findings the two agent sweeps under-weighted — most importantly that the
query-translator projection sites inline **runtime** string values, not just compile-time constants.
This corrected an earlier under-fix here (the escaper-only pass #8 assumed those sites were
developer-constants-only instead of run-and-diffing them). All are now fixed with fail-first / run-and-diff
regressions.

| # | Sev | Finding | Status | Commit | Test |
|---|-----|---------|--------|--------|------|
| F1 | **High** | Projection string ops (`Contains`/`StartsWith`/`EndsWith`, `string.Join` separator, `TrimEnd`) inline runtime values → MySQL backslash breakout | **Fixed** | `15d5c2b7` | `ProjectionStringMatchClosureCacheTests` |
| F2 | **High** | Same inlined values baked into the cached plan → cross-caller plan-cache poisoning (silent-wrong) | **Fixed** | `15d5c2b7` | `ProjectionStringMatchClosureCacheTests` |
| F3 | Medium | MySQL `/*! … */` executable comments stripped as inert → validator bypass | **Fixed** | `df413b2d` | `RawSqlCommentBypassTests` |
| F4 | Medium | Block-comment nesting differential (MySQL/SQLite don't nest) → text after first `*/` hidden | **Fixed** | `df413b2d` | `RawSqlCommentBypassTests` |
| F5 | Low | `load_extension`/`readfile`/`writefile`/`pragma_*` bypass the `_`-joined token checks | **Fixed** | `df413b2d` | `RawSqlDenylistAndIdentifierTests` |
| F6 | Low | `IsSafeIdentifier` `^[\w\s]+$` admitted newlines/tabs (statement breaks) | **Fixed** | `df413b2d` | `RawSqlDenylistAndIdentifierTests` |
| F7 | Low | Threat-model T1 evidence overstated ("only two interpolated CommandText sites") | **Fixed** | this doc + `security-threat-model.md` | — |
| F8 | Low | `"SCRIPT"` dangerous-pattern was a substring → rejected `script_id`/`transcript` | **Fixed** | `df413b2d` | `RawSqlDenylistAndIdentifierTests` |

The F1/F2 fix mirrors the WHERE path: values in positions that accept a bound parameter are parameterized;
values in literal-only positions (LIKE-with-wildcards, GROUP_CONCAT `SEPARATOR`, TRIM set) are escaped via
`EscapeStringLiteral` AND mark the plan fold-no-cache (`HasClosureFoldedIntoSql`) so a runtime value is never
reused across executions. Compile-time constants (format templates, enum names, type metadata) only need the
escape. The LINQ fuzzer was also extended to re-run each shape with two different closure values (it
previously executed each shape once, which is why it missed F2) — see `fuzzer-dry-log.md`.

## Reproduce

```
dotnet test tests/ -c Release --filter "FullyQualifiedName~ExecuteUpdateStringLiteralParameterization|FullyQualifiedName~SqliteRegexpUdfTimeout|FullyQualifiedName~TphDiscriminatorImmutableOnUpdate|FullyQualifiedName~BulkWriteReadOnlyEntityGuard|FullyQualifiedName~CacheLockDrainBound|FullyQualifiedName~GreatestNPerGroupTenantIsolation|FullyQualifiedName~SqlServerMigrationConstraintQuoteName|FullyQualifiedName~ProviderStringLiteralEscape|FullyQualifiedName~ProjectionStringMatchClosureCache|FullyQualifiedName~ProjectionClosureValueReuseFuzz|FullyQualifiedName~RawSqlCommentBypass|FullyQualifiedName~RawSqlDenylistAndIdentifier"
```
