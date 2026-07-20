# Optimistic Concurrency Contract

nORM treats `[Timestamp]` and equivalent mapping metadata as an optimistic
concurrency token. Updates and deletes include the original token value in the
`WHERE` predicate. A missing match raises `DbConcurrencyException` on normal
tracked writes and direct writes.

## Token Types

nORM handles a `[Timestamp]` / `IsRowVersion()` token in one of two ways
depending on its CLR type:

| Token type | Behavior |
| --- | --- |
| `byte[]` | Native rowversion on SQL Server (DB-generated); nORM-managed on SQLite / PostgreSQL / MySQL. |
| `Guid`, `int`, `uint`, `long`, `ulong` | **nORM-managed** on SQLite / PostgreSQL / MySQL: nORM stamps a fresh value (a new `Guid` or an incremented integer) into the `SET` clause on every UPDATE and compares the original snapshot in `WHERE`. Fully automatic — a stale writer is always rejected. |
| any other type (e.g. `string`) | **Compare-only**: nORM emits the snapshot comparison in `WHERE`, but does not advance the token value itself. The application (or a database trigger) must change the token on every write for the protection to be effective. |

For an nORM-managed token you never assign the token value yourself — nORM owns
it. For a compare-only token you (or the database) own advancing it; if the value
never changes, two writers that loaded the same value will not conflict. Prefer a
`byte[]`, `Guid`, or integer token for automatic, no-discipline-required
protection.

## Provider Row-Count Modes

| Provider | Default nORM mode | Guarantee |
| --- | --- | --- |
| SQL Server | Matched-row semantics | Full row-count OCC checks for updates and deletes. |
| PostgreSQL | Matched-row semantics | Full row-count OCC checks for updates and deletes. |
| SQLite | Matched-row semantics | Full row-count OCC checks for updates and deletes. |
| MySQL / MariaDB | Strict gate by default | Timestamp-tracked updates are refused unless the provider is configured for matched-row semantics or the application explicitly accepts affected-row semantics. |

MySQL drivers commonly report affected rows for UPDATE statements. A successful
UPDATE that matched the row but wrote the same values can return `0`, which is
ambiguous with a stale token conflict. Because affected-row mode cannot provide
the full optimistic-concurrency guarantee, nORM's default v1 behavior is to fail
fast before saving timestamp-tracked updates when `UseAffectedRowsSemantics` is
true.

Applications may explicitly set `RequireMatchedRowOccSemantics=false` to accept
affected-row semantics. In that opt-in mode nORM verifies a zero-row update by
checking whether the original key and token still exist. That fallback prevents
false-positive conflicts for same-value updates and detects ordinary stale-token
conflicts. It cannot detect the pathological case where a concurrent writer
changes the row but leaves the concurrency token equal to the original token
value.

## Strict MySQL Mode

For full MySQL OCC guarantees, configure the driver to return matched rows and
create the provider with affected-row semantics disabled:

```csharp
var connectionString = "Server=localhost;Database=app;Uid=app;Pwd=secret;UseAffectedRows=false";
var provider = new MySqlProvider(useAffectedRowsSemantics: false);
```

When using a custom parameter factory:

```csharp
var provider = new MySqlProvider(parameterFactory, useAffectedRowsSemantics: false);
```

The provider option and connection string must agree. If the provider is set to
matched-row mode while the driver still reports affected rows, same-value updates
can be reported as concurrency conflicts even though the row matched.

## Default Safety Gate

`DbContextOptions.RequireMatchedRowOccSemantics` defaults to `true`. With the
default MySQL affected-row mode, nORM fails fast before saving timestamp-tracked
updates unless the application explicitly opts into the weaker MySQL semantics:

```csharp
var options = new DbContextOptions
{
    RequireMatchedRowOccSemantics = false
};
```

Only disable the gate when the application accepts the same-value token edge
case or uses an application-level token discipline that makes same-value token
conflicts impossible, such as monotonically increasing versions. This is a
deliberate weakened guarantee and should be called out in application review.

## MySQL Same-Value Limitation

> **MySQL same-value limitation**: When a concurrency token column's value is updated to the same value it already has, MySQL `UPDATE` affected-row count can remain 0 regardless of whether the row was concurrently modified. Use `byte[]`/`timestamp` columns that guarantee value changes on every update. The `RequireMatchedRowOccSemantics` option (default `true`) enables fail-fast behavior for this scenario.

## Delete Semantics

Deletes do not have the same-value update ambiguity. A deleted row is counted as
affected, so nORM always treats a zero-row delete with a concurrency token as a
conflict.
