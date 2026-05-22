# Optimistic Concurrency Contract

nORM treats `[Timestamp]` and equivalent mapping metadata as an optimistic
concurrency token. Updates and deletes include the original token value in the
`WHERE` predicate. A missing match raises `DbConcurrencyException` on normal
tracked writes and direct writes.

## Provider Row-Count Modes

| Provider | Default nORM mode | Guarantee |
| --- | --- | --- |
| SQL Server | Matched-row semantics | Full row-count OCC checks for updates and deletes. |
| PostgreSQL | Matched-row semantics | Full row-count OCC checks for updates and deletes. |
| SQLite | Matched-row semantics | Full row-count OCC checks for updates and deletes. |
| MySQL / MariaDB | Affected-row semantics | SELECT-then-verify fallback detects stale tokens except the same-value token edge case described below. |

MySQL drivers commonly report affected rows for UPDATE statements. A successful
UPDATE that matched the row but wrote the same values can return `0`, which is
ambiguous with a stale token conflict. nORM therefore verifies a zero-row update
by checking whether the original key and token still exist.

That fallback prevents false-positive conflicts for same-value updates and
detects ordinary stale-token conflicts. It cannot detect the pathological case
where a concurrent writer changes the row but leaves the concurrency token equal
to the original token value.

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
conflicts impossible, such as monotonically increasing versions.

## Delete Semantics

Deletes do not have the same-value update ambiguity. A deleted row is counted as
affected, so nORM always treats a zero-row delete with a concurrency token as a
conflict.
