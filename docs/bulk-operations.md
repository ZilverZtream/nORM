# Bulk Operation Contract

Bulk operations are stable v1 APIs for high-volume inserts, updates, and
deletes. The contract applies to both provider-native paths and fallback batched
SQL paths.

## Public APIs

| API | Result |
| --- | --- |
| `BulkInsertAsync<T>` | Returns the number of rows inserted. |
| `BulkUpdateAsync<T>` | Returns the number of rows updated. Rows blocked by tenant or concurrency predicates are not counted. |
| `BulkDeleteAsync<T>` | Returns the number of rows deleted. Rows blocked by tenant or concurrency predicates are not counted. |

Input sequences are enumerated once at the context API boundary. Null inputs and
invalid entity shapes fail before provider execution.

## Semantic Rules

- Entity validation runs before execution.
- Tenant validation runs before execution, and update/delete SQL includes tenant
  predicates when a mapped tenant column exists.
- Key columns identify update/delete targets.
- Timestamp/concurrency columns participate in update/delete predicates where
  the provider path supports optimistic concurrency checks.
- Database-generated columns are excluded from insert column lists.
- Cache tags for the mapped table are invalidated after successful bulk writes.
- Command interceptors run for commands issued through nORM bulk paths.
- Cancellation tokens are honored before and during command execution. Once a
  nORM-owned transaction reaches commit or rollback, completion uses
  `CancellationToken.None` to avoid ambiguous partial cleanup.

## Transactions and Partial Failures

If the context has an active transaction, bulk operations use it and the caller
owns commit or rollback. If no transaction is active, nORM creates and owns a
transaction for the operation where the provider path supports transactional
execution.

When nORM owns the transaction and a batch fails, nORM rolls back and rethrows
the original exception. If rollback also fails, nORM throws an
`AggregateException` containing both the original and rollback exceptions.

Provider-native copy APIs can have provider-specific server behavior. The v1
contract is that nORM either executes them inside the active/owned transaction or
documents the provider limitation before marketing that path as transactional.

## Provider Paths

| Provider | Insert Path | Update/Delete Path |
| --- | --- | --- |
| SQL Server | `SqlBulkCopy` where applicable, fallback SQL otherwise | Provider-specific set-based/batched paths |
| PostgreSQL | Native copy path where applicable, fallback SQL otherwise | Provider-specific set-based/batched paths |
| MySQL | Native bulk copy when driver support is available, fallback SQL otherwise | Provider-specific set-based/batched paths |
| SQLite | Optimized batched SQL | Batched SQL |

Applications can inspect `DatabaseProvider.Capabilities.SupportsNativeBulkInsert`
to decide whether a provider-native insert path is part of the active provider
contract.

## Testing and Evidence

The v1 gate covers empty inputs, tenant validation, tenant-scoped updates and
deletes, transaction nesting and rollback, cache invalidation, interceptors,
provider parameter limits, generated column handling, cancellation, and provider
bulk SQL generation. Benchmark claims must state whether they measure native
bulk, fallback batched SQL, or another explicitly named path.
