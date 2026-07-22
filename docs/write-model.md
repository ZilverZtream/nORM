# Write model: direct writes vs change tracking

nORM offers three ways to write. Unlike EF Core (tracking only) or Dapper (direct only), it
supports all three - this guide is about *which to use when*. For the tracking mechanics
themselves see [Change Tracking](change-tracking.md). All writes are async-only (there is no
synchronous `SaveChanges`); see [Sync and Async Policy](sync-policy.md).

## The three modes

### 1. Direct writes - immediate, single entity

`InsertAsync(entity)`, `UpdateAsync(entity)`, and `DeleteAsync(entity)` execute one statement
immediately and return the affected row count. No `SaveChangesAsync` call is needed.

```csharp
await ctx.InsertAsync(user);      // one INSERT now; a store-generated key (a plain int Id by
                                  // convention, or [DatabaseGenerated(Identity)]) is read back onto user
user.Email = "new@example.com";
await ctx.UpdateAsync(user);      // one UPDATE now
await ctx.DeleteAsync(user);      // one DELETE now
```

Use them for: simple one-shot writes, scripts and background jobs, and handlers that write a
single row where you do not need graph or unit-of-work semantics. They still honour tenant
filters, retry policies, optimistic-concurrency tokens, and the current transaction.

### 2. Change tracking - a unit of work over an object graph

`Add`/`Update`/`Remove`/`Attach` register intent; `SaveChangesAsync()` computes the diff and
writes everything as one ordered, batched unit, with relationship fixup, cascade, and
optimistic-concurrency batching.

```csharp
ctx.Add(order);                   // order and its order.Lines tracked as a graph
order.Lines.Add(new OrderLine { /* ... */ });
await ctx.SaveChangesAsync();     // INSERT/UPDATE/DELETE in dependency order, one unit
```

Use it for: request-scoped business logic over related entities, object graphs (parent plus
children), and anywhere add/modify/remove must commit atomically in the correct order. This is
the EF Core-style path and pairs with the DI-scoped `DbContext` (see the README DI section).

### 3. Bulk and set-based - many rows

- `BulkInsertAsync` / `BulkUpdateAsync` / `BulkDeleteAsync(items)` push many rows efficiently via
  provider-native paths (see [Bulk Operations](bulk-operations.md)).
- `query.ExecuteUpdateAsync(...)` / `query.ExecuteDeleteAsync()` run a set-based `UPDATE` /
  `DELETE` on the server with no materialisation.

Use these when the row count is large or the operation is naturally set-based ("archive every
order older than 90 days").

## Choosing

| You have... | Use |
|---|---|
| one entity, no graph | direct `InsertAsync` / `UpdateAsync` / `DeleteAsync` |
| a related object graph, or add/modify/remove that must commit together | `Add` / `Remove` + `SaveChangesAsync` |
| many rows | `Bulk*Async` |
| a server-side set operation over a predicate | `ExecuteUpdateAsync` / `ExecuteDeleteAsync` |

## Mixing modes on the same entity

Direct writes are tracker-aware and reconcile with pending tracked state, so the historical
mixing hazards are handled rather than silent:

- Adding an entity (tracked, pending INSERT) and then calling `DeleteAsync` on it is a net no-op:
  the pending insert is discarded, not double-processed.
- A direct `UpdateAsync` on an entity whose INSERT is still pending writes nothing; the pending
  INSERT already carries the current values.

Even so, the clearest mental model is **one mode per unit of work per entity**: either drive an
entity through the tracker to `SaveChangesAsync`, or write it directly - do not interleave both
on the same entity within one logical operation. Reads used only to compute a write can use
`AsNoTracking()` to skip tracking cost; entities you intend to modify and `SaveChangesAsync`
should stay tracked (the default).

## See also

- [Change Tracking](change-tracking.md) - entity states, attach/detach, fixup, PK-mutation rules.
- [Optimistic Concurrency](optimistic-concurrency.md) - concurrency tokens across every write path.
- [Bulk Operations](bulk-operations.md) - provider-native bulk semantics.
