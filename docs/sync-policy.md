# nORM Sync and Async Policy

nORM is async-first for writes and database lifecycle operations. The stable v1
write API is `SaveChangesAsync`, direct `InsertAsync` / `UpdateAsync` /
`DeleteAsync`, async bulk operations, async migrations, and async raw SQL.

There is no synchronous `SaveChanges` API. Applications should not block on
`SaveChangesAsync` with `.Result`, `.Wait()`, or `.GetAwaiter().GetResult()`.
Those patterns can deadlock under UI, legacy ASP.NET, custom schedulers, or other
single-threaded synchronization contexts.

## Supported Sync APIs

nORM keeps synchronous query helpers as a supported compatibility surface:

- `ToListSync`
- `CountSync`
- synchronous `IEnumerable<T>` enumeration from nORM queries

These helpers use nORM's synchronous query pipeline and synchronous ADO.NET
reader APIs. They are not implemented by blocking on `ToListAsync` or
`CountAsync`, so they do not depend on continuation posting to a captured
`SynchronizationContext`.

Use sync query APIs only when the caller is already synchronous and cannot
reasonably be made async. Prefer async APIs in request handlers, background
services, UI event handlers, and any path that needs cancellation.

## Unsupported Sync Patterns

The following are outside the v1 support contract:

- blocking on `SaveChangesAsync` or other async write APIs
- blocking on migrations, scaffolding, temporal bootstrap, or provider startup
  APIs from a single-threaded synchronization context
- relying on sync query helpers for cancellation
- mixing sync and async operations concurrently on the same `DbContext`

`DbContext` remains short lived and not thread safe regardless of sync or async
entry point.

## Temporal Versioning Note

Temporal versioning has async initialization requirements. A sync query can
trigger temporal bootstrap when temporal versioning is enabled. nORM's bootstrap
path avoids captured continuations internally, but applications that use temporal
versioning should prefer async query APIs so cancellation and startup errors
flow naturally through the async pipeline.
