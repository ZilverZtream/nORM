# Temporal Precision

nORM-managed temporal history uses provider database clocks in triggers. Built-in
providers also create temporal tags from the same database clock where possible
so `AsOf(tag)` compares timestamps from one clock source. The API is
provider-neutral, but timestamp precision is not identical across database
engines.

## Contract

- Temporal APIs normalize `DateTimeKind.Local` to UTC and store/read UTC-like
  timestamps as provider values.
- `AsOf(DateTime)` and `AsOf(tag)` use half-open validity windows:
  `ValidFrom <= timestamp < ValidTo`.
- `CreateTagAsync(name)` stores a provider UTC database-clock timestamp for the
  built-in providers.
- Insert, update, and delete history rows use the provider trigger clock.
- Tests that create a tag immediately before a write must leave enough time for
  the provider's timestamp precision. SQLite requires a delay when the test
  needs deterministic before/after ordering.

## Provider Notes

| Provider | Trigger Clock | Practical Precision Note |
| --- | --- | --- |
| SQL Server | `SYSUTCDATETIME()` | High precision UTC database clock for history and tags. |
| PostgreSQL | `now()` / timestamp columns | Transaction timestamp semantics; avoid assuming every row in one transaction has a different instant. |
| MySQL | `UTC_TIMESTAMP(6)` | nORM bootstraps tag/history period columns as `DATETIME(6)` and upgrades existing tag tables to that precision. |
| SQLite | SQLite datetime expressions | Second-level precision in the v1 trigger and tag contract. Use a delay between tag and write in deterministic tests. |

## Guidance

Use tags for business events such as "before import" or "release cut" rather
than for sub-millisecond ordering. When a workflow needs strict ordering inside
one timestamp bucket, use the temporal history ordering returned by
`GetTemporalHistoryAsync<T>()`, which sorts by validity start and version id.

Do not compare provider-specific timestamp strings directly in application
code. Use nORM temporal APIs or parse values as UTC timestamps with invariant
culture.
