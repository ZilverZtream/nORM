# Provider Capabilities

Each `DatabaseProvider` exposes a `Capabilities` descriptor for startup
validation, diagnostics, and release documentation. `IsAvailableAsync` remains
the runtime probe that checks whether the driver can be loaded and a compatible
server can be reached.

| Provider | Minimum Version | Notes | JSON | Temporal | Native Bulk Insert | Savepoints | Driver |
| --- | --- | --- | --- | --- | --- | --- | --- |
| SQL Server | 13.0 / SQL Server 2016 | JSON support requires 2016 | Yes | Yes | Yes | Yes | `Microsoft.Data.SqlClient` |
| PostgreSQL | 12+ | | Yes | Yes | Yes | Yes | `Npgsql` |
| MySQL | 8.0+ | RENAME COLUMN requires 8.0 | Yes | Yes | Yes | Yes | `MySqlConnector` or `MySql.Data` |
| SQLite | 3.25+ | RENAME COLUMN requires 3.25 | JSON1-dependent | Yes | No | Yes | `Microsoft.Data.Sqlite` |

## Startup Validation

Applications that need hard startup validation should call
`provider.IsAvailableAsync()` during service startup and fail deployment if it
returns `false`. That probe validates driver availability and minimum server
version for the provider's default local connection behavior.

Every `DbContext` connection initialization also validates the actual opened
connection against `Capabilities.MinimumServerVersion`. Unsupported server
versions fail before query execution with a `NormConfigurationException` that
names the provider, actual version, and minimum supported version.

Applications with non-local databases should still validate the actual
configured connection during service startup. Open the configured connection,
construct the matching provider, and call `InitializeConnectionAsync` or create
a short-lived `DbContext` and run a startup probe. Then use the capability
descriptor to enforce feature requirements. For example, require
`Capabilities.SupportsNativeBulkInsert` before enabling a native bulk-only path.

## Feature Flags

Capability flags describe nORM's v1 provider contract, not every feature a
database engine might support. A provider can still choose fallback
implementations for a feature. SQLite, for example, does not advertise native
bulk insert because nORM uses optimized batched SQL rather than a provider-native
copy API.

Unsupported database versions and missing drivers fail early with actionable
messages instead of surfacing later as translation or command execution errors.
