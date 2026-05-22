# Provider Capabilities

Each `DatabaseProvider` exposes a `Capabilities` descriptor for startup
validation, diagnostics, and release documentation. `IsAvailableAsync` remains
the runtime probe that checks whether the driver can be loaded and a compatible
server can be reached.

| Provider | Minimum Version | JSON | Temporal | Native Bulk Insert | Savepoints | Driver |
| --- | --- | --- | --- | --- | --- | --- |
| SQL Server | 13.0 / SQL Server 2016 | Yes | Yes | Yes | Yes | `Microsoft.Data.SqlClient` |
| PostgreSQL | 9.5 | Yes | Yes | Yes | Yes | `Npgsql` |
| MySQL | 8.0 | Yes | Yes | Yes | Yes | `MySqlConnector` or `MySql.Data` |
| SQLite | 3.9 | JSON1-dependent | Yes | No | Yes | `Microsoft.Data.Sqlite` |

## Startup Validation

Applications that need hard startup validation should call
`provider.IsAvailableAsync()` during service startup and fail deployment if it
returns `false`. That probe validates driver availability and minimum server
version for the provider's default local connection behavior.

Applications with non-local databases should validate the actual configured
connection by opening it during startup and then use the provider capability
descriptor to enforce feature requirements. For example, require
`Capabilities.SupportsNativeBulkInsert` before enabling a native bulk-only path,
or require `Capabilities.MinimumServerVersion` in operational checks.

## Feature Flags

Capability flags describe nORM's v1 provider contract, not every feature a
database engine might support. A provider can still choose fallback
implementations for a feature. SQLite, for example, does not advertise native
bulk insert because nORM uses optimized batched SQL rather than a provider-native
copy API.

Unsupported database versions or missing drivers should fail early with
actionable messages instead of surfacing later as translation or command
execution errors.
