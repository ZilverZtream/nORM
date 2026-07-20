# Provider Packages

nORM uses one runtime package, published on NuGet as `TheNorm` (the `nORM` id is
taken; the API namespace is still `nORM`), with a deliberately mixed dependency
model:

| Provider | nORM type | Driver dependency | Why |
| --- | --- | --- | --- |
| SQL Server | `SqlServerProvider` | Included: `Microsoft.Data.SqlClient` | The provider uses typed SQL Server APIs for parameters, error handling, retries, savepoints, and bulk copy. |
| SQLite | `SqliteProvider` | Included: `Microsoft.Data.Sqlite` | SQLite is the default local/test provider and is required by scaffolding and package smoke tests. |
| PostgreSQL | `PostgresProvider` | User installs `Npgsql` | The provider is available from the core package, but loads Npgsql types by reflection so the core package does not force PostgreSQL dependencies into every app. |
| MySQL | `MySqlProvider` | User installs `MySqlConnector` or `MySql.Data` | The provider is available from the core package, but loads MySQL types by reflection so the core package does not force MySQL dependencies into every app. |

This keeps the v1 install path simple while avoiding accidental PostgreSQL and
MySQL transitive dependencies for applications that do not use those providers.

## Installation

All applications install the runtime package:

```bash
dotnet add package TheNorm
```

For PostgreSQL, also install Npgsql:

```bash
dotnet add package Npgsql
```

For MySQL, install one supported driver:

```bash
dotnet add package MySqlConnector
```

or:

```bash
dotnet add package MySql.Data
```

SQL Server and SQLite do not require additional driver packages beyond `TheNorm`.

## Missing Driver Behavior

`new PostgresProvider()` and `new MySqlProvider()` are public and usable from the
runtime package. They create provider-specific parameters through reflection.
If the matching driver is missing when a command needs provider parameters, nORM
throws an actionable `InvalidOperationException` naming the required package.

The same rule applies to `DbConnectionFactory.Create(...)`: connection creation
for PostgreSQL or MySQL requires the matching driver assembly to be available.

## Package Governance

The package-consumer release tests validate this dependency contract by
inspecting the produced `.nupkg`:

- SQL Server and SQLite remain explicit runtime dependencies.
- PostgreSQL and MySQL drivers are not transitive dependencies of `nORM`.
- The runtime package contains XML docs, README, source generator analyzer, and
  SourceLink/symbol package artifacts.
- The `dotnet-norm` tool package can be installed from the produced `.nupkg` and
  run as a local tool.

If the package architecture changes after v1, the dependency table above and the
package-consumer tests must change in the same commit.
