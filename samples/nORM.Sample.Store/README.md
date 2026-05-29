# nORM Sample Store

This sample is a small tenant-aware store web app that runs the same backend
workflow on SQLite, SQL Server, PostgreSQL, and MySQL by changing only provider
configuration. It includes a browser frontend, cookie-based tenant login,
authenticated APIs, bulk writes, and temporal history actions.

It demonstrates the v1 supported shape for:

- generated-path tenant boundaries;
- provider-swappable application code;
- representative SQL-backed LINQ queries;
- bulk insert;
- compiled query;
- `Include().AsSplitQuery()`;
- nORM-managed temporal history with `AsOf(tag)` and provider-neutral history rows.

It is not a complete EF Core compatibility claim and it is not evidence that raw SQL or stored procedures receive automatic tenant predicates.

## Run

SQLite needs no server. This starts the web app:

```powershell
dotnet run --project samples/nORM.Sample.Store -- --provider sqlite
```

Open the printed local URL and sign in as Tenant A or Tenant B.

Server providers use sample-specific connection string variables, with the live-test variables as fallback:

```powershell
$env:NORM_SAMPLE_SQLSERVER='Server=localhost\SQLEXPRESS;Database=normtest;Integrated Security=True;TrustServerCertificate=True;Encrypt=False;Connect Timeout=10'
$env:NORM_SAMPLE_POSTGRES='Host=127.0.0.1;Port=5432;Database=normtest;Username=postgres;Password=<password>'
$env:NORM_SAMPLE_MYSQL='Server=127.0.0.1;Port=3306;Database=normtest;User ID=root;Password=<password>;AllowPublicKeyRetrieval=True'

dotnet run --project samples/nORM.Sample.Store -- --provider sqlserver
dotnet run --project samples/nORM.Sample.Store -- --provider postgres
dotnet run --project samples/nORM.Sample.Store -- --provider mysql
```

To run every configured provider:

```powershell
dotnet run --project samples/nORM.Sample.Store -- verify-providers
```

`verify-providers` prints a provider-by-provider PASS/SKIP/FAIL summary. SQLite
always runs against an isolated in-memory database. SQL Server, PostgreSQL, and
MySQL are skipped when neither `NORM_SAMPLE_*` nor `NORM_TEST_*` is configured.

To run the strict provider mobility certification gate and write a report:

```powershell
dotnet run --project samples/nORM.Sample.Store -c Release --no-build -- certify-provider-swap --report ../../artifacts/provider-swap/sample-store.json
```

`certify-provider-swap` fails when any requested provider is missing or fails.
Use `--providers sqlite,postgres` to certify a smaller target set; aliases such
as `mssql` and `postgresql` are canonicalized before the run. `mariadb` maps to
the MySQL-compatible profile for inventory convenience; it is not a separate
MariaDB-certified provider target in v1. The
machine-readable JSON report records the provider mobility contract name,
strict mode, scan status, per-provider status, provider capability profile, and
the checks that passed. It also includes error/warning totals and recommended
fix/review rows for source findings and provider-target caveats. When a
provider opens successfully, the capability profile includes the actual
connected server version parsed from the driver.
SQLite certification uses an isolated in-memory database so repeated or
parallel certification runs do not collide on a shared sample file.
`ScanStatus` is `NotRequested`, `Pass`, or `Fail`; this keeps an empty findings
list from looking like a source scan ran when it did not. The certification
scenario runs with `DbContextOptions.UseStrictProviderMobility()`, so raw SQL,
stored procedures, direct connection/provider access, raw transaction/command
handles, command interceptors, provider-native tenant DDL, provider-native
temporal storage, and client-eval projection tails cannot accidentally pass as
provider-mobile application paths.
Relative report paths are resolved from the sample app working directory; the
`../../artifacts/...` form writes to the repository-level artifact folder when
the command is run from the repository root.

To include a conservative source inventory of provider-bound nORM usage:

```powershell
dotnet run --project samples/nORM.Sample.Store -c Release --no-build -- certify-provider-swap --scan-path src/MyApp --report ../../artifacts/provider-swap/myapp.json
```

The scan flags raw SQL APIs, stored procedure APIs, direct `DbContext.Connection`
access, direct `DbContext.Provider` access, raw `DbTransaction`/`DbCommand`
handles, command interceptors, provider-specific connection classes/packages, dynamic `Query(string)` table
queries, custom `[SqlFunction]` SQL fragments, `[CompileTimeQuery]` raw SQL, provider-native tenant/temporal
configuration, client-evaluation opt-ins, `.sql` stored procedure definitions,
and obvious provider-specific SQL syntax. Findings are not deleted or hidden.
They are remediation work: replace them with generated nORM APIs where the
semantics are clear, emulate them in nORM if that can be proven equivalent, or
mark them for human review when the database-specific behavior cannot be
inferred safely.

## Web App Workflow

The browser app demonstrates:

- cookie-based tenant login for Tenant A and Tenant B;
- tenant-scoped product and order dashboard APIs;
- redacted tenant-boundary diagnostics for the active mapped entity;
- `Include().AsSplitQuery()` for order lines;
- `GroupBy` aggregate totals;
- DTO projection;
- compiled query inside the verification scenario;
- bulk event insertion;
- temporal tag creation, price update, `AsOf(tag)` readback, history timeline
  inspection, changed-property diffing, restore-from-tag, and tenant-scoped
  closed-history pruning.

The backend intentionally uses nORM generated paths for tenant-sensitive
queries and writes. The scenario runner behind `verify-providers` is reused by
the app's `/api/admin/verify` endpoint so the UI and gate exercise the same
product-proof workflow.

## Provider-Swappable Meaning

Provider-swappable means this sample keeps the same domain model, tenant options, temporal options, and application scenario while changing the connection and nORM provider. It does not mean every possible SQL dialect feature, raw SQL statement, stored procedure, migration, or provider-native type behaves identically without review.

For existing applications, provider-bound assets such as hundreds of SQL Server
stored procedures should be treated as portability findings. Where the intent is
clear, rewrite them to generated nORM LINQ/write/temporal APIs. Where the
database code encodes business logic that cannot be inferred safely, the
certificate should flag it with a remediation note instead of pretending it is
portable.

DDL and provider setup are still provider-aware. Application migrations own live-table schema changes. nORM temporal bootstrap creates history tables, tags, and triggers for the mapped model.

## Tenant Boundary

The sample uses `DbContextOptions.TenantProvider` and `TenantColumnName = "TenantId"`. Generated query and write paths run with tenant predicates:

- tenant A product/order queries do not return tenant B rows;
- cross-tenant `ExecuteUpdateAsync` by a known tenant B ID affects zero rows;
- cross-tenant `ExecuteDeleteAsync` by a known tenant B ID affects zero rows;
- compiled query preserves the current tenant boundary;
- `Include().AsSplitQuery()` loads tenant-visible order lines.

Raw SQL, stored procedures, migrations, scaffolding, and direct connection usage are privileged/caller-controlled paths. They do not receive automatic tenant predicate injection.

## Temporal Versioning

The sample enables nORM-managed temporal versioning with `EnableTemporalVersioning()`. nORM creates history tables, trigger/function infrastructure, and the `__NormTemporalTags` table.

The scenario creates a tag before updating a product, then proves:

- `AsOf(tag)` returns the old tenant A product state;
- a current query returns the new product state;
- `GetTemporalHistoryAsync<T>()` returns operation and validity metadata for
  the tenant-visible product history;
- `GetTemporalDiffAsync<T>()` returns changed mapped properties for the
  tenant-visible product timeline;
- `RestoreTemporalVersionAsync<T>()` restores the existing current product from
  a tenant-visible tag without reinserting deleted rows;
- `PruneTemporalHistoryAsync<T>()` removes closed history rows for the active
  tenant while global tag pruning remains an administrative path;
- tenant B remains invisible through the temporal query.

This is provider-neutral nORM temporal history. It is not SQL Server system-versioned temporal tables, and it does not imply automatic migration cleanup for renames or rollbacks. Temporal bootstrap requires DDL permissions and triggers add write overhead.
