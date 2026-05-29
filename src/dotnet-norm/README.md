# dotnet-norm CLI

`dotnet-norm` provides command-line tooling for the nORM ORM framework. Use it to scaffold, validate, and manage nORM projects from the terminal.

## Features
- Scaffold entity classes and a DbContext from existing SQLite, SQL Server,
  PostgreSQL, and MySQL schemas.
- Validate connection strings before applying migrations.
- Generate boilerplate configuration for nORM projects.
- Certify provider mobility by scanning app source and nORM schema metadata.
- Offer helpful diagnostics to keep projects aligned with nORM best practices.

## Getting Started
Install the tool locally using the .NET CLI:

```bash
dotnet tool install --global dotnet-norm
```

Then run the tool from your project directory:

```bash
norm --help
```

The tool package follows the same versioning as the nORM repository release it ships with.

## Scaffolding

Use `norm scaffold` to bootstrap a nORM model from an existing database:

```bash
norm scaffold --provider sqlite --connection "Data Source=app.db" --output Models --namespace App.Data
norm scaffold --provider postgres --connection "$NORM_POSTGRES" --tables public.customer,public.order --no-overwrite --fail-on-warnings
```

The scaffolder emits nullable-enabled entity classes, `[Table]`/`[Column]`/
`[Key]`/identity/`[Required]`/`[MaxLength]` metadata, deterministic C#
identifier cleanup, de-duplicated generated names, `IQueryable<T>` context
properties backed by nORM's query provider, single-column FK navigations, and single-column/composite index
metadata, including columns that participate in multiple indexes. It is a
bounded bootstrap tool, not a database-first completeness claim; unsupported
composite foreign keys are reported in `nORM.ScaffoldWarnings.md` and
`nORM.ScaffoldWarnings.json`, while
composite FK navigation generation, owned-type, inheritance, and
provider-specific computed/default/trigger inference remain explicit
post-processing. Use `--fail-on-warnings` in CI to reject lossy scaffolds after
the warning report is written.

## Provider Mobility Certification

Use the portability command to inventory provider-bound code before claiming an
application is provider-mobile:

```bash
norm portability certify --scan-path src/MyApp --assembly bin/Release/net8.0/MyApp.dll --report artifacts/provider-mobility.json --html artifacts/provider-mobility.html
```

Use `--providers sqlite,postgres,mysql` to narrow the provider target capability
profile in the report. Aliases such as `mssql` and `postgresql` are
canonicalized to the nORM provider targets before reporting. `mariadb` currently
canonicalizes to the MySQL-compatible target profile; it is compatibility
inventory, not a separate MariaDB-certified provider claim. When omitted,
`--profile all-four` emits SQLite, SQL Server, PostgreSQL, and MySQL target
decisions. Unknown provider names are reported as error-level findings.

Pass target connection strings to turn the profile into live target evidence:

```bash
norm portability certify --scan-path src/MyApp --providers sqlite,postgres --sqlite-connection "Data Source=app.db" --postgres-connection "Host=..."
```

The CLI opens each supplied target, runs nORM provider startup validation, parses
the actual server version exposed by the driver, and records it in the provider
target decision. Open/version failures become `provider-target-open` or
`provider-target-capability` findings. Live target probes also execute a small
provider JSON expression so a target cannot claim JSON translation evidence when
the required database feature or extension is unavailable.
The tool package includes the supported live-provider drivers needed for these
target probes.

The source scan fails generated-path violations such as raw SQL, stored
procedures, direct provider/connection access from repositories, custom SQL
function fragments, command interceptors, dynamic table queries, provider-native
tenant/temporal options, and client-evaluation opt-ins. Provider package and
connection bootstrapping is warning-level inventory when it stays in the
composition root. Build/generated folders such as `bin`, `obj`, `artifacts`,
`.tmp`, `node_modules`, and `packages` are ignored so reports stay focused on
application source. The scanner covers `.cs`, `.sql`, `.csproj`, `.props`, and
`.targets` files so source usage and package-level provider dependencies are
both visible in the certificate.
The scanner also inventories common migration-source risks from EF Core and
Dapper applications, including EF provider selection, EF raw SQL APIs,
provider-specific SQL defaults/computed columns, collations, provider migration
annotations, value-generation annotations, Dapper package/SQL entry points, EF
provider package references, and direct ADO.NET provider command/data-adapter
objects. Constrained LINQ shapes such as regex predicates/replacement are
warning-level findings because they depend on target-specific regex support.

The scanner shares its support class, severity, reason, and suggested-fix
contract with runtime strict mode through
`nORM.Configuration.ProviderMobilityTranslator`. See
`docs/provider-mobility-translation-layer.md` in the repository for the full
decision table. Findings keep that centralized decision text and append
pattern-specific remediation when a concrete EF/Dapper/migration pattern has a
more specific fix. JSON/HTML reports include provider target decisions for minimum
server versions, JSON, temporal, bulk, savepoint, parameter-limit behavior, and
concrete translation strategies such as paging, identifier escaping, parameter
binding, boolean predicates, null semantics, LIKE escaping, string
concatenation, DateTime/decimal/TimeSpan normalization, temporal clock source,
generated-key retrieval, bitwise XOR, case-sensitive string comparison,
regex translation, temporal construction/arithmetic, row-tuple comparison,
ordered string aggregation, and SQL statement length
limits. Warning-level target decisions also appear in the recommended-fix
section.

Schema inspection runs when `--assembly` or `--schema-snapshot` is supplied. It
checks that the model uses provider-mobile CLR column types, portable defaults,
valid FK metadata, and identity columns that every migration generator can emit.
Database-evaluated defaults such as `CURRENT_TIMESTAMP` are warning-level review
items because precision/timezone/user semantics can vary even when the token is
accepted by every target.
