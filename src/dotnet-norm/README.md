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
norm scaffold "Data Source=app.db" sqlite --output-dir Models -n App.Data -c AppDbContext
norm scaffold "Data Source=app.db" Microsoft.EntityFrameworkCore.Sqlite --project src/App/App.csproj --output-dir Models --context AppDbContext --no-build
norm scaffold "Name=ConnectionStrings:AppDb" Microsoft.EntityFrameworkCore.Sqlite --project src/App/App.csproj --output-dir Models
norm scaffold "Data Source=app.db" sqlite --output-dir Models --json
norm scaffold --provider sqlite --connection "Data Source=app.db" --output-dir Models -n App.Data -c AppDbContext -t Customer -d -f
norm scaffold --provider postgres --connection "$NORM_POSTGRES" --tables public.customer,public.order --no-overwrite --fail-on-warnings
norm scaffold --provider postgres --connection "$NORM_POSTGRES" --schema accounting --schema sales
norm scaffold --provider sqlite --connection "Data Source=app.db" --output Models --no-pluralize
norm scaffold --provider sqlite --connection "Data Source=app.db" --output Models --use-database-names
norm scaffold --provider sqlite --connection "Data Source=app.db" --output Models --namespace App.Data.Entities --context-dir Data/Contexts --context-namespace App.Data.Contexts
```

The scaffolder emits project-aware nullable entity classes, `[Table]`/`[Column]`/
`[Key]`/provider metadata-backed identity/computed/`[Timestamp]`/`[Required]`/`[MaxLength]` metadata,
generated fluent string/binary length, Unicode, and fixed-length facets,
deterministic C# identifier cleanup, de-duplicated generated names,
partial entity/context classes with an `OnModelCreatingPartial(ModelBuilder)`
hook for reviewed custom configuration, `IQueryable<T>` context properties backed by nORM's query provider,
generated context constructors for both `DbConnection` and connection strings
that still require an explicit `DatabaseProvider` and never hard-code the
scaffold connection string,
single-column and composite FK navigations when the constraint targets the
generated principal primary key or an exact ordered unfiltered unique index,
one-to-one reference navigations when the dependent FK columns are exact unique
keys, with
supported delete/update referential actions preserved, role-based self-referencing FK and self-join navigations, pure many-to-many join mappings including
schema-qualified, composite-key, shared-tenant-column, and alternate-key join
tables, and single-column/composite index
metadata, including columns that participate in multiple indexes. SQL Server
`IDENTITY(seed, increment)` metadata is emitted as `HasIdentityOptions(...)`
configuration for SQL Server migration round-trips. SQL Server
and PostgreSQL schemas are preserved, SQLite attached database schemas are
preserved, and MySQL uses the current database for discovery without emitting
the database/catalog name as a model schema. The CLI preflights all target files
before writing and refuses output conflicts by default, so a collision does not
leave a half-generated model. Use `--force`/`-f` to overwrite existing generated
files; `--no-overwrite` is accepted as an explicit guard.
Repeated scaffolds of the same schema are ordered deterministically for
reviewable diffs. Clean later runs remove stale `nORM.ScaffoldWarnings.*`
reports when overwrite is explicitly allowed, so old warnings do not leak into
CI logs.

`--tables` accepts comma-separated bare table names plus `schema.table` and
`schema.view` filters. Use repeatable `--table` for literal table names that
contain commas and must not be split; EF-style multi-value `--table First Second`
tokens are also accepted. Literal dotted table names are supported, but if a literal dotted
table name collides with the same text as a schema-qualified table, scaffolding
fails with an actionable error because the v1 filter syntax cannot disambiguate
those objects safely.
Blank CLI table/schema filters are rejected so an empty option cannot broaden
the run to every table.
`--schemas`, repeatable `--schema`, and EF-style multi-value
`--schema Accounting Sales` include all discovered user tables and supported
query artifacts from matching schemas, and are unioned with explicit table
filters. Schema filters apply where nORM preserves schema identity
(SQL Server, PostgreSQL, and SQLite attached databases including `main`);
SQLite `main` matches the unqualified default database and still emits
unqualified `[Table]` metadata. MySQL remains scoped to the current database
without emitting that database name as a model schema.
Generated `IQueryable<T>` context properties use collection-style names by
default, with deterministic cleanup for descriptor-like entity names. Use
`--no-pluralize` when you want singular query property names; entity class
names and database object names are unchanged.
Use `--use-database-names` when legal table, view, sequence, routine, column,
and routine result-column names should be preserved as generated CLR names
instead of normalized to PascalCase. Invalid C# identifiers and C# keywords are
still escaped or normalized so generated code compiles; synthetic navigation
members remain C#-style names derived from FK roles.
`--no-onconfiguring` is accepted for EF Core scaffold command compatibility; it
is a no-op because nORM generated contexts never emit `OnConfiguring` or a
hard-coded connection string.
EF-style aliases are accepted for common options: `--output-dir`/`-o`,
`-n`, `-c`, `-t`, `--data-annotations`/`-d`, and `--force`/`-f`.
EF-style positional arguments are also accepted as
`norm scaffold <connection> <provider> ...`; explicit `--connection` and
`--provider` options take precedence when both forms are supplied. A single
positional value after `--connection` is treated as the provider.
`norm dbcontext scaffold <connection> <provider> ...` is also accepted as an
EF-style alias for the same bounded nORM scaffold command, and `norm --help`
advertises `dbcontext` as an EF-style alias group.
The provider argument also accepts EF Core package names and normalizes them to
nORM providers, including `Microsoft.EntityFrameworkCore.SqlServer`,
`Microsoft.EntityFrameworkCore.Sqlite`, `Npgsql.EntityFrameworkCore.PostgreSQL`,
`Pomelo.EntityFrameworkCore.MySql`, and `MySql.EntityFrameworkCore`.
Named connection references such as `Name=ConnectionStrings:AppDb`,
`name=ConnectionStrings:AppDb`, or shorthand `Name=AppDb` are resolved from
environment variables first (for example `ConnectionStrings__AppDb`), then
startup-project and target-project user secrets declared through `UserSecretsId`, then `appsettings.json` / `appsettings.{Environment}.json` in
the startup project, target project, or current directory. nORM reads these
configuration files directly and does not execute startup code for this lookup.
Use `--project`/`-p` to target a `.csproj` or a directory containing exactly one
project; relative output paths are resolved under that project directory, and
the namespace defaults to the project's `RootNamespace`, `AssemblyName`, or
sanitized project file name plus sanitized output directory segments when
`--namespace` is omitted. If `--project` is omitted and the command working
directory contains exactly one `.csproj`, nORM uses that project for the same
defaults, including nullable-reference output. `Nullable` values `enable` and
`annotations` emit `#nullable enable`; `disable`, `warnings`, or an omitted
property emit `#nullable disable`. The nearest `Directory.Build.props` is read
before project overrides for `RootNamespace`, `AssemblyName`, `UserSecretsId`,
and `Nullable`. `--context-dir` follows EF-style placement: relative
paths are resolved under the target project directory, or under the command
working directory when `--project` is omitted. When `--context-dir` is supplied
without `--context-namespace`, a qualified `--context`, or an explicit
`--namespace`, the context namespace defaults to the project's root namespace
plus the sanitized context directory segments.
`--context` also accepts EF-style namespace-qualified names such as
`MyApp.Data.AppDbContext`; nORM generates class `AppDbContext` in namespace
`MyApp.Data`. If `--context-namespace` is also supplied, it overrides the
namespace portion while the final `--context` segment remains the class name.
`--namespace`, `--context-namespace`, and the namespace portion of qualified
`--context` values are validated as C# namespaces before generation starts.
Explicit `--context` class-name segments are validated as C# type identifiers
rather than silently corrected.
When `--context` is omitted, the CLI derives the context class name from the
database name, SQLite database file name, or final `Name=...` configuration-key
segment and appends `Context`; if no stable name is available, it falls back to
`AppDbContext`.
`--startup-project`/`-s`, `--framework`, `--configuration`, `--runtime`, and
`--no-build` are accepted for EF Core command-line compatibility. nORM
scaffolding connects directly to the database and does not build the target
project or load a startup application for schema discovery. Legacy EF-style
`--msbuildprojectextensionspath` is also accepted as a no-op because nORM
scaffold does not invoke MSBuild.
EF-style application arguments after `--` are accepted for command
compatibility. `-- --environment Production` is used only to include
`appsettings.Production.json` in named-connection lookup, with startup-project
environment files searched before target-project environment files when
`--startup-project` is supplied; other application arguments are ignored
because nORM does not execute startup code. When no
pass-through environment is supplied, `ASPNETCORE_ENVIRONMENT` and then
`DOTNET_ENVIRONMENT` select the matching `appsettings.{Environment}.json` file.
Unmatched scaffold tokens before `--` still fail fast.
EF-style `.config/dotnet-ef.json` defaults are read for `project`,
`startupProject`, `outputDir`/`output`, `namespace`, `context`, `contextDir`,
`contextNamespace`, `schema`/`schemas`, `table`/`tables`, `framework`,
`configuration`, `runtime`, `msbuildProjectExtensionsPath`, `verbose`,
`noColor`, `prefixOutput`, `noPluralize`, `useDatabaseNames`, `force`,
`noOverwrite`, `dryRun`, `failOnWarnings`, `emitRoutineStubs`,
`emitSequenceStubs`, `emitViewEntities`, and `emitQueryArtifacts`; relative project paths are resolved relative to the
parent of `.config`, comma-separated or array table/schema defaults are
accepted, and explicit CLI options override config values. When any CLI
table/schema filter is supplied, config table/schema defaults are ignored so
they cannot expand the explicit selection. Explicit CLI `--force` and
`--no-overwrite` values also override the opposite config default instead of
conflicting with it.
`--json` emits a machine-readable scaffold result summary for successful runs
and scaffold failures. `--verbose`/`-v`,
`--no-color`, and `--prefix-output` are accepted for EF Core command-line
compatibility because nORM scaffold output is already plain and explicit.
Unfiltered ordinary views and PostgreSQL materialized views are scaffolded by
default as read-oriented generated types; explicit `--table`/`--schema` filters
also include matching supported query artifacts. SQLite virtual tables and
SQL Server local table/view synonyms remain opt-in through explicit filters or
`--emit-query-artifacts`/`--emit-view-entities`.
`--data-annotations` is a no-op because nORM already emits supported annotation
metadata, and `--force` opts into overwriting existing generated files.
Use `--context-dir` to place the generated DbContext in a relative child
directory under the target project or current directory, and
`--context-namespace` to give it a separate namespace. Entity files and warning
reports remain in the entity output directory; split-namespace contexts,
including contexts split from a qualified `--context` value, import the entity
namespace automatically. Absolute context paths are available only through the
runtime `ScaffoldOptions.ContextOutputDirectory` API, not the CLI.

It is a bounded bootstrap tool, not a database-first completeness claim.
Composite foreign keys that do not target the generated principal primary key
or an exact ordered unfiltered unique index, pure junction tables whose FK/key/nullability
shape cannot be emitted safely, unmodeled complex/provider-specific defaults,
provider column types that cannot be
represented as safe scalar storage,
unparsed provider-specific identity strategies that make the generated entity read-only,
triggers, SQL Server provider-native
temporal tables, SQLite virtual tables/shadow tables, routines,
sequences, synonyms, and events are reported in
`nORM.ScaffoldWarnings.md` and `nORM.ScaffoldWarnings.json`. Composite FK
navigation generation beyond generated primary keys and exact ordered unfiltered
unique indexes, owned-type inference, inheritance inference, and provider-specific
schema semantics remain explicit post-processing. Use `--fail-on-warnings` in CI to
reject lossy scaffolds after the warning report is written.
Use `--dry-run` to validate scaffold output and print warning summaries without
creating, deleting, or overwriting files in the requested output path.
Tables and query artifacts without primary keys are generated with
`[ReadOnlyEntity]`, so they remain queryable but generated writes are rejected
before SQL generation until the model has a real key.
Tables with provider-owned triggers are also generated with `[ReadOnlyEntity]`
until trigger side effects are hand-modeled.
SQL Server provider-native temporal base and history tables are also generated
with `[ReadOnlyEntity]`; native period/history behavior remains provider-owned
until explicitly modeled.
Tables with unsafe provider-specific columns such as SQL Server/SQLite spatial
types, PostgreSQL `inet`, or MySQL spatial columns, plus unsafe MySQL `set(...)` declarations, are also generated with
`[ReadOnlyEntity]`; safe scalar promotions and MySQL
unsigned numeric widths remain ordinary generated properties, including
unsigned decimal/numeric precision and optional-scale metadata.
SQL Server alias types over scaffoldable scalar/binary bases remain diagnostics
for provider-mobility review, but generated writes stay enabled because nORM
binds the safe base CLR type and the database enforces the alias type.
PostgreSQL domains over safe scalar/array/enum base types remain diagnostics for
provider-mobility review, but generated writes stay enabled because nORM binds
the safe base CLR type, preserves bounded string/numeric facets where provider
metadata exposes them, and the database enforces the domain constraint.
Safe SQL defaults, including vetted hex/binary literals and PostgreSQL
typed-cast defaults, are emitted as `HasDefaultValueSql(...)`; SQL Server
explicit non-system default-constraint names are preserved with
`HasDefaultValueSql(..., constraintName: ...)`. Only unmodeled
complex/provider-specific defaults make the generated entity read-only.
Foreign keys from keyless dependent tables are reported as relationship
diagnostics instead of generating unsafe navigations.
Foreign keys with unknown provider-specific referential actions also suppress
generated navigations/fluent relationships instead of treating those actions as
`NO ACTION`.
Table CHECK constraints are emitted as provider-bound fluent migration metadata
with `HasCheckConstraint` instead of being dropped into the warning report.
Computed/generated column expressions are emitted as provider-bound fluent
migration metadata with `HasComputedColumnSql`.
Column collations are emitted as provider-bound fluent migration metadata with
`HasCollation`.
SQLite expression indexes, ordinary PostgreSQL B-tree expression indexes, and
MySQL expression indexes exposed by `SHOW INDEX` are emitted as provider-bound
fluent migration metadata with `HasExpressionIndex`, including representable
PostgreSQL expression-index `INCLUDE` and null-semantics metadata.
SQLite `UUID` declared columns scaffold as `Guid`; declared `JSON` and `XML`
columns scaffold as string storage. SQL Server `xml`, PostgreSQL
`citext`/`json`/`jsonb`/`xml`/`uuid`, and MySQL `json`/`year` columns also scaffold as
safe scalar CLR storage while native JSON/XML query semantics remain explicit
provider-bound work. PostgreSQL arrays over safe scalar elements, including
numeric, text/citext, UUID, binary, date/time, interval, and timestamp arrays,
scaffold as CLR arrays while remaining provider-specific schema for
provider-mobility review.
Use `--emit-sequence-stubs` to generate provider-bound next-value wrappers for
SQL Server and PostgreSQL standalone sequences; sequence DDL and allocation
semantics remain provider-owned.
With `--emit-routine-stubs`, SQL Server scalar/table-valued functions plus
PostgreSQL and MySQL functions are emitted as provider-bound `SELECT` wrappers
instead of stored-procedure calls. Stored-procedure stubs include buffered and
streaming result wrappers; stubs with discovered output metadata include a
convenience overload that uses the scaffolded `OutputParameter` definitions plus
an explicit-output overload for reviewed signature changes, including decimal
precision and optional scale, INOUT, and return-value metadata where providers expose them. PostgreSQL
set-returning functions scaffold as table-valued `SELECT * FROM function(...)`
wrappers.
Views and materialized views scaffold as `[ReadOnlyEntity]` read-oriented
entities by default. With `--emit-query-artifacts`, SQLite virtual tables and
SQL Server synonyms whose local base object resolves as a table or view can also
be emitted as read-oriented entities; non-query, remote, or unresolved synonyms
remain diagnostics.
When warnings are present, the CLI prints a compact summary with stable
diagnostic codes and categories, for example `SCF100=1` and
`schema-feature=1`, so CI logs can route scaffold follow-up without parsing the
markdown report.

## Provider Mobility Certification

Use the portability command to inventory provider-bound code before claiming an
application is provider-mobile:

```bash
norm portability certify --scan-path src/MyApp --assembly bin/Release/net8.0/MyApp.dll --report artifacts/provider-mobility.json --html artifacts/provider-mobility.html
```

Use `--providers sqlite,postgres,mysql` to narrow the provider target capability
profile in the report. The same EF Core provider package aliases accepted by
scaffolding, such as `Microsoft.EntityFrameworkCore.SqlServer`,
`Microsoft.EntityFrameworkCore.Sqlite`, `Npgsql.EntityFrameworkCore.PostgreSQL`,
`Pomelo.EntityFrameworkCore.MySql`, and `MySql.EntityFrameworkCore`, are
canonicalized to the nORM provider targets before reporting. Shorthand aliases
such as `mssql` and `postgresql` are also accepted. `mariadb` currently
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
