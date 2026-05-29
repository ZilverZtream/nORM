# Provider Mobility Translation Layer

nORM's provider mobility promise is enforced through one rule:

> If nORM owns the semantics, nORM must translate, emulate, or fail
> deterministically. If the caller owns provider language or provider handles,
> the path is explicit compatibility surface and cannot pass strict provider
> mobility certification.

The shared decision table lives in
`nORM.Configuration.ProviderMobilityTranslator`. Runtime strict mode, static
certification, sample provider-swap certification, and documentation must use
the same classifications so release evidence cannot drift from runtime
behavior.

## Support Classes

| Support class | Meaning | Strict runtime | Certification behavior |
| --- | --- | --- | --- |
| `Portable` | The same nORM-facing application code preserves visible behavior across SQLite, SQL Server, PostgreSQL, and MySQL. | Allowed. | Informational evidence item. |
| `Emulated` | nORM provides provider-specific SQL, history infrastructure, fallback SQL, or materialization logic to preserve semantics where providers differ. | Allowed when the emulation is documented and tested. | Warning or informational evidence depending on caveat severity. |
| `ProviderBound` | The app directly uses provider language, provider handles, or provider-native infrastructure. nORM cannot translate that usage without taking ownership of its semantics. | Blocked in strict mode except composition-root bootstrap inventory. | Error unless it is explicit bootstrap inventory. |
| `Unsupported` | nORM cannot preserve the semantics safely. | Blocked before execution. | Error. |

## Runtime Decisions

Strict mode allows generated nORM paths that nORM owns:

| Feature | Decision |
| --- | --- |
| Generated LINQ queries | `Portable`; supported shapes must translate, emulate, or throw a nORM exception before execution. |
| Generated writes | `Portable`; provider SQL may differ, but visible write semantics must match. |
| Generated tenant boundary | `Portable`; tenant predicates/stamping are nORM-owned generated behavior. |
| nORM-managed temporal history | `Emulated`; nORM owns provider-specific history tables, triggers, tags, and `AsOf` translation. |
| Wrapped savepoints through `DbContextTransaction` | `Portable`; nORM owns the provider abstraction. |
| Explicit top-level client projection tails under `ClientEvaluationPolicy.Warn` | `Emulated`; allowed only after server filters, ordering, and paging have run. |

Strict mode blocks compatibility escape hatches:

| Feature | Reason |
| --- | --- |
| Raw SQL and compile-time raw SQL | SQL text is provider language. Rewrite to generated LINQ/write APIs or introduce a nORM-owned translator. |
| Stored procedures | Procedure bodies are provider assets. Move semantics into generated nORM APIs or keep them outside provider-mobile certification. |
| Custom SQL functions | Function fragments are provider SQL. Add provider translations before certifying the shape. |
| Dynamic table queries | Runtime table names bypass model metadata and provider-swap validation. Use mapped entity sets or generated model metadata. |
| Direct `DbConnection`, `DbCommand`, `DbTransaction`, or provider access | Caller controls provider-specific handles. Use nORM wrappers such as `DbContextTransaction` where available. |
| External transaction writes | The transaction lifecycle is outside nORM's portable contract. |
| Raw transaction savepoint overloads | Caller owns the provider transaction handle. Use nORM's wrapped savepoint API. |
| Command interceptors | Interceptors can rewrite SQL after translation. They require explicit provider review. |
| Provider-native tenant security | RLS/session-context DDL is provider infrastructure, not generated tenant filtering. |
| Provider-native temporal tables | Native temporal DDL/history semantics are provider infrastructure, not nORM-managed temporal history. |
| Silent client evaluation | Silent buffering changes query semantics and performance. Use server translation or explicit warning-level projection tails. |
| Unsupported LINQ shapes | Unsupported translation must fail deterministically instead of falling back to silent semantic drift. |

Provider package/connection bootstrapping is warning-level inventory when it
stays in the composition root. An application must still configure a concrete
provider somewhere; that does not make generated repository code provider-bound.

## Certification Contract

`dotnet-norm portability certify` and
`samples/nORM.Sample.Store certify-provider-swap` use
`ProviderMobilityTranslator.TryDecideFindingKind` for source findings. The
resulting severity, reason, and suggested fix come from the same decision table
used by runtime strict failures. Scanner rules may append pattern-specific
remediation after the shared suggested fix so EF/Dapper/migration findings keep
their concrete rewrite guidance without changing the centralized support class.

| Finding kind | Certification behavior |
| --- | --- |
| `raw-sql` | Error; rewrite to generated nORM LINQ/write APIs or add a provider-neutral nORM abstraction. |
| `stored-procedure` | Error; procedure semantics are provider assets unless nORM gains a generated equivalent. |
| `dynamic-table-query` | Error; map the table shape or add metadata-driven routing. |
| `client-evaluation` | Error; silent client evaluation is not provider-mobile evidence. |
| `client-projection-tail` | Warning; allowed only as explicit top-level projection inventory. |
| `provider-bootstrap-connection` | Warning; acceptable in composition-root provider selection. |
| `provider-specific-package` | Warning; acceptable when isolated to provider bootstrap. |
| `constrained-linq-shape` | Warning; review provider target caveats and live evidence before certifying as all-provider portable. Current examples include SQL Server's simple-subset `Regex.IsMatch`, SQL Server's literal-subset `Regex.Replace`, ordered and index-aware `TakeWhile`/`SkipWhile`, and ordered queryable/local `SequenceEqual`. |
| `unsupported-linq-shape` | Error; rewrite to a documented generated LINQ shape or materialize explicitly before applying CLR-only sequence logic. Static examples are reserved for shapes with no supported generated-query subset. |

Certification must fail on error-level findings. Warning-level findings remain
visible so reviewers can verify they are limited to composition-root bootstrap
or explicitly documented emulation.
Certification reports also fail on `certification-unclassified-finding` if any
scanner, schema inspector, or provider target probe emits a finding kind that is
not registered in `ProviderMobilityTranslator`.

Schema inspection findings are also classified by this layer. Table/column
specific schema messages keep their contextual text, but the support class and
default remediation are centralized:

| Schema finding kind | Decision |
| --- | --- |
| Invalid/duplicate/missing schema metadata | Unsupported error; regenerate the snapshot and fix invalid identifiers, duplicate objects, missing CLR types, or invalid relations. |
| Unsupported CLR type | Unsupported error; use a supported scalar, enum, or value converter to a provider-mobile type. |
| Non-portable identity | Unsupported error; use an integral identity column type. |
| Provider-specific default SQL | Provider-bound error; replace with literal/application-stamped values or keep as provider-specific migration work. |
| Database-evaluated or function-like default needing review | Provider-bound warning; verify timezone, precision, user, and function semantics on every target provider or replace before strict certification. |
| Missing primary key | Emulated warning; keyless/read-only shapes are constrained for generated writes, includes, tenant, and temporal behavior. |
| Provider generator failure | Unsupported error; fix metadata until every target migration generator can emit DDL. |

## Provider Target Decisions

Provider mobility is also a target contract. The same generated nORM code can
only be certified when the selected provider and server version satisfy the
capability profile required by that code. `ProviderMobilityTranslator` therefore
classifies provider/version capabilities as `ProviderMobilityProviderDecision`
rows.

| Provider target feature | Decision rule |
| --- | --- |
| `ServerVersion` | Descriptor-only reports record the declared `Capabilities.MinimumServerVersion` without pretending it is a live server version. Live target reports are portable only when the actual connected server version is known and greater than or equal to the floor; unknown or too-old live versions are error-level startup failures. |
| `JsonTranslation` | Portable when `Capabilities.SupportsJson` is true; otherwise JSON query shapes are unsupported for that target. |
| `JsonPathTranslation` | Portable only when the concrete provider emits nORM-owned JSON path extraction SQL and rejects unsafe path text during capability probing. |
| `TemporalVersioning` | Portable when nORM-managed temporal history is supported by the provider. |
| `BulkInsert` | Portable when a provider-native path exists; emulated warning when nORM preserves generated bulk semantics through fallback SQL. Public claims must not call the fallback native bulk. |
| `Savepoints` | Portable when provider transaction savepoints are supported; otherwise savepoint-dependent workflows are unsupported. |
| `ParameterLimit` | Portable or emulated depending on `Capabilities.MaxParameters`; generated paths must batch or split before reaching the provider limit. |
| `NativeTenantSessionContext` | Provider-bound info. Native RLS/session context is optional deployment defense in depth, not generated-path provider mobility. App usage is still strict-blocked unless explicitly reviewed. |
| `ProviderNativeTemporalTables` | Provider-bound info. Native temporal tables are optional provider infrastructure, not nORM-managed provider-neutral history. App usage is still strict-blocked unless explicitly reviewed. |
| `RowTupleComparison` | Portable when the concrete provider supports native row-value tuple predicates; emulated warning when nORM rewrites tuple semantics into provider-compatible predicates. |
| `OrderedStringAggregate` | Portable when the concrete provider supports native ordered string aggregate translation at the required feature version; emulated warning when nORM must preserve ordering through provider-specific rewrites or bounded fallback behavior. SQL Server targets require 14.0+ for `STRING_AGG` even though the provider floor is 13.0. |
| `RegexTranslation` | Portable when the concrete provider has native regex predicates/replacement; emulated when SQLite uses deterministic nORM-registered managed regex functions or SQL Server uses documented subset lowerings (`Regex.IsMatch` simple subset to `LIKE` / `LEN` / `RIGHT`, `Regex.Replace` literal subset to `REPLACE`); unsupported warning for shapes the target cannot preserve, such as complex SQL Server regex predicates or replacement substitutions. App code using regex LINQ must stay inside the documented subset for all-four mobility or provide an explicit provider-owned function with live tests. |
| `IdentifierEscaping` | Portable when identifiers are emitted through the concrete provider escaping strategy, not caller-authored delimited SQL. |
| `ParameterBinding` | Portable when parameter prefixes and `DbParameter` objects are owned by the provider implementation. |
| `PagingTranslation` | Portable when `OrderBy`/`Skip`/`Take` are emitted as the target dialect's paging primitive, such as SQL Server `OFFSET/FETCH` or `LIMIT/OFFSET`. |
| `BooleanPredicateTranslation` | Portable when boolean predicates are emitted through provider-owned boolean literal or bare-predicate formatting. |
| `NullSemantics` | Emulated when null-safe equality/inequality expands to provider-compatible SQL instead of relying on CLR two-valued logic. |
| `LikeEscapeTranslation` | Portable when wildcard escaping is generated by the provider strategy for `StartsWith`, `EndsWith`, and `Contains`. |
| `StringConcatTranslation` | Portable when string concatenation is emitted as the provider's concat primitive, such as `CONCAT` or `||`. |
| `TypeConversionTranslation` | Portable when casts, `Convert.*`, `ToString`, boolean conversion, real/decimal conversion, and truncate-to-int are emitted through provider-owned SQL. |
| `StringPredicateTranslation` | Portable or emulated depending on whether string null/empty/whitespace predicates need provider-specific safeguards, such as SQL Server byte-exact empty checks. |
| `CharacterTranslation` | Portable when char code-point and reverse-code-point SQL are provider-owned for `char.Is*` and related translations. |
| `FormattingTranslation` | Portable when fixed decimal formatting and supported .NET date/time format tokens have provider-owned SQL translations. |
| `DateTimeComparisonNormalization` | Portable or emulated depending on whether the provider needs generated normalization for chronological comparison. |
| `DecimalComparisonNormalization` | Portable on native decimal providers; emulated warning where nORM must coerce storage for numeric semantics, such as SQLite `REAL` normalization. |
| `TimeSpanComparisonNormalization` | Portable or emulated depending on whether the provider needs generated normalization for duration ordering. |
| `TemporalConstructionTranslation` | Portable when the provider exposes nORM-owned DateTime/DateOnly/TimeOnly from-parts SQL for column-driven constructors. |
| `TemporalArithmeticTranslation` | Portable or emulated depending on whether date/time/interval arithmetic uses native primitives or generated normalization, such as SQLite TEXT/julianday/strftime rewrites. |
| `TemporalClockSource` | Portable when temporal tags and history infrastructure use a consistent database clock source. |
| `GeneratedKeyRetrieval` | Portable or emulated depending on whether generated keys come from command execution or provider identity SQL. |
| `InsertOrIgnoreTranslation` | Portable or emulated depending on whether idempotent join-table insertion is native (`ON CONFLICT`/`IGNORE`) or generated through guarded SQL such as SQL Server `IF NOT EXISTS`. |
| `CudSubqueryRewrite` | Portable or emulated depending on whether generated update/delete subqueries can use the standard shape or require a provider-specific rewrite such as MySQL's double-wrap self-reference workaround. |
| `BitwiseXorTranslation` | Portable or emulated depending on whether the provider has native XOR syntax or nORM rewrites it algebraically. |
| `WindowFunctionTranslation` | Portable when the provider/version is inside nORM's supported window-function floor for `ROW_NUMBER`, partitioned aggregates, paging, and grouped first/last rewrites. |
| `CaseSensitiveStringComparison` | Portable or emulated depending on whether generated equality/IN/string-compare semantics need provider-specific collation, binary wrappers, or generated lower-case normalization for literal/captured ignore-case comparison modes. |
| `SqlStatementLengthLimit` | Portable or emulated depending on whether the provider exposes a bounded maximum SQL statement length that generated SQL must split around. |

Runtime provider startup validation uses this layer for version failures, so
the exception text and certification decision come from the same contract. A
server whose version cannot be determined, or whose version is below the
provider floor, fails before query execution with `NormConfigurationException`.
Provider-swap sample reports parse the opened connection's `ServerVersion`
through `ProviderMobilityTranslator.ParseProviderVersion` so the artifact shows
the actual connected version when the driver exposes one.
The `dotnet-norm portability certify` command can do the same for supplied
target connection strings and turns failed opens/version checks into blocking
provider-target findings. Without a supplied live connection, the report is a
descriptor profile only: it records the provider's version floor but leaves
`ActualServerVersion` empty.
CLI and sample reports use `DecideProviderImplementationProfile` so concrete
translation strategy quirks, such as SQL Server tuple-comparison rewrites or
SQLite ordered-string-aggregate emulation, appear as explicit evidence rows.
The same profile now records the everyday dialect rewrites that usually break
provider swaps: identifier escaping, parameter binding, paging, boolean
predicates, null semantics, LIKE escaping, string concatenation,
JSON path extraction, type conversion, string predicates, character
classification, formatting, DateTime/decimal/TimeSpan normalization, temporal
clock source, generated-key retrieval, idempotent insert, generated CUD
subquery rewrites, bitwise XOR, window functions, case-sensitive string
comparison, regex translation, temporal construction/arithmetic, and SQL statement
length limits. These rows are not marketing claims by themselves; they are the
release artifact surface reviewers can use to see which behavior is native,
which behavior is emulated, and which emulations carry warning-level caveats.

## Promotion Rule

A provider-bound or unsupported shape can move into strict mode only when all of
these are true:

1. nORM owns the application-facing semantics.
2. The implementation translates or emulates the shape on every supported
   provider, or throws a deterministic nORM exception before execution.
3. Local strict-mode tests prove the shape is allowed and preserves behavior.
4. Live-provider tests cover SQLite, SQL Server, PostgreSQL, and MySQL when the
   shape reaches release evidence.
5. Static certification uses the same severity/reason/suggested-fix decision.
6. Documentation states any precision, performance, privilege, or migration
   caveats without overclaiming.

This is the path for making strict mode broader without making it weak. Strict
mode should be a wall only for semantics nORM cannot safely own yet, not for
portable features that have evidence-backed translations.
