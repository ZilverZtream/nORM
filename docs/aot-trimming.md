# AOT and Trimming Policy

nORM v1 targets normal JIT-based .NET 8 applications first. It is not a
NativeAOT-compatible ORM today.

The runtime uses features that are intentionally dynamic:

- expression-tree compilation for query and materializer delegates
- Reflection.Emit for `DbContext.Query(string)` dynamic table queries
- Reflection.Emit for dynamic scaffolding/entity generation
- reflection-based optional provider loading for PostgreSQL and MySQL drivers
- reflection over entity constructors, properties, attributes, and relationship
  metadata

## Supported

- Regular .NET 8 JIT applications.
- Source-generated materializers through `[GenerateMaterializer]`.
- `[CompileTimeQuery]` generated query entry points for supported query shapes.
- Interpreted expression fallback when runtime expression compilation is not
  available for selected internal delegate paths.

## Not Supported For v1

- NativeAOT publish.
- Fully trimmed publish as a guaranteed-correct deployment mode.
- `DbContext.Query(string)` under NativeAOT or dynamic-code-disabled runtimes.
- Runtime scaffolding/entity generation under NativeAOT or dynamic-code-disabled
  runtimes.

## Deterministic Behavior

Public APIs that require runtime code generation are annotated with
`RequiresDynamicCode` and `RequiresUnreferencedCode`. Consumers who publish with
trimming or NativeAOT should get build-time warnings for those APIs instead of a
silent runtime failure.

## Annotated Boundary and the Empty Diagnostic Baseline

Every dynamic surface inside the runtime is either:

- annotated with `RequiresDynamicCode`/`RequiresUnreferencedCode` at its true
  boundary (the query-translation pipeline, materializer factories, mapping
  construction, bulk-operation transfer building, compiled-query entry points,
  and database scaffolding),
- refactored to be statically analyzable (batch sizing no longer serializes
  sample entities to JSON; enum SQL casing uses
  `Enum.GetValuesAsUnderlyingType`; PostgreSQL array casts come from a static
  type table instead of `MakeArrayType`; SQL Server transient-error
  classification uses typed `SqlException` access instead of reflection), or
- suppressed at a specific member with an `UnconditionalSuppressMessage` whose
  justification documents why the pattern stays functional (for example the
  `object[]` fallback for exotic PostgreSQL array element types, or optional
  driver types that are loaded by name and fail fast with package guidance).

As a result `eng/aot-baseline.txt` contains zero accepted diagnostics. The
release gate publishes the runtime with `PublishAot=true` and fails on any IL
diagnostic at all, so a new unannotated reflection or dynamic-code site cannot
land silently. This does not make nORM NativeAOT-supported — the annotations
are the documented boundary of what is *not* supported — but it makes the
boundary mechanically complete instead of a list of individually accepted
warnings.

The v1 test suite includes a negative `PublishTrimmed=true` smoke test. It
publishes a small nORM/SQLite application and requires the publish to fail with
ILLink trim diagnostics or SDK publish diagnostics before a usable artifact is
produced. That test locks the current support boundary: trimmed and NativeAOT
deployment are explicitly unsupported for v1 rather than accidentally marketed
as working.

Provider packages that are loaded by reflection, such as Npgsql and MySQL
drivers, must be referenced directly by the application. See
[Provider Packages](provider-packages.md) for the provider dependency contract.

## Guidance

Applications that need restricted dynamic code should avoid dynamic table queries
and runtime scaffolding. Prefer explicit entity types, source-generated
materializers, and generated compile-time query methods. Full NativeAOT support
would require a separate source-generator-first materialization and metadata
pipeline and is outside the v1 release contract.
