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

Provider packages that are loaded by reflection, such as Npgsql and MySQL
drivers, must be referenced directly by the application. See
[Provider Packages](provider-packages.md) for the provider dependency contract.

## Guidance

Applications that need restricted dynamic code should avoid dynamic table queries
and runtime scaffolding. Prefer explicit entity types, source-generated
materializers, and generated compile-time query methods. Full NativeAOT support
would require a separate source-generator-first materialization and metadata
pipeline and is outside the v1 release contract.
