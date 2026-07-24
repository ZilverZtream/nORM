# AOT and Trimming Policy

nORM v1 targets normal JIT-based .NET 8 applications first, but its
**source-generated path runs correct under NativeAOT with zero consumer ceremony** —
reads *and* writes, including `[Key]`/`[Column]`-driven mapping (see
[NativeAOT via the source-generated path](#nativeaot-via-the-source-generated-path)).
The default reflection path (dynamic table queries, runtime scaffolding) stays
AOT/trim-unsafe, so nORM does not market itself as a wholesale drop-in NativeAOT ORM.

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
- **NativeAOT publish of the source-generated path — reads and writes, zero
  ceremony** — verified by `eng/aot-smoke` (see below).

## NativeAOT via the source-generated path

nORM's code is not AOT-hostile: under NativeAOT `Expression.Compile()` transparently
interprets, so the query, materializer, and write delegates all execute. The only
NativeAOT hazard is *trimming*: the write mapping reflects over entity properties and
attributes (`GetProperties`, `[Key]`/`[Column]`/`[DatabaseGenerated]`,
`GetSetMethod`), and the trimmer would remove that metadata — leaving an empty column
set and silently wrong writes.

The source generator closes that automatically. For every `[GenerateMaterializer]`
entity it emits, alongside the materializer registration, a
`[DynamicDependency(PublicProperties | PublicParameterlessConstructor, typeof(TEntity))]`
on its module initializer (and one per owned type). `DynamicDependency` is the
sanctioned way to declare a reflection dependency the trimmer cannot see, so the
trimmer preserves exactly the property/attribute metadata the mapping needs and the
parameterless constructor the runtime materializer invokes. It also source-generates
the property getters/setters (`GeneratedAccessors`) so the hot read/write delegates
are compile-time rather than interpreted. **No `<TrimmerRootAssembly>`, no
`DynamicallyAccessedMembers` annotations, and no per-property preservation are required
of the consumer.**

Both query entry points work: the `[CompileTimeQuery]` methods **and** the runtime
LINQ path (`ctx.Query<T>().Where(...).OrderBy(...)`) for source-generated entities.
The runtime path builds a cached query plan whose fingerprint is derived without
`MemberInfo.MetadataToken` (unavailable under NativeAOT), and materializes through the
registered generated materializer.

The consumer contract is just:

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
  <!-- The DbContext constructor is annotated dynamic; suppress the two boundary
       diagnostics as you would for any annotated API consumed under AOT. -->
  <NoWarn>$(NoWarn);IL2026;IL3050</NoWarn>
</PropertyGroup>
```

A minimal, runnable starting point you can copy is
[`samples/nORM.NativeAot`](../samples/nORM.NativeAot) — a small product-catalog console app
(tracked insert, runtime-LINQ read, tracked update) that publishes native with the contract
above and nothing else.

`eng/aot-smoke` is a committed proof: a minimal consumer with **no rooting** that
publishes to a native binary and runs a scenario matrix — simple and parameterized
reads, rich-type materialization (`long`/`double`/`bool`/`DateTime`/`Guid`), a direct
`InsertAsync`, a tracked `Update` + `SaveChangesAsync`, and a write whose mapping
depends on a non-`Id` `[Key]` and a `[Column]` rename. Run it with:

```
pwsh eng/aot-smoke.ps1            # or: powershell -File eng/aot-smoke.ps1
```

It is opt-in rather than part of the normal test run because a native publish needs
the platform AOT toolchain and takes minutes; run it before releases or from a
dedicated CI job. All scenarios pass on a ~13 MB self-contained native binary.

## Not supported for v1

The following remain explicitly unsupported for v1 and fail closed rather than
producing wrong results:

- NativeAOT/trimmed publish of the **reflection path** — entities that do not use
  `[GenerateMaterializer]`, dynamic table queries, runtime scaffolding, un-annotated
  projections. Only the source-generated path carries the `[DynamicDependency]`
  preservation above; a plain reflection entity under trimming is not supported.
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
land silently. The annotations remain the documented boundary of what is dynamic;
they make that boundary mechanically complete instead of a list of individually
accepted warnings.

The v1 test suite includes a negative `PublishTrimmed=true` smoke test. It
publishes a small nORM/SQLite application **without metadata preservation** and
requires the publish to fail with ILLink trim diagnostics or SDK publish
diagnostics before a usable artifact is produced. That test locks the *default*
support boundary: an un-preserved trimmed/NativeAOT deployment fails closed rather
than being accidentally marketed as working. `eng/aot-smoke` documents the opposite
end — the supported, metadata-preserved native path — so both the guaranteed-safe
and the fails-closed cases are pinned by an asset.

Provider packages that are loaded by reflection, such as Npgsql and MySQL
drivers, must be referenced directly by the application. See
[Provider Packages](provider-packages.md) for the provider dependency contract.

## Guidance

Applications that need restricted dynamic code should avoid dynamic table queries
and runtime scaffolding. Prefer explicit entity types, source-generated
materializers, and generated compile-time query methods, and preserve entity
metadata as shown above. The remaining work for zero-ceremony NativeAOT — dropping
the `<TrimmerRootAssembly>` requirement — is source-generated write accessors so
the write path stops reflecting over entity properties.
