# AOT and Trimming Policy

nORM v1 targets normal JIT-based .NET 8 applications first. Its **source-generated
path runs correctly under NativeAOT when entity metadata is preserved** (see
[NativeAOT via the source-generated path](#nativeaot-via-the-source-generated-path)),
but the default reflection-based path is not AOT- or trim-safe, so nORM does not
market itself as a drop-in NativeAOT ORM.

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
- **NativeAOT publish of the source-generated path, with entity metadata
  preserved** — verified by `eng/aot-smoke` (see below).

## NativeAOT via the source-generated path

The blocker for NativeAOT is not that nORM's code is AOT-hostile — under NativeAOT
`Expression.Compile()` transparently interprets, so the query, materializer, and
write delegates all execute. The blocker is *trimming*: the write path reads entity
values through reflection-built property getters, and the trimmer removes the
property metadata those getters depend on unless the consumer preserves it. When it
is removed, getters return defaults and writes silently send wrong values — which is
why an un-preserved trimmed publish stays unsupported (see the negative test below).

When the consumer both (a) uses the source-generated path and (b) preserves entity
metadata, a native publish reads *and writes* correctly. The consumer recipe:

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
</PropertyGroup>
<ItemGroup>
  <!-- Preserve the entity assembly's property metadata for the write path. -->
  <TrimmerRootAssembly Include="MyApp" />
</ItemGroup>
```

`eng/aot-smoke` is a committed proof of this: a minimal consumer that publishes to a
native binary and runs a scenario matrix — simple and parameterized reads, rich-type
materialization (`long`/`double`/`bool`/`DateTime`/`Guid`), a direct `InsertAsync`,
and a tracked `Update` + `SaveChangesAsync`. Run it with:

```
pwsh eng/aot-smoke.ps1            # or: powershell -File eng/aot-smoke.ps1
```

It is opt-in rather than part of the normal test run because a native publish needs
the platform AOT toolchain and takes minutes; run it before releases or from a
dedicated CI job. All five scenarios pass on a ~13 MB self-contained native binary.

The remaining requirement — the consumer's `<TrimmerRootAssembly>` line — is the one
piece of ceremony left. Removing it (source-generated write accessors so the write
path never reflects over entity properties) is the tracked path to zero-ceremony
NativeAOT support.

## Not supported for v1

The following remain explicitly unsupported for v1 and fail closed rather than
producing wrong results:

- NativeAOT/trimmed publish of the **reflection path** (dynamic table queries,
  runtime scaffolding, un-annotated projections).
- Trimmed publish **without** entity-metadata preservation — silently unsafe for
  writes, so it is not supported and fails closed rather than shipping wrong data.
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
