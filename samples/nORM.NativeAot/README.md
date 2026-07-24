# nORM under NativeAOT — zero-ceremony sample

A minimal, runnable demonstration that nORM's source-generated path publishes to a
self-contained **NativeAOT** binary and runs reads and writes correctly — with **no
`<TrimmerRootAssembly>`, no `[DynamicallyAccessedMembers]`, and no per-entity
preservation**. The source generator emits the metadata preservation itself.

## Run it (JIT)

```
dotnet run -c Release --project samples/nORM.NativeAot
```

## Publish and run it natively

```
dotnet publish -c Release -r win-x64 samples/nORM.NativeAot
# then run the produced binary, e.g.:
./samples/nORM.NativeAot/bin/Release/net8.0/win-x64/publish/nORM.NativeAot
```

Swap the RID for your platform (`linux-x64`, `osx-arm64`, …). A native publish needs the
platform AOT toolchain (a C/C++ compiler + linker: MSVC on Windows, clang/zlib on Linux/macOS).

## The consumer contract (the whole thing)

From [`nORM.NativeAot.csproj`](nORM.NativeAot.csproj):

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
  <!-- The DbContext constructor is annotated dynamic; suppress the two boundary
       diagnostics as you would for any annotated API consumed under AOT. -->
  <NoWarn>$(NoWarn);IL2026;IL3050</NoWarn>
</PropertyGroup>
<ItemGroup>
  <PackageReference Include="TheNorm" Version="..." />
  <!-- Reference the source generator as an analyzer (bundled with the package). -->
</ItemGroup>
```

Then mark each entity `[GenerateMaterializer]` and query with the ordinary
`ctx.Query<T>()...` API (or `[CompileTimeQuery]` methods). That's it.

## What's verified

The same source-generated path is exercised end-to-end by `eng/aot-smoke` (a 14-scenario
native matrix: compile-time + runtime reads, direct/tracked/bulk writes, attribute-driven
mapping, one-to-many + many-to-many `Include`, value converters, owned types, DTO
projections, JSON) and guarded in CI by the **AOT Smoke** workflow. See
[`docs/aot-trimming.md`](../../docs/aot-trimming.md) for the full policy and boundaries.

The reflection path (entities without `[GenerateMaterializer]`, `DbContext.Query(string)`,
runtime scaffolding) is **not** AOT-supported and fails closed rather than shipping wrong
results.
