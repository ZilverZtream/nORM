# Public API Namespace Policy

nORM v1 has a fixed list of public namespaces. Every exported type must live in one of them.
The list is enforced by `NamespacePolicyContractTests`.

## Approved Namespaces

| Namespace | Support Tier | Purpose |
| --- | --- | --- |
| `nORM.Configuration` | Stable user API | `DbContextOptions`, `EntityTypeBuilder<T>`, `ModelBuilder`, `ClientEvaluationPolicy`, ambient-transaction policy enums |
| `nORM.Core` | Stable user API | `DbContext`, `ChangeTracker`, `EntityEntry`, `EntityState`, `QueryTrackingBehavior`, `NormException` and the rest of the public exception taxonomy, async/sync extension helpers |
| `nORM.Mapping` | Stable user API | Attributes and primitives: `[Key]`, `[Column]`, `[RenameColumn]`, `IValueConverter`, `OwnedCollectionNavigation`, mapping primitives consumers compose against |
| `nORM.Migration` | Stable user API + provider API | `Migration`, provider-specific runners (`SqlServerMigrationRunner`, `PostgresMigrationRunner`, ...), `MigrationOptions`, schema diff types |
| `nORM.Providers` | Stable provider API | `DatabaseProvider` and its subclasses, `ProviderCapabilities`, `IDbParameterFactory` |
| `nORM.Query` | Stable user API | `INormQueryable<T>`, query extension methods (Include, ThenInclude, AsSplitQuery, etc.) |
| `nORM.Execution` | Stable user API | `AdaptiveTimeoutManager` and operational primitives |
| `nORM.Navigation` | Stable user API | Lazy-loading proxy / navigation primitives |
| `nORM.SourceGeneration` | Stable user API (compile-time integration) | `CompiledMaterializerStore`, `[GenerateMaterializer]`, `[CompileTimeQuery]` |
| `nORM.Scaffolding` | Stable tooling | `DatabaseScaffolder`, `DynamicEntityTypeGenerator`, `ScaffoldOptions` (bounded v1 scaffolding contract; see `docs/scaffolding.md`) |
| `nORM.Enterprise` | Stable provider API | Enterprise integration extension points; opt-in |
| `nORM.Internal` | **Deprecated namespace, tracked for v1.x relocation** | Existing public types here remain reachable for compatibility. New types must NOT be added; existing entries (e.g., `ConcurrentLruCache<T, TValue>`, `ParameterOptimizer`) are scheduled for relocation to `nORM.Caching` / `nORM.Diagnostics` in a v1.x release with type forwarders. |
| `Microsoft.Extensions.Logging` | Stable user API (extension methods) | Hosts `DbContextLoggingExtensions` (and any future `Microsoft.Extensions.*` extensions) per the standard convention of registering extension methods under the namespace of the type they extend. |

## Rules

- Adding a new public type to any namespace not listed above fails
  `NamespacePolicyContractTests.Every_public_type_lives_in_an_approved_namespace`.
- Adding a new public type to `nORM.Internal` fails the same test even if the namespace itself
  is on the list, because the namespace is closed for additions.
- Moving a public type to a different namespace is a breaking change and requires updating
  `tests/PublicApi.Shipped.txt`, `docs/public-api-policy.md` (v1 Public API Additions table),
  and the namespace table above in the same commit.

## Future Relocations (v1.x)

| Current Location | Target Location | Status |
| --- | --- | --- |
| `nORM.Internal.ConcurrentLruCache<T, TValue>` | `nORM.Caching.ConcurrentLruCache<T, TValue>` | Planned for v1.1 with type forwarder for v1.0 compatibility |
| `nORM.Internal.ParameterOptimizer` | `nORM.Diagnostics.ParameterOptimizer` | Planned for v1.1 with type forwarder for v1.0 compatibility |

Type forwarders preserve binary compatibility for v1.0 consumers; the new locations become the
documented surface.
