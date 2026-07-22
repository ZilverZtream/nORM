using System.Data.Common;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;

namespace nORM.Sample.Store;

/// <summary>
/// Builds <see cref="StoreContext"/> instances bound to the currently-active engine
/// (<see cref="ProviderSettings"/>) and scoped to a tenant.
///
/// The request path gets its context from DI (<c>AddNorm&lt;StoreContext&gt;</c>, one scoped instance
/// per request); this factory covers the cases a per-request scope can't serve — startup/swap
/// bootstrap and the parallel work in the concurrency-isolation proof — where the app needs a
/// caller-owned context for a specific tenant on demand. Every context it builds still flows through
/// the same active provider + tenant + temporal configuration, so no code here names a database.
/// </summary>
public sealed class StoreContextFactory(ProviderSettings settings)
{
    /// <summary>A caller-owned, tenant-scoped context on the active engine. The caller disposes it.</summary>
    public StoreContext CreateForTenant(int tenantId, bool temporal = true)
        => Build(settings.ActiveKind, settings, tenantId, temporal);

    /// <summary>A caller-owned context on a specific engine (used by swap bootstrap before it goes live).</summary>
    public StoreContext CreateForProvider(StoreProviderKind kind, int tenantId, bool temporal = true)
        => Build(kind, settings, tenantId, temporal);

    private static StoreContext Build(StoreProviderKind kind, ProviderSettings settings, int tenantId, bool temporal)
    {
        var connection = settings.OpenConnection(kind);
        var provider = ProviderSettings.CreateDatabaseProvider(kind);
        return new StoreContext(connection, provider, CreateOptions(tenantId, temporal));
    }

    /// <summary>The single source of truth for the store's model — shared by every context flavour.</summary>
    public static readonly Action<ModelBuilder> ConfigureModel = mb =>
    {
        mb.Entity<StoreTenant>();
        mb.Entity<StoreCustomer>();
        mb.Entity<StoreProduct>();
        mb.Entity<StoreEvent>();
        mb.Entity<StoreOrder>()
            .HasMany(o => o.Lines)
            .WithOne()
            .HasForeignKey(l => l.OrderId, o => o.Id);
        mb.Entity<StoreOrderLine>();
    };

    /// <summary>The one place the tenant boundary + temporal versioning are configured for requests.</summary>
    public static DbContextOptions CreateOptions(int tenantId, bool temporal)
    {
        var options = new DbContextOptions
        {
            TenantColumnName = "TenantId",
            TenantProvider = new FixedTenantProvider(tenantId),
            OnModelCreating = ConfigureModel
        };
        if (temporal)
            options.EnableTemporalVersioning();
        return options;
    }

    /// <summary>
    /// A context with NO tenant boundary and no temporal versioning — used only by cross-tenant admin
    /// work (the engine clone reads/writes every tenant's rows). Never handed to a request.
    /// </summary>
    public StoreContext CreateUnscoped(StoreProviderKind kind)
    {
        var connection = settings.OpenConnection(kind);
        var provider = ProviderSettings.CreateDatabaseProvider(kind);
        return new StoreContext(connection, provider, new DbContextOptions { OnModelCreating = ConfigureModel });
    }

    private sealed class FixedTenantProvider(int tenantId) : ITenantProvider
    {
        public object GetCurrentTenantId() => tenantId;
    }
}
