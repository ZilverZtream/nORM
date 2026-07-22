using System.Data.Common;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;

namespace nORM.Sample.Store;

/// <summary>
/// The application's data-access context — an idiomatic nORM <see cref="DbContext"/> subclass with
/// <see cref="DbSet{T}"/> accessors, exactly how a real ASP.NET Core app models its data layer.
///
/// It is resolved <b>per request from DI</b> (see the <c>AddNorm&lt;StoreContext&gt;</c> registration in
/// <see cref="StoreWebApp"/>), bound to whichever engine is currently active (see
/// <see cref="ProviderSettings"/>) and scoped to the calling tenant. The application code below and in
/// the endpoints never mentions a specific database — that is the whole point of the sample.
/// </summary>
public sealed class StoreContext : DbContext
{
    public StoreContext(DbConnection connection, DatabaseProvider provider, DbContextOptions options)
        : base(connection, provider, options)
    {
    }

    public DbSet<StoreTenant> Tenants => this.Set<StoreTenant>();
    public DbSet<StoreCustomer> Customers => this.Set<StoreCustomer>();
    public DbSet<StoreProduct> Products => this.Set<StoreProduct>();
    public DbSet<StoreOrder> Orders => this.Set<StoreOrder>();
    public DbSet<StoreOrderLine> OrderLines => this.Set<StoreOrderLine>();
    public DbSet<StoreEvent> Events => this.Set<StoreEvent>();
}
