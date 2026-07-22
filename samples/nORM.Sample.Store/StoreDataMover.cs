using nORM.Core;

namespace nORM.Sample.Store;

/// <summary>
/// Copies the entire store from one database engine to another using nothing but nORM: read every
/// row out of the source context, clear the target, bulk-insert into it. It is completely
/// provider-agnostic — the same code moves SQLite → PostgreSQL or MySQL → SQL Server — which is why
/// "switch engine" carries your actual data across instead of resetting to seed. This is nORM's
/// provider mobility doing real work, live.
/// </summary>
public sealed class StoreDataMover(StoreContextFactory factory)
{
    public async Task<int> CloneAsync(StoreProviderKind from, StoreProviderKind to)
    {
        // Read the whole store out of the current engine (unscoped: every tenant's rows).
        await using var src = factory.CreateUnscoped(from);
        var tenants = await src.Query<StoreTenant>().ToListAsync();
        var customers = await src.Query<StoreCustomer>().ToListAsync();
        var products = await src.Query<StoreProduct>().ToListAsync();
        var orders = await src.Query<StoreOrder>().ToListAsync();
        var lines = await src.Query<StoreOrderLine>().ToListAsync();
        var events = await src.Query<StoreEvent>().ToListAsync();

        // Empty the target, children first so foreign keys never block the clear.
        await using var dst = factory.CreateUnscoped(to);
        await dst.Query<StoreOrderLine>().ExecuteDeleteAsync();
        await dst.Query<StoreEvent>().ExecuteDeleteAsync();
        await dst.Query<StoreOrder>().ExecuteDeleteAsync();
        await dst.Query<StoreProduct>().ExecuteDeleteAsync();
        await dst.Query<StoreCustomer>().ExecuteDeleteAsync();
        await dst.Query<StoreTenant>().ExecuteDeleteAsync();

        // Refill, parents first.
        var moved = 0;
        moved += await InsertAll(dst, tenants);
        moved += await InsertAll(dst, customers);
        moved += await InsertAll(dst, products);
        moved += await InsertAll(dst, orders);
        moved += await InsertAll(dst, lines);
        moved += await InsertAll(dst, events);
        return moved;
    }

    private static async Task<int> InsertAll<T>(StoreContext ctx, IReadOnlyList<T> rows) where T : class
        => rows.Count == 0 ? 0 : await ctx.BulkInsertAsync(rows);
}
