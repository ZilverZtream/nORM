using System.Data.Common;
using System.Globalization;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using nORM.Query;

namespace nORM.Sample.Store;

public static class StoreScenario
{
    private const int TenantA = 101;
    private const int TenantB = 202;

    public static async Task<StoreScenarioResult> RunAsync(
        DbConnection connection,
        DatabaseProvider provider,
        StoreProvider storeProvider,
        CancellationToken cancellationToken = default)
    {
        await ResetSchemaAsync(connection, provider, storeProvider, cancellationToken);

        var optionsA = CreateOptions(TenantA, temporal: true);
        await using var setup = new DbContext(connection, provider, optionsA);

        await setup.Query<StoreProduct>().CountAsync(cancellationToken);
        await SeedAsync(setup, connection, provider, cancellationToken);
        await RequireAsync(await setup.Query<StoreProduct>().CountAsync(cancellationToken) == 2,
            "tenant A should only see tenant A products");

        await VerifyTenantBoundaryAsync(setup, connection, provider, cancellationToken);
        await VerifyBulkAndRepresentativeLinqAsync(setup, cancellationToken);
        await VerifyTemporalAsync(setup, connection, provider, cancellationToken);

        return new StoreScenarioResult(true,
            "tenant boundary, representative LINQ, bulk insert, compiled query, temporal AsOf(tag), and temporal history passed");
    }

    private static DbContextOptions CreateOptions(int tenantId, bool temporal)
    {
        var options = new DbContextOptions
        {
            TenantColumnName = "TenantId",
            TenantProvider = new FixedTenantProvider(tenantId),
            OnModelCreating = mb =>
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
            }
        };
        options.UseStrictProviderMobility();
        if (temporal)
            options.EnableTemporalVersioning();
        return options;
    }

    private static async Task SeedAsync(DbContext ctx, DbConnection connection, DatabaseProvider provider, CancellationToken cancellationToken)
    {
        // Tenant A rows are written through the tenant-A context: their tenant matches the context, so
        // the fail-closed tenant guard on writes accepts them.
        ctx.Add(new StoreTenant { Id = 1, TenantId = TenantA, Name = "Tenant A" });
        ctx.Add(new StoreCustomer { Id = 100, TenantId = TenantA, Name = "Aster Operations", Email = "ops@aster.test", IsActive = true });
        ctx.Add(new StoreProduct { Id = 1000, TenantId = TenantA, Sku = "A-COFFEE", Name = "Coffee", Price = 12.50m, IsActive = true });
        ctx.Add(new StoreProduct { Id = 1001, TenantId = TenantA, Sku = "A-TEA", Name = "Tea", Price = 8.25m, IsActive = true });
        ctx.Add(new StoreOrder { Id = 5000, TenantId = TenantA, CustomerId = 100, Status = "Open", Total = 33.25m });
        ctx.Add(new StoreOrder { Id = 5001, TenantId = TenantA, CustomerId = 100, Status = "Open", Total = 18.50m });
        ctx.Add(new StoreOrderLine { Id = 7000, TenantId = TenantA, OrderId = 5000, ProductId = 1000, Quantity = 2, UnitPrice = 12.50m });
        ctx.Add(new StoreOrderLine { Id = 7001, TenantId = TenantA, OrderId = 5000, ProductId = 1001, Quantity = 1, UnitPrice = 8.25m });
        await ctx.SaveChangesAsync(cancellationToken);

        // Tenant B fixture data is written directly (a system/admin path). A tenant-A context is
        // fail-closed against inserting another tenant's rows, so cross-tenant seeding uses a raw write.
        // These rows exist only to prove the tenant boundary: tenant A must never see or modify them.
        await InsertRawAsync(connection, provider, "SampleTenant", cancellationToken,
            ("Id", 2), ("TenantId", TenantB), ("Name", "Tenant B"));
        await InsertRawAsync(connection, provider, "SampleCustomer", cancellationToken,
            ("Id", 200), ("TenantId", TenantB), ("Name", "Boreal Retail"), ("Email", "ops@boreal.test"), ("IsActive", true));
        await InsertRawAsync(connection, provider, "SampleProduct", cancellationToken,
            ("Id", 2000), ("TenantId", TenantB), ("Sku", "B-SECRET"), ("Name", "Tenant B Product"), ("Price", 99m), ("IsActive", true));
        await InsertRawAsync(connection, provider, "SampleOrder", cancellationToken,
            ("Id", 6000), ("TenantId", TenantB), ("CustomerId", 200), ("Status", "Open"), ("Total", 999m));
        await InsertRawAsync(connection, provider, "SampleOrderLine", cancellationToken,
            ("Id", 8000), ("TenantId", TenantB), ("OrderId", 6000), ("ProductId", 2000), ("Quantity", 1), ("UnitPrice", 999m));
    }

    private static async Task InsertRawAsync(DbConnection connection, DatabaseProvider provider, string table,
        CancellationToken cancellationToken, params (string Column, object Value)[] columns)
    {
        await using var command = connection.CreateCommand();
        var cols = string.Join(", ", columns.Select(c => provider.Escape(c.Column)));
        var vals = string.Join(", ", columns.Select((_, i) => provider.ParamPrefix + "p" + i));
        command.CommandText = $"INSERT INTO {provider.Escape(table)} ({cols}) VALUES ({vals})";
        for (var i = 0; i < columns.Length; i++)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = provider.ParamPrefix + "p" + i;
            parameter.Value = columns[i].Value;
            command.Parameters.Add(parameter);
        }
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task VerifyTenantBoundaryAsync(
        DbContext ctx,
        DbConnection connection,
        DatabaseProvider provider,
        CancellationToken cancellationToken)
    {
        var tenantBProductId = 2000;
        var updated = await ctx.Query<StoreProduct>()
            .Where(p => p.Id == tenantBProductId)
            .ExecuteUpdateAsync(s => s.SetProperty(p => p.Name, "cross-tenant-write"), cancellationToken);
        await RequireAsync(updated == 0, "cross-tenant ExecuteUpdate should affect zero rows");

        var deleted = await ctx.Query<StoreOrder>()
            .Where(o => o.Id == 6000)
            .ExecuteDeleteAsync(cancellationToken);
        await RequireAsync(deleted == 0, "cross-tenant ExecuteDelete should affect zero rows");

        await RequireAsync(await ScalarAsync<string>(connection, provider,
                "SampleProduct", "Name", "Id", tenantBProductId, cancellationToken) == "Tenant B Product",
            "tenant B product should remain unchanged");
        await RequireAsync(await CountRowsAsync(connection, provider, "SampleOrder", "Id", 6000, cancellationToken) == 1,
            "tenant B order should remain undeleted");
    }

    private static async Task VerifyBulkAndRepresentativeLinqAsync(DbContext ctx, CancellationToken cancellationToken)
    {
        await ctx.BulkInsertAsync(new[]
        {
            new StoreEvent { Id = 9000, TenantId = TenantA, EventType = "order", Message = "packed", CreatedUtc = DateTime.UtcNow },
            new StoreEvent { Id = 9001, TenantId = TenantA, EventType = "order", Message = "shipped", CreatedUtc = DateTime.UtcNow }
        }, cancellationToken);

        var productDtos = await ctx.Query<StoreProduct>()
            .Where(p => p.IsActive && p.Price > 5m)
            .Select(p => new StoreProductDto { Id = p.Id, Sku = p.Sku, Name = p.Name, Price = p.Price })
            .ToListAsync(cancellationToken);
        await RequireAsync(productDtos.Count == 2 && productDtos.All(p => p.Sku.StartsWith("A-", StringComparison.Ordinal)),
            "projected product query should only return tenant A rows; returned " +
            string.Join(", ", productDtos.Select(p => p.Sku)));

        var paged = await ctx.Query<StoreProduct>()
            .Where(p => p.IsActive)
            .OrderBy(p => p.Name)
            .Skip(0)
            .Take(1)
            .ToListAsync(cancellationToken);
        await RequireAsync(paged.Count == 1 && paged[0].Sku.StartsWith("A-", StringComparison.Ordinal),
            "OrderBy/Skip/Take should page tenant A rows only");

        var included = await ((INormQueryable<StoreOrder>)ctx.Query<StoreOrder>())
            .AsSplitQuery()
            .Include(o => o.Lines)
            .Where(o => o.Id == 5000)
            .ToListAsync(cancellationToken);
        await RequireAsync(included.Count == 1 && included[0].Lines.Count == 2,
            "Include().AsSplitQuery() should load tenant-visible order lines");

        var totals = await ctx.Query<StoreOrder>()
            .GroupBy(o => o.Status)
            .Select(g => new { Status = g.Key, Total = g.Sum(o => o.Total) })
            .ToListAsync(cancellationToken);
        await RequireAsync(totals.Count == 1 && totals[0].Total == 51.75m,
            "GroupBy aggregate should summarize tenant A orders only");

        var compiled = Norm.CompileQuery<DbContext, decimal, StoreProduct>(
            (db, minPrice) => db.Query<StoreProduct>()
                .Where(p => p.Price >= minPrice)
                .OrderBy(p => p.Id));
        var compiledRows = await compiled(ctx, 10m);
        await RequireAsync(compiledRows.Count == 1 && compiledRows[0].Sku == "A-COFFEE",
            "compiled query should preserve tenant boundary");

        // Join to customers: the tenant filter must apply to the INNER (joined-to)
        // sequence, not just the outer orders — a cross-tenant customer must never
        // surface through the join. Every visible order belongs to tenant A, whose
        // sole customer is "Aster Operations"; tenant B's "Boreal Retail" must not leak.
        var orderCustomers = await ctx.Query<StoreOrder>()
            .Join(ctx.Query<StoreCustomer>(), o => o.CustomerId, c => c.Id,
                (o, c) => new { o.Id, Customer = c.Name })
            .ToListAsync(cancellationToken);
        await RequireAsync(
            orderCustomers.Count > 0 && orderCustomers.All(x => x.Customer == "Aster Operations"),
            "join to customers must apply the tenant filter to the inner sequence; " +
            "no cross-tenant customer may surface. Got: " +
            string.Join(", ", orderCustomers.Select(x => x.Customer).Distinct()));
    }

    private static async Task VerifyTemporalAsync(
        DbContext ctx,
        DbConnection connection,
        DatabaseProvider provider,
        CancellationToken cancellationToken)
    {
        // SQLite temporal triggers use second precision, so keep tag timestamps away
        // from adjacent writes. Server providers tolerate this delay as deterministic evidence.
        await Task.Delay(TimeSpan.FromSeconds(2.2), cancellationToken);
        var tagName = "sample-before-price-change-" + Guid.NewGuid().ToString("N");
        await ctx.CreateTagAsync(tagName);
        await Task.Delay(TimeSpan.FromSeconds(2.2), cancellationToken);

        var product = await ctx.Query<StoreProduct>().Where(p => p.Id == 1000).SingleAsync(cancellationToken);
        product.Price = 15.75m;
        product.Name = "Coffee Updated";
        await ctx.SaveChangesAsync(cancellationToken);

        var oldRows = (await ctx.Query<StoreProduct>()
            .AsOf(tagName)
            .ToListAsync(cancellationToken))
            .Where(p => p.Id == 1000)
            .ToList();
        var history = await ReadProductHistoryAsync(connection, provider, cancellationToken);
        var tagTimestamp = await ScalarAsync<string>(connection, provider,
            "__NormTemporalTags", "Timestamp", "TagName", tagName, cancellationToken);
        await RequireAsync(oldRows.Count == 1 && oldRows[0].Name == "Coffee" && oldRows[0].Price == 12.50m,
            "AsOf(tag) should return tenant A's old product state; returned " +
            string.Join(", ", oldRows.Select(p => $"{p.Id}:{p.Name}:{p.Price}")) +
            "; tag=" + tagTimestamp +
            "; history=" + history);

        var current = await ctx.Query<StoreProduct>().Where(p => p.Id == 1000).SingleAsync(cancellationToken);
        await RequireAsync(current.Name == "Coffee Updated" && current.Price == 15.75m,
            "current query should return updated product state");

        var historyRows = await ctx.GetTemporalHistoryAsync<StoreProduct>(1000, cancellationToken);
        await RequireAsync(historyRows.Count >= 2 &&
                           historyRows.Any(h => h.Operation == "I" && h.Entity.Name == "Coffee") &&
                           historyRows.Any(h => h.Operation == "U" && h.Entity.Name == "Coffee Updated"),
            "GetTemporalHistoryAsync should return insert/update history for tenant A product");

        var tenantBTemporalLeak = (await ctx.Query<StoreProduct>()
            .AsOf(tagName)
            .ToListAsync(cancellationToken))
            .Where(p => p.Id == 2000)
            .ToList();
        await RequireAsync(tenantBTemporalLeak.Count == 0,
            "AsOf(tag) should keep tenant B invisible to tenant A");

        var tenantBHistoryLeak = await ctx.GetTemporalHistoryAsync<StoreProduct>(2000, cancellationToken);
        await RequireAsync(tenantBHistoryLeak.Count == 0,
            "GetTemporalHistoryAsync should keep tenant B invisible to tenant A");

        var restored = await ctx.RestoreTemporalVersionAsync<StoreProduct>(1000, tagName, cancellationToken);
        var restoredProduct = await ctx.Query<StoreProduct>().Where(p => p.Id == 1000).SingleAsync(cancellationToken);
        await RequireAsync(restored == 1 &&
                           restoredProduct.Name == "Coffee" &&
                           restoredProduct.Price == 12.50m,
            "RestoreTemporalVersionAsync should restore the tenant-visible product from the tag");
    }

    private static async Task ResetSchemaAsync(
        DbConnection connection,
        DatabaseProvider provider,
        StoreProvider storeProvider,
        CancellationToken cancellationToken)
    {
        var tables = new[]
        {
            "SampleOrderLine", "SampleOrder", "SampleStoreEvent", "SampleProduct", "SampleCustomer", "SampleTenant"
        };

        foreach (var table in tables)
            await DropTemporalArtifactsAsync(connection, provider, storeProvider, table, cancellationToken);

        foreach (var table in tables)
        {
            await ExecuteAsync(connection, DropTableSql(storeProvider, table, provider.Escape(table + "_History")), cancellationToken);
            await ExecuteAsync(connection, DropTableSql(storeProvider, table, provider.Escape(table)), cancellationToken);
        }

        await ExecuteAsync(connection, DropTableSql(storeProvider, "__NormTemporalTags", provider.Escape("__NormTemporalTags")), cancellationToken);

        await ExecuteAsync(connection, CreateTableSql(storeProvider, provider, "SampleTenant",
            ("Id", IntType(storeProvider) + " PRIMARY KEY"),
            ("TenantId", IntType(storeProvider) + " NOT NULL"),
            ("Name", StringType(storeProvider, 200) + " NOT NULL")), cancellationToken);

        await ExecuteAsync(connection, CreateTableSql(storeProvider, provider, "SampleCustomer",
            ("Id", IntType(storeProvider) + " PRIMARY KEY"),
            ("TenantId", IntType(storeProvider) + " NOT NULL"),
            ("Name", StringType(storeProvider, 200) + " NOT NULL"),
            ("Email", StringType(storeProvider, 200) + " NOT NULL"),
            ("IsActive", BoolType(storeProvider) + " NOT NULL")), cancellationToken);

        await ExecuteAsync(connection, CreateTableSql(storeProvider, provider, "SampleProduct",
            ("Id", IntType(storeProvider) + " PRIMARY KEY"),
            ("TenantId", IntType(storeProvider) + " NOT NULL"),
            ("Sku", StringType(storeProvider, 80) + " NOT NULL"),
            ("Name", StringType(storeProvider, 200) + " NOT NULL"),
            ("Price", DecimalType(storeProvider) + " NOT NULL"),
            ("IsActive", BoolType(storeProvider) + " NOT NULL")), cancellationToken);

        await ExecuteAsync(connection, CreateTableSql(storeProvider, provider, "SampleOrder",
            ("Id", IntType(storeProvider) + " PRIMARY KEY"),
            ("TenantId", IntType(storeProvider) + " NOT NULL"),
            ("CustomerId", IntType(storeProvider) + " NOT NULL"),
            ("Status", StringType(storeProvider, 40) + " NOT NULL"),
            ("Total", DecimalType(storeProvider) + " NOT NULL")), cancellationToken);

        await ExecuteAsync(connection, CreateTableSql(storeProvider, provider, "SampleOrderLine",
            ("Id", IntType(storeProvider) + " PRIMARY KEY"),
            ("TenantId", IntType(storeProvider) + " NOT NULL"),
            ("OrderId", IntType(storeProvider) + " NOT NULL"),
            ("ProductId", IntType(storeProvider) + " NOT NULL"),
            ("Quantity", IntType(storeProvider) + " NOT NULL"),
            ("UnitPrice", DecimalType(storeProvider) + " NOT NULL")), cancellationToken);

        await ExecuteAsync(connection, CreateTableSql(storeProvider, provider, "SampleStoreEvent",
            ("Id", IntType(storeProvider) + " PRIMARY KEY"),
            ("TenantId", IntType(storeProvider) + " NOT NULL"),
            ("EventType", StringType(storeProvider, 80) + " NOT NULL"),
            ("Message", StringType(storeProvider, 400) + " NOT NULL"),
            ("CreatedUtc", DateTimeType(storeProvider) + " NOT NULL")), cancellationToken);
    }

    private static async Task DropTemporalArtifactsAsync(
        DbConnection connection,
        DatabaseProvider provider,
        StoreProvider storeProvider,
        string table,
        CancellationToken cancellationToken)
    {
        try
        {
            if (storeProvider.Kind == StoreProviderKind.Postgres)
            {
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(table + "_TemporalTrigger")} ON {provider.Escape(table)}", cancellationToken);
                await ExecuteAsync(connection, $"DROP FUNCTION IF EXISTS {provider.Escape(table + "_TemporalFunction")}()", cancellationToken);
            }
            else if (storeProvider.Kind == StoreProviderKind.SqlServer)
            {
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(table + "_TemporalInsert")}", cancellationToken);
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(table + "_TemporalUpdate")}", cancellationToken);
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(table + "_TemporalDelete")}", cancellationToken);
            }
            else if (storeProvider.Kind == StoreProviderKind.MySql)
            {
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(table + "_ai")}", cancellationToken);
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(table + "_au")}", cancellationToken);
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(table + "_ad")}", cancellationToken);
            }
        }
        catch
        {
            // Best-effort cleanup; the following create path reports real failures.
        }
    }

    private static string DropTableSql(StoreProvider storeProvider, string tableName, string escapedTable)
        => storeProvider.Kind == StoreProviderKind.SqlServer
            ? $"IF OBJECT_ID(N'{tableName}', N'U') IS NOT NULL DROP TABLE {escapedTable};"
            : $"DROP TABLE IF EXISTS {escapedTable};";

    private static string CreateTableSql(StoreProvider storeProvider, DatabaseProvider provider, string table, params (string Name, string Definition)[] columns)
    {
        var columnSql = string.Join(", ", columns.Select(c => provider.Escape(c.Name) + " " + c.Definition));
        var suffix = storeProvider.Kind == StoreProviderKind.MySql ? " ENGINE=InnoDB" : "";
        return $"CREATE TABLE {provider.Escape(table)} ({columnSql}){suffix}";
    }

    private static string IntType(StoreProvider provider) => provider.Kind == StoreProviderKind.Sqlite ? "INTEGER" : "INT";

    private static string StringType(StoreProvider provider, int length) => provider.Kind == StoreProviderKind.SqlServer
        ? $"NVARCHAR({length})"
        : $"VARCHAR({length})";

    private static string DecimalType(StoreProvider provider) => provider.Kind == StoreProviderKind.Sqlite
        ? "TEXT"
        : "DECIMAL(18,4)";

    private static string BoolType(StoreProvider provider) => provider.Kind switch
    {
        StoreProviderKind.Sqlite => "INTEGER",
        StoreProviderKind.SqlServer => "BIT",
        StoreProviderKind.Postgres => "BOOLEAN",
        StoreProviderKind.MySql => "BOOLEAN",
        _ => throw new ArgumentOutOfRangeException(nameof(provider))
    };

    private static string DateTimeType(StoreProvider provider) => provider.Kind switch
    {
        StoreProviderKind.Sqlite => "TEXT",
        StoreProviderKind.SqlServer => "DATETIME2",
        StoreProviderKind.Postgres => "TIMESTAMP",
        StoreProviderKind.MySql => "DATETIME(6)",
        _ => throw new ArgumentOutOfRangeException(nameof(provider))
    };

    private static async Task ExecuteAsync(DbConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<T?> ScalarAsync<T>(
        DbConnection connection,
        DatabaseProvider provider,
        string table,
        string column,
        string keyColumn,
        object keyValue,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {provider.Escape(column)} FROM {provider.Escape(table)} WHERE {provider.Escape(keyColumn)} = {provider.ParamPrefix}p0";
        var parameter = command.CreateParameter();
        parameter.ParameterName = provider.ParamPrefix + "p0";
        parameter.Value = keyValue;
        command.Parameters.Add(parameter);
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is null or DBNull ? default : (T)Convert.ChangeType(result, typeof(T), CultureInfo.InvariantCulture);
    }

    private static async Task<long> CountRowsAsync(
        DbConnection connection,
        DatabaseProvider provider,
        string table,
        string keyColumn,
        object keyValue,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM {provider.Escape(table)} WHERE {provider.Escape(keyColumn)} = {provider.ParamPrefix}p0";
        var parameter = command.CreateParameter();
        parameter.ParameterName = provider.ParamPrefix + "p0";
        parameter.Value = keyValue;
        command.Parameters.Add(parameter);
        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
    }

    private static async Task<string> ReadProductHistoryAsync(DbConnection connection, DatabaseProvider provider, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT " +
            provider.Escape("Id") + ", " +
            provider.Escape("Name") + ", " +
            provider.Escape("Price") + ", " +
            provider.Escape("__ValidFrom") + ", " +
            provider.Escape("__ValidTo") +
            " FROM " + provider.Escape("SampleProduct_History") +
            " WHERE " + provider.Escape("Id") + " = " + provider.ParamPrefix + "p0" +
            " ORDER BY " + provider.Escape("__ValidFrom");
        var parameter = command.CreateParameter();
        parameter.ParameterName = provider.ParamPrefix + "p0";
        parameter.Value = 1000;
        command.Parameters.Add(parameter);
        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add($"{reader.GetValue(0)}:{reader.GetValue(1)}:{reader.GetValue(2)}:{reader.GetValue(3)}..{reader.GetValue(4)}");
        }

        return string.Join("|", rows);
    }

    private static Task RequireAsync(bool condition, string message)
        => condition ? Task.CompletedTask : throw new StoreScenarioException(message);

    private sealed class FixedTenantProvider(int tenantId) : ITenantProvider
    {
        public object GetCurrentTenantId() => tenantId;
    }
}

public sealed record StoreScenarioResult(bool Success, string Summary);

public sealed class StoreScenarioException(string message) : Exception(message);
