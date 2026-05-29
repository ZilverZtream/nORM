using System.Data.Common;
using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using nORM.Query;

namespace nORM.Sample.Store;

public static class StoreWebApp
{
    public static async Task RunAsync(string[] args)
    {
        var providerName = ReadProviderName(args) ?? "sqlite";
        var provider = StoreProvider.Parse(providerName)
            ?? throw new InvalidOperationException($"Unknown provider '{providerName}'.");

        var app = CreateBuilder(args).Build();
        Configure(app);
        var db = StoreDatabase.Open(provider);
        await StoreScenario.RunAsync(db.Connection, db.Provider, provider);
        await db.DisposeAsync();

        app.Logger.LogInformation("nORM Sample Store running with {Provider}.", provider.Name);
        await app.RunAsync();

        WebApplicationBuilder CreateBuilder(string[] rawArgs)
        {
            var builder = WebApplication.CreateBuilder(rawArgs);
            builder.Services.AddSingleton(provider);
            builder.Services.AddSingleton<StoreDatabaseFactory>();
            builder.Services
                .AddAuthentication(CookieAuthenticationDefaults.AuthenticationScheme)
                .AddCookie(options =>
                {
                    options.Cookie.Name = "norm.sample.store";
                    options.LoginPath = "/";
                    options.AccessDeniedPath = "/";
                    options.SlidingExpiration = true;
                });
            builder.Services.AddAuthorization();
            builder.Services.AddHttpContextAccessor();
            return builder;
        }
    }

    private static void Configure(WebApplication app)
    {
        app.UseDefaultFiles();
        app.UseStaticFiles();
        app.UseAuthentication();
        app.UseAuthorization();

        app.MapPost("/api/login", async (LoginRequest request, HttpContext http) =>
        {
            var tenant = request.Tenant.Equals("tenant-b", StringComparison.OrdinalIgnoreCase) ? 202 : 101;
            var display = tenant == 101 ? "Tenant A" : "Tenant B";
            var claims = new[]
            {
                new Claim(ClaimTypes.NameIdentifier, tenant.ToString()),
                new Claim(ClaimTypes.Name, display)
            };
            await http.SignInAsync(
                CookieAuthenticationDefaults.AuthenticationScheme,
                new ClaimsPrincipal(new ClaimsIdentity(claims, CookieAuthenticationDefaults.AuthenticationScheme)));
            return Results.Ok(new { tenantId = tenant, name = display });
        });

        app.MapPost("/api/logout", async (HttpContext http) =>
        {
            await http.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
            return Results.Ok();
        });

        var api = app.MapGroup("/api").RequireAuthorization();

        api.MapGet("/me", (HttpContext http, StoreProvider provider) =>
            Results.Ok(new
            {
                tenantId = CurrentTenant(http),
                tenantName = http.User.Identity?.Name ?? "Unknown tenant",
                provider = provider.Name
            }));

        api.MapGet("/dashboard", async (HttpContext http, StoreDatabaseFactory dbFactory) =>
        {
            await using var scope = await dbFactory.OpenTenantAsync(CurrentTenant(http), temporal: true);
            var products = await scope.Context.Query<StoreProduct>()
                .Where(p => p.IsActive)
                .OrderBy(p => p.Name)
                .Select(p => new StoreProductDto { Id = p.Id, Sku = p.Sku, Name = p.Name, Price = p.Price })
                .ToListAsync();
            var orders = await ((INormQueryable<StoreOrder>)scope.Context.Query<StoreOrder>())
                .AsSplitQuery()
                .Include(o => o.Lines)
                .OrderBy(o => o.Id)
                .ToListAsync();
            var totals = await scope.Context.Query<StoreOrder>()
                .GroupBy(o => o.Status)
                .Select(g => new { Status = g.Key, Total = g.Sum(o => o.Total) })
                .ToListAsync();
            var revenue = totals.Sum(t => t.Total);
            var recentEvents = await scope.Context.Query<StoreEvent>()
                .OrderByDescending(e => e.CreatedUtc)
                .Take(5)
                .ToListAsync();

            var tenantBoundary = scope.Context.GetTenantBoundaryDiagnostics<StoreProduct>("dashboard query");
            return Results.Ok(new
            {
                tenantBoundary = new
                {
                    tenantBoundary.TableName,
                    tenantBoundary.IsTenantScoped,
                    tenantBoundary.TenantColumnName,
                    tenantBoundary.TenantIdType,
                    tenantBoundary.ParameterName,
                    tenantBoundary.PredicateSql
                },
                metrics = new
                {
                    activeProducts = products.Count,
                    openOrders = orders.Count,
                    revenue,
                    eventCount = await scope.Context.Query<StoreEvent>().CountAsync()
                },
                products,
                orders = orders.Select(o => new
                {
                    o.Id,
                    o.Status,
                    o.Total,
                    lineCount = o.Lines.Count,
                    itemCount = o.Lines.Sum(l => l.Quantity)
                }),
                totals,
                recentEvents = recentEvents.Select(e => new
                {
                    e.Id,
                    e.EventType,
                    e.Message,
                    e.CreatedUtc
                })
            });
        });

        api.MapPost("/tenant-boundary/prove", async (HttpContext http, StoreDatabaseFactory dbFactory) =>
        {
            await using var scope = await dbFactory.OpenTenantAsync(CurrentTenant(http), temporal: true);
            var attemptedProductId = CurrentTenant(http) == 101 ? 2000 : 1000;
            var attemptedOrderId = CurrentTenant(http) == 101 ? 6000 : 5000;

            var updated = await scope.Context.Query<StoreProduct>()
                .Where(p => p.Id == attemptedProductId)
                .ExecuteUpdateAsync(s => s.SetProperty(p => p.Name, "cross-tenant-write"));
            var deleted = await scope.Context.Query<StoreOrder>()
                .Where(o => o.Id == attemptedOrderId)
                .ExecuteDeleteAsync();

            return Results.Ok(new
            {
                attemptedProductId,
                attemptedOrderId,
                updated,
                deleted,
                passed = updated == 0 && deleted == 0
            });
        });

        api.MapPost("/products/{id:int}/price", async (int id, UpdatePriceRequest request, HttpContext http, StoreDatabaseFactory dbFactory) =>
        {
            await using var scope = await dbFactory.OpenTenantAsync(CurrentTenant(http), temporal: true);
            var updated = await scope.Context.Query<StoreProduct>()
                .Where(p => p.Id == id)
                .ExecuteUpdateAsync(s => s.SetProperty(p => p.Price, request.Price));
            return Results.Ok(new { updated });
        });

        api.MapPost("/temporal/tags", async (CreateTagRequest request, HttpContext http, StoreDatabaseFactory dbFactory) =>
        {
            await using var scope = await dbFactory.OpenTenantAsync(CurrentTenant(http), temporal: true);
            await scope.Context.CreateTagAsync(request.Name);
            return Results.Ok(new { request.Name });
        });

        api.MapGet("/temporal/products/{id:int}", async (int id, string tag, HttpContext http, StoreDatabaseFactory dbFactory) =>
        {
            await using var scope = await dbFactory.OpenTenantAsync(CurrentTenant(http), temporal: true);
            var rows = await scope.Context.Query<StoreProduct>()
                .AsOf(tag)
                .ToListAsync();
            var row = rows.SingleOrDefault(p => p.Id == id);
            return row is null ? Results.NotFound() : Results.Ok(new { row.Id, row.Sku, row.Name, row.Price });
        });

        api.MapGet("/temporal/products/{id:int}/history", async (int id, HttpContext http, StoreDatabaseFactory dbFactory) =>
        {
            await using var scope = await dbFactory.OpenTenantAsync(CurrentTenant(http), temporal: true);
            var history = await scope.Context.GetTemporalHistoryAsync<StoreProduct>(id);
            return Results.Ok(history.Select(h => new
            {
                h.Operation,
                h.ValidFrom,
                h.ValidTo,
                h.Entity.Id,
                h.Entity.Sku,
                h.Entity.Name,
                h.Entity.Price
            }));
        });

        api.MapGet("/temporal/products/{id:int}/diff", async (int id, HttpContext http, StoreDatabaseFactory dbFactory) =>
        {
            await using var scope = await dbFactory.OpenTenantAsync(CurrentTenant(http), temporal: true);
            var diff = await scope.Context.GetTemporalDiffAsync<StoreProduct>(id);
            return Results.Ok(diff.Select(d => new
            {
                from = d.Previous.ValidFrom,
                to = d.Current.ValidFrom,
                operation = d.Current.Operation,
                changes = d.Changes.Select(c => new
                {
                    c.PropertyName,
                    c.PreviousValue,
                    c.CurrentValue
                })
            }));
        });

        api.MapPost("/temporal/products/{id:int}/restore", async (int id, RestoreTemporalRequest request, HttpContext http, StoreDatabaseFactory dbFactory) =>
        {
            await using var scope = await dbFactory.OpenTenantAsync(CurrentTenant(http), temporal: true);
            var restored = await scope.Context.RestoreTemporalVersionAsync<StoreProduct>(id, request.Tag);
            return Results.Ok(new { restored });
        });

        api.MapPost("/temporal/products/{id:int}/history/prune", async (int id, HttpContext http, StoreDatabaseFactory dbFactory) =>
        {
            await using var scope = await dbFactory.OpenTenantAsync(CurrentTenant(http), temporal: true);
            var before = await scope.Context.GetTemporalHistoryAsync<StoreProduct>(id);
            var pruned = await scope.Context.PruneTemporalHistoryAsync<StoreProduct>(DateTime.UtcNow.AddSeconds(1));
            var after = await scope.Context.GetTemporalHistoryAsync<StoreProduct>(id);
            return Results.Ok(new
            {
                before = before.Count,
                pruned,
                after = after.Count
            });
        });

        api.MapPost("/events/bulk", async (HttpContext http, StoreDatabaseFactory dbFactory) =>
        {
            var tenant = CurrentTenant(http);
            await using var scope = await dbFactory.OpenTenantAsync(tenant, temporal: true);
            var seed = tenant * 100000 + Random.Shared.Next(1000, 9999);
            var events = Enumerable.Range(0, 5)
                .Select(i => new StoreEvent
                {
                    Id = seed + i,
                    TenantId = tenant,
                    EventType = "ui-demo",
                    Message = "Dashboard event " + (i + 1),
                    CreatedUtc = DateTime.UtcNow
                })
                .ToArray();
            var inserted = await scope.Context.BulkInsertAsync(events);
            return Results.Ok(new { inserted });
        });

        api.MapPost("/admin/verify", async (StoreProvider provider) =>
        {
            await using var db = StoreDatabase.Open(provider);
            var result = await StoreScenario.RunAsync(db.Connection, db.Provider, provider);
            return Results.Ok(result);
        });

    }

    private static string? ReadProviderName(string[] args)
    {
        for (var i = 0; i < args.Length - 1; i++)
        {
            if (args[i].Equals("--provider", StringComparison.OrdinalIgnoreCase))
                return args[i + 1];
        }

        return null;
    }

    private static int CurrentTenant(HttpContext http)
        => int.Parse(http.User.FindFirstValue(ClaimTypes.NameIdentifier)
            ?? throw new InvalidOperationException("Missing tenant claim."));
}

internal sealed class StoreDatabaseFactory(StoreProvider provider)
{
    public Task<StoreTenantScope> OpenTenantAsync(int tenantId, bool temporal)
    {
        var db = StoreDatabase.Open(provider);
        var options = StoreDbContextOptions.Create(tenantId, temporal);
        var context = new DbContext(db.Connection, db.Provider, options);
        return Task.FromResult(new StoreTenantScope(db, context));
    }
}

internal sealed class StoreTenantScope(StoreDatabase database, DbContext context) : IAsyncDisposable
{
    public DbContext Context { get; } = context;

    public async ValueTask DisposeAsync()
    {
        Context.Dispose();
        await database.DisposeAsync();
    }
}

internal sealed class StoreDatabase(DbConnection connection, DatabaseProvider provider) : IAsyncDisposable
{
    public DbConnection Connection { get; } = connection;
    public DatabaseProvider Provider { get; } = provider;

    public static StoreDatabase Open(StoreProvider provider)
    {
        if (provider.Kind == StoreProviderKind.Sqlite)
        {
            Directory.CreateDirectory(Path.Combine(AppContext.BaseDirectory, "data"));
            var path = Path.Combine(AppContext.BaseDirectory, "data", "nORM.Sample.Store.sqlite.db");
            var sqliteConnection = new SqliteConnection($"Data Source={path}");
            sqliteConnection.Open();
            return new StoreDatabase(sqliteConnection, new SqliteProvider());
        }

        var connectionString = StoreProviderConnectionStrings.Get(provider)
            ?? throw new InvalidOperationException($"Connection string for {provider.Name} is not configured.");
        var connection = provider.Kind switch
        {
            StoreProviderKind.SqlServer => CreateConnection("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient", connectionString),
            StoreProviderKind.Postgres => CreateConnection("Npgsql.NpgsqlConnection, Npgsql", connectionString),
            StoreProviderKind.MySql => CreateConnection("MySqlConnector.MySqlConnection, MySqlConnector", connectionString),
            _ => throw new ArgumentOutOfRangeException(nameof(provider))
        };
        connection.Open();
        DatabaseProvider dbProvider = provider.Kind switch
        {
            StoreProviderKind.SqlServer => new SqlServerProvider(),
            StoreProviderKind.Postgres => new PostgresProvider(),
            StoreProviderKind.MySql => new MySqlProvider(),
            _ => throw new ArgumentOutOfRangeException(nameof(provider))
        };
        return new StoreDatabase(connection, dbProvider);
    }

    private static DbConnection CreateConnection(string typeName, string connectionString)
    {
        var type = Type.GetType(typeName)
            ?? throw new InvalidOperationException($"Could not load '{typeName}'. Ensure the provider driver package is restored.");
        var connection = (DbConnection)Activator.CreateInstance(type)!;
        connection.ConnectionString = connectionString;
        return connection;
    }

    public async ValueTask DisposeAsync()
        => await Connection.DisposeAsync();
}

public static class StoreDbContextOptions
{
    public static DbContextOptions Create(int tenantId, bool temporal)
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
        if (temporal)
            options.EnableTemporalVersioning();
        return options;
    }

    private sealed class FixedTenantProvider(int tenantId) : ITenantProvider
    {
        public object GetCurrentTenantId() => tenantId;
    }
}

public sealed record LoginRequest(string Tenant);
public sealed record UpdatePriceRequest(decimal Price);
public sealed record CreateTagRequest(string Name);
public sealed record RestoreTemporalRequest(string Tag);
