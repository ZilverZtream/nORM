using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.Extensions.DependencyInjection;
using nORM.Core;
using nORM.Query;

namespace nORM.Sample.Store;

/// <summary>
/// The ASP.NET Core host, wired the way a real team would wire nORM: a swappable
/// <see cref="ProviderSettings"/> singleton, one scoped <see cref="StoreContext"/> per request from
/// <c>AddNorm&lt;StoreContext&gt;</c> (bound to the active engine + the caller's tenant), and endpoints
/// that inject that context directly. No endpoint below names a database — switching the engine at
/// runtime via the Settings API is all it takes.
/// </summary>
public static class StoreWebApp
{
    public static async Task RunAsync(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        builder.Services.AddSingleton<ProviderSettings>();
        builder.Services.AddSingleton<StoreContextFactory>();
        builder.Services.AddHttpContextAccessor();

        // Dogfood nORM's DI: one scoped StoreContext per request, built for whatever engine is live
        // and scoped to the authenticated tenant. The container disposes it (and its connection) at
        // request end. This is the registration a real app writes once and never revisits.
        builder.Services.AddNorm<StoreContext>(sp =>
        {
            var settings = sp.GetRequiredService<ProviderSettings>();
            var http = sp.GetRequiredService<IHttpContextAccessor>().HttpContext;
            var tenantId = ResolveTenant(http);
            var kind = settings.ActiveKind;
            var connection = settings.OpenConnection(kind);
            var provider = ProviderSettings.CreateDatabaseProvider(kind);
            return new StoreContext(connection, provider, StoreContextFactory.CreateOptions(tenantId, temporal: true));
        }, ServiceLifetime.Scoped);

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

        var app = builder.Build();

        // Bring the initially-selected engine online: apply the requested --provider (falling back to
        // SQLite), bootstrap its schema + seed, and make it the active engine.
        var settingsSvc = app.Services.GetRequiredService<ProviderSettings>();
        var initial = StoreProvider.Parse(ReadProviderName(args) ?? "sqlite")?.Kind ?? StoreProviderKind.Sqlite;
        await BootstrapAsync(settingsSvc, initial, app.Logger);
        settingsSvc.Activate(initial);

        Configure(app);
        app.Logger.LogInformation("nORM Sample Store running. Active engine: {Provider}.", settingsSvc.ActiveProvider.Name);
        await app.RunAsync();
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

        app.MapGet("/api/me", (HttpContext http, ProviderSettings settings) =>
            Results.Ok(new
            {
                authenticated = http.User.Identity?.IsAuthenticated ?? false,
                tenantId = (http.User.Identity?.IsAuthenticated ?? false) ? CurrentTenant(http) : (int?)null,
                tenantName = http.User.Identity?.Name,
                provider = settings.ActiveProvider.Name
            }));

        var api = app.MapGroup("/api").RequireAuthorization();

        api.MapGet("/dashboard", async (StoreContext ctx) =>
        {
            var products = await ctx.Query<StoreProduct>()
                .Where(p => p.IsActive)
                .OrderBy(p => p.Name)
                .Select(p => new StoreProductDto { Id = p.Id, Sku = p.Sku, Name = p.Name, Price = p.Price })
                .ToListAsync();
            var orders = await ((INormQueryable<StoreOrder>)ctx.Query<StoreOrder>())
                .AsSplitQuery()
                .Include(o => o.Lines)
                .OrderBy(o => o.Id)
                .ToListAsync();
            var totals = await ctx.Query<StoreOrder>()
                .GroupBy(o => o.Status)
                .Select(g => new { Status = g.Key, Total = g.Sum(o => o.Total) })
                .ToListAsync();
            var revenue = totals.Sum(t => t.Total);
            var recentEvents = await ctx.Query<StoreEvent>()
                .OrderByDescending(e => e.CreatedUtc)
                .Take(5)
                .ToListAsync();

            var tenantBoundary = ctx.GetTenantBoundaryDiagnostics<StoreProduct>("dashboard query");
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
                    eventCount = await ctx.Query<StoreEvent>().CountAsync()
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
                recentEvents = recentEvents.Select(e => new { e.Id, e.EventType, e.Message, e.CreatedUtc })
            });
        });

        api.MapPost("/tenant-boundary/prove", async (HttpContext http, StoreContext ctx) =>
        {
            var attemptedProductId = CurrentTenant(http) == 101 ? 2000 : 1000;
            var attemptedOrderId = CurrentTenant(http) == 101 ? 6000 : 5000;
            var updated = await ctx.Query<StoreProduct>()
                .Where(p => p.Id == attemptedProductId)
                .ExecuteUpdateAsync(s => s.SetProperty(p => p.Name, "cross-tenant-write"));
            var deleted = await ctx.Query<StoreOrder>()
                .Where(o => o.Id == attemptedOrderId)
                .ExecuteDeleteAsync();
            return Results.Ok(new { attemptedProductId, attemptedOrderId, updated, deleted, passed = updated == 0 && deleted == 0 });
        });

        api.MapPost("/products/{id:int}/price", async (int id, UpdatePriceRequest request, StoreContext ctx) =>
        {
            var updated = await ctx.Query<StoreProduct>()
                .Where(p => p.Id == id)
                .ExecuteUpdateAsync(s => s.SetProperty(p => p.Price, request.Price));
            return Results.Ok(new { updated });
        });

        api.MapPost("/temporal/tags", async (CreateTagRequest request, StoreContext ctx) =>
        {
            await ctx.CreateTagAsync(request.Name);
            return Results.Ok(new { request.Name });
        });

        api.MapGet("/temporal/products/{id:int}", async (int id, string tag, StoreContext ctx) =>
        {
            var rows = await ctx.Query<StoreProduct>().AsOf(tag).ToListAsync();
            var row = rows.SingleOrDefault(p => p.Id == id);
            return row is null ? Results.NotFound() : Results.Ok(new { row.Id, row.Sku, row.Name, row.Price });
        });

        api.MapGet("/temporal/products/{id:int}/history", async (int id, StoreContext ctx) =>
        {
            var history = await ctx.GetTemporalHistoryAsync<StoreProduct>(id);
            return Results.Ok(history.Select(h => new { h.Operation, h.ValidFrom, h.ValidTo, h.Entity.Id, h.Entity.Sku, h.Entity.Name, h.Entity.Price }));
        });

        api.MapGet("/temporal/products/{id:int}/diff", async (int id, StoreContext ctx) =>
        {
            var diff = await ctx.GetTemporalDiffAsync<StoreProduct>(id);
            return Results.Ok(diff.Select(d => new
            {
                from = d.Previous.ValidFrom,
                to = d.Current.ValidFrom,
                operation = d.Current.Operation,
                changes = d.Changes.Select(c => new { c.PropertyName, c.PreviousValue, c.CurrentValue })
            }));
        });

        api.MapPost("/temporal/products/{id:int}/restore", async (int id, RestoreTemporalRequest request, StoreContext ctx) =>
        {
            var restored = await ctx.RestoreTemporalVersionAsync<StoreProduct>(id, request.Tag);
            return Results.Ok(new { restored });
        });

        api.MapPost("/temporal/products/{id:int}/history/prune", async (int id, StoreContext ctx) =>
        {
            var before = await ctx.GetTemporalHistoryAsync<StoreProduct>(id);
            var pruned = await ctx.PruneTemporalHistoryAsync<StoreProduct>(DateTime.UtcNow.AddSeconds(1));
            var after = await ctx.GetTemporalHistoryAsync<StoreProduct>(id);
            return Results.Ok(new { before = before.Count, pruned, after = after.Count });
        });

        api.MapPost("/events/bulk", async (HttpContext http, StoreContext ctx) =>
        {
            var tenant = CurrentTenant(http);
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
            var inserted = await ctx.BulkInsertAsync(events);
            return Results.Ok(new { inserted });
        });

        // ── Provider settings + live engine swap (the headline feature) ──────────────────────────
        api.MapGet("/settings/providers", (ProviderSettings settings) =>
            Results.Ok(new { active = settings.ActiveProvider.Name, providers = settings.Snapshot() }));

        api.MapPost("/settings/providers/{name}/test", async (string name, TestConnectionRequest request, ProviderSettings settings) =>
        {
            var kind = StoreProvider.Parse(name)?.Kind;
            if (kind is null) return Results.BadRequest(new { error = $"Unknown engine '{name}'." });
            var result = await settings.TestConnectionAsync(kind.Value, request.ConnectionString);
            return Results.Ok(result);
        });

        api.MapPost("/settings/providers/{name}/connection", (string name, TestConnectionRequest request, ProviderSettings settings) =>
        {
            var kind = StoreProvider.Parse(name)?.Kind;
            if (kind is null) return Results.BadRequest(new { error = $"Unknown engine '{name}'." });
            try
            {
                settings.SetConnectionString(kind.Value, request.ConnectionString ?? "");
                return Results.Ok(new { saved = true });
            }
            catch (Exception ex) { return Results.BadRequest(new { error = ex.Message }); }
        });

        api.MapPost("/settings/providers/{name}/activate", async (string name, TestConnectionRequest request, ProviderSettings settings, StoreContextFactory factory, ILoggerFactory logs) =>
        {
            var kind = StoreProvider.Parse(name)?.Kind;
            if (kind is null) return Results.BadRequest(new { error = $"Unknown engine '{name}'." });
            if (!string.IsNullOrWhiteSpace(request.ConnectionString) && kind != StoreProviderKind.Sqlite)
                settings.SetConnectionString(kind.Value, request.ConnectionString!);
            try
            {
                await BootstrapAsync(settings, kind.Value, logs.CreateLogger("swap"));
                settings.Activate(kind.Value);
                // Prove the swap took: a fresh tenant-A context on the now-active engine reads seeded data.
                await using var proof = factory.CreateForTenant(101);
                var productCount = await proof.Query<StoreProduct>().CountAsync();
                return Results.Ok(new { active = settings.ActiveProvider.Name, ready = true, productCount });
            }
            catch (Exception ex)
            {
                return Results.BadRequest(new { active = settings.ActiveProvider.Name, ready = false, error = ex.Message });
            }
        });

        api.MapPost("/admin/verify", async (ProviderSettings settings) =>
        {
            var storeProvider = settings.ActiveProvider;
            await using var lease = new StoreConnectionLease(settings.OpenConnection(storeProvider.Kind));
            var result = await StoreScenario.RunAsync(lease.Connection, ProviderSettings.CreateDatabaseProvider(storeProvider.Kind), storeProvider);
            return Results.Ok(result);
        });

        // Multi-store isolation must hold under concurrent load, not just single-threaded. This fans
        // out many parallel per-tenant contexts (via the factory — the caller-owned path for parallel
        // work) hammering reads on the active engine, and asserts every context saw ONLY its own
        // tenant's rows. A single leaked row across any of the parallel requests fails the proof.
        api.MapPost("/isolation/concurrent-proof", async (StoreContextFactory factory) =>
        {
            const int parallelRequests = 32;
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var results = new System.Collections.Concurrent.ConcurrentBag<(int tenant, bool ownOnly, int crossTenantId, bool crossReachable)>();
            await Parallel.ForEachAsync(Enumerable.Range(0, parallelRequests), async (i, ct) =>
            {
                var tenant = i % 2 == 0 ? 101 : 202;
                var foreignProductId = tenant == 101 ? 2000 : 1000; // a product owned by the OTHER tenant
                await using var ctx = factory.CreateForTenant(tenant, temporal: false);
                // An isolated context must not surface a single row belonging to another tenant.
                var foreignProductVisible = await ctx.Query<StoreProduct>().AnyAsync(p => p.TenantId != tenant);
                var foreignOrderVisible = await ctx.Query<StoreOrder>().AnyAsync(o => o.TenantId != tenant);
                var ownOnly = !foreignProductVisible && !foreignOrderVisible;
                // And a direct by-id read of a known other-tenant row must return nothing.
                var crossReachable = await ctx.Query<StoreProduct>().AnyAsync(p => p.Id == foreignProductId);
                results.Add((tenant, ownOnly, foreignProductId, crossReachable));
            });
            sw.Stop();
            var all = results.ToList();
            var bleed = all.Any(r => !r.ownOnly || r.crossReachable);
            return Results.Ok(new
            {
                parallelRequests,
                tenantARuns = all.Count(r => r.tenant == 101),
                tenantBRuns = all.Count(r => r.tenant == 202),
                crossTenantBleed = bleed,
                isolationHeld = !bleed,
                elapsedMs = sw.ElapsedMilliseconds
            });
        });
    }

    /// <summary>Creates schema + seed for an engine so it is ready to serve (startup and on swap).</summary>
    private static async Task BootstrapAsync(ProviderSettings settings, StoreProviderKind kind, ILogger logger)
    {
        var storeProvider = ProviderSettings.Describe(kind);
        logger.LogInformation("Bootstrapping {Provider}...", storeProvider.Name);
        var connection = settings.OpenConnection(kind);
        try
        {
            await StoreScenario.RunAsync(connection, ProviderSettings.CreateDatabaseProvider(kind), storeProvider);
        }
        finally
        {
            await connection.DisposeAsync();
        }
    }

    private static string? ReadProviderName(string[] args)
    {
        for (var i = 0; i < args.Length - 1; i++)
            if (args[i].Equals("--provider", StringComparison.OrdinalIgnoreCase))
                return args[i + 1];
        return null;
    }

    private static int CurrentTenant(HttpContext http)
        => int.Parse(http.User.FindFirstValue(ClaimTypes.NameIdentifier)
            ?? throw new InvalidOperationException("Missing tenant claim."));

    private static int ResolveTenant(HttpContext? http)
        => http is not null && (http.User.Identity?.IsAuthenticated ?? false)
            ? CurrentTenant(http)
            : throw new InvalidOperationException("A StoreContext was requested outside an authenticated tenant request.");
}

/// <summary>Disposes a bare connection with await-using ergonomics.</summary>
internal sealed class StoreConnectionLease(System.Data.Common.DbConnection connection) : IAsyncDisposable
{
    public System.Data.Common.DbConnection Connection { get; } = connection;
    public ValueTask DisposeAsync() => Connection.DisposeAsync();
}

public sealed record LoginRequest(string Tenant);
public sealed record UpdatePriceRequest(decimal Price);
public sealed record CreateTagRequest(string Name);
public sealed record RestoreTemporalRequest(string Tag);
public sealed record TestConnectionRequest(string? ConnectionString);
