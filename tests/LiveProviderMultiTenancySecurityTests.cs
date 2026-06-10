using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Live-provider security gate for multi-tenancy.
///
/// Evidence gap closed: the existing AMT_AllProviders_* tests in
/// AdversarialTenantFuzzTests use SQLite as the database regardless of which
/// provider dialect is exercised (the other providers supply SQL generation;
/// SQLite executes it). These tests use real SQL Server, MySQL, and PostgreSQL
/// connections to prove the security properties hold under actual RDBMS enforcement.
///
/// Security contracts verified per provider:
///   1. SELECT only returns rows belonging to the current tenant.
///   2. UPDATE via SaveChanges only modifies the current tenant's rows.
///   3. DELETE via SaveChanges only removes the current tenant's rows.
///   4. Adversarial SQL-injection-style tenant IDs are parameterized — the tenant
///      context filter does not concatenate the tenant ID into SQL text.
///   5. INSERT via SaveChanges stamps the correct TenantId on the new row.
///   6. Norm.CompileQuery respects the current tenant context.
///   7. ExecuteUpdate/ExecuteDelete include tenant predicates under live providers.
///
/// Schema: LivTenRow (Id INT PK, TenantId VARCHAR, Payload VARCHAR).
/// Explicit int keys — no identity column — so all four providers accept the
/// same INSERT syntax.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderMultiTenancySecurityTests
{
    private const string Table = "LivTenRow";

    [Table(Table)]
    private sealed class LivTenRow
    {
        [Key]
        public int    Id       { get; set; }
        public string TenantId { get; set; } = "";
        public string Payload  { get; set; } = "";
    }

    private sealed class StringTenantProvider : ITenantProvider
    {
        private readonly string _tenantId;
        public StringTenantProvider(string tenantId) => _tenantId = tenantId;
        public object GetCurrentTenantId() => _tenantId;
    }

    // ── DDL helpers ───────────────────────────────────────────────────────────

    private static string IntCol(ProviderKind kind) =>
        kind == ProviderKind.Sqlite ? "INTEGER" : "INT";

    private static string VarCol(ProviderKind kind, int len) => kind switch
    {
        ProviderKind.SqlServer => $"NVARCHAR({len})",
        _ => $"VARCHAR({len})"
    };

    private static string DropDdl(ProviderKind kind, string esc) =>
        kind == ProviderKind.SqlServer
            ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {esc};"
            : $"DROP TABLE IF EXISTS {esc};";

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        var esc  = ctx.Provider.Escape(Table);
        var eId  = ctx.Provider.Escape("Id");
        var eTen = ctx.Provider.Escape("TenantId");
        var ePay = ctx.Provider.Escape("Payload");
        await ExecAsync(ctx, DropDdl(kind, esc));
        await ExecAsync(ctx,
            $"CREATE TABLE {esc} ({eId} {IntCol(kind)} PRIMARY KEY, {eTen} {VarCol(kind, 100)} NOT NULL, {ePay} {VarCol(kind, 200)} NOT NULL)");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try { await ExecAsync(ctx, DropDdl(kind, ctx.Provider.Escape(Table))); }
        catch { /* best-effort */ }
    }

    private static async Task ExecAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static DbContext TenantContext(DbConnection cn, DatabaseProvider prov, string tenantId)
    {
        var opts = new DbContextOptions
        {
            TenantColumnName = "TenantId",
            TenantProvider   = new StringTenantProvider(tenantId)
        };
        return new DbContext(cn, prov, opts);
    }

    // Inserts a row via raw SQL bypassing nORM (for "other tenant" seed data).
    private static async Task SeedRawAsync(DbContext ctx, int id, string tenantId, string payload)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        var esc = ctx.Provider.Escape(Table);
        cmd.CommandText = $"INSERT INTO {esc} VALUES ({id}, '{tenantId.Replace("'", "''")}', '{payload.Replace("'", "''")}')";
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<string?> ReadPayloadRawAsync(DbContext ctx, int id)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = $"SELECT {ctx.Provider.Escape("Payload")} FROM {ctx.Provider.Escape(Table)} WHERE {ctx.Provider.Escape("Id")} = {id}";
        var result = await cmd.ExecuteScalarAsync();
        return result is DBNull or null ? null : (string)result;
    }

    private static async Task<long> CountRawAsync(DbContext ctx)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {ctx.Provider.Escape(Table)}";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    // ── 1: SELECT returns only the current tenant's rows ─────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Select_ReturnsOnlyCurrentTenantRows(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live {kind} not configured")) return;
        var (cn, prov) = live!.Value;
        await using (cn)
        using (var setup = new DbContext(cn, prov))
        {
            await SetupAsync(setup, kind);
            try
            {
                // Seed two rows: one for tenant-A, one for tenant-B.
                await SeedRawAsync(setup, 1, "tenant-A", "secret-A");
                await SeedRawAsync(setup, 2, "tenant-B", "safe-B");

                using var ctxA = TenantContext(cn, prov, "tenant-A");
                var rows = ctxA.Query<LivTenRow>().ToList();

                Assert.Single(rows);
                Assert.Equal("tenant-A", rows[0].TenantId);
                Assert.Equal("secret-A", rows[0].Payload);
            }
            finally { await TeardownAsync(setup, kind); }
        }
    }

    // ── 2: UPDATE modifies only the current tenant's rows ────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Update_ModifiesOnlyCurrentTenantRows(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live {kind} not configured")) return;
        var (cn, prov) = live!.Value;
        await using (cn)
        using (var setup = new DbContext(cn, prov))
        {
            await SetupAsync(setup, kind);
            try
            {
                await SeedRawAsync(setup, 1, "tenant-A", "original-A");
                await SeedRawAsync(setup, 2, "tenant-B", "original-B");

                using var ctxA = TenantContext(cn, prov, "tenant-A");

                // Read through tenant-A context, update, save.
                var rows = ctxA.Query<LivTenRow>().ToList();
                Assert.Single(rows);
                rows[0].Payload = "updated-A";
                await ctxA.SaveChangesAsync();

                // Verify: tenant-A row updated, tenant-B row unchanged.
                Assert.Equal("updated-A",  await ReadPayloadRawAsync(setup, 1));
                Assert.Equal("original-B", await ReadPayloadRawAsync(setup, 2));
            }
            finally { await TeardownAsync(setup, kind); }
        }
    }

    // ── 3: DELETE removes only the current tenant's rows ─────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Delete_RemovesOnlyCurrentTenantRows(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live {kind} not configured")) return;
        var (cn, prov) = live!.Value;
        await using (cn)
        using (var setup = new DbContext(cn, prov))
        {
            await SetupAsync(setup, kind);
            try
            {
                await SeedRawAsync(setup, 1, "tenant-A", "will-delete");
                await SeedRawAsync(setup, 2, "tenant-B", "must-survive");

                using var ctxA = TenantContext(cn, prov, "tenant-A");

                // Delete through tenant-A context.
                var rows = ctxA.Query<LivTenRow>().ToList();
                Assert.Single(rows);
                ctxA.Remove(rows[0]);
                await ctxA.SaveChangesAsync();

                // Verify: tenant-A row gone, tenant-B row still present.
                Assert.Equal(1L, await CountRawAsync(setup));
                Assert.Equal("must-survive", await ReadPayloadRawAsync(setup, 2));
            }
            finally { await TeardownAsync(setup, kind); }
        }
    }

    // ── 4: Adversarial tenant IDs are parameterized, not concatenated ─────────

    [Theory]
    [InlineData(ProviderKind.SqlServer, "' OR '1'='1")]
    [InlineData(ProviderKind.SqlServer, "tenant'; DROP TABLE LivTenRow; --")]
    [InlineData(ProviderKind.Postgres,  "' OR '1'='1")]
    [InlineData(ProviderKind.Postgres,  "tenant'; DROP TABLE LivTenRow; --")]
    [InlineData(ProviderKind.MySql,     "' OR '1'='1")]
    [InlineData(ProviderKind.MySql,     "tenant'; DROP TABLE LivTenRow; --")]
    [InlineData(ProviderKind.Sqlite,    "' OR '1'='1")]
    [InlineData(ProviderKind.Sqlite,    "tenant'; DROP TABLE LivTenRow; --")]
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
    public async Task AdversarialTenantId_IsParameterized_NoDataLeakage(ProviderKind kind, string adversarialId)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live {kind} not configured")) return;
        var (cn, prov) = live!.Value;
        await using (cn)
        using (var setup = new DbContext(cn, prov))
        {
            await SetupAsync(setup, kind);
            try
            {
                // Seed a row with a legitimate tenant.
                await SeedRawAsync(setup, 1, "safe-tenant", "should-not-leak");

                // Query using the injection-style string as the tenant ID.
                // If the tenant ID is parameterized, the WHERE clause becomes:
                //   TenantId = @p0  (with @p0 = adversarialId)
                // No rows have that literal string as TenantId, so 0 rows are returned.
                // If the ID were concatenated into SQL, '1'='1' style injections would return all rows.
                using var ctx = TenantContext(cn, prov, adversarialId);
                var rows = ctx.Query<LivTenRow>().ToList();

                Assert.Empty(rows);

                // The safe-tenant row must still exist (DDL injection failed too).
                Assert.Equal(1L, await CountRawAsync(setup));
            }
            finally { await TeardownAsync(setup, kind); }
        }
    }

    // ── 5: SaveChanges stamps the correct TenantId on new rows ───────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Insert_ViaSaveChanges_StampsCorrectTenantId(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live {kind} not configured")) return;
        var (cn, prov) = live!.Value;
        await using (cn)
        using (var setup = new DbContext(cn, prov))
        {
            await SetupAsync(setup, kind);
            try
            {
                using var ctx = TenantContext(cn, prov, "stamp-tenant");
                ctx.Add(new LivTenRow { Id = 1, TenantId = "stamp-tenant", Payload = "my-data" });
                await ctx.SaveChangesAsync();

                // Read back via raw SQL to verify the stored TenantId.
                await using var cmd = cn.CreateCommand();
                cmd.CommandText = $"SELECT {ctx.Provider.Escape("TenantId")} FROM {ctx.Provider.Escape(Table)} WHERE {ctx.Provider.Escape("Id")} = 1";
                var stored = (string)(await cmd.ExecuteScalarAsync())!;
                Assert.Equal("stamp-tenant", stored);
            }
            finally { await TeardownAsync(setup, kind); }
        }
    }

    // ── 6: Norm.CompileQuery respects the current tenant context ─────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task CompiledQuery_RespectsCurrentTenantContext(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live {kind} not configured")) return;
        var (cn, prov) = live!.Value;
        await using (cn)
        using (var setup = new DbContext(cn, prov))
        {
            await SetupAsync(setup, kind);
            try
            {
                // Seed rows for two tenants.
                await SeedRawAsync(setup, 1, "cq-alpha", "alpha-payload");
                await SeedRawAsync(setup, 2, "cq-beta",  "beta-payload");
                await SeedRawAsync(setup, 3, "cq-alpha", "alpha-payload-2");

                var compiled = Norm.CompileQuery((DbContext c, string prefix) =>
                    c.Query<LivTenRow>().Where(r => r.Payload.StartsWith(prefix)));

                // Execute in alpha context — must return only alpha's rows.
                using var ctxAlpha = TenantContext(cn, prov, "cq-alpha");
                var alphaRows = await compiled(ctxAlpha, "alpha");
                Assert.Equal(2, alphaRows.Count);
                Assert.All(alphaRows, r => Assert.Equal("cq-alpha", r.TenantId));

                // Execute in beta context — must return only beta's row.
                using var ctxBeta = TenantContext(cn, prov, "cq-beta");
                var betaRows = await compiled(ctxBeta, "beta");
                Assert.Single(betaRows);
                Assert.Equal("cq-beta", betaRows[0].TenantId);
            }
            finally { await TeardownAsync(setup, kind); }
        }
    }

    // -- 7: Generated ExecuteUpdate/ExecuteDelete keep tenant predicates under live providers --

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ExecuteUpdate_KnownCrossTenantId_AffectsZeroRows(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live {kind} not configured")) return;
        var (cn, prov) = live!.Value;
        await using (cn)
        using (var setup = new DbContext(cn, prov))
        {
            await SetupAsync(setup, kind);
            try
            {
                await SeedRawAsync(setup, 1, "tenant-A", "safe-A");
                await SeedRawAsync(setup, 2, "tenant-B", "must-not-change");

                using var ctxA = TenantContext(cn, prov, "tenant-A");
                var affected = await ctxA.Query<LivTenRow>()
                    .Where(r => r.Id == 2)
                    .ExecuteUpdateAsync(setters => setters.SetProperty(r => r.Payload, "blocked"));

                Assert.Equal(0, affected);
                Assert.Equal("must-not-change", await ReadPayloadRawAsync(setup, 2));
            }
            finally { await TeardownAsync(setup, kind); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ExecuteDelete_KnownCrossTenantId_AffectsZeroRows(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live {kind} not configured")) return;
        var (cn, prov) = live!.Value;
        await using (cn)
        using (var setup = new DbContext(cn, prov))
        {
            await SetupAsync(setup, kind);
            try
            {
                await SeedRawAsync(setup, 1, "tenant-A", "safe-A");
                await SeedRawAsync(setup, 2, "tenant-B", "must-survive");

                using var ctxA = TenantContext(cn, prov, "tenant-A");
                var affected = await ctxA.Query<LivTenRow>()
                    .Where(r => r.Id == 2)
                    .ExecuteDeleteAsync();

                Assert.Equal(0, affected);
                Assert.Equal(2L, await CountRawAsync(setup));
                Assert.Equal("must-survive", await ReadPayloadRawAsync(setup, 2));
            }
            finally { await TeardownAsync(setup, kind); }
        }
    }
}
