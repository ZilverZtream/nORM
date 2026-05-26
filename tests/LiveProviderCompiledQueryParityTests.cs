using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Blocker 19 — proves that <see cref="Norm.CompileQuery"/> produces the same
/// results as normal queries on every live provider. The compiled path uses a
/// separate translation, cache, parameter-binding, and materialization path;
/// this matrix guards against path divergence on real servers.
///
/// Shapes tested:
///   1. Basic filter parameter — same result as ctx.Query + Where.
///   2. Repeated invocation with different parameters — correct rebinding.
///   3. OrderBy + Take — correct pagination matches normal path.
///   4. Empty result set — compiled path returns empty list, not throws.
///   5. Where on string column — string binding correct on every provider.
///
/// Schema: CqpRow(Id INT PK, Name TEXT, Val INT, Cat VARCHAR)
///   (1,'alpha','a',10), (2,'beta','a',20), (3,'gamma','b',30), (4,'delta','b',40)
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderCompiledQueryParityTests
{
    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Compiled_query_with_int_parameter_matches_normal_query_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured (set NORM_TEST_*)")) return;
        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await Setup(ctx, kind);
            try
            {
                var compiled = Norm.CompileQuery((DbContext c, int minVal) =>
                    c.Query<CqpRow>().Where(r => r.Val >= minVal).OrderBy(r => r.Id));

                var compiledRows = await compiled(ctx, 25);
                var normalRows   = await ctx.Query<CqpRow>().Where(r => r.Val >= 25).OrderBy(r => r.Id).ToListAsync();

                Assert.Equal(normalRows.Count, compiledRows.Count);
                for (int i = 0; i < normalRows.Count; i++)
                {
                    Assert.Equal(normalRows[i].Id,  compiledRows[i].Id);
                    Assert.Equal(normalRows[i].Val, compiledRows[i].Val);
                }
            }
            finally { await Teardown(ctx); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Compiled_query_rebinds_parameter_correctly_on_repeated_calls_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured (set NORM_TEST_*)")) return;
        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await Setup(ctx, kind);
            try
            {
                var compiled = Norm.CompileQuery((DbContext c, int minVal) =>
                    c.Query<CqpRow>().Where(r => r.Val >= minVal).OrderBy(r => r.Id));

                var callA = await compiled(ctx, 35); // Val >= 35 → delta/40
                var callB = await compiled(ctx, 15); // Val >= 15 → beta/20, gamma/30, delta/40
                var callC = await compiled(ctx, 50); // Val >= 50 → empty

                Assert.Single(callA);
                Assert.Equal(40, callA[0].Val);
                Assert.Equal(3, callB.Count);
                Assert.Empty(callC);
            }
            finally { await Teardown(ctx); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Compiled_query_with_OrderBy_Take_matches_normal_path_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured (set NORM_TEST_*)")) return;
        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await Setup(ctx, kind);
            try
            {
                var compiled = Norm.CompileQuery((DbContext c, int n) =>
                    c.Query<CqpRow>().OrderBy(r => r.Val).Take(n));

                var rows = await compiled(ctx, 2);

                Assert.Equal(2, rows.Count);
                Assert.Equal(10, rows[0].Val); // alpha
                Assert.Equal(20, rows[1].Val); // beta
            }
            finally { await Teardown(ctx); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Compiled_query_with_string_parameter_filters_correctly_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured (set NORM_TEST_*)")) return;
        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await Setup(ctx, kind);
            try
            {
                var compiled = Norm.CompileQuery((DbContext c, string cat) =>
                    c.Query<CqpRow>().Where(r => r.Cat == cat).OrderBy(r => r.Id));

                var catA = await compiled(ctx, "a"); // alpha, beta
                var catB = await compiled(ctx, "b"); // gamma, delta

                Assert.Equal(2, catA.Count);
                Assert.Equal(new[] { "alpha", "beta" }, catA.Select(r => r.Name).ToArray());
                Assert.Equal(2, catB.Count);
                Assert.Equal(new[] { "gamma", "delta" }, catB.Select(r => r.Name).ToArray());
            }
            finally { await Teardown(ctx); }
        }
    }

    private static async Task Setup(DbContext ctx, ProviderKind kind)
    {
        await Teardown(ctx);
        await using var c = ctx.Connection.CreateCommand();
        c.CommandText = kind switch
        {
            ProviderKind.SqlServer => "CREATE TABLE LiveCqpRow (Id INT PRIMARY KEY, Name NVARCHAR(50) NOT NULL, Cat NVARCHAR(10) NOT NULL, Val INT NOT NULL);",
            ProviderKind.Postgres  => "CREATE TABLE \"LiveCqpRow\" (\"Id\" INT PRIMARY KEY, \"Name\" VARCHAR(50) NOT NULL, \"Cat\" VARCHAR(10) NOT NULL, \"Val\" INT NOT NULL);",
            ProviderKind.MySql     => "CREATE TABLE LiveCqpRow (Id INT PRIMARY KEY, Name VARCHAR(50) NOT NULL, Cat VARCHAR(10) NOT NULL, Val INT NOT NULL);",
            ProviderKind.Sqlite    => "CREATE TABLE LiveCqpRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Cat TEXT NOT NULL, Val INTEGER NOT NULL);",
            _ => throw new ArgumentOutOfRangeException()
        };
        await c.ExecuteNonQueryAsync();
        await using var c2 = ctx.Connection.CreateCommand();
        c2.CommandText = kind == ProviderKind.Postgres
            ? "INSERT INTO \"LiveCqpRow\" VALUES (1,'alpha','a',10),(2,'beta','a',20),(3,'gamma','b',30),(4,'delta','b',40);"
            : "INSERT INTO LiveCqpRow VALUES (1,'alpha','a',10),(2,'beta','a',20),(3,'gamma','b',30),(4,'delta','b',40);";
        await c2.ExecuteNonQueryAsync();
    }

    private static async Task Teardown(DbContext ctx)
    {
        await using var c = ctx.Connection.CreateCommand();
        c.CommandText = "DROP TABLE IF EXISTS LiveCqpRow;";
        try { await c.ExecuteNonQueryAsync(); } catch { }
        await using var c2 = ctx.Connection.CreateCommand();
        c2.CommandText = "DROP TABLE IF EXISTS \"LiveCqpRow\";";
        try { await c2.ExecuteNonQueryAsync(); } catch { }
    }

    [Table("LiveCqpRow")]
    public sealed class CqpRow
    {
        [Key] public int    Id   { get; set; }
        public string       Name { get; set; } = "";
        public string       Cat  { get; set; } = "";
        public int          Val  { get; set; }
    }
}
