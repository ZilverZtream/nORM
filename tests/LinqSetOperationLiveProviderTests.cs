using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider parity for SQL set operations: Intersect, Except, and Concat
/// (UNION ALL). Union is covered by LiveProviderPostTakeSkipParityTests; this
/// class closes the remaining three operators per Blocker 17.
///
/// Schema: SopRow(Id INT PK, Name TEXT, Cat VARCHAR, Val INT).
/// Test data (all 4 providers):
///   (1,'apple','a',10), (2,'banana','a',35), (3,'cherry','b',15), (4,'date','b',30)
///
/// Left side  = Cat == 'a' → {apple/10, banana/35}
/// Right side = Val &lt; 25  → {apple/10, cherry/15}
///   Intersect: {apple/10}
///   Except:    {banana/35}
///   Concat:    {apple/10, banana/35, apple/10, cherry/15} (4 rows, keeps duplicates)
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LinqSetOperationLiveProviderTests
{
    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Intersect_returns_rows_present_in_both_sides_on_live_provider(ProviderKind kind)
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
                var rows = (await ctx.Query<SopRow>()
                    .Where(r => r.Cat == "a")
                    .Select(r => new { r.Name, r.Val })
                    .Intersect(ctx.Query<SopRow>()
                        .Where(r => r.Val < 25)
                        .Select(r => new { r.Name, r.Val }))
                    .ToListAsync())
                    .OrderBy(r => r.Name).ToArray();

                Assert.Single(rows);
                Assert.Equal("apple", rows[0].Name);
                Assert.Equal(10, rows[0].Val);
            }
            finally { await Teardown(ctx); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Except_returns_rows_in_left_but_not_right_on_live_provider(ProviderKind kind)
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
                var rows = (await ctx.Query<SopRow>()
                    .Where(r => r.Cat == "a")
                    .Select(r => new { r.Name, r.Val })
                    .Except(ctx.Query<SopRow>()
                        .Where(r => r.Val < 25)
                        .Select(r => new { r.Name, r.Val }))
                    .ToListAsync())
                    .ToArray();

                Assert.Single(rows);
                Assert.Equal("banana", rows[0].Name);
                Assert.Equal(35, rows[0].Val);
            }
            finally { await Teardown(ctx); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Concat_preserves_duplicates_unlike_union_on_live_provider(ProviderKind kind)
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
                // Left: apple/10, banana/35 (Cat=='a')
                // Right: apple/10, cherry/15 (Val<25)
                // Concat: 4 rows including apple/10 twice.
                var rows = (await ctx.Query<SopRow>()
                    .Where(r => r.Cat == "a")
                    .Select(r => new { r.Name, r.Val })
                    .Concat(ctx.Query<SopRow>()
                        .Where(r => r.Val < 25)
                        .Select(r => new { r.Name, r.Val }))
                    .ToListAsync())
                    .OrderBy(r => r.Name).ThenBy(r => r.Val).ToArray();

                Assert.Equal(4, rows.Length);
                Assert.Equal(2, rows.Count(r => r.Name == "apple")); // duplicate preserved
                Assert.Equal(1, rows.Count(r => r.Name == "banana"));
                Assert.Equal(1, rows.Count(r => r.Name == "cherry"));
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
            ProviderKind.SqlServer => "CREATE TABLE LiveSopRow (Id INT PRIMARY KEY, Name NVARCHAR(50) NOT NULL, Cat NVARCHAR(10) NOT NULL, Val INT NOT NULL);",
            ProviderKind.Postgres  => "CREATE TABLE \"LiveSopRow\" (\"Id\" INT PRIMARY KEY, \"Name\" VARCHAR(50) NOT NULL, \"Cat\" VARCHAR(10) NOT NULL, \"Val\" INT NOT NULL);",
            ProviderKind.MySql     => "CREATE TABLE LiveSopRow (Id INT PRIMARY KEY, Name VARCHAR(50) NOT NULL, Cat VARCHAR(10) NOT NULL, Val INT NOT NULL);",
            ProviderKind.Sqlite    => "CREATE TABLE LiveSopRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Cat TEXT NOT NULL, Val INTEGER NOT NULL);",
            _ => throw new ArgumentOutOfRangeException()
        };
        await c.ExecuteNonQueryAsync();
        await using var c2 = ctx.Connection.CreateCommand();
        c2.CommandText = kind == ProviderKind.Postgres
            ? "INSERT INTO \"LiveSopRow\" VALUES (1,'apple','a',10),(2,'banana','a',35),(3,'cherry','b',15),(4,'date','b',30);"
            : "INSERT INTO LiveSopRow VALUES (1,'apple','a',10),(2,'banana','a',35),(3,'cherry','b',15),(4,'date','b',30);";
        await c2.ExecuteNonQueryAsync();
    }

    private static async Task Teardown(DbContext ctx)
    {
        await using var c = ctx.Connection.CreateCommand();
        c.CommandText = "DROP TABLE IF EXISTS LiveSopRow;";
        try { await c.ExecuteNonQueryAsync(); } catch { }
        await using var c2 = ctx.Connection.CreateCommand();
        c2.CommandText = "DROP TABLE IF EXISTS \"LiveSopRow\";";
        try { await c2.ExecuteNonQueryAsync(); } catch { }
    }

    [Table("LiveSopRow")]
    public sealed class SopRow
    {
        [Key] public int    Id   { get; set; }
        public string       Name { get; set; } = "";
        public string       Cat  { get; set; } = "";
        public int          Val  { get; set; }
    }
}
