using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider proof for nullable aggregate cardinality semantics.
/// SQL aggregate null behavior differs from LINQ for Sum, so this locks the
/// public matrix row against all four providers.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderNullableAggregateParityTests
{
    private const string Table = "NullableAggregateParityRow";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Nullable_aggregates_match_linq_cardinality_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await SetupAsync(ctx, kind);
            try
            {
                var mixed = ctx.Query<NullableAggregateParityRow>().Where(r => r.GroupId == 1);
                Assert.Equal(12, await mixed.SumAsync(r => r.NullableInt));
                Assert.Equal(5, await mixed.MinAsync(r => r.NullableInt));
                Assert.Equal(7, await mixed.MaxAsync(r => r.NullableInt));
                Assert.Equal(2.0, await mixed.AverageAsync(r => r.NullableDouble));

                var allNull = ctx.Query<NullableAggregateParityRow>().Where(r => r.GroupId == 2);
                Assert.Equal(0, await allNull.SumAsync(r => r.NullableInt));
                Assert.Null(await allNull.MinAsync(r => r.NullableInt));
                Assert.Null(await allNull.MaxAsync(r => r.NullableDouble));
                Assert.Null(await allNull.AverageAsync(r => r.NullableDouble));

                var empty = ctx.Query<NullableAggregateParityRow>().Where(r => r.GroupId == 99);
                Assert.Equal(0, await empty.SumAsync(r => r.NullableInt));
                Assert.Null(await empty.MinAsync(r => r.NullableInt));
                Assert.Null(await empty.AverageAsync(r => r.NullableDouble));
            }
            finally
            {
                await TeardownAsync(ctx, kind);
            }
        }
    }

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        var table = ctx.Provider.Escape(Table);
        var id = ctx.Provider.Escape(nameof(NullableAggregateParityRow.Id));
        var groupId = ctx.Provider.Escape(nameof(NullableAggregateParityRow.GroupId));
        var nullableInt = ctx.Provider.Escape(nameof(NullableAggregateParityRow.NullableInt));
        var nullableDouble = ctx.Provider.Escape(nameof(NullableAggregateParityRow.NullableDouble));
        var drop = kind == ProviderKind.SqlServer
            ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {table};"
            : $"DROP TABLE IF EXISTS {table};";
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        var doubleType = kind switch
        {
            ProviderKind.SqlServer => "FLOAT",
            ProviderKind.Postgres => "DOUBLE PRECISION",
            ProviderKind.MySql => "DOUBLE",
            _ => "REAL"
        };

        await ExecuteAsync(ctx, drop);
        await ExecuteAsync(ctx,
            $"CREATE TABLE {table} ({id} {intType} PRIMARY KEY, {groupId} {intType} NOT NULL, {nullableInt} {intType} NULL, {nullableDouble} {doubleType} NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{groupId},{nullableInt},{nullableDouble}) VALUES " +
            "(1,1,5,1.5),(2,1,NULL,NULL),(3,1,7,2.5),(4,2,NULL,NULL),(5,2,NULL,NULL)");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try
        {
            var table = ctx.Provider.Escape(Table);
            var drop = kind == ProviderKind.SqlServer
                ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {table}"
                : $"DROP TABLE IF EXISTS {table}";
            await ExecuteAsync(ctx, drop);
        }
        catch
        {
            // best-effort cleanup only
        }
    }

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    [Table(Table)]
    private sealed class NullableAggregateParityRow
    {
        [Key] public int Id { get; set; }
        public int GroupId { get; set; }
        public int? NullableInt { get; set; }
        public double? NullableDouble { get; set; }
    }
}
